#include "filesystem/File.h"

#include <fcntl.h>

#include <cerrno>
#include <cstdio>
#include <cstring>
#include <memory>

#ifdef _DEBUG
#define DCHECK_ALIGNMENT(o, l, b)                                     \
  do {                                                                \
    assert(reinterpret_cast<uintptr_t>(b) % device_alignment() == 0); \
    assert((o) % device_alignment() == 0);                            \
    assert((l) % device_alignment() == 0);                            \
  } while (0)
#else
#define DCHECK_ALIGNMENT(o, l, b) \
  do {                            \
  } while (0)
#endif

Status File::open(int flags, FileCreateDisposition create_disposition,
                  bool* exists) {
  if (exists) {
    *exists = false;
  }

  int create_flags = getCreateDisposition(create_disposition);

  // Always unbuffered (O_DIRECT).
  m_fd = ::open(m_filename.c_str(), flags | O_RDWR | O_DIRECT | create_flags,
                S_IRUSR | S_IWUSR);

  if (exists) {
    // Let the caller know whether the file we tried to open or create (already)
    // exists.
    if (create_disposition == FileCreateDisposition::CreateOrTruncate ||
        create_disposition == FileCreateDisposition::OpenOrCreate) {
      *exists = (errno == EEXIST);
    } else if (create_disposition == FileCreateDisposition::OpenExisting) {
      *exists = (errno != ENOENT);
      if (!*exists) {
        // The file doesn't exist. Don't return an error, since the caller is
        // expecting this case.
        return Status::Ok;
      }
    }
  }
  if (m_fd == -1) {
    int error = errno;
    return Status::IOError;
  }

  Status result = getDeviceAlignment();
  if (result != Status::Ok) {
    close();
  }
  m_owner = true;
  return result;
}

Status File::close() {
  if (m_fd != -1) {
    int result = ::close(m_fd);
    m_fd = -1;
    if (result == -1) {
      int error = errno;
      return Status::IOError;
    }
  }
  m_owner = false;
  return Status::Ok;
}

Status File::erase() {
  int result = ::remove(m_filename.c_str());
  if (result == -1) {
    int error = errno;
    return Status::IOError;
  }
  return Status::Ok;
}

Status File::getDeviceAlignment() {
  // For now, just hardcode 512-byte alignment.
  m_device_alignment = 512;
  return Status::Ok;
}

int File::getCreateDisposition(FileCreateDisposition create_disposition) {
  switch (create_disposition) {
    case FileCreateDisposition::CreateOrTruncate:
      return O_CREAT | O_TRUNC;
    case FileCreateDisposition::OpenOrCreate:
      return O_CREAT;
    case FileCreateDisposition::OpenExisting:
      return 0;
    default:
      assert(false);
  }
}

void QueueIoHandler::ioCompletionCallback(io_context_t ctx, struct iocb* iocb,
                                          long res, long res2) {
  auto callback_context = std::unique_ptr<IoCallbackContext>(
      reinterpret_cast<IoCallbackContext*>(iocb));
  size_t bytes_transferred;
  Status return_status;
  if (res < 0) {
    return_status = Status::IOError;
    bytes_transferred = 0;
  } else {
    return_status = Status::Ok;
    bytes_transferred = res;
  }
  callback_context->m_callback(callback_context->m_callerContext, return_status,
                               bytes_transferred);
}

bool QueueIoHandler::tryComplete() {
  struct timespec timeout;
  std::memset(&timeout, 0, sizeof(timeout));
  struct io_event events[1];
  int result = ::io_getevents(m_ioObject, 1, 1, events, &timeout);
  if (result == 1) {
    io_callback_t callback = reinterpret_cast<io_callback_t>(events[0].data);
    callback(m_ioObject, events[0].obj, events[0].res, events[0].res2);
    return true;
  } else {
    return false;
  }
}

bool QueueIoHandler::tryCompleteMultiple() {
  const int numOfReq = 10;
  struct timespec timeout;
  std::memset(&timeout, 0, sizeof(timeout));
  struct io_event events[numOfReq];
  int result = ::io_getevents(m_ioObject, 1, numOfReq, events, &timeout);
  if (result >= 1) {
    for (int i = 0; i < result; ++i) {
      io_callback_t callback = reinterpret_cast<io_callback_t>(events[i].data);
      callback(m_ioObject, events[i].obj, events[i].res, events[i].res2);
    }
    return true;
  } else {
    return false;
  }
}

Status QueueFile::open(FileCreateDisposition create_disposition,
                       const FileOptions& options, QueueIoHandler* handler,
                       bool* exists, long size) {
  int flags = 0;
  if (options.m_unbuffered) {
    flags |= O_DIRECT;
  }
  RETURN_NOT_OK(File::open(flags, create_disposition, exists));

  if (size > 0 && ftruncate(m_fd, size) != 0) {
    throw std::runtime_error("error: problem in setting the size of the file");
  }

  if (exists && !*exists) {
    return Status::Ok;
  }

  m_ioObject = handler->getIoObject();
  return Status::Ok;
}

Status QueueFile::read(size_t offset, uint32_t length, uint8_t* buffer,
                       IAsyncContext& context, AsyncIOCallback callback) const {
  DCHECK_ALIGNMENT(offset, length, buffer);
  return const_cast<QueueFile*>(this)->scheduleOperation(
      FileOperationType::Read, buffer, offset, length, context, callback);
}

Status QueueFile::readSync(size_t offset, uint32_t length, uint8_t* buffer) const {
  DCHECK_ALIGNMENT(offset, length, buffer);
  auto res = pread(const_cast<QueueFile*>(this)->m_fd, buffer, length, offset);
  return (res != length) ? Status::IOError : Status::Ok;
}

Status QueueFile::write(size_t offset, uint32_t length, const uint8_t* buffer,
                        IAsyncContext& context, AsyncIOCallback callback) {
  DCHECK_ALIGNMENT(offset, length, buffer);
  return scheduleOperation(FileOperationType::Write,
                           const_cast<uint8_t*>(buffer), offset, length,
                           context, callback);
}

Status QueueFile::scheduleOperation(FileOperationType operationType,
                                    uint8_t* buffer, size_t offset,
                                    uint32_t length, IAsyncContext& context,
                                    AsyncIOCallback callback) {
  IAsyncContext* callerContextCopy;
  RETURN_NOT_OK(context.deepCopy(callerContextCopy));

  // TODO: check if this scales with multiple threads
  auto ioContext = std::make_unique<QueueIoHandler::IoCallbackContext>(
      operationType, m_fd, offset, length, buffer, callerContextCopy, callback);
  if (!ioContext.get()) return Status::OutOfMemory;

  struct iocb* iocbs[1];
  iocbs[0] = reinterpret_cast<struct iocb*>(ioContext.get());

  int result = ::io_submit(m_ioObject, 1, iocbs);
  if (result != 1) {
    return Status::IOError;
  }

  ioContext.release();
  return Status::Ok;
}