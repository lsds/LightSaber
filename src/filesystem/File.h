#pragma once

#include <libaio.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <atomic>
#include <cassert>
#include <cstdint>
#include <ostream>
#include <string>

#include "utils/Async.h"
#include "utils/Status.h"

/*
 * \brief Utilities for File operations.
 *
 * The code is heavily based on https://github.com/microsoft/FishStore/blob/master/src/environment/file_linux.h.
 * */

#define CREATE_MODE_RW (S_IWUSR | S_IRUSR)

constexpr const char* kPathSeparator = "/";

enum class FileCreateDisposition : uint8_t {
  CreateOrTruncate,
  OpenOrCreate,
  OpenExisting
};

inline std::ostream& operator<<(std::ostream& os, FileCreateDisposition val) {
  switch (val) {
    case FileCreateDisposition::CreateOrTruncate:
      os << "CreateOrTruncate";
      break;
    case FileCreateDisposition::OpenOrCreate:
      os << "OpenOrCreate";
      break;
    case FileCreateDisposition::OpenExisting:
      os << "OpenExisting";
      break;
    default:
      os << "UNKNOWN: " << static_cast<uint8_t>(val);
      break;
  }
  return os;
}

enum class FileOperationType : uint8_t { Read, Write };

struct FileOptions {
  bool m_unbuffered;
  bool m_deleteOnClose;

  FileOptions() : m_unbuffered{false}, m_deleteOnClose{false} {}
  FileOptions(bool unbuffered, bool deleteOnClose)
      : m_unbuffered{unbuffered}, m_deleteOnClose{deleteOnClose} {}
};

/*
 * \brief The File class represents the OS file handle
 *
 * */

class File {
 protected:
  int m_fd;

 private:
  size_t m_device_alignment;
  std::string m_filename;
  bool m_owner;

 protected:
  File() : m_fd{-1}, m_device_alignment{0}, m_filename{}, m_owner{false} {}

  File(const std::string& filename)
      : m_fd{-1}, m_device_alignment{0}, m_filename{filename}, m_owner{false} {}

  /// Move constructor.
  File(File&& other)
      : m_fd{other.m_fd},
        m_device_alignment{other.m_device_alignment},
        m_filename{std::move(other.m_filename)},
        m_owner{other.m_owner} {
    other.m_owner = false;
  }

  // Move assignment operator.
  File& operator=(File&& other) {
    m_fd = other.m_fd;
    m_device_alignment = other.m_device_alignment;
    m_filename = std::move(other.m_filename);
    m_owner = other.m_owner;
    other.m_owner = false;
    return *this;
  }

 protected:
  Status open(int flags, FileCreateDisposition createDisposition,
              bool* exists = nullptr);
  Status getDeviceAlignment();
  static int getCreateDisposition(FileCreateDisposition createDisposition);

 public:
  Status close();
  Status erase();

  uint64_t getSize() const {
    struct stat stat_buffer;
    int result = ::fstat(m_fd, &stat_buffer);
    return (result == 0) ? stat_buffer.st_size : 0;
  }

  size_t getDeviceAlignment() const { return m_device_alignment; }

  const std::string& getFilename() const { return m_filename; }

  ~File() {
    if (m_owner) {
      Status s = close();
    }
  }
};

class QueueFile;

/*
 * \brief The QueueIoHandler class encapsulates completions for async file I/O,
 * where the completions are put on the AIO completion queue.
 *
 * */

class QueueIoHandler {
 private:
  // The Linux AIO context used for IO completions.
  io_context_t m_ioObject;

  constexpr static int kMaxEvents = 128;

 public:
  typedef QueueFile async_file_t;

  QueueIoHandler() : m_ioObject{} {}
  QueueIoHandler(size_t maxThreads) : m_ioObject{} {
    int result = ::io_setup(kMaxEvents, &m_ioObject);
    assert(result >= 0);
  }

  // Move constructor
  QueueIoHandler(QueueIoHandler&& other) {
    m_ioObject = other.m_ioObject;
    other.m_ioObject = {};
  }

  // Invoked whenever a Linux AIO completes.
  static void ioCompletionCallback(io_context_t ctx, struct iocb* iocb,
                                   long res, long res2);

  struct IoCallbackContext {
    IoCallbackContext(FileOperationType operation, int fd, size_t offset,
                      uint32_t length, uint8_t* buffer, IAsyncContext* context,
                      AsyncIOCallback callback)
        : m_callerContext{context}, m_callback{callback} {
      if (FileOperationType::Read == operation) {
        ::io_prep_pread(&this->m_parentIocb, fd, buffer, length, offset);
      } else {
        ::io_prep_pwrite(&this->m_parentIocb, fd, buffer, length, offset);
      }
      ::io_set_callback(&this->m_parentIocb, ioCompletionCallback);
    }

    // WARNING: "m_parentIocb" must be the first field in AioCallbackContext.
    // This class is a C-style subclass of "struct iocb".

    // The iocb structure for Linux AIO.
    struct iocb m_parentIocb;

    // Caller callback context.
    IAsyncContext* m_callerContext;

    // The caller's asynchronous callback function
    AsyncIOCallback m_callback;
  };

  inline io_context_t getIoObject() const { return m_ioObject; }

  // Try to execute the next IO completion on the queue, if any.
  bool tryComplete();
  bool tryCompleteMultiple();

  ~QueueIoHandler() {
    if (m_ioObject != nullptr) ::io_destroy(m_ioObject);
  }
};

/*
 * \brief The QueueFile class encapsulates asynchronous reads and writes, using
 * the specified AIO context.
 *
 * */

class QueueFile : public File {
 private:
  io_context_t m_ioObject;

 public:
  QueueFile() : File(), m_ioObject{} {}

  QueueFile(const std::string& filename) : File(filename), m_ioObject{} {}

  // Move constructor
  QueueFile(QueueFile&& other)
      : File(std::move(other)), m_ioObject{other.m_ioObject} {}

  // Move assignment operator.
  QueueFile& operator=(QueueFile&& other) {
    File::operator=(std::move(other));
    m_ioObject = other.m_ioObject;
    return *this;
  }

  Status open(FileCreateDisposition create_disposition,
              const FileOptions& options, QueueIoHandler* handler,
              bool* exists = nullptr, long size = 0);

  Status read(size_t offset, uint32_t length, uint8_t* buffer,
              IAsyncContext& context, AsyncIOCallback callback) const;
  Status readSync(size_t offset, uint32_t length, uint8_t* buffer) const;
  Status write(size_t offset, uint32_t length, const uint8_t* buffer,
               IAsyncContext& context, AsyncIOCallback callback);

 private:
  Status scheduleOperation(FileOperationType operationType, uint8_t* buffer,
                           size_t offset, uint32_t length,
                           IAsyncContext& context, AsyncIOCallback callback);
};

/*
 * \brief Used by the disk devices
 *
 * */
typedef void (*truncate_callback_t)(uint64_t offset);