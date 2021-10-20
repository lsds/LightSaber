#pragma once

#include <cstdint>
#include <experimental/filesystem>
#include <iostream>
#include <mutex>
#include <string>

#include "filesystem/File.h"
#include "utils/Guid.h"

/*
 *
 * \brief A poor-man's file system implementation for handling file operations.
 *
 * The code heavily based on https://github.com/microsoft/FishStore/blob/master/src/device/file_system_disk.h.
 *
 * */

template <class H>
class FileSystemDisk;

template <class H>
class FileSystemFile {
 public:
  typedef H handler_t;
  typedef typename handler_t::async_file_t file_t;

  // Default constructor
  FileSystemFile() : m_file{}, m_fileOptions{} {}

  FileSystemFile(const std::string& filename, const FileOptions& fileOptions)
      : m_file{filename}, m_fileOptions{fileOptions} {}

  // Move constructor.
  FileSystemFile(FileSystemFile&& other)
      : m_file{std::move(other.m_file)}, m_fileOptions{other.m_fileOptions} {}

  /// Move assignment operator.
  FileSystemFile& operator=(FileSystemFile&& other) {
    m_file = std::move(other.file_);
    m_fileOptions = other.m_fileOptions;
    return *this;
  }

  Status open(handler_t* handler, long size = 0) {
    return m_file.open(FileCreateDisposition::OpenOrCreate, m_fileOptions,
                       handler, nullptr, size);
  }

  Status close() { return m_file.close(); }

  Status erase() { return m_file.erase(); }

  void truncate(uint64_t new_begin_offset, truncate_callback_t callback) {
    // Truncation is a no-op.
    if (callback) {
      callback(new_begin_offset);
    }
  }

  Status readAsync(uint64_t source, void* dest, uint32_t length,
                   AsyncIOCallback callback, IAsyncContext& context) const {
    return m_file.read(source, length, reinterpret_cast<uint8_t*>(dest),
                       context, callback);
  }

  Status readSync(uint64_t source, void* dest, uint32_t length) const {
    return m_file.readSync(source, length, reinterpret_cast<uint8_t*>(dest));
  }

  Status writeAsync(const void* source, uint64_t dest, uint32_t length,
                    AsyncIOCallback callback, IAsyncContext& context) {
    return m_file.write(dest, length, reinterpret_cast<const uint8_t*>(source),
                        context, callback);
  }

  size_t getAlignment() const { return m_file.device_alignment(); }

 private:
  file_t m_file;
  FileOptions m_fileOptions;
};

template <class H>
class FileSystemDisk {
 public:
  typedef H handler_t;
  typedef FileSystemFile<handler_t> file_t;
  typedef std::vector<std::unique_ptr<file_t>> log_file_t;

 private:
  std::string m_rootPath;
  handler_t m_handler;
  FileOptions m_defaultFileOptions;
  std::mutex m_mutex;
  // Contains all files.
  log_file_t m_files;

  static std::string normalizePath(std::string rootPath) {
    if (rootPath.empty() || rootPath.back() != kPathSeparator[0]) {
      rootPath += kPathSeparator;
    }
    return rootPath;
  }

 public:
  FileSystemDisk(const std::string& rootPath, size_t maxThreads = 16,
                 bool enablePrivileges = false, bool unbuffered = true,
                 bool delete_on_close = false)
      : m_rootPath{normalizePath(rootPath)},
        m_handler{maxThreads},
        m_defaultFileOptions{unbuffered, delete_on_close} {
    // create file path if it doesn't exist
    std::experimental::filesystem::path path{m_rootPath};
    if (!std::experimental::filesystem::exists(
            std::experimental::filesystem::status(path))) {
      std::experimental::filesystem::create_directories(path);
    }
  }

  // Methods required by the (implicit) disk interface.
  uint32_t getSectorSize() const {
    return 512;  // For now, assume all disks have 512-bytes alignment.
  }

  const log_file_t& getFilesUnsafe() const { return m_files; }
  log_file_t& getFilesUnsafe() { return m_files; }

  std::string getRelativeCheckpointPath(const Guid& token) const {
    std::string retval = "scabbard";
    retval += kPathSeparator;
    retval += token.ToString();
    retval += kPathSeparator;
    return retval;
  }

  std::string getRootPath() const { return m_rootPath; }

  std::string getCheckpointPath(const Guid& token) const {
    return m_rootPath + getRelativeCheckpointPath(token);
  }

  std::string getRelativeNamingCheckpointPath(const Guid& token) const {
    std::string retval = "naming-checkpoint-";
    retval += token.ToString();
    retval += ".txt";
    return retval;
  }

  std::string getNamingCheckpointPath(const Guid& token) const {
    return m_rootPath + getRelativeNamingCheckpointPath(token);
  }

  void createCheckpointDirectory(const Guid& token) {
    std::string indexDir = getCheckpointPath(token);
    std::experimental::filesystem::path path{indexDir};
    try {
      std::experimental::filesystem::remove_all(path);
    } catch (std::experimental::filesystem::filesystem_error&) {
      // Ignore; throws when path doesn't exist yet.
    }
    std::experimental::filesystem::create_directories(path);
  }

  void createOrOpenCheckpointDirectory(const Guid& token) {
    std::string indexDir = getCheckpointPath(token);
    std::experimental::filesystem::path path{indexDir};
    if (!std::experimental::filesystem::exists(
            std::experimental::filesystem::status(path))) {
      std::experimental::filesystem::create_directories(path);
    }
  }

  void tryDeleteCheckpointDirectory(const Guid& token) {
    std::string indexDir = getCheckpointPath(token);
    std::experimental::filesystem::path path{indexDir};
    try {
      std::experimental::filesystem::remove_all(path);
    } catch (std::experimental::filesystem::filesystem_error&) {
      // Ignore; throws when path doesn't exist yet.
    }
  }

  file_t newUnmanagedFile(const std::string& relativePath) {
    return file_t{m_rootPath + relativePath, m_defaultFileOptions};
  }

  file_t* newFile(const std::string& relativePath, long size = 0) {
    // Only one thread can modify the list of files at a given time.
    std::lock_guard<std::mutex> lock{m_mutex};
    m_files.push_back(std::make_unique<file_t>(m_rootPath + relativePath,
                                               m_defaultFileOptions));
    std::cout << "[DBG] Creating file " << m_rootPath << relativePath << std::endl;
    assert(m_files.back()->open(&m_handler, size) == Status::Ok);
    return m_files.back().get();
  }

  // Implementation-specific accessor.
  handler_t& getHandler() { return m_handler; }

  bool tryComplete() { return m_handler.tryComplete(); }

  void eraseFile(file_t *file) {
    auto it = m_files.begin();
    while (it != m_files.end()) {
      if (file == (*it).get()) {
        assert((*it)->erase() == Status::Ok);
        m_files.erase(it);
        return;
      } else {
        ++it;
      }
    }
  }

  void eraseFiles() {
    for (auto& f : m_files) {
      assert(f->erase() == Status::Ok);
    }
    m_files.clear();
  }
};