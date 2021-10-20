#pragma once

#include <atomic>
#include <cstdint>
#include <string>

#include "filesystem/File.h"
#include "utils/Guid.h"

/*
 *
 * \brief A null disk, used for in-memory-only execution to test the system's
 * overhead.
 *
 * The code heavily based on https://github.com/microsoft/FishStore/blob/master/src/device/null_disk.h.
 *
 * */

struct NullHandler {
  inline static constexpr bool tryComplete() { return false; }
};

class NullFile {
 public:
  NullFile() {}

  Status open(NullHandler* handler) { return Status::Ok; }
  Status close() { return Status::Ok; }
  Status erase() { return Status::Ok; }
  void truncate(uint64_t new_begin_offset, truncate_callback_t callback) {
    if (callback) {
      callback(new_begin_offset);
    }
  }

  Status readAsync(uint64_t source, void* dest, uint32_t length,
                   AsyncIOCallback callback, IAsyncContext& context) const {
    callback(&context, Status::Ok, length);
    return Status::Ok;
  }

  Status writeAsync(const void* source, uint64_t dest, uint32_t length,
                    AsyncIOCallback callback, IAsyncContext& context) {
    callback(&context, Status::Ok, length);
    return Status::Ok;
  }

  static size_t getAlignment() {
    // Align null device to cache line.
    return 64;
  }

  void setHandler(NullHandler* handler) {}
};

class NullDisk {
 public:
  typedef NullHandler handler_t;
  typedef NullFile file_t;
  typedef std::vector<std::unique_ptr<NullFile>> log_file_t;

 private:
  handler_t m_handler;
  std::mutex m_mutex;
  log_file_t m_files;
  std::string m_rootPath;

  static std::string normalizePath(std::string rootPath) {
    if (rootPath.empty() || rootPath.back() != kPathSeparator[0]) {
      rootPath += kPathSeparator;
    }
    return rootPath;
  }

 public:
  NullDisk(const std::string& rootPath, size_t maxThreads = 16)
      : m_rootPath{normalizePath(rootPath)} {
    (void)maxThreads;
  }

  static uint32_t getSectorSize() { return 64; }

  // Methods required by the (implicit) disk interface.
  const log_file_t& getFiles() const { return m_files; }

  log_file_t& getFiles() { return m_files; }

  std::string getRootPath() const { return m_rootPath; }

  std::string getCheckpointPath(const Guid& token) const {
    assert(false);
    return "";
  }

  void createCheckpointDirectory(const Guid& token) { assert(false); }

  void createOrOpenCheckpointDirectory(const Guid& token) { assert(false); }

  void tryDeleteCheckpointDirectory(const Guid& token) { assert(false); }

  std::string getRelativeCheckpointPath(const Guid& token) const { return ""; }

  file_t* newFile(const std::string& relativePath, long size = 0) {
    std::lock_guard<std::mutex> lock{m_mutex};
    m_files.push_back(std::make_unique<NullFile>());
    assert(m_files.back()->open(&m_handler) == Status::Ok);
    return m_files.back().get();
  }

  handler_t& getHandler() { return m_handler; }

  inline static constexpr bool tryComplete() { return false; }

  void eraseFiles() {
    for (auto& f : m_files) {
      assert(f->erase() == Status::Ok);
    }
    m_files.clear();
  }
};