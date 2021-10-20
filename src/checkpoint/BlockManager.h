#pragma once

#include <climits>
#include <list>
#include <memory>
#include <vector>

#include <tbb/cache_aligned_allocator.h>

#include "filesystem/File.h"
#include "filesystem/FileSystemDisk.h"

class Query;
class QueryBuffer;

/*
 * \brief The BlockManager is responsible for manages the persistent data
 * of a query by returning valid file pointers for persistence operations and
 * tracking files for GC.
 *
 * */

class BlockManager {
 private:
  const int m_listSize = 10;
  int m_numberOfQueries;
  std::vector<std::shared_ptr<Query>> m_queries;

  struct FileHelper;
  struct CircularFileList;
  typedef QueueIoHandler adapter_t;
  typedef FileSystemDisk<adapter_t> disk_t;
  typedef typename FileSystemDisk<adapter_t>::file_t file_t;

  std::vector<std::vector<std::shared_ptr<disk_t>, tbb::cache_aligned_allocator<std::shared_ptr<disk_t>>>> m_filesystems;
  std::vector<std::mutex, tbb::cache_aligned_allocator<std::mutex>> m_locks;
  std::vector<CircularFileList, tbb::cache_aligned_allocator<CircularFileList>> m_lFiles;
  std::vector<CircularFileList, tbb::cache_aligned_allocator<CircularFileList>> m_rFiles;

  const bool m_debug = false;

 public:
  BlockManager(std::vector<std::shared_ptr<Query>> &queries, bool clearFiles = true);

  file_t *getFilePtr(int query, int bufferId, long index);

  file_t *getUnsafeFilePtr(int query, int bufferId, long index, int fileId);

  void freePersistent(int query, int bufferId, long index);

  void freeSlot(int query, int bufferId, long index);

 private:
  void loadFiles(int query, int bufferId, std::vector<std::string> &files);

  struct alignas(64) FileHelper  {
    std::shared_ptr<std::string> m_fileName;
    file_t *m_filePtr;
    long m_start, m_end;
    FileHelper(std::shared_ptr<std::string> fileName = nullptr, file_t *filePtr = nullptr, long start = INT_MIN, long end = INT_MIN)
        : m_fileName(fileName), m_filePtr(filePtr), m_start(start), m_end(end) {}
    ~FileHelper() {
      m_fileName.reset();
    }
    void reset() {
      m_fileName.reset();
      m_filePtr = nullptr;
      m_start = INT_MIN;
      m_end = INT_MIN;
    }
  };

  struct CircularFileList {
    std::vector<FileHelper, tbb::cache_aligned_allocator<FileHelper>> m_buffer;
    int m_size;
    int m_readIdx;
    int m_writeIdx;
    int m_elements = 0;
    int m_counter = 0;
    CircularFileList(int size = 0) : m_buffer(size, FileHelper()), m_size(size) {
      m_readIdx = 0;
      m_writeIdx = size - 1;
    }
    void set_capacity(int size) {
      m_buffer.resize(size, FileHelper());
      m_size = size;
      m_readIdx = 0;
      m_writeIdx = size - 1;
    }
    void push_back(std::shared_ptr<std::string> &fileName, file_t *filePtr, long start, long end) {
      if (m_elements == m_size) {
        //m_buffer.resize(m_size * 2, FileHelper());
        //m_size = 2 * m_size;
        throw std::runtime_error("error: increase the size of the list holding the files");
      }

      m_counter++;
      m_writeIdx++;
      if (m_writeIdx == (int) m_buffer.size())
        m_writeIdx = 0;

      m_buffer[m_writeIdx].m_fileName = fileName;
      m_buffer[m_writeIdx].m_filePtr = filePtr;
      m_buffer[m_writeIdx].m_start = start;
      m_buffer[m_writeIdx].m_end = end;

      m_elements++;
    }
    file_t *push_back_dummy(std::shared_ptr<std::string> &name) {
      if (m_elements == m_size) {
        throw std::runtime_error("error: increase the size of the list holding the files");
      }

      auto filePtr = m_buffer[m_writeIdx].m_filePtr;
      name = m_buffer[m_writeIdx].m_fileName;
      m_writeIdx++;
      if (m_writeIdx == (int) m_buffer.size())
        m_writeIdx = 0;

      m_elements++;
      return filePtr;
    }
    FileHelper *front() {
      if (m_elements > 0)
        return &m_buffer[m_readIdx];
      else
        return nullptr;
      //throw std::runtime_error("error: empty CircularList");
    }
    FileHelper *secondFront() {
      if (m_elements > 1)
        return &m_buffer[(m_readIdx+1)%m_buffer.size()];
      else
        throw std::runtime_error("error: empty CircularList in BlockManager");
    }
    void pop_front() {
      m_elements--;
      //m_buffer[m_readIdx].reset();
      m_readIdx++;
      if (m_readIdx == (int) m_buffer.size())
        m_readIdx = 0;
    }
    int size() { return m_elements; }
    int capacity() { return m_size; }
    std::vector<FileHelper, tbb::cache_aligned_allocator<FileHelper>> &getUnsafeFiles() {
      return m_buffer;
    }
    bool hasWrapped () {
      if (m_counter >= m_size) {
        return true;
      }
      return false;
    }
  };
};