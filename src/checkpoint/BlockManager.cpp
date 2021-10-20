#include "checkpoint/BlockManager.h"

#include <list>
#include <memory>
#include <string>
#include <vector>

#include "buffers/QueryBuffer.h"
#include "filesystem/File.h"
#include "filesystem/FileSystemDisk.h"
#include "utils/Query.h"

BlockManager::BlockManager(std::vector<std::shared_ptr<Query>> &queries, bool clearFiles)
    : m_numberOfQueries(queries.size()),
      m_queries(queries),
      m_filesystems(m_numberOfQueries, std::vector<std::shared_ptr<disk_t>, tbb::cache_aligned_allocator<std::shared_ptr<disk_t>>>(2)),
      m_locks(m_numberOfQueries),
      m_lFiles(m_numberOfQueries, CircularFileList(m_listSize)),
      m_rFiles(m_numberOfQueries, CircularFileList(m_listSize)) {

  std::vector<std::string> files;
  auto path = SystemConf::FILE_ROOT_PATH + "/scabbard";
  Utils::tryCreateDirectory(path);
  Utils::readDirectory(path, files);
  std::sort(files.begin(), files.end());
  if (clearFiles) {
    for (auto &f : files) {
      if (f.find("fs_queue_data_") != std::string::npos) {
        auto res = std::remove((path+"/"+f).c_str());
        if (res != 0)
          std::cout << "Failed to remove file " << (path+"/"+f) << std::endl;
      }
    }
  }

  for (auto &q : m_queries) {
    auto qid = q->getId();
    q->getBuffer()->setFileStore(this);
    if (q->getBuffer()->getFilesystem()) {
      m_filesystems[qid][0] = q->getBuffer()->getFilesystem();
      if (!clearFiles) {
        loadFiles(qid, 0, files);
      }
    } else {
      //throw std::runtime_error("error: set the filesystem!");
    }

    q->getSecondBuffer()->setFileStore(this);
    if (q->getSecondBuffer()->getFilesystem()) {
      m_filesystems[qid][1] = q->getBuffer()->getFilesystem();
      if (!clearFiles) {
        loadFiles(qid, 1, files);
      }
    } else {
      // throw std::runtime_error("error: set the filesystem!");
    }


  }
}

BlockManager::file_t *BlockManager::getFilePtr(int query, int bufferId, long index) {
  if (query >= m_numberOfQueries)
    throw std::runtime_error("error: invalid query id");
  if (bufferId >= 2) throw std::runtime_error("error: invalid bufferId id");

  bool found = false;
  file_t *filePtr = nullptr;
  {
    //std::lock_guard<std::mutex> l(m_locks[query]);
    auto &files = (bufferId == 0) ? m_lFiles[query] : m_rFiles[query];

    auto f = files.front();
    if (f) {
      if (index < f->m_end) {
        filePtr = f->m_filePtr;
        found = true;
      } else {
        if (files.m_elements > 1) {
          f = files.secondFront();
          filePtr = f->m_filePtr;
          found = true;
        }
      }
    }

    if (!found) {
      auto buffer = (bufferId == 0) ? m_queries[query]->getBuffer()
                          : m_queries[query]->getSecondBuffer();
      auto bufferSize = buffer->getCapacity();
      auto filesystem =
          (bufferId == 0) ? m_filesystems[query][0] : m_filesystems[query][1];
      auto id = index / bufferSize;

      if (!files.hasWrapped()) {
        auto newFileName = std::make_shared<std::string>(
            "scabbard/fs_queue_data_" + std::to_string(bufferId) +"_" + std::to_string(id));

        if (m_debug) {
          std::cout << "[FS] allocating file " << newFileName << std::endl;
        }

        if (!filesystem)
          throw std::runtime_error("error: filesystem is not initialized");
        auto newFilePtr = filesystem->newFile(*newFileName);
        if (!newFilePtr)
          throw std::runtime_error("error: filesystem failed to initialize the filePtr");

        files.push_back(newFileName, newFilePtr, (long)id * bufferSize,
                        (id + 1) * bufferSize);
        filePtr = newFilePtr;
      } else {
        std::shared_ptr<std::string> newFileName;
        filePtr = files.push_back_dummy(newFileName);
        if (m_debug) {
          std::cout << "[FS] allocating file " << newFileName << std::endl;
        }
      }

      buffer->updateFileEndPtr(id);
    }
  }

  return filePtr;
}

BlockManager::file_t *BlockManager::getUnsafeFilePtr(int query, int bufferId, long index, int fileId) {
  if (query >= m_numberOfQueries)
    throw std::runtime_error("error: invalid query id");
  if (bufferId >= 2) throw std::runtime_error("error: invalid bufferId id");

  bool found = false;
  file_t *filePtr = nullptr;
  {
    // std::lock_guard<std::mutex> l(m_locks[query]);
    auto &files = (bufferId == 0) ? m_lFiles[query] : m_rFiles[query];
    auto &fileVector = files.getUnsafeFiles();
    auto numOfFiles = fileVector.size();
    auto id = fileId;
    int counter = 0;
    while (true) {
      // todo: fix this
      if (index < fileVector[id].m_end || true) {
        filePtr = fileVector[id].m_filePtr;
        break;
      }
      id++;
      counter++;
      if (id == numOfFiles) {
        id = 0;
      }
      if (counter == numOfFiles) {
        throw std::runtime_error("error: file not found!");
      }
    }
  }
  return filePtr;
}

void BlockManager::freePersistent(int query, int bufferId, long index) {
  if (query >= m_numberOfQueries)
    throw std::runtime_error("error: invalid query id");
  if (bufferId >= 2) throw std::runtime_error("error: invalid bufferId id");

  auto &files = (bufferId == 0) ? m_lFiles[query] : m_rFiles[query];
  auto filesystem =
      (bufferId == 0) ? m_filesystems[query][0] : m_filesystems[query][1];
  auto buffer = (bufferId == 0) ? m_queries[query]->getBuffer()
                                : m_queries[query]->getSecondBuffer();
  auto capacity = buffer->getCapacity();
  auto id = index / capacity;
  {
    std::lock_guard<std::mutex> l(m_locks[query]);
    while (files.m_elements > 0) {
      auto f = files.front();
      if (index < f->m_end) {
        break;
      } else {
        if (!filesystem)
          throw std::runtime_error("error: filesystem is not initialized");
        if(m_debug) {
          std::cout << "[FS] freeing file " << f->m_fileName << std::endl;
        }
        //filesystem->eraseFile(f->m_filePtr);
        // reset the slot;
        f->m_end = f->m_end + capacity;
        files.pop_front();
      }
    }
    buffer->updateFileStartPtr(id, index);
  }
}

void BlockManager::freeSlot(int query, int bufferId, long index) {
  if (query >= m_numberOfQueries)
    throw std::runtime_error("error: invalid query id");
  if (bufferId >= 2) throw std::runtime_error("error: invalid bufferId id");

  throw std::runtime_error("error: this operation is not supported yet");
}

void BlockManager::loadFiles(int query, int bufferId, std::vector<std::string> &fileNames) {
  auto path = SystemConf::FILE_ROOT_PATH + "/scabbard";
  Utils::tryCreateDirectory(path);
  auto fileSuffix = "fs_queue_data_" + std::to_string(bufferId);
  auto &files = (bufferId == 0) ? m_lFiles[query] : m_rFiles[query];
  auto filesystem =
      (bufferId == 0) ? m_filesystems[query][0] : m_filesystems[query][1];
  if (!filesystem)
    throw std::runtime_error("error: filesystem is not initialized");
  auto buffer = (bufferId == 0) ? m_queries[query]->getBuffer()
                                : m_queries[query]->getSecondBuffer();
  auto bufferSize = buffer->getCapacity();
  int id = 0;
  for (auto &f : fileNames) {
    if (f.find(fileSuffix) != std::string::npos) {
      if (m_debug) {
        std::cout << "[FS] loading file " << f << std::endl;
      }
      std::string name;
      name.append("scabbard").append("/").append(f);
      std::shared_ptr<std::string>  newFileName(new std::string(name));
      auto newFilePtr = filesystem->newFile(*newFileName);
      files.push_back(newFileName, newFilePtr, (long)id * bufferSize,
                      (id + 1) * bufferSize);
      id++;
    }
  }
}
