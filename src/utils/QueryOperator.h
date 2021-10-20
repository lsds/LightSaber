#pragma once

#include <fcntl.h>
#include <libpmemobj.h>
#include <libpmemobj/pool_base.h>
#include <sys/mman.h>

#include <libpmemobj++/container/vector.hpp>
#include <libpmemobj++/detail/common.hpp>
#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/make_persistent_atomic.hpp>
#include <libpmemobj++/p.hpp>
#include <libpmemobj++/persistent_ptr_base.hpp>
#include <libpmemobj++/pool.hpp>
#include <libpmemobj++/transaction.hpp>
#include <string>

#include "cql/operators/OperatorCode.h"

class Query;

/*
 * \brief This class is the building block for creating an execution
 * graph.
 *
 * The @OperatorCode variable is an instance of the OperatorKernel for now.
 *
 * */

class QueryOperator {
 private:
  Query *m_parent;
  QueryOperator *m_downstream;
  QueryOperator *m_upstream;
  OperatorCode &m_code;
  bool m_isFT;

  struct PMem;
  typedef QueueIoHandler adapter_t;
  typedef FileSystemDisk<adapter_t> disk_t;
  typedef typename FileSystemDisk<adapter_t>::file_t file_t;
  std::shared_ptr<disk_t> m_filesystem = nullptr;
  // Variables for persisting the progress vector
  const size_t m_poolSize;
  pmem::obj::pool<PMem> m_pop;
  pmem::obj::persistent_ptr<PMem> m_root;
  std::string m_pmFileName;

  // used for logging non-deterministic operations
  bool m_useLog = false;
  std::string m_logFileName;
  size_t m_logSize;
  size_t m_logIdx = 0;
  int m_logFD;
  char *m_logMF;

  void setUpstream(QueryOperator *qOperator) {
    m_upstream = qOperator;
  }

 public:
  QueryOperator(OperatorCode &code, bool isFT = false) :
      m_downstream(nullptr), m_upstream(nullptr), m_code(code), m_isFT(isFT),
      m_poolSize(PMEMOBJ_MIN_POOL),
      m_pmFileName("scabbard/operator_pm_"),
      m_logFileName("scabbard/operator_log_pm_") {}

  OperatorCode &getCode() {
    return m_code;
  }

  void setCode(OperatorCode &code) {
    m_code = code;
  }

  void setParent(Query *parent, int id = 0, long logSize = 0) {
    m_parent = parent;
    if (m_isFT && m_parent) {
      try {
        m_pmFileName += std::to_string(id);
        if (!m_filesystem) {
          m_filesystem = std::make_shared<disk_t>(SystemConf::FILE_ROOT_PATH, SystemConf::getInstance().WORKER_THREADS);
        }

        auto pmPath = m_filesystem->getRootPath() + m_pmFileName;
        if (Utils::fileExists(pmPath.c_str()) != 0) {
          m_pop = pmem::obj::pool<PMem>::create(pmPath.c_str(),
                                                "", m_poolSize, CREATE_MODE_RW);
          m_root = m_pop.root();
          pmem::obj::make_persistent_atomic<PMem>(m_pop, m_root->next);
          pmem::obj::transaction::run(m_pop, [&] { m_root = m_root->next; });
        } else {
          m_pop = pmem::obj::pool<PMem>::open(pmPath, "");
          m_root = m_pop.root();
          m_root = m_root->next;
          if (!SystemConf::getInstance().RECOVER) {
            m_root->m_leftP.get_rw().store(0);
            m_root->m_rightP.get_rw().store(0);
            m_root->m_outputP.get_rw().store(0);
          }
        }

        if (m_code.getSecondInputCols() != nullptr) {
          if (m_useLog) {
            m_logFileName += std::to_string(id);
            m_logSize = 2 * logSize * 4 * sizeof(long);
            if ((m_logFD =
                     open((m_filesystem->getRootPath() + m_logFileName).c_str(),
                          O_RDWR | O_CREAT | O_TRUNC, (mode_t)0600)) < 0) {
              throw std::runtime_error("error: failed to open fd");
            }
            ftruncate(m_logFD, m_logSize);
            fsync(m_logFD);
            if ((m_logMF =
                     (char *)mmap64(nullptr, m_logSize, PROT_READ | PROT_WRITE,
                                    MAP_SHARED, m_logFD, 0)) == MAP_FAILED) {
              throw std::runtime_error("error: failed to mmap");
            }
          } else {
            if (!SystemConf::getInstance().RECOVER) {
              pmem::obj::transaction::run(m_pop, [&] {
                m_root->m_offsets = pmem::obj::make_persistent<vector_type>(2 * logSize);
              });
              m_root->m_writePos.get_rw() = 0;
            }
          }
        }
      } catch (const pmem::pool_error &e) {
        std::cerr << "Exception: " << e.what() << std::endl;
        return;
      } catch (const pmem::transaction_error &e) {
        std::cerr << "Exception: " << e.what() << std::endl;
        return;
      }
    }
  }

  Query *getParent() {
    return m_parent;
  }

  void connectTo(QueryOperator *qOperator) {
    m_downstream = qOperator;
    qOperator->setUpstream(this);
  }

  bool isMostUpstream() {
    return (m_upstream == nullptr);
  }

  bool isMostDownstream() {
    return (m_downstream == nullptr);
  }

  QueryOperator *getDownstream() {
    return m_downstream;
  }

  std::string toString() {
    return "";
  }

  void updateInputPtr(long inputPtr, bool isLeft) {
    if (!m_root) {
      throw std::runtime_error("error: pmem is not initialized");
    }
    if (isLeft) {
      auto prevOffset = m_root->m_leftP.get_ro().load();
      /*if (inputPtr < prevOffset)
        throw std::runtime_error("error: trying to set an invalid offset " +
            std::to_string(inputPtr) + " < " +
            std::to_string(prevOffset));*/
      m_root->m_leftP.get_rw().store(inputPtr);
    } else {
      auto prevOffset = m_root->m_rightP.get_ro().load();
      /*if (inputPtr < prevOffset)
        throw std::runtime_error("error: trying to set an invalid offset " +
            std::to_string(inputPtr) + " < " +
            std::to_string(prevOffset));*/
      m_root->m_rightP.get_rw().store(inputPtr);
    }
  }

  long getInputPtr(bool isLeft) {
    if (!m_root) {
      throw std::runtime_error("error: pmem is not initialized");
    }
    auto ptr = (isLeft) ? m_root->m_leftP.get_ro().load() : m_root->m_rightP.get_ro().load();
    return ptr;
  }

  void updateOutputPtr(long outputPtr) {
    if (!m_root) {
      throw std::runtime_error("error: pmem is not initialized");
    }
    auto prevOffset = m_root->m_outputP.get_ro().load();

    if (outputPtr < prevOffset) {
      //throw std::runtime_error("error: trying to free an invalid offset in qoperator " +
      //                         std::to_string(outputPtr) + " < " +
      //                         std::to_string(prevOffset));
      //std::cout << "warning: trying to free an invalid offset in qoperator " +
      //    std::to_string(outputPtr) + " < " + std::to_string(prevOffset) << std::endl;
      return;
    }
    m_root->m_outputP.get_rw().store(outputPtr);
  }

  long getOutputPtr() {
    if (!m_root) {
      throw std::runtime_error("error: pmem is not initialized");
    }
    auto ptr = m_root->m_outputP.get_ro().load();
    return ptr;
  }

  void writeOffsets(long tid, long offset1, long offset2) {
    if (m_useLog) {
      if (m_logMF) {
        auto buf = (offsetTuple *)m_logMF;
        buf[m_logIdx] = {tid, offset1, offset2, 0};
        m_logIdx++;
        if (m_logIdx >= m_logSize / sizeof(offsetTuple)) {
          if (msync((void *)m_logMF, m_logSize, MS_SYNC) < 0) {
            throw std::runtime_error("error: failed to msync");
          }
          m_logIdx = 0;
        }
      } else {
        std::cout << "warning: no file was initialized for logging" << std::endl;
      }
    } else {
      vector_type &pvector = *(m_root->m_offsets);
      pvector[m_root->m_writePos.get_rw()++] = {tid, offset1, offset2, 0};
      if (m_root->m_writePos.get_ro().load() == pvector.size()) {
        m_root->m_writePos.get_rw() = 0;
      }
    }
  }

 private:

  struct offsetTuple {
    long _1;
    long _2;
    long _3;
    long _4;
  };
  using vector_type = pmem::obj::vector<offsetTuple>;
  struct PMem {
    pmem::obj::p<std::atomic<long>> m_leftP{};
    pmem::obj::p<std::atomic<long>> m_rightP{};
    pmem::obj::p<std::atomic<long>> m_outputP{};
    pmem::obj::p<std::atomic<int>> m_leftId{};
    pmem::obj::p<std::atomic<int>> m_rightId{};
    pmem::obj::p<std::atomic<int>> m_outputId{};
    pmem::obj::p<std::atomic<int>> m_writePos{};
    pmem::obj::persistent_ptr<vector_type> m_offsets;
    pmem::obj::persistent_ptr<PMem> next;
    PMem() {
        m_leftP.get_rw() = 0L;
        m_rightP.get_rw() = 0L;
        m_outputP.get_rw() = 0L;
        m_writePos.get_rw() = 0;
    };

    /** Copy constructor is deleted */
    PMem(const PMem &) = delete;
    /** Assignment operator is deleted */
    PMem &operator=(const PMem &) = delete;
  };
};