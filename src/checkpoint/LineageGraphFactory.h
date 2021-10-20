#pragma once

#include <atomic>
#include <memory>

#include "tbb/concurrent_queue.h"

#include "checkpoint/LineageGraph.h"
#include "utils/SystemConf.h"

/*
 * \brief This class creates a single pool of LineageGraphs used to perform
 * dependency tracking.
 *
 * */

class LineageGraphFactory {
 private:
  bool isReady = false;
  std::atomic<long> count;
  std::shared_ptr<LineageGraph> m_graph;
  tbb::concurrent_queue<std::shared_ptr<LineageGraph>> m_pool;
  LineageGraphFactory() {};

 public:
  static LineageGraphFactory &getInstance() {
    static LineageGraphFactory instance;
    return instance;
  }

  void setGraph(std::vector<std::shared_ptr<Query>> &queries) {
    if (!isReady) {
      m_graph = std::make_shared<LineageGraph>(queries);
      isReady = true;
    }
  }

  LineageGraphFactory(LineageGraphFactory const &) = delete;
  void operator=(LineageGraphFactory const &) = delete;

  std::shared_ptr<LineageGraph> newInstance() {
    if (!isReady)
      throw std::runtime_error("error: the lineage graph is not set");

    std::shared_ptr<LineageGraph> graph;
    bool hasRemaining = m_pool.try_pop(graph);
    if (!hasRemaining) {
      count.fetch_add(1);
      graph = std::make_shared<LineageGraph>(*m_graph);
    } else {
      while (graph.use_count() != 1) {
        hasRemaining = m_pool.try_pop(graph);
        if (!hasRemaining) {
          count.fetch_add(1, std::memory_order_seq_cst);
          graph = std::make_shared<LineageGraph>(*m_graph);
        }
      }
    }

    //std::stringstream ss; ss << graph->m_graph[0].get();
    //std::cout << "allocating share_ptr " + ss.str() << std::endl;
    return graph;
  }

  void free(std::shared_ptr<LineageGraph> &graph) {
    //std::stringstream ss; ss << graph->m_graph[0].get();
    //std::cout << "returning share_ptr " + ss.str() << std::endl;
    graph->clear();
    m_pool.push(graph);
  }

  long getCount() {
    return count.load();
  }
};
