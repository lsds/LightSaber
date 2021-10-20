#pragma once

#include <list>
#include <memory>
#include <vector>
#include <climits>

class Query;

/*
 * \brief The lineage tracks the data dependencies between input-output tuples
 * for all the operators in a pipeline. It can be serialized to a vector clock.
 *
 * */

struct LineageGraph {
  struct LineageNode;
  bool m_isValid = false;
  size_t m_vertices;
  std::shared_ptr<LineageNode> m_root;
  std::vector<std::shared_ptr<LineageNode>> m_graph;
  std::vector<long> m_clockVector;

  explicit LineageGraph(std::vector<std::shared_ptr<Query>> &queries);

  LineageGraph(LineageGraph &lineageGraph);

  void mergeGraphs(std::shared_ptr<LineageGraph> &lineageGraph);

  void advanceOffsets(std::shared_ptr<LineageGraph> &lineageGraph);

  void freePersistentState(int qId);

  void setOutputPtr (int qId, long outputPtr);

  void clear();

  void bfs(std::vector<std::shared_ptr<Query>> &queries);

  void serialize();

  struct LineageNode {
    std::vector<std::shared_ptr<LineageNode>> m_children;
    std::shared_ptr<Query> m_query;
    long m_freePtr1, m_freePtr2, m_freeOffset1, m_freeOffset2, m_outputPtr;
    explicit LineageNode(std::shared_ptr<Query> query = nullptr, long freePtr1 = INT_MIN,
                long freePtr2 = INT_MIN, long m_outputPtr = INT_MIN);
    void set (std::shared_ptr<Query> query, long freePtr1, long freePtr2, long outputPtr);
    void addChild (std::shared_ptr<LineageNode> &node);
    void tryToFree();
  };
};