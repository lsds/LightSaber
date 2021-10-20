#include "checkpoint/LineageGraph.h"

#include <list>
#include <memory>
#include <unordered_set>
#include <vector>

#include "buffers/QueryBuffer.h"
#include "utils/Query.h"
#include "utils/QueryOperator.h"

LineageGraph::LineageGraph(std::vector<std::shared_ptr<Query>> &queries)
    : m_vertices(queries.size()), m_graph(m_vertices), m_clockVector(m_vertices * 3) {
  bfs(queries);
}

LineageGraph::LineageGraph(LineageGraph &lineageGraph) {
  m_vertices = lineageGraph.m_vertices;
  //std::unordered_set<LineageNode *> set;
  for (auto &v : lineageGraph.m_graph) {
    //set.insert(v.get());
    m_graph.push_back(std::make_shared<LineageNode>(v->m_query));
    for (auto &c : v->m_children) {
      m_graph.back()->m_children.push_back(m_graph[c->m_query->getId()]);
    }
  }

  // sanity check
  //for (auto &v : m_graph) {
  //  if (set.find(v.get()) != set.end())
  //    throw std::runtime_error("error: the deep copy of the lineage graph failed");
  //}
  m_root = m_graph[m_vertices - 1];
}

void LineageGraph::mergeGraphs(std::shared_ptr<LineageGraph> &lineageGraph) {
  if (lineageGraph) {
    for (size_t v = 0; v < m_vertices; ++v) {
      // todo: is this correct?
      m_graph[v]->m_freePtr1 = (lineageGraph->m_graph[v]->m_freeOffset1 > m_graph[v]->m_freeOffset1) ?
                               lineageGraph->m_graph[v]->m_freePtr1 : m_graph[v]->m_freePtr1;
      m_graph[v]->m_freeOffset1 = std::max(lineageGraph->m_graph[v]->m_freeOffset1,
                                           m_graph[v]->m_freeOffset1);
      m_graph[v]->m_freePtr2 = (lineageGraph->m_graph[v]->m_freeOffset2 > m_graph[v]->m_freeOffset2) ?
                               lineageGraph->m_graph[v]->m_freePtr2 : m_graph[v]->m_freePtr2;
      m_graph[v]->m_freeOffset2 = std::max(lineageGraph->m_graph[v]->m_freeOffset2,
                                        m_graph[v]->m_freeOffset2);
      m_graph[v]->m_outputPtr = std::max(lineageGraph->m_graph[v]->m_outputPtr,
                                        m_graph[v]->m_outputPtr);
    }
    if (!m_isValid && lineageGraph->m_isValid) {
      m_isValid = true;
    }
    if (lineageGraph.use_count() == 1) {
      LineageGraphFactory::getInstance().free(lineageGraph);
    } else {
      lineageGraph.reset();
    }
  }
}

void LineageGraph::advanceOffsets(std::shared_ptr<LineageGraph> &lineageGraph) {
  if (lineageGraph) {
    for (size_t v = 0; v < m_vertices; ++v) {
      m_graph[v]->m_freeOffset1 = std::max(lineageGraph->m_graph[v]->m_freeOffset1,
                                           m_graph[v]->m_freeOffset1);
      m_graph[v]->m_freeOffset2 = std::max(lineageGraph->m_graph[v]->m_freeOffset2,
                                           m_graph[v]->m_freeOffset2);
    }
  }
}

void LineageGraph::freePersistentState(int qId) {
  if (qId >= (int)m_vertices)
    throw std::runtime_error("error: invalid query id");

  if (!m_isValid)
    return;

  std::vector<bool> visited(qId + 1, false);
  std::list<int> queue;
  visited[qId] = true;
  queue.push_back(qId);

  while (!queue.empty()) {
    auto q = queue.front();
    queue.pop_front();
    m_graph[q]->tryToFree();
    for (auto &v : m_graph[q]->m_children) {
      auto id = v->m_query->getId();
      if (id >= (int)m_vertices)
        throw std::runtime_error("error: invalid query id");
      if (!visited[id]) {
        visited[id] = true;
        queue.push_back(id);
      }
      //m_graph[q]->m_children.push_back(m_graph[id]);
    }
    /*for (int i = 0; i < queries[q]->getNumberOfDownstreamQueries(); i++) {
      auto qId = queries[q]->getDownstreamQuery(i)->getId();
      if (qId >= (int) m_vertices)
        throw std::runtime_error("error: invalid query id");
      if (!visited[qId]) {
        visited[qId] = true;
        m_graph[qId] = std::make_shared<LineageNode>(queries[qId]);
        queue.push_back(qId);
      }
    }*/
  }
}

void LineageGraph::setOutputPtr (int qId, long outputPtr) {
  if (outputPtr <= 0)
    return;
  m_graph[qId]->m_outputPtr = outputPtr;
  m_isValid = true;
}

void LineageGraph::clear() {
  for (auto &v : m_graph) {
    v->m_freePtr1 = INT_MIN;
    v->m_freeOffset1 = INT_MIN;
    v->m_freePtr2 = INT_MIN;
    v->m_freeOffset2 = INT_MIN;
    v->m_outputPtr = INT_MIN;
  }
  m_isValid = false;
}

void LineageGraph::bfs(std::vector<std::shared_ptr<Query>> &queries) {
  if (queries.empty()) return;

  std::vector<bool> visited(m_vertices, false);
  std::list<int> queue;
  visited[m_vertices - 1] = true;
  m_graph[m_vertices - 1] =
      std::make_shared<LineageNode>(queries[m_vertices - 1]);
  queue.push_back(m_vertices - 1);

  while (!queue.empty()) {
    auto q = queue.front();
    queue.pop_front();
    for (int i = 0; i < queries[q]->getNumberOfUpstreamQueries(); i++) {
      auto qId = queries[q]->getUpstreamQuery(i)->getId();
      if (qId >= (int)m_vertices)
        throw std::runtime_error("error: invalid query id");
      if (!visited[qId]) {
        visited[qId] = true;
        m_graph[qId] = std::make_shared<LineageNode>(queries[qId]);
        queue.push_back(qId);
      }
      m_graph[q]->m_children.push_back(m_graph[qId]);
    }
    /*for (int i = 0; i < queries[q]->getNumberOfDownstreamQueries(); i++) {
      auto qId = queries[q]->getDownstreamQuery(i)->getId();
      if (qId >= (int) m_vertices)
        throw std::runtime_error("error: invalid query id");
      if (!visited[qId]) {
        visited[qId] = true;
        m_graph[qId] = std::make_shared<LineageNode>(queries[qId]);
        queue.push_back(qId);
      }
    }*/
  }

  // assume that the root is the last query
  m_root = m_graph[m_vertices - 1];
}

void LineageGraph::serialize() {
  size_t idx = 0;
  for (auto &l: m_graph) {
    m_clockVector[idx++] = l->m_freeOffset1;
    m_clockVector[idx++] = l->m_freeOffset2;
    m_clockVector[idx++] = l->m_outputPtr;
  }
}

LineageGraph::LineageNode::LineageNode(std::shared_ptr<Query> query,
                                       long freePtr1, long freePtr2, long outputPtr)
    : m_query(query), m_freePtr1(freePtr1), m_freePtr2(freePtr2), m_freeOffset1(INT_MIN), m_freeOffset2(INT_MIN), m_outputPtr(outputPtr) {}

void LineageGraph::LineageNode::set(std::shared_ptr<Query> query, long freePtr1, long freePtr2, long outputPtr) {
  m_query = query;
  m_freePtr1 = freePtr1;
  m_freePtr2 = freePtr2;
  m_outputPtr = outputPtr;
}
void LineageGraph::LineageNode::addChild(std::shared_ptr<LineageNode> &node) {
  m_children.push_back(node);
}
// todo: fix that this gets called only when results are outputted externally
void LineageGraph::LineageNode::tryToFree() {
  if (m_freePtr1 != INT_MIN) {
    //std::cout << "freeing ptr " << m_freePtr1 << " from shared_ptr " << this << std::endl;
    if (m_freeOffset1 != INT_MIN) {
      //m_query->getOperator()->updateInputPtr(m_freeOffset1, true);
      m_query->getBuffer()->getFileStore()->freePersistent(m_query->getId(), 0, m_freeOffset1);
    }
    m_query->getOperator()->updateInputPtr(m_freePtr1, true);
    m_query->getBuffer()->free(m_freePtr1, true);
  }
  if (m_freePtr2 != INT_MIN) {
    if (m_freeOffset2 != INT_MIN) {
      //m_query->getOperator()->updateInputPtr(m_freeOffset2, false);
      m_query->getBuffer()->getFileStore()->freePersistent(m_query->getId(), 1, m_freeOffset2);
    }
    m_query->getOperator()->updateInputPtr(m_freePtr2, false);
    m_query->getBuffer()->free(m_freePtr2, true);
  }
  if (m_outputPtr != INT_MIN) {
    m_query->getOperator()->updateOutputPtr(m_outputPtr);
  }
}
