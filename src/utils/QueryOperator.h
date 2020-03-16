#pragma once

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

  void setUpstream(QueryOperator *qOperator) {
    m_upstream = qOperator;
  }

 public:
  QueryOperator(OperatorCode &code) :
      m_downstream(nullptr), m_upstream(nullptr), m_code(code) {}

  OperatorCode &getCode() {
    return m_code;
  }

  void setCode(OperatorCode &code) {
    m_code = code;
  }

  void setParent(Query *parent) {
    m_parent = parent;
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
};