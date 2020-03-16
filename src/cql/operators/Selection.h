#pragma once

#include "cql/predicates/Predicate.h"
#include "cql/operators/OperatorCode.h"

/*
 * \brief This base class is used for generating code for filtering.
 *
 * */

class Selection : public OperatorCode {
  Predicate *m_predicate;
 public:
  Selection(Predicate *predicate) : m_predicate(predicate) {}
  std::string toSExpr() const override {
    std::string s;
    s.append("Selection (");
    s.append(m_predicate->toSExpr());
    s.append(")");
    return s;
  }
  explicit operator std::string() const {
    std::string s;
    s.append("Selection (");
    s.append(m_predicate->toSExpr());
    s.append(")");
    return s;
  }
  void processData(std::shared_ptr<WindowBatch> batch, Task &task, int pid) override {
    (void) batch;
    (void) task;
    (void) pid;
    throw std::runtime_error("error: this operator cannot be used directly");
  }
  Predicate *getPredicate() { return m_predicate; }
  TupleSchema &getOutputSchema() override {
    throw std::runtime_error("error: cannot take the output schema from selection directly");
  }
};