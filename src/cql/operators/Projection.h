#pragma once

#include "cql/expressions/Expression.h"
#include "cql/operators/OperatorCode.h"

/*
 * \brief This base class is used for generating code for projections.
 *
 * If the @toIntermediate variable is set true, the result is materialized for
 * further computation.
 *
 * */

class Projection : public OperatorCode {
 private:
  std::vector<Expression *> m_expressions;
  TupleSchema m_outputSchema;
  bool m_toIntermediate;
 public:
  Projection(std::vector<Expression *> expressions, bool toIntermediate = false) : m_expressions(expressions),
                                                                                   m_outputSchema(ExpressionUtils::getTupleSchemaFromExpressions(
                                                                                       expressions)),
                                                                                   m_toIntermediate(toIntermediate) {}
  std::string toSExpr() const override {
    std::string s;
    s.append("Projection (");
    int i = 0;
    for (auto e: m_expressions) {
      s.append(e->toSExpr());
      if (i != (int) m_expressions.size() - 1)
        s.append(", ");
      i++;
    }
    s.append(")");
    return s;
  }
  explicit operator std::string() const {
    std::string s;
    s.append("Projection (");
    int i = 0;
    for (auto e: m_expressions) {
      s.append(e->toSExpr());
      if (i != (int) m_expressions.size() - 1)
        s.append(", ");
      i++;
    }
    s.append(")");
    return s;
  }
  void processData(std::shared_ptr<WindowBatch> batch, Task &task, int pid) override {
    (void) batch;
    (void) task;
    (void) pid;
    throw std::runtime_error("error: this operator cannot be used directly");
  }
  std::vector<Expression *> &getExpressions() {
    return m_expressions;
  }
  TupleSchema &getOutputSchema() override {
    return m_outputSchema;
  }
  bool isIntermediate() {
    return m_toIntermediate;
  }
};