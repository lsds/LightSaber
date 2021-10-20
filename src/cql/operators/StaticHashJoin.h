#pragma once

#include "cql/predicates/Predicate.h"
#include "cql/expressions/Expression.h"
#include "cql/operators/OperatorCode.h"

/*
 * \brief This base class is used for generating code for static hash joins.
 *
 * The predicate for the hash join is passed as an argument at the moment.
 *
 * */

class StaticHashJoin : public OperatorCode {
  Predicate *m_predicate;
  TupleSchema m_streamInputSchema, m_relationInputSchema;
  TupleSchema m_outputSchema;
  std::vector<char> *m_staticBuffer = nullptr;

  std::string m_staticInitialization;
  std::string m_staticHashTable;
  std::string m_staticComputation;

 public:
  StaticHashJoin(Predicate *predicate,
                 TupleSchema streamInputSchema,
                 TupleSchema relationInputSchema,
                 std::vector<char> *staticBuffer,
                 std::string staticInitialization,
                 std::string staticHashTable,
                 std::string staticComputation) :
      m_predicate(predicate), m_streamInputSchema(streamInputSchema), m_relationInputSchema(relationInputSchema),
      m_outputSchema(ExpressionUtils::mergeTupleSchemas(streamInputSchema, relationInputSchema)),
      m_staticBuffer(staticBuffer), m_staticInitialization(staticInitialization),
      m_staticHashTable(staticHashTable), m_staticComputation(staticComputation) {}
  std::string toSExpr() const override {
    std::string s;
    s.append("HashJoin (");
    s.append(m_predicate->toSExpr());
    s.append(")");
    return s;
  }
  explicit operator std::string() const {
    std::string s;
    s.append("HashJoin (");
    s.append(m_predicate->toSExpr());
    s.append(")");
    return s;
  }
  void processData(const std::shared_ptr<WindowBatch>& batch, Task &task, int pid) override {
    (void) batch;
    (void) task;
    (void) pid;
    throw std::runtime_error("error: this operator cannot be used directly");
  }
  void processData(const std::shared_ptr<WindowBatch>& lBatch, const std::shared_ptr<WindowBatch>& rBatch, Task &task, int pid) override {
    (void) lBatch;
    (void) rBatch;
    (void) task;
    (void) pid;
    throw std::runtime_error("error: this operator cannot be used directly");
  }
  Predicate *getPredicate() { return m_predicate; }
  TupleSchema &getOutputSchema() override {
    return m_outputSchema;
  }
  std::vector<char> *getStaticBuffer() {
    return m_staticBuffer;
  }
  std::string getStaticHashTable() {
    return m_staticHashTable;
  }
  std::string getStaticComputation() {
    return m_staticComputation;
  }
  std::string getStaticInitialization() {
    return m_staticInitialization;
  }
};