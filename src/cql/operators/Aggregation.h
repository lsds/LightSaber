#pragma once

#include <vector>
#include <iostream>

#include "utils/WindowDefinition.h"
#include "cql/expressions/ColumnReference.h"
#include "cql/operators/OperatorCode.h"
#include "cql/operators/AggregationType.h"
#include "cql/operators/AggregateOperatorCode.h"

/*
 * \brief This base class is used for generating code for aggregation.
 *
 * */

class Aggregation : public OperatorCode, public AggregateOperatorCode {
 private:
  WindowDefinition m_windowDefinition;
  std::vector<AggregationType> m_aggregationTypes;
  std::vector<ColumnReference *> m_aggregationAttributes;
  ColumnReference m_timestampReference;
  std::vector<Expression *> m_groupByAttributes;
  int m_keyLength, m_valueLength, m_bucketSize;
  bool m_groupBy;
  bool m_processIncremental;
  bool m_invertible;
  bool m_nonInvertible;
  TupleSchema m_outputSchema;

 public:

  Aggregation(WindowDefinition windowDefinition, std::vector<AggregationType> aggregationTypes,
              std::vector<ColumnReference *> aggregationAttributes, std::vector<Expression *> groupByAttributes) :
      m_windowDefinition(windowDefinition),
      m_aggregationTypes(aggregationTypes),
      m_aggregationAttributes(aggregationAttributes),
      m_timestampReference(0),
      m_groupByAttributes(groupByAttributes),
      m_invertible(false),
      m_nonInvertible(false),
      m_outputSchema(2 + (int) groupByAttributes.size() + (int) aggregationAttributes.size(), "outputStream") {

    int numberOfKeyAttributes;
    if (!groupByAttributes.empty()) {
      m_groupBy = true;
      numberOfKeyAttributes = (int) groupByAttributes.size();
    } else {
      m_groupBy = false;
      numberOfKeyAttributes = 0;
    }

    int numberOfOutputAttributes = 1 + 1 + numberOfKeyAttributes + (int) aggregationAttributes.size();
    /* The first attribute is the timestamp */
    auto _timestamp = AttributeType(BasicType::Long);
    m_outputSchema.setAttributeType(0, _timestamp);
    m_keyLength = 0;
    if (numberOfKeyAttributes > 0) {
      for (int idx = 1; idx <= numberOfKeyAttributes; ++idx) {
        auto e = groupByAttributes[idx - 1];
        if (e->getBasicType() == BasicType::Integer) {
          auto attr = AttributeType(BasicType::Integer);
          m_outputSchema.setAttributeType(idx, attr);
          m_keyLength += 4;
        } else if (e->getBasicType() == BasicType::Float) {
          auto attr = AttributeType(BasicType::Float);
          m_outputSchema.setAttributeType(idx, attr);
          m_keyLength += 4;
        } else if (e->getBasicType() == BasicType::Long) {
          auto attr = AttributeType(BasicType::Long);
          m_outputSchema.setAttributeType(idx, attr);
          m_keyLength += 8;
        } else if (e->getBasicType() == BasicType::LongLong) {
          auto attr = AttributeType(BasicType::LongLong);
          m_outputSchema.setAttributeType(idx, attr);
          m_keyLength += 16;
        } else
          throw std::invalid_argument("error: invalid group-by attribute");
      }
    }
    for (int idx = numberOfKeyAttributes + 1; idx < numberOfOutputAttributes - 1; ++idx) {
      auto attr = AttributeType(BasicType::Float);
      m_outputSchema.setAttributeType(idx, attr);
    }
    /* The last attribute is a counter */
    auto count = AttributeType(BasicType::Integer);
    m_outputSchema.setAttributeType(numberOfOutputAttributes - 1, count);

    for (auto type : aggregationTypes) {
      if (type == CNT || type == SUM || type == AVG)
        m_invertible = true;
      if (type == MIN || type == MAX)
        m_nonInvertible = true;
    }
    m_valueLength = 4 * (int) aggregationTypes.size();

    if (windowDefinition.getSlide() > windowDefinition.getSize() / 2
        && windowDefinition.getSlide() != windowDefinition.getSize())
      throw std::runtime_error("error: fix the computation when the slide is greater than half the window size");

    m_processIncremental = (windowDefinition.getSlide() < windowDefinition.getSize() / 2);
    if (m_processIncremental) {
      std::cout << "[DBG] operator contains incremental aggregation type" << std::endl;
    }

    m_bucketSize = (m_groupBy) ? (8 + 8 + m_keyLength + m_valueLength + 4) :
                   (8 + m_valueLength + 4);
    m_bucketSize = (m_bucketSize % 16 != 0) ? (((int)m_bucketSize/16) + 1) * 16 : m_bucketSize;
  }
  bool hasIncremental() override { return m_processIncremental; }
  bool hasInvertible() override { return m_invertible; }
  bool hasNonInvertible() override { return m_nonInvertible; }
  bool hasBoth() override { return hasInvertible() && hasNonInvertible(); }
  bool hasGroupBy() override { return m_groupBy; }
  TupleSchema &getOutputSchema() override { return m_outputSchema; }
  AggregationType &getAggregationType(int idx) override {
    if (idx < 0 || idx > (int) m_aggregationTypes.size() - 1)
      throw std::out_of_range("error: invalid aggregation type index");
    return m_aggregationTypes[idx];
  }
  std::string toSExpr() const override {
    std::string s;
    s.append("[Partial window u-aggregation] ");
    for (int i = 0; i < (int) m_aggregationTypes.size(); ++i)
      s.append(AggregationTypes::toString(m_aggregationTypes[i])).append("(").append(m_aggregationAttributes[i]->toSExpr()).append(
          ")").append(" ");
    s.append("(group-by ?").append(" ").append(std::to_string(m_groupBy)).append(") ");
    s.append("(incremental ?").append(" ").append(std::to_string(m_processIncremental)).append(")");
    return s;
  }
  explicit operator std::string() const {
    std::string s;
    s.append("[Partial window u-aggregation] ");
    for (int i = 0; i < (int) m_aggregationTypes.size(); ++i)
      s.append(AggregationTypes::toString(m_aggregationTypes[i])).append("(").append(m_aggregationAttributes[i]->toSExpr()).append(
          ")").append(" ");
    s.append("(group-by ?").append(" ").append(std::to_string(m_groupBy)).append(") ");
    s.append("(incremental ?").append(" ").append(std::to_string(m_processIncremental)).append(")");
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
  void aggregatePartials(std::shared_ptr<PartialWindowResults> openingWindows,
                         std::shared_ptr<PartialWindowResults> closingOrPendingWindows,
                         std::shared_ptr<PartialWindowResults> completeWindows,
                         int numOfWindows,
                         long &windowsPos,
                         int &tupleSize,
                         bool pack) override {
    (void) openingWindows;
    (void) closingOrPendingWindows;
    (void) completeWindows;
    (void) numOfWindows;
    (void) windowsPos;
    (void) tupleSize;
    (void) pack;
    throw std::runtime_error("error: this operator cannot be used directly");
  }
  void aggregateSinglePartial(std::shared_ptr<PartialWindowResults> completeWindows,
                              int completeWindow,
                              int completeWindowsStartPos,
                              std::shared_ptr<PartialWindowResults> partialWindows,
                              int windowPos,
                              int &startPos,
                              int &endPos,
                              int &tupleSize,
                              bool pack) override {
    (void) completeWindows;
    (void) completeWindow;
    (void) completeWindowsStartPos;
    (void) partialWindows;
    (void) windowPos;
    (void) startPos;
    (void) endPos;
    (void) tupleSize;
    (void) pack;
    throw std::runtime_error("error: this operator cannot be used directly");
  }
  WindowDefinition &getWindowDefinition() override { return m_windowDefinition; }
  AggregationType &getAggregationType() override { return getAggregationType(0); }
  std::vector<AggregationType> &getAggregationTypes() override { return m_aggregationTypes; }
  std::vector<ColumnReference *> &getAggregationAttributes() override { return m_aggregationAttributes; }
  std::vector<Expression *> &getGroupByAttributes() override { return m_groupByAttributes; }
  int getKeyLength() override { return m_keyLength; }
  int getValueLength() override { return m_valueLength; }
  int numberOfValues() override { return (int) m_aggregationAttributes.size(); }
  int getBucketSize() override { return m_bucketSize; }
};