#pragma once

#include "utils/TupleSchema.h"
#include "utils/WindowDefinition.h"
#include "cql/operators/AggregationType.h"

class Expression;
class ColumnReference;
class PartialWindowResults;
class OperatorKernel;

/*
 * \brief This base class is used for implementing aggregation operators.
 *
 * */

class AggregateOperatorCode {
 protected:
  long m_hashTableSize = 0;

 public:
  virtual void aggregatePartials(std::shared_ptr<PartialWindowResults> openingWindows,
                                 std::shared_ptr<PartialWindowResults> closingOrPendingWindows,
                                 std::shared_ptr<PartialWindowResults> completeWindows,
                                 int numOfWindows,
                                 long &windowsPos,
                                 int &tupleSize,
                                 bool pack) = 0;
  virtual void aggregateSinglePartial(std::shared_ptr<PartialWindowResults> completeWindows,
                                      int completeWindow,
                                      int completeWindowsStartPos,
                                      std::shared_ptr<PartialWindowResults> partialWindows,
                                      int windowPos,
                                      int &startPos,
                                      int &endPos,
                                      int &tupleSize,
                                      bool pack) = 0;
  virtual bool hasIncremental() = 0;
  virtual bool hasInvertible() = 0;
  virtual bool hasNonInvertible() = 0;
  virtual bool hasBoth() = 0;
  virtual bool hasGroupBy() = 0;
  virtual AggregationType &getAggregationType() = 0;
  virtual AggregationType &getAggregationType(int idx) = 0;
  virtual WindowDefinition &getWindowDefinition() = 0;
  virtual std::vector<AggregationType> &getAggregationTypes() = 0;
  virtual std::vector<ColumnReference *> &getAggregationAttributes() = 0;
  virtual std::vector<Expression *> &getGroupByAttributes() = 0;
  virtual int getKeyLength() = 0;
  virtual int getValueLength() = 0;
  virtual int numberOfValues() = 0;
  virtual int getBucketSize() = 0;
  long getHashTableSizeAfterCodeGeneration() { return m_hashTableSize; }
};