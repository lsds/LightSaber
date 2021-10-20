#pragma once

#include "tasks/Task.h"
#include "tasks/WindowBatch.h"

class ColumnReference;

/*
 * \brief This base class is used for implementing operators.
 *
 * */

class OperatorCode {
 public:
  virtual std::string toSExpr() const = 0;
  virtual void processData(const std::shared_ptr<WindowBatch>& batch, Task &api, int pid) = 0;
  virtual void processData(const std::shared_ptr<WindowBatch>& lBatch, const std::shared_ptr<WindowBatch>& rBatch, Task &api, int pid) = 0;
  virtual TupleSchema &getOutputSchema() = 0;
  virtual std::vector<ColumnReference*> *getInputCols() { return nullptr; }
  virtual std::vector<ColumnReference*> *getSecondInputCols() { return nullptr; }
  [[nodiscard]] virtual bool hasSelection() const { return false; }
  virtual std::string getInputSchemaString() { return ""; }
  virtual std::string getSelectionExpr() { return ""; }
  virtual std::string getHashTableExpr() { return ""; }
  [[nodiscard]] virtual bool hasStaticHashJoin() const { return false; }
  virtual ~OperatorCode() = default;
};