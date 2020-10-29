#pragma once

#include "tasks/Task.h"
#include "tasks/WindowBatch.h"

/*
 * \brief This base class is used for implementing operators.
 *
 * */

class OperatorCode {
 public:
  virtual std::string toSExpr() const = 0;
  virtual void processData(const std::shared_ptr<WindowBatch>& batch, Task &api, int pid) = 0;
  virtual TupleSchema &getOutputSchema() = 0;
  virtual ~OperatorCode() = default;
};