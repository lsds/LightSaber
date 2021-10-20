#pragma once

#include <stdexcept>

#include "cql/expressions/Expression.h"
#include "cql/operators/OperatorCode.h"
#include "buffers/PartialWindowResultsFactory.h"

/*
 * \brief This class is used for testing data movement in the system.
 *
 * */

class NoOp : public OperatorCode {
 private:
  TupleSchema m_inputSchema;
  bool debugFlag = false;
 public:
  NoOp (TupleSchema inputSchema) : m_inputSchema(inputSchema) {}
  std::string toSExpr() const override {
    std::string s;
    s.append("NoOp (").append(")");
    return s;
  }
  void processData(const std::shared_ptr<WindowBatch>& batch, Task &task, int pid) override {

    //batch->initPartialCountBasedWindowPointers();

    /* Copy input to output */
    auto inputBuffer = batch->getInputQueryBuffer();
    int startP = batch->getBufferStartPointer();
    int endP = batch->getBufferEndPointer();

    if (debugFlag) {
      auto mark = batch->getLatencyMark();
      if (mark >= 0)
        std::cout
            << "[DBG] task " + std::to_string(batch->getTaskId()) + " mark " + std::to_string(batch->getLatencyMark())
            << std::endl;
    }

    auto outputBuffer = PartialWindowResultsFactory::getInstance().newInstance(pid);

    inputBuffer->appendBytesTo(startP, endP, outputBuffer->getBufferRaw());
    outputBuffer->setPosition(batch->getBatchSize());
    batch->setOutputBuffer(outputBuffer);

    /*auto tupleSize = batch->getSchema()->getTupleSize();
    auto output = (_InputSchema *) batch->getOutputBuffer()->getBufferRaw();
    for (int i = 0; i < batch->getBatchSize()/tupleSize; i++) {
        std::cout << "[DBG] timestamp "+std::to_string(output[i].timestamp)+", attr1 "+std::to_string(output[i].attr_1)+", attr2 "+std::to_string(output[i].attr_2) << std::endl;
    }*/

    task.outputWindowBatchResult(batch);
  }
  void processData(const std::shared_ptr<WindowBatch>& lBatch, const std::shared_ptr<WindowBatch>& rBatch, Task &task, int pid) override {
    (void) lBatch;
    (void) rBatch;
    (void) task;
    (void) pid;
    throw std::runtime_error("error: this operator cannot be used directly");
  }
  TupleSchema &getOutputSchema() override {
    return m_inputSchema;
  }
};