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
  void processData(const std::shared_ptr<WindowBatch> &batch, Task &task, int pid) override {

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

    inputBuffer->appendBytesTo(startP, endP, outputBuffer->getBuffer());
    outputBuffer->setPosition(batch->getBatchSize());
    batch->setOutputBuffer(outputBuffer);

    /*auto tupleSize = batch->getSchema()->getTupleSize();
    auto output = (_InputSchema *) batch->getOutputBuffer()->getBuffer().data();
    for (int i = 0; i < batch->getBatchSize()/tupleSize; i++) {
        std::cout << "[DBG] timestamp "+std::to_string(output[i].timestamp)+", attr1 "+std::to_string(output[i].attr_1)+", attr2 "+std::to_string(output[i].attr_2) << std::endl;
    }*/

    task.outputWindowBatchResult(batch);
  }
  TupleSchema &getOutputSchema() override {
    return m_inputSchema;
  }
};