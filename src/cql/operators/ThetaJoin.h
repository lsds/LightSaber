#pragma once

#include <unordered_set>

#include "buffers/PartialWindowResultsFactory.h"
#include "cql/expressions/Expression.h"
#include "cql/operators/OperatorCode.h"
#include "cql/operators/codeGeneration/OperatorJit.h"
#include "cql/predicates/Predicate.h"
#include "utils/Utils.h"
#include "utils/WindowDefinition.h"

/*
 * \brief This class is used for generating code for theta join.
 *
 * */

class ThetaJoin : public OperatorCode {
 private:
  bool m_isReady = false;
  bool m_debug = false;
  bool m_genCode = true;
  bool m_monitorSelectivity = false;
  std::vector<PaddedLong> m_invoked;
  std::vector<PaddedLong> m_matched;
  Predicate *m_predicate;
  TupleSchema m_leftInputSchema, m_rightInputSchema;
  TupleSchema m_outputSchema;
  size_t m_circularBufferSize;

  int m_id = -1;
  CodeGenWrapper m_codeGen;
  std::unique_ptr<Utils::DynamicLoader> m_dLoader;
  std::function<void(long, long, long, long, char *, char *, char *, long&, bool, long&, long&)> process;

 public:
  ThetaJoin(TupleSchema lSchema, TupleSchema rSchema, Predicate *predicate)
      : m_invoked(SystemConf::getInstance().WORKER_THREADS),
        m_matched(SystemConf::getInstance().WORKER_THREADS),
        m_predicate(predicate),
        m_leftInputSchema(lSchema),
        m_rightInputSchema(rSchema),
        m_outputSchema(ExpressionUtils::mergeTupleSchemas(lSchema, rSchema)) {}

  void setQueryId(int qId) {
    m_id = qId;
  }

  std::string toSExpr() const override {
    std::string s;
    s.append("ThetaJoin (");
    s.append(m_predicate->toSExpr());
    s.append(")");
    return s;
  }

  explicit operator std::string() const {
    std::string s;
    s.append("ThetaJoin (");
    s.append(m_predicate->toSExpr());
    s.append(")");
    return s;
  }

  void setup(WindowDefinition *winDef1, WindowDefinition *winDef2, size_t circularBufferSize = 0) {
    // create file path if it doesn't exist
    std::experimental::filesystem::path path{
        SystemConf::getInstance().FILE_ROOT_PATH};
    if (!std::experimental::filesystem::exists(
        std::experimental::filesystem::status(path))) {
      std::experimental::filesystem::create_directories(path);
    }
    path = {SystemConf::getInstance().FILE_ROOT_PATH  + "/scabbard"};
    if (!std::experimental::filesystem::exists(
        std::experimental::filesystem::status(path))) {
      std::experimental::filesystem::create_directories(path);
    }

    // setup operator
    m_circularBufferSize = (circularBufferSize != 0) ? circularBufferSize : SystemConf::getInstance().CIRCULAR_BUFFER_SIZE;
    std::string s;

    if (!SystemConf::getInstance().RECOVER) {
      // add definitions
      s.append(getIncludesString());
      s.append(getQueryDefinitionString(winDef1, winDef2));
      // add schemas
      s.append(getInputSchemaString(true));
      s.append(getInputSchemaString(false));
      s.append(getOutputSchemaString());
      // get predicate
      auto predicate1 = getSelectionExpr(true);
      auto predicate2 = getSelectionExpr(false);
      // get code for row/range based
      auto fWin = getFirstWindowExpr(winDef1, winDef2);
      auto sWin = getSecondWindowExpr(winDef1, winDef2);
      // construct code
      s.append(getComputationCode(predicate1, predicate2, fWin, sWin));
      s.append(getC_Definitions());

      //auto path = Utils::getCurrentWorkingDir();
      auto path = SystemConf::getInstance().FILE_ROOT_PATH + "/scabbard";
      std::ofstream out(path + "/GeneratedCode_" + std::to_string(m_id) + ".cpp");
      out << s;
      out.close();
    }
    if (m_genCode) {
      int argc = 2;
      //auto path = Utils::getCurrentWorkingDir();
      auto path = SystemConf::getInstance().FILE_ROOT_PATH + "/scabbard";
      std::string mainPath = path + "/LightSaber";
      std::string generatedPath = path + "/GeneratedCode_" + std::to_string(m_id) + ".cpp";
      std::string libPath = path + "/GeneratedCode_" + std::to_string(m_id) + ".so";
      const char *str0 = mainPath.c_str();
      const char *str1 = generatedPath.c_str();
      const char **argv = (const char **) malloc(2 * sizeof(char *));
      argv[0] = str0;
      argv[1] = str1;
      if (!SystemConf::getInstance().RECOVER) {
        // generate shared library
        std::thread slt([&]{
          std::string command = "clang -shared -fPIC -O3 -march=native -g -o " + libPath + " " + generatedPath;
          system(command.c_str());
        });
        m_codeGen.parseAndCodeGen(argc, argv, SystemConf::getInstance().RECOVER);

        auto processFn = m_codeGen.getFunction<void(long, long, long, long, char *, char *,
                                       char *, long &, bool, long &, long &)>("process");
        if (!processFn) {
          std::cout << "Failed to fetch the pointers." << std::endl;
          exit(1);
        }
        process = *processFn;

        slt.join();
      } else {
        m_dLoader = std::make_unique<Utils::DynamicLoader>(libPath.c_str());
        process = m_dLoader->load<void(long, long, long, long, char *, char *,
                                       char *, long &, bool, long &, long &)>(libPath, "process");
      }
    }

    m_isReady = true;
  }

  void processData(const std::shared_ptr<WindowBatch> &batch, Task &task,
                   int pid) override {
    (void)batch;
    (void)task;
    (void)pid;
    throw std::runtime_error("error: this operator cannot be used directly");
  }

  void processData(const std::shared_ptr<WindowBatch> &lBatch,
                   const std::shared_ptr<WindowBatch> &rBatch, Task &task,
                   int pid) override {
    if (!m_isReady)
      throw std::runtime_error("error: the operator has not been set");

    if (m_debug) {
      processInDebug(lBatch, rBatch, task, pid);
    } else {
      processCodeGen
      //processInDebug
          (lBatch, rBatch, task, pid);
    }
  }

  Predicate *getPredicate() { return m_predicate; }

  TupleSchema &getOutputSchema() override {
    return m_outputSchema;
  }

  std::vector<ColumnReference*> *getInputCols() override {
    auto cols = new std::vector<ColumnReference*>;
    std::unordered_set<int> colNums;
    for (int i = 0; i < m_leftInputSchema.numberOfAttributes(); ++i) {
      auto col = new ColumnReference(i, m_leftInputSchema.getAttributeType(i));
      cols->push_back(col);
      colNums.insert(0);
    }
    return cols;
  }

  std::vector<ColumnReference*> *getSecondInputCols() override {
    auto cols = new std::vector<ColumnReference*>;
    std::unordered_set<int> colNums;
    for (int i = 0; i < m_rightInputSchema.numberOfAttributes(); ++i) {
      auto col = new ColumnReference(i, m_rightInputSchema.getAttributeType(i));
      cols->push_back(col);
      colNums.insert(0);
    }
    return cols;
  }

 private:
  void processInDebug(const std::shared_ptr<WindowBatch> &lBatch,
                      const std::shared_ptr<WindowBatch> &rBatch, Task &task,
                      int pid) {
    long currentIndex1 = lBatch->getBufferStartPointer();
    long currentIndex2 = rBatch->getBufferStartPointer();
    long endIndex1 = lBatch->getBufferEndPointer() + m_leftInputSchema.getTupleSize();
    long endIndex2 = rBatch->getBufferEndPointer() + m_rightInputSchema.getTupleSize();
    long currentWindowStart1 = currentIndex1;
    long currentWindowEnd1 = currentIndex1;
    long currentWindowStart2 = currentIndex2;
    long currentWindowEnd2 = currentIndex2;

    auto lBuffer = lBatch->getInputQueryBuffer();
    auto rBuffer = rBatch->getInputQueryBuffer();
    auto outputBuffer =
        PartialWindowResultsFactory::getInstance().newInstance(pid);

    int tupleSize1 = m_leftInputSchema.getTupleSize();
    int tupleSize2 = m_rightInputSchema.getTupleSize();

    // todo: fix the padding logic
    // Actual Tuple Size without padding
    int pointerOffset1 = tupleSize1 - m_leftInputSchema.getPadLength();
    int pointerOffset2 = tupleSize2 - m_rightInputSchema.getPadLength();

    auto windowDef1 = lBatch->getWindowDefinition();
    auto windowDef2 = rBatch->getWindowDefinition();

    if (m_debug) {
      std::cout << "[DBG] t " + std::to_string(lBatch->getTaskId()) +
          " batch-1 [" + std::to_string(currentIndex1) + ", " +
          std::to_string(endIndex1) + "] " +
          std::to_string((endIndex1 - currentIndex1) /
              tupleSize1) +
          " tuples [f " +
          std::to_string(lBatch->getFreePointer()) +
          "] / batch-2 [" + std::to_string(currentIndex2) + ", " +
          std::to_string(endIndex2) + "] " +
          std::to_string((endIndex2 - currentIndex2) /
              tupleSize2) +
          " tuples [f " +
          std::to_string(lBatch->getSecondFreePointer()) + "]"
                << std::endl;
    }

    long currentTimestamp1, startTimestamp1;
    long currentTimestamp2, startTimestamp2;

    if (m_monitorSelectivity) m_invoked[pid].m_value = m_matched[pid].m_value = 0L;

    // Is one of the windows empty?
    if (currentIndex1 != endIndex1 && currentIndex2 != endIndex2) {
      long prevCurrentIndex1 = -1;
      long countMatchPositions = 0;

      // Changed <=, <=, || to &&
      // while (currentIndex1 < endIndex1 && currentIndex2 <= endIndex2) {
      // OLD
      while (currentIndex1 < endIndex1 || currentIndex2 < endIndex2) {
        //std::cout << "[DBG] batch-1 index " + std::to_string(currentIndex1) + " end "
        //+ std::to_string(endIndex1) + " batch-2 index " +
        //std::to_string(currentIndex2) + " end " + std::to_string(endIndex1) << std::endl;

        // Get timestamps of currently processed tuples in either batch
        currentTimestamp1 = lBatch->getTimestamp(currentIndex1);
        currentTimestamp2 = rBatch->getTimestamp(currentIndex2);

        // Move in first batch?
        if ((currentTimestamp1 < currentTimestamp2) ||
            (currentTimestamp1 == currentTimestamp2 &&
                currentIndex2 >= endIndex2)) {
          // Scan second window
          // Changed here: <=
          // for (long i = currentWindowStart2; i <= currentWindowEnd2; i +=
          // tupleSize2) { OLD
          for (long i = currentWindowStart2; i < currentWindowEnd2; i += tupleSize2) {
            //std::cout << "[DBG] 1st window index " +
            //std::to_string(currentIndex1) + " 2nd window index " +
            //                 std::to_string(i) << std::endl;

            if (m_monitorSelectivity) m_invoked[pid].m_value++;

            if (m_predicate == nullptr
                // m_predicate.satisfied (buffer1, schema1, currentIndex1, buffer2, schema2, i)
                ) {
              if (prevCurrentIndex1 != currentIndex1) {
                prevCurrentIndex1 = currentIndex1;
                countMatchPositions++;
              }

              //std::cout << "[DBG] match at currentIndex1 = " + std::to_string(currentIndex1) + " (count = " + std::to_string(countMatchPositions) + ")" << std::endl;

              auto writePos = outputBuffer->getPosition();
              std::memcpy(outputBuffer->getBufferRaw() + writePos, lBuffer->getBufferRaw() + currentIndex1, tupleSize1);
              writePos += tupleSize1;
              std::memcpy(outputBuffer->getBufferRaw() + writePos, rBuffer->getBufferRaw() + i, tupleSize2);
              writePos += tupleSize2;
              // Write dummy content, if needed
              writePos += m_outputSchema.getPadLength();
              outputBuffer->setPosition(writePos);

              if (m_monitorSelectivity) m_matched[pid].m_value++;
            }
          }

          // Add current tuple to window over first batch
          currentWindowEnd1 = currentIndex1;

          // Remove old tuples in window over first batch
          if (windowDef1->isRowBased()) {
            if ((currentWindowEnd1 - currentWindowStart1) / tupleSize1 > windowDef1->getSize())
              currentWindowStart1 += windowDef1->getSlide() * tupleSize1;
          } else if (windowDef1->isRangeBased()) {
            startTimestamp1 = lBatch->getTimestamp(currentWindowStart1);
            while (startTimestamp1 < currentTimestamp1 - windowDef1->getSize()) {
              currentWindowStart1 += tupleSize1;
              startTimestamp1 = lBatch->getTimestamp(currentWindowStart1);
            }
          }

          // Remove old tuples in window over second batch (only for range
          // windows)
          if (windowDef2->isRangeBased()) {
            startTimestamp2 = rBatch->getTimestamp(currentWindowStart2);
            while (startTimestamp2 < currentTimestamp1 - windowDef2->getSize()) {
              currentWindowStart2 += tupleSize2;
              startTimestamp2 = rBatch->getTimestamp(currentWindowStart2);
            }
          }

          // Do the actual move in first window batch
          currentIndex1 += tupleSize1;
        } else {
          // Move in second batch
          // Scan first window

          //std::cout << "[DBG] move in second window..." << std::endl;
          //std::cout << "[DBG] scan first window: start " + std::to_string(currentWindowStart1) +
          //" end " + std::to_string(currentWindowEnd1) << std::endl;

          // Changed here: <=
          // for (long i = currentWindowStart1; i <= currentWindowEnd1; i += tupleSize1) {
          for (long i = currentWindowStart1; i < currentWindowEnd1; i += tupleSize1) {
            if (m_monitorSelectivity) m_invoked[pid].m_value++;

            if (m_predicate == nullptr
                // m_predicate.satisfied (buffer1, schema1, i, buffer2, schema2, currentIndex2)
                ) {
              //std::cout << "[DBG] Match in first window..." << std::endl;

              auto writePos = outputBuffer->getPosition();
              std::memcpy(outputBuffer->getBufferRaw() + writePos, lBuffer->getBufferRaw() + i, tupleSize1);
              writePos += tupleSize1;
              std::memcpy(outputBuffer->getBufferRaw() + writePos, rBuffer->getBufferRaw() + currentIndex2, tupleSize2);
              writePos += tupleSize2;
              // Write dummy content, if needed
              writePos += m_outputSchema.getPadLength();
              outputBuffer->setPosition(writePos);

              if (m_monitorSelectivity) m_matched[pid].m_value++;
            }
          }

          // Add current tuple to window over second batch
          currentWindowEnd2 = currentIndex2;

          //std::cout << "[DBG] currentWindowStart2 = " + std::to_string(currentWindowStart2) << std::endl;
          //std::cout << "[DBG] currentWindowEnd2 = " + std::to_string(currentWindowEnd2) << std::endl;

          // Remove old tuples in window over second batch
          if (windowDef2->isRowBased()) {
            if ((currentWindowEnd2 - currentWindowStart2) / tupleSize2 > windowDef2->getSize())
              currentWindowStart2 += windowDef2->getSlide() * tupleSize2;
          } else if (windowDef2->isRangeBased()) {
            startTimestamp2 = rBatch->getTimestamp(currentWindowStart2);
            while (startTimestamp2 < currentTimestamp2 - windowDef2->getSize()) {
              currentWindowStart2 += tupleSize2;
              startTimestamp2 = rBatch->getTimestamp(currentWindowStart2);
            }
          }

          // Remove old tuples in window over first batch (only for range windows)
          if (windowDef1->isRangeBased()) {
            startTimestamp1 = lBatch->getTimestamp(currentWindowStart1);
            while (startTimestamp1 < currentTimestamp2 - windowDef1->getSize()) {
              currentWindowStart1 += tupleSize1;
              startTimestamp1 = lBatch->getTimestamp(currentWindowStart1);
            }
          }
          // Do the actual move in second window batch
          currentIndex2 += tupleSize2;
        }
      }
    }

    // lBuffer->release();
    // rBuffer->release();
    lBatch->setOutputBuffer(outputBuffer);
    lBatch->setSchema(&m_outputSchema);

    if (m_debug) {
      std::cout << "[DBG] output buffer position is " +
          std::to_string(outputBuffer->getPosition())
                << std::endl;
    }

    if (m_monitorSelectivity) {
      double selectivity = 0;
      if (m_invoked[pid].m_value > 0)
        selectivity = ((double)m_matched[pid].m_value / (double)m_invoked[pid].m_value) * 100;
      std::cout << "[DBG] task " + std::to_string(lBatch->getTaskId()) + " " +
          std::to_string(m_matched[pid].m_value) + " out of " +
          std::to_string(m_invoked[pid].m_value) + " tuples selected (" +
          std::to_string(selectivity) + ")"
                << std::endl;
    }
    task.outputWindowBatchResult(lBatch);
  }

  void processCodeGen(const std::shared_ptr<WindowBatch> &lBatch,
                      const std::shared_ptr<WindowBatch> &rBatch, Task &task,
                      int pid) {
    long currentIndex1 = lBatch->getBufferStartPointer();
    long currentIndex2 = rBatch->getBufferStartPointer();
    long endIndex1 = lBatch->getBufferEndPointer() + m_leftInputSchema.getTupleSize();
    long endIndex2 = rBatch->getBufferEndPointer() + m_rightInputSchema.getTupleSize();
    long currentWindowStart1 = currentIndex1;
    long currentWindowEnd1 = currentIndex1;
    long currentWindowStart2 = currentIndex2;
    long currentWindowEnd2 = currentIndex2;

    auto lBuffer = lBatch->getInputQueryBuffer();
    auto rBuffer = rBatch->getInputQueryBuffer();
    auto outputBuffer =
        PartialWindowResultsFactory::getInstance().newInstance(pid);

    int tupleSize1 = m_leftInputSchema.getTupleSize();
    int tupleSize2 = m_rightInputSchema.getTupleSize();

    // todo: fix the padding logic
    // Actual Tuple Size without padding
    int pointerOffset1 = tupleSize1 - m_leftInputSchema.getPadLength();
    int pointerOffset2 = tupleSize2 - m_rightInputSchema.getPadLength();

    auto windowDef1 = lBatch->getWindowDefinition();
    auto windowDef2 = rBatch->getWindowDefinition();

    if (m_debug) {
      std::cout << "[DBG] t " + std::to_string(lBatch->getTaskId()) +
          " batch-1 [" + std::to_string(currentIndex1) + ", " +
          std::to_string(endIndex1) + "] " +
          std::to_string((endIndex1 - currentIndex1) /
              tupleSize1) +
          " tuples [f " +
          std::to_string(lBatch->getFreePointer()) +
          "] / batch-2 [" + std::to_string(currentIndex2) + ", " +
          std::to_string(endIndex2) + "] " +
          std::to_string((endIndex2 - currentIndex2) /
              tupleSize2) +
          " tuples [f " +
          std::to_string(lBatch->getSecondFreePointer()) + "]"
                << std::endl;
    }

    long currentTimestamp1, startTimestamp1;
    long currentTimestamp2, startTimestamp2;

    if (m_monitorSelectivity) m_invoked[pid].m_value = m_matched[pid].m_value = 0L;

    // Is one of the windows empty?
    if (currentIndex1 != endIndex1 && currentIndex2 != endIndex2) {
      long writePos = outputBuffer->getPosition();
      process(currentIndex1, currentIndex2, endIndex1, endIndex2,
              lBuffer->getBufferRaw(), rBuffer->getBufferRaw(),
              outputBuffer->getBufferRaw(), writePos, m_monitorSelectivity,
              m_invoked[pid].m_value, m_matched[pid].m_value);
      outputBuffer->setPosition(writePos);
    }

    // lBuffer->release();
    // rBuffer->release();
    lBatch->setOutputBuffer(outputBuffer);
    lBatch->setSchema(&m_outputSchema);

    if (m_debug) {
      std::cout << "[DBG] output buffer position is " +
          std::to_string(outputBuffer->getPosition())
                << std::endl;
    }

    if (m_monitorSelectivity) {
      double selectivity = 0;
      if (m_invoked[pid].m_value > 0)
        selectivity = ((double)m_matched[pid].m_value / (double)m_invoked[pid].m_value) * 100;
      std::cout << "[DBG] task " + std::to_string(lBatch->getTaskId()) + " " +
          std::to_string(m_matched[pid].m_value) + " out of " +
          std::to_string(m_invoked[pid].m_value) + " tuples selected (" +
          std::to_string(selectivity) + ")"
                << std::endl;
    }
    task.outputWindowBatchResult(lBatch);
  }

  std::string getIncludesString() {
    std::string s;
    s.append("#include <stdlib.h>\n");
    s.append("#include <climits>\n");
    s.append("#include <cfloat>\n");
    s.append("#include <stdexcept>\n");
    s.append("#include <iterator>\n");
    s.append("#include <utility>\n");
    s.append("#include <cstring>\n");
    s.append("#include <mm_malloc.h>\n");
    s.append("\n");
    return s;
  }

  std::string getInputSchemaString(bool isFirst) {
    auto schema = (isFirst) ? m_leftInputSchema : m_rightInputSchema;
    auto suffix = (isFirst) ? "1" : "2";
    std::string s;
    s.append("struct alignas(16) input_tuple_t_");
    s.append(suffix);
    s.append(" {\n");
    /* The first attribute is always a timestamp */
    s.append("\tlong timestamp;\n");
    for (int i = 1; i < schema.numberOfAttributes(); i++) {
      auto type = schema.getAttributeType(i);
      switch (type) {
        case BasicType::Integer  : s.append("\tint _" + std::to_string(i) + ";\n");
          break;
        case BasicType::Float    : s.append("\tfloat _" + std::to_string(i) + ";\n");
          break;
        case BasicType::Long     : s.append("\tlong _" + std::to_string(i) + ";\n");
          break;
        case BasicType::LongLong : s.append("\t__uint128_t _" + std::to_string(i) + ";\n");
          break;
        default :
          throw std::runtime_error(
              "error: failed to generate tuple struct (attribute " + std::to_string(i) + " is undefined)");
      }
    }
    s.append("};\n");
    s.append("\n");
    return s;
  }

  std::string getOutputSchemaString() {
    std::string s;
    s.append("struct alignas(16) output_tuple_t {\n");
    int i = 0;
    if (m_outputSchema.hasTime()) {
      s.append("\tlong timestamp;\n");
      i++;
    }
    for (; i < m_outputSchema.numberOfAttributes(); i++) {
      auto type = m_outputSchema.getAttributeType(i);
      switch (type) {
        case BasicType::Integer  : s.append("\tint _" + std::to_string(i) + ";\n");
          break;
        case BasicType::Float    : s.append("\tfloat _" + std::to_string(i) + ";\n");
          break;
        case BasicType::Long     : s.append("\tlong _" + std::to_string(i) + ";\n");
          break;
        case BasicType::LongLong : s.append("\t__uint128_t _" + std::to_string(i) + ";\n");
          break;
        default :
          throw std::runtime_error(
              "error: failed to generate tuple struct (attribute " + std::to_string(i) + " is undefined)");
      }
    }
    s.append("};\n");
    s.append("\n");
    return s;
  }

  std::string getQueryDefinitionString(WindowDefinition *winDef1, WindowDefinition *winDef2) {
    std::string s;

    s.append("#define WINDOW_SIZE_1    " + std::to_string(winDef1->getSize()) + "L\n");
    s.append("#define WINDOW_SLIDE_1   " + std::to_string(winDef1->getSlide()) + "L\n");
    s.append("#define WINDOW_SIZE_2    " + std::to_string(winDef2->getSize()) + "L\n");
    s.append("#define WINDOW_SLIDE_2   " + std::to_string(winDef2->getSlide()) + "L\n");
    s.append("#define PAD_LENGTH       " + std::to_string(m_outputSchema.getPadLength()) + "L\n");
    s.append("#define BUFFER_SIZE      " + std::to_string(m_circularBufferSize) + "L\n");
    s.append("#define MASK             " + std::to_string(m_circularBufferSize-1) + "L\n");
    s.append("#define UNBOUNDED_SIZE   " + std::to_string(SystemConf::getInstance().UNBOUNDED_BUFFER_SIZE) + "L\n");
    s.append("\n");
    return s;
  }

  std::string getSelectionExpr(bool isFirst) {
    std::string s;
    if (m_predicate != nullptr) {

      s.append("        auto maskedI = (i&MASK);\n");
      s.append("        if ( ");
      s.append(m_predicate->toSExprForCodeGen());
      s.append(" )\n");

      // todo: fix the predicate definition
      std::string str = "data[bufferPtr]";
      if (isFirst) {
        s.replace(s.find(str), str.length(), "leftB[maskedCI1/tupleSize1]");
        s.replace(s.find(str), str.length(), "rightB[maskedI/tupleSize2]");
      } else {
        s.replace(s.find(str), str.length(), "leftB[maskedI/tupleSize1]");
        s.replace(s.find(str), str.length(), "rightB[maskedCI2/tupleSize2]");
      }

    } else {
      s.append("if (true)");
    }
    return s;
  }

  std::string getFirstWindowExpr(WindowDefinition *winDef1, WindowDefinition *winDef2) {
    std::string s;
    s.append("      // Remove old tuples in window over first batch\n");
    if (winDef1->isRowBased()) {
      s.append("      if ((currentWindowEnd1 - currentWindowStart1) / tupleSize1 > WINDOW_SIZE_1)\n"
          "       currentWindowStart1 += WINDOW_SLIDE_1 * tupleSize1;\n");
    } else {
      s.append("      startTimestamp1 = (int) leftB[currentWindowStart1/tupleSize1].timestamp;\n"
          "      while (startTimestamp1 < currentTimestamp1 - WINDOW_SIZE_1) {\n"
          "        currentWindowStart1 += tupleSize1;\n"
          "        startTimestamp1 = (int) leftB[currentWindowStart1/tupleSize1].timestamp;\n"
          "      }\n");

    }
    if (winDef2->isRangeBased()) {
      s.append("      // Remove old tuples in window over second batch (only for range\n"
               "      // windows)\n");
      s.append("      startTimestamp2 = (int) rightB[currentWindowStart2/tupleSize2].timestamp;\n"
          "      while (startTimestamp2 < currentTimestamp1 - WINDOW_SIZE_2) {\n"
          "        currentWindowStart2 += tupleSize2;\n"
          "        startTimestamp2 = (int) rightB[currentWindowStart2/tupleSize2].timestamp;\n"
          "      }\n");
    }
    return s;
  }

  std::string getSecondWindowExpr(WindowDefinition *winDef1, WindowDefinition *winDef2) {
    std::string s;
    s.append("      // Remove old tuples in window over second batch\n");
    if (winDef2->isRowBased()) {
      s.append("      if ((currentWindowEnd2 - currentWindowStart2) / tupleSize2 > WINDOW_SIZE_2)\n"
          "        currentWindowStart2 += WINDOW_SLIDE_2 * tupleSize2;\n");
    } else {
      s.append("      startTimestamp2 = (int) rightB[currentWindowStart2/tupleSize2].timestamp;\n"
          "      while (startTimestamp2 < currentTimestamp2 - WINDOW_SIZE_2) {\n"
          "        currentWindowStart2 += tupleSize2;\n"
          "        startTimestamp2 = (int) rightB[currentWindowStart2/tupleSize2].timestamp;\n"
          "      }\n");

    }
    if (winDef1->isRangeBased()) {
      s.append("      // Remove old tuples in window over first batch (only for range windows)\n");
      s.append("      startTimestamp1 = (int) leftB[currentWindowStart1/tupleSize1].timestamp;\n"
          "      while (startTimestamp1 < currentTimestamp2 - WINDOW_SIZE_1) {\n"
          "        currentWindowStart1 += tupleSize1;\n"
          "        startTimestamp1 = (int) leftB[currentWindowStart1/tupleSize1].timestamp;\n"
          "      }\n");
    }
    return s;
  }

  std::string getComputationCode(std::string &predicate1, std::string &predicate2, std::string &firstWindowExpr, std::string &secondWindowExpr) {
    std::string s = "void processJoin(long currentIndex1, long currentIndex2, long endIndex1,\n"
        "                 long endIndex2, char *lBuffer, char *rBuffer,\n"
        "                 char *outputBuffer, long &writePos, bool monitorSelectivity,\n"
        "                 long &invoked, long &matched) {\n"
        "  long currentWindowStart1 = currentIndex1;\n"
        "  long currentWindowEnd1 = currentIndex1;\n"
        "  long currentWindowStart2 = currentIndex2;\n"
        "  long currentWindowEnd2 = currentIndex2;\n"
        "  long prevCurrentIndex1 = -1;\n"
        "  long countMatchPositions = 0;\n"
        "  long currentTimestamp1, startTimestamp1;\n"
        "  long currentTimestamp2, startTimestamp2;\n"
        "\n"
        "  input_tuple_t_1 *leftB = (input_tuple_t_1 *)lBuffer;\n"
        "  int tupleSize1 = sizeof(input_tuple_t_1);\n"
        "  input_tuple_t_2 *rightB = (input_tuple_t_2 *)rBuffer;\n"
        "  int tupleSize2 = sizeof(input_tuple_t_2);\n"
        "\n"
        "  // Changed <=, <=, || to &&\n"
        "  // while (currentIndex1 < endIndex1 && currentIndex2 <= endIndex2) {\n"
        "  // OLD\n"
        "  while (currentIndex1 < endIndex1 && currentIndex2 < endIndex2) {\n // the && was ||"
        "    // std::cout << \"[DBG] batch-1 index \" + std::to_string(currentIndex1) + \"\n"
        "    // end \"\n"
        "    //+ std::to_string(endIndex1) + \" batch-2 index \" +\n"
        "    // std::to_string(currentIndex2) + \" end \" + std::to_string(endIndex1) <<\n"
        "    // std::endl;\n"
        "\n"
        "    // Get timestamps of currently processed tuples in either batch\n"
        "    auto maskedCI1 = (currentIndex1 & MASK);\n"
        "    auto maskedCI2 = (currentIndex2 & MASK);\n"
        "    currentTimestamp1 = (int) leftB[maskedCI1 / tupleSize1].timestamp;\n"
        "    currentTimestamp2 = (int) rightB[maskedCI2 / tupleSize2].timestamp;\n"
        "\n"
        "    // Move in first batch?\n"
        "    if ((currentTimestamp1 < currentTimestamp2) ||\n"
        "        (currentTimestamp1 == currentTimestamp2 &&\n"
        "         currentIndex2 >= endIndex2)) {\n"
        "      // Scan second window\n"
        "      // Changed here: <=\n"
        "      // for (long i = currentWindowStart2; i <= currentWindowEnd2; i +=\n"
        "      // tupleSize2) { OLD\n"
        "      for (long i = currentWindowStart2; i < currentWindowEnd2;\n"
        "           i += tupleSize2) {\n"
        "        // std::cout << \"[DBG] 1st window index \" +\n"
        "        // std::to_string(currentIndex1) + \" 2nd window index \" +\n"
        "        //                 std::to_string(i) << std::endl;\n"
        "\n"
        "        if (monitorSelectivity) invoked++;\n"
        "\n" + predicate1 +
        "        // if (true)\n"
        "        {\n"
        "          if (prevCurrentIndex1 != currentIndex1) {\n"
        "            prevCurrentIndex1 = currentIndex1;\n"
        "            countMatchPositions++;\n"
        "          }\n"
        "\n"
        "          // std::cout << \"[DBG] match at currentIndex1 = \" +\n"
        "          // std::to_string(currentIndex1) + \" (count = \" +\n"
        "          // std::to_string(countMatchPositions) + \")\" << std::endl;\n"
        "          if (writePos + tupleSize1 + tupleSize2 >= UNBOUNDED_SIZE)\n"
        "            throw std::runtime_error(\"error: increase the size of unbounded buffers for join (\" \n"
        "                                     + std::to_string(writePos + tupleSize1 + tupleSize2) + \" < \"\n"
        "                                     + std::to_string(UNBOUNDED_SIZE) + \" - selectivity \"\n"
        "                                     + std::to_string(((double)matched / (double)invoked) * 100) + \" - matched \"\n"
        "                                     + std::to_string(matched) + \")\");\n"
        "\n"
        "          std::memcpy(outputBuffer + writePos, lBuffer + maskedCI1,\n"
        "                      tupleSize1);\n"
        "          writePos += tupleSize1;\n"
        "          std::memcpy(outputBuffer + writePos, rBuffer + maskedI, tupleSize2);\n"
        "          writePos += tupleSize2;\n"
        "          // Write dummy content, if needed\n"
        "          writePos += PAD_LENGTH;\n"
        "\n"
        "          if (monitorSelectivity) matched++;\n"
        "        }\n"
        "      }\n"
        "\n"
        "      // Add current tuple to window over first batch\n"
        "      currentWindowEnd1 = currentIndex1;\n"
        "\n" + firstWindowExpr +
        "\n"
        "      // Do the actual move in first window batch\n"
        "      currentIndex1 += tupleSize1;\n"
        "    } else {\n"
        "      // Move in second batch\n"
        "      // Scan first window\n"
        "\n"
        "      // std::cout << \"[DBG] move in second window...\" << std::endl;\n"
        "      // std::cout << \"[DBG] scan first window: start \" +\n"
        "      // std::to_string(currentWindowStart1) + \" end \" +\n"
        "      // std::to_string(currentWindowEnd1) << std::endl;\n"
        "\n"
        "      // Changed here: <=\n"
        "      // for (long i = currentWindowStart1; i <= currentWindowEnd1; i +=\n"
        "      // tupleSize1) {\n"
        "      for (long i = currentWindowStart1; i < currentWindowEnd1;\n"
        "           i += tupleSize1) {\n"
        "        if (monitorSelectivity) invoked++;\n"
        "\n" + predicate2 +
        "        // if (true)\n"
        "        {\n"
        "          // std::cout << \"[DBG] Match in first window...\" << std::endl;\n"
        "          if (writePos + tupleSize1 + tupleSize2 >= UNBOUNDED_SIZE)\n"
        "            throw std::runtime_error(\"error: increase the size of unbounded buffers for join (\" \n"
        "                                     + std::to_string(writePos + tupleSize1 + tupleSize2) + \" < \"\n"
        "                                     + std::to_string(UNBOUNDED_SIZE) + \" - selectivity \"\n"
        "                                     + std::to_string(((double)matched / (double)invoked) * 100) + \" - matched \"\n"
        "                                     + std::to_string(matched) + \")\");\n"
        "\n"
        "          std::memcpy(outputBuffer + writePos, lBuffer + maskedI, tupleSize1);\n"
        "          writePos += tupleSize1;\n"
        "          std::memcpy(outputBuffer + writePos, rBuffer + maskedCI2,\n"
        "                      tupleSize2);\n"
        "          writePos += tupleSize2;\n"
        "          // Write dummy content, if needed\n"
        "          writePos += PAD_LENGTH;\n"
        "\n"
        "          if (monitorSelectivity) matched++;\n"
        "        }\n"
        "      }\n"
        "\n"
        "      // Add current tuple to window over second batch\n"
        "      currentWindowEnd2 = currentIndex2;\n"
        "\n"
        "      // std::cout << \"[DBG] currentWindowStart2 = \" +\n"
        "      // std::to_string(currentWindowStart2) << std::endl; std::cout << \"[DBG]\n"
        "      // currentWindowEnd2 = \" + std::to_string(currentWindowEnd2) << std::endl;\n"
        "\n" + secondWindowExpr +
        "      // Do the actual move in second window batch\n"
        "      currentIndex2 += tupleSize2;\n"
        "    }\n"
        "  }\n"
        "}\n"
        "\n";

    return s;
  }

  std::string getC_Definitions() {
    std::string s;
    s.append("extern \"C\" {\n"
        "void process(long currentIndex1, long currentIndex2, long endIndex1, long endIndex2,\n"
        "             char *lBuffer, char *rBuffer, char *outputBuffer, long &writePos,\n"
        "             bool monitorSelectivity, long &invoked, long &matched) {\n"
        "  processJoin(currentIndex1, currentIndex2, endIndex1, endIndex2, lBuffer,\n"
        "              rBuffer, outputBuffer, writePos, monitorSelectivity, invoked,\n"
        "              matched);\n"
        "};\n"
        "}");
    return s;
  }
};