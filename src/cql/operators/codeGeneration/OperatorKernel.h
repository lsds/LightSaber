#pragma once

#include <dlfcn.h>

#include <fstream>
#include <iostream>
#include <utility>
#include <vector>

#include "buffers/PartialWindowResultsFactory.h"
#include "cql/expressions/ColumnReference.h"
#include "cql/operators/AggregateOperatorCode.h"
#include "cql/operators/Aggregation.h"
#include "cql/operators/AggregationType.h"
#include "cql/operators/OperatorCode.h"
#include "cql/operators/Projection.h"
#include "cql/operators/Selection.h"
#include "cql/operators/StaticHashJoin.h"
#include "cql/operators/codeGeneration/AggregationTree.h"
#include "cql/operators/codeGeneration/GeneralAggregationGraph.h"
#include "cql/operators/codeGeneration/OperatorJit.h"
#include "cql/predicates/Predicate.h"
#include "utils/QueryConfig.h"
#include "utils/Utils.h"
#include "utils/WindowDefinition.h"

class OperatorKernel : public OperatorCode, public AggregateOperatorCode {
 private:
  WindowDefinition *m_windowDefinition;
  TupleSchema *m_inputSchema;
  TupleSchema *m_outputSchema;
  // Projection
  std::vector<Expression *> *m_expressions;
  bool m_hasIntermMaterialization = false;
  // Selection
  Predicate *m_predicate;
  // Aggregation
  std::vector<AggregationType> *m_aggregationTypes;
  std::vector<ColumnReference *> *m_aggregationAttributes;
  ColumnReference m_timestampReference;
  std::vector<Expression *> *m_groupByAttributes;
  int m_keyLength = 0, m_valueLength = 0, m_bucketSize = 0;
  bool m_groupBy;
  int m_numberOfAggregationAttributes = 0;
  int m_numberOfKeyAttributes = 0;
  bool m_processIncremental;
  bool m_invertible;
  bool m_nonInvertible;
  int m_collisionBarrier = 0;
  std::string m_customHashTable;
  bool m_useParallelMerge;
  // Static Hash Join
  Predicate *m_staticJoinPredicate;
  char *m_staticBuffer = nullptr;
  std::string m_staticHashTable;
  std::string m_staticComputation;
  std::string m_staticInitialization;
  // Having
  Predicate *m_havingPredicate;
  // Post window operation
  std::string m_postWindowOp;
  std::string m_postWindowPredicate;
  std::string m_postMergeOperation;


  // Code Generation Variables
  bool m_isReady;
  bool m_genCode;
  bool m_generateFile;
  int m_id = -1;
  bool m_usePtrs;
  const bool m_doProcessing = true; // set this false this for ingestion benchmarking
  CodeGenWrapper m_codeGen;

  std::unique_ptr<Utils::DynamicLoader> m_dLoader;
  std::function<void(int, char *, long, long, char *, long &)> process;
  std::function<void(int, char *, long, long, long, long, long *, long *,
                     char *, char *, char *, char *, int *, int *, int *,
                     int *, /*long *, long *, long *, long * ,*/ long,
                     int *, char *)> processFragments;
  std::function<void(int, char *, long, long, long, long, long *, long *,
                     char **, char **, char **, char *, int *, int *,
                     int *, int *, /*long *, long *, long *, long *,*/
                     long, int *, char *)> processFragmentsWithPtrs;
  std::function<long(char *, int *, int, char *, int *, int, int, int, bool, char *, long, int &)> aggregate;
  std::function<long(char **, int *, int, char **, int *, int, int, int, bool, char *, long, int &)> aggregateWithPtrs;
  std::function<long(char *, int, int, char **, int, int &, int &, int &, bool)> aggregateSingleHashTableWithPtrs;
  std::function<long()> getHashTableSize;

  QueryConfig *m_config = nullptr;

  const bool m_debug = false;

 public:
  OperatorKernel(bool genCode = true, bool usePtrs = true, bool useParallelMerge = false, bool generateFile = true) :
      m_windowDefinition(nullptr),
      m_inputSchema(nullptr),
      m_outputSchema(nullptr),
      m_expressions(nullptr),
      m_predicate(nullptr),
      m_aggregationTypes(nullptr),
      m_aggregationAttributes(nullptr),
      m_timestampReference(0),
      m_groupBy(false),
      m_processIncremental(false),
      m_invertible(false),
      m_nonInvertible(false),
      m_useParallelMerge(useParallelMerge),
      m_staticJoinPredicate(nullptr),
      m_staticBuffer(nullptr),
      m_havingPredicate(nullptr),
      m_isReady(false),
      m_genCode(genCode),
      m_generateFile(generateFile),
      m_usePtrs(usePtrs) {}

  void setQueryId(int qId) {
    m_id = qId;
  }

  void setInputSchema(TupleSchema *schema) { m_inputSchema = schema; }

  void setProjection(Projection *projection) {
    if (m_expressions != nullptr)
      throw std::runtime_error("error: projection has already been set, try setting static join after projection");
    m_expressions = &projection->getExpressions();
    m_outputSchema = &projection->getOutputSchema();
    m_hasIntermMaterialization = projection->isIntermediate();
  }

  void setSelection(Selection *selection) {
    if (m_predicate != nullptr)
      throw std::runtime_error("error: selection has already been set");
    m_predicate = selection->getPredicate();
    if (m_outputSchema == nullptr)
      m_outputSchema = m_inputSchema;
  }

  void setStaticHashJoin(StaticHashJoin *hasJoin) {
    m_staticJoinPredicate = hasJoin->getPredicate();
    //m_outputSchema = &hasJoin->getOutputSchema();
    m_staticBuffer = hasJoin->getStaticBuffer()->data();
    m_staticHashTable = hasJoin->getStaticHashTable();
    m_staticComputation = hasJoin->getStaticComputation();
    m_staticInitialization = hasJoin->getStaticInitialization();
  }

  void setAggregation(Aggregation *aggregation) {
    if (m_aggregationTypes != nullptr || m_aggregationAttributes != nullptr)
      throw std::runtime_error("error: aggregation has already been set");
    m_windowDefinition = &aggregation->getWindowDefinition();
    if (!hasProjection())
      m_outputSchema = &aggregation->getOutputSchema();
    m_aggregationTypes = &aggregation->getAggregationTypes();
    m_aggregationAttributes = &aggregation->getAggregationAttributes();
    if (!(*m_aggregationAttributes).empty())  {
      auto set = std::unordered_set<int>();
      for (auto &at: (*m_aggregationAttributes)) {
        set.insert(at->getColumn());
      }
      m_numberOfAggregationAttributes = set.size();
    }
    m_keyLength = aggregation->getKeyLength();
    m_valueLength = aggregation->getValueLength();
    m_bucketSize = aggregation->getBucketSize();
    m_groupByAttributes = &aggregation->getGroupByAttributes();
    m_groupBy = aggregation->hasGroupBy();
    m_processIncremental = aggregation->hasIncremental();
    m_invertible = aggregation->hasInvertible();
    m_nonInvertible = aggregation->hasNonInvertible();
    if (!(*m_groupByAttributes).empty()) {
      m_numberOfKeyAttributes = (int) (*m_groupByAttributes).size();
    }
  }

  void setHaving(Selection *selection) {
    if (m_havingPredicate != nullptr)
      throw std::runtime_error("error: having has already been set");
    m_havingPredicate = selection->getPredicate();
  }

  void setPostWindowOperation(std::string operation, std::string predicate, std::string mergeOperation) {
    if (!m_postWindowOp.empty() || !m_postWindowPredicate.empty() || !m_postMergeOperation.empty())
      throw std::runtime_error("error: post window operation has already been set");
    m_postWindowOp = std::move(operation);
    m_postWindowPredicate = std::move(predicate);
    m_postMergeOperation = std::move(mergeOperation);
  }

  void setCollisionBarrier(int barrier) {
    m_collisionBarrier = barrier;
  }

  void setCustomHashTable(std::string hashTable) {
    std::cout << "Setting custom hashtable..." << std::endl;
    m_customHashTable = hashTable;
  }

  void setup(QueryConfig *config = nullptr) {
    m_config = config;
    if (m_id == -1) {
      throw std::runtime_error("error: query id has not be set up");
    }
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
    if (!hasAggregation())
      setupWithoutAggregation();
    else
      setupWithAggregation();

  }

  void setupWithoutAggregation() {
    std::string s;
    if (!SystemConf::getInstance().RECOVER) {
      s.append(getIncludesString());
      // s.append(getQueryDefinitionString());
      s.append(getInputSchemaString());
      if (hasProjection())
        s.append(getOutputSchemaString());
      else
        s.append("using output_tuple_t = input_tuple_t;\n\n");

      std::string computation = getComputeString();
      s.append(getC_Definitions(computation));

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
        auto processFn = m_codeGen.getFunction<void(int, char *, long, long, char *, long &)>("process");

        if (!processFn) {
          std::cout << "Failed to fetch the pointers." << std::endl;
          exit(1);
        }
        process = *processFn;

        slt.join();
      } else {
        m_dLoader = std::make_unique<Utils::DynamicLoader>(libPath.c_str());
        process = m_dLoader->load<void(int, char *, long, long, char *, long &)>(libPath, "process");
      }
    }
    m_isReady = true;
  }

  void setupWithAggregation() {
    std::string s;

    if (!SystemConf::getInstance().RECOVER) {
      s.append(getIncludesString());
      s.append(getQueryDefinitionString());
      s.append(getInputSchemaString());
      s.append(getOutputSchemaString());

      s.append(getSingleKeyDataStructureString());
      if (hasGroupBy()) {
        std::string key = getKeyType();
        s.append(getHashTableBucketString());
        s.append(getHashTableString(key));
        s.append(getHashTableStaticDeclaration());
        if (!m_usePtrs) {
          s.append(getHashTableMergeString());
        } else {
          if (!m_useParallelMerge) {
            s.append(getHashTableMergeWithPtrsString());
          } else {
            s.append(getHashTableParallelMergeWithPtrsString());
          }
        }

      } else {
        if (hasIncremental()) s.append(getSingleStaticDeclaration());
        s.append(getSingleBucketString());
        s.append(getSingleKeyMergeString());
      }

      if (hasStaticHashJoin()) s.append(m_staticHashTable);

      s.append(getComputeString());
      s.append(getAggregate_C_Definitions());

      if (m_generateFile) {
        //auto path = Utils::getCurrentWorkingDir();
        auto path = SystemConf::getInstance().FILE_ROOT_PATH + "/scabbard";
        std::ofstream out(path + "/GeneratedCode_" + std::to_string(m_id) + ".cpp");
        out << s;
        out.close();
      }
    }
    if (m_genCode) {
      Utils::Timer timer;
      int argc = 2;
      //auto path = Utils::getCurrentWorkingDir();
      auto path = SystemConf::getInstance().FILE_ROOT_PATH + "/scabbard";
      std::string mainPath = path + "/LightSaber";
      std::string generatedPath = path + "/GeneratedCode_" + std::to_string(m_id) + ".cpp";
      std::string libPath = path + "/GeneratedCode_" + std::to_string(m_id) + ".so";
      const char *str0 = mainPath.c_str();
      const char *str1 = generatedPath.c_str();
      const char **argv = (const char **)malloc(2 * sizeof(char *));
      argv[0] = str0;
      argv[1] = str1;
      if (!SystemConf::getInstance().RECOVER) {
        // generate shared library
        std::thread slt([&] {
          std::string command = "clang -shared -fPIC -O3 -march=native -g -o " + libPath + " " + generatedPath;
          system(command.c_str());
        });
        m_codeGen.parseAndCodeGen(argc, argv, SystemConf::getInstance().RECOVER);
        if (!hasGroupBy() ||
            !m_usePtrs) {  // use the actual values stored sequentially in a
                           // buffer to aggregate
          auto processFn = m_codeGen.getFunction<void(
              int, char *, long, long, long, long, long *, long *, char *,
              char *, char *, char *, int *, int *, int *,
              int *,  // long *, long *, long *, long *,
              long, int *, char *)>("process");
          auto aggregateFn =
              m_codeGen.getFunction<long(char *, int *, int, char *, int *, int,
                                         int, int, bool, char *, long, int &)>(
                  "aggregate");
          if (!processFn || !aggregateFn) {
            std::cout << "Failed to fetch the pointers." << std::endl;
            exit(1);
          }
          processFragments = *processFn;
          aggregate = *aggregateFn;
        } else {  // use pointers to hashtables to aggregate
          auto processFn = m_codeGen.getFunction<void(
              int, char *, long, long, long, long, long *, long *, char **,
              char **, char **, char *, int *, int *, int *,
              int *,  // long *, long *, long *, long *,
              long, int *, char *)>("process");
          auto computeSizeFn =
              m_codeGen.getFunction<long()>("getHashTableSizeInBytes");
          if (!processFn || !computeSizeFn) {
            std::cout << "Failed to fetch the pointers." << std::endl;
            exit(1);
          }
          processFragmentsWithPtrs = *processFn;
          getHashTableSize = *computeSizeFn;

          if (!m_useParallelMerge) {
            auto aggregateFn =
                m_codeGen.getFunction<long(char **, int *, int, char **, int *, int,
                                      int, int, bool, char *, long, int &)>(
                        "aggregate");
            if (!aggregateFn) {
              std::cout << "Failed to fetch the pointers." << std::endl;
              exit(1);
            }
            aggregateWithPtrs = *aggregateFn;
          } else {
            auto aggregateFn = m_codeGen.getFunction<long(
                char *, int, int, char **, int, int &, int &, int &, bool)>(
                "aggregate");
            if (!aggregateFn) {
              std::cout << "Failed to fetch the pointers." << std::endl;
              exit(1);
            }
            aggregateSingleHashTableWithPtrs = *aggregateFn;
          }

          // Initialize the size needed for the hashtable here
          m_hashTableSize = getHashTableSize();
        }
        slt.join();
        timer.printElapsed("CodeGeneration-");
      } else {
        m_dLoader = std::make_unique<Utils::DynamicLoader>(libPath.c_str());
        if (!hasGroupBy() || !m_usePtrs) {  // use the actual values stored sequentially in a
                                            // buffer to aggregate
          processFragments = m_dLoader->load<void(
              int, char *, long, long, long, long, long *, long *, char *,
              char *, char *, char *, int *, int *, int *,
              int *,  // long *, long *, long *, long *,
              long, int *, char *)>(libPath, "process");
          aggregate =
              m_dLoader->load<long(char *, int *, int, char *, int *, int, int,
                                   int, bool, char *, long, int &)>(
                  libPath, "aggregate");
        } else {  // use pointers to hashtables to aggregate
          processFragmentsWithPtrs = m_dLoader->load<void(
              int, char *, long, long, long, long, long *, long *, char **,
              char **, char **, char *, int *, int *, int *,
              int *,  // long *, long *, long *, long *,
              long, int *, char *)>(libPath, "process");
          getHashTableSize = m_dLoader->load<long()>(libPath, "getHashTableSizeInBytes");

          if (!m_useParallelMerge) {
            aggregateWithPtrs =
                m_dLoader->load<long(char **, int *, int, char **, int *, int,
                                     int, int, bool, char *, long, int &)>(
                    libPath, "aggregate");
          } else {
            aggregateSingleHashTableWithPtrs =
                m_dLoader->load<long(char *, int, int, char **, int, int &,
                                     int &, int &, bool)>(libPath, "aggregate");
          }

          // Initialize the size needed for the hashtable here
          m_hashTableSize = getHashTableSize();
        }
        timer.printElapsed("DLibLoad-");
      }
    }
    m_isReady = true;
  }

  bool hasGroupBy() override {
    return m_groupBy;
  }

  TupleSchema &getOutputSchema() override {
    if (m_outputSchema == nullptr)
      throw std::runtime_error("error: outputSchema hasn't been set up");
    return *m_outputSchema;
  }

  std::vector<ColumnReference*> *getInputCols() override {
    if (hasProjection())
      throw std::runtime_error("error: projection operator hasn't been implemented yet");
    auto cols = new std::vector<ColumnReference*>;
    std::unordered_set<int> colNums;
    // always add the timestamp
    auto col = new ColumnReference(0, BasicType::Long);
    cols->push_back(col);
    colNums.insert(0);
    for (int i = 0; i < m_numberOfAggregationAttributes; ++i) {
      col = (*m_aggregationAttributes)[i];
      // todo: fix this -- works only for YSB
      if (hasStaticHashJoin() && col->getColumn() == 2)
        continue;
      if (colNums.find(col->getColumn()) == colNums.end()) {
        colNums.insert(col->getColumn());
        cols->push_back(col);
      }
    }
    for (int i = 0; i < m_numberOfKeyAttributes; ++i) {
      col = dynamic_cast<ColumnReference *>((*m_groupByAttributes)[i]);
      if (col) {
        if (colNums.find(col->getColumn()) == colNums.end()) {
          colNums.insert(col->getColumn());
          cols->push_back(col);
        }
      } else {
        col = new ColumnReference(-1, (*m_groupByAttributes)[i]->getBasicType());
        col->setExpression((*m_groupByAttributes)[i]->toSExprForCodeGen());
        cols->push_back(col);
        colNums.insert(-1);
      }
    }

    return cols;
  }

  AggregationType &getAggregationType() override {
    return getAggregationType(0);
  }

  AggregationType &getAggregationType(int idx) override {
    if (!hasAggregation())
      throw std::runtime_error("error: aggregation operator hasn't been set up");
    if (idx < 0 || idx > (int) (*m_aggregationTypes).size() - 1)
      throw std::out_of_range("error: invalid aggregation type index");
    return (*m_aggregationTypes)[idx];
  }

  bool hasIncremental() override { return m_processIncremental; }

  bool hasInvertible() override { return m_invertible; }

  bool hasNonInvertible() override { return m_nonInvertible; }

  bool hasBoth() override { return hasInvertible() && hasNonInvertible(); }

  int getBucketSize() override { return m_bucketSize; }

  std::string toSExpr() const override {
    std::string s;
    if (hasAggregation() && m_windowDefinition->isRowBased()) {
      s.append(getAggregationString());
    }
    if (hasSelection()) {
      s.append(getSelectionString());
    }
    if (hasAggregation() && m_windowDefinition->isRangeBased()) {
      s.append(getAggregationString());
    }
    if (hasProjection()) {
      s.append(getProjectionString());
    }
    return s;
  }

  explicit operator std::string() const {
    return toSExpr();
  }

  void processData(const std::shared_ptr<WindowBatch>& batch, Task &task, int pid) override {
    if (!m_isReady)
      throw std::runtime_error("error: the operator has not been set");

    if (hasAggregation()) {
      processWithFragments(batch, task, pid);
    } else {
      processWithoutFragments(batch, task, pid);
    }
  }

  void processWithoutFragments(const std::shared_ptr<WindowBatch>& batch, Task &task, int pid) {
    auto inputBuffer = batch->getInputQueryBuffer();
    long startP = batch->getBufferStartPointer();
    long endP = batch->getBufferEndPointer();
    long pos = 0;
    auto outputBuffer = PartialWindowResultsFactory::getInstance().newInstance(pid);

    if (m_doProcessing) {
      process(pid, inputBuffer->getBufferRaw(), startP, endP,
              outputBuffer->getBufferRaw(), pos);
    }

    outputBuffer->setPosition(pos);
    batch->setOutputBuffer(outputBuffer);
    task.outputWindowBatchResult(batch);
  }

  void processWithFragments(const std::shared_ptr<WindowBatch>& batch, Task &task, int pid) {
    auto circularBuffer = batch->getInputQueryBuffer();
    auto circularBufferSize = circularBuffer->getBufferCapacity(task.getNumaNodeId());
    auto &buffer = batch->getBuffer();
    auto startPointer = batch->getBufferStartPointer();
    auto endPointer = batch->getBufferEndPointer();
    batch->resetWindowPointers();

    // TODO: the hashtable sizes has to be variable size!!!
    auto openingWindows = (!hasGroupBy() || !m_usePtrs) ? PartialWindowResultsFactory::getInstance().newInstance(pid) :
                          PartialWindowResultsFactory::getInstance().newInstance(pid, m_hashTableSize);
    auto closingWindows = (!hasGroupBy() || !m_usePtrs) ? PartialWindowResultsFactory::getInstance().newInstance(pid) :
                          PartialWindowResultsFactory::getInstance().newInstance(pid, m_hashTableSize);
    auto pendingWindows = (!hasGroupBy() || !m_usePtrs) ? PartialWindowResultsFactory::getInstance().newInstance(pid) :
                          PartialWindowResultsFactory::getInstance().newInstance(pid, m_hashTableSize);
    auto completeWindows = PartialWindowResultsFactory::getInstance().newInstance(pid);
    auto &openingStartPointers = openingWindows->getStartPointers();
    auto &closingStartPointers = closingWindows->getStartPointers();
    auto &pendingStartPointers = pendingWindows->getStartPointers();
    auto &completeStartPointers = completeWindows->getStartPointers();

    //if (batch->hasTimestampOffset())
    //  batch->updateTimestamps();

    auto streamStartPointer = batch->getStreamStartPointer();
    int pointersAndCounts[8] = {};

    long timestampFromPrevBatch = 0;
    if (batch->getWindowDefinition()->isRangeBased())
      timestampFromPrevBatch = batch->getPrevEndTimestamp();

    if (m_debug) {
      std::cout << "[DBG] processWithFragments [TID " << task.getTaskId() << "]:"
                << " pipeline " << m_id
                << " entering generated function with "
                << batch->getBatchStartTimestamp() << " startTimestamp "
                << batch->getBatchEndTimestamp() << " endTimestamp "
                << batch->getBufferStartPointer() << " startPointer "
                << batch->getBufferEndPointer() << " endPointer "
                << streamStartPointer << " streamStartPointer "
                << timestampFromPrevBatch << " prevTimestamp "
                << circularBufferSize << " circularBufferSize "
                << std::endl;
    }

    if (m_doProcessing) {
      if (!hasGroupBy() ||
          !m_usePtrs) {  // use the actual values stored sequentially in a buffer to aggregate
        processFragments(
            pid, buffer.data(), circularBufferSize, startPointer, endPointer,
            timestampFromPrevBatch, batch->getWindowStartPointers().data(),
            batch->getWindowEndPointers().data(),
            openingWindows->getBufferRaw(), closingWindows->getBufferRaw(),
            pendingWindows->getBufferRaw(), completeWindows->getBufferRaw(),
            openingStartPointers.data(), closingStartPointers.data(),
            pendingStartPointers.data(), completeStartPointers.data(),
            // openingWindowIds.data(), closingWindowIds.data(), pendingWindowIds.data(), completeWindowIds.data(),
            streamStartPointer, &pointersAndCounts[0], m_staticBuffer);
      } else {  // use pointers to hashtables to aggregate
        processFragmentsWithPtrs(
            pid, buffer.data(), circularBufferSize, startPointer, endPointer,
            timestampFromPrevBatch, batch->getWindowStartPointers().data(),
            batch->getWindowEndPointers().data(),
            openingWindows->getBufferPtrs().data(),
            closingWindows->getBufferPtrs().data(),
            pendingWindows->getBufferPtrs().data(),
            completeWindows->getBufferRaw(), openingStartPointers.data(),
            closingStartPointers.data(), pendingStartPointers.data(),
            completeStartPointers.data(),
            // openingWindowIds.data(), closingWindowIds.data(), pendingWindowIds.data(), completeWindowIds.data(),
            streamStartPointer, &pointersAndCounts[0], m_staticBuffer);
      }
    }
    // Set positions
    openingWindows->setPosition((size_t) pointersAndCounts[0]);
    closingWindows->setPosition((size_t) pointersAndCounts[1]);
    pendingWindows->setPosition((size_t) pointersAndCounts[2]);
    completeWindows->setPosition((size_t) pointersAndCounts[3]);
    // Set counters
    openingWindows->incrementCount(pointersAndCounts[4]);
    closingWindows->incrementCount(pointersAndCounts[5]);
    pendingWindows->incrementCount(pointersAndCounts[6]);
    completeWindows->incrementCount(pointersAndCounts[7]);

    /* At the end of processing, set window batch accordingly */
    batch->setOpeningWindows(openingWindows);
    batch->setClosingWindows(closingWindows);
    batch->setPendingWindows(pendingWindows);
    batch->setCompleteWindows(completeWindows);
    // Set window ids
    //batch->setWindowIds();

    //batch->getBuffer().release();
    batch->setSchema(m_outputSchema);
    task.outputWindowBatchResult(batch);

    if (m_debug) {
      std::cout << "[DBG] processWithFragments [TID " << task.getTaskId() << "]:"
                << " pipeline " << m_id
                << " exiting generated function with "
                << batch->getBatchStartTimestamp() << " startTimestamp "
                << batch->getBatchEndTimestamp() << " endTimestamp "
                << streamStartPointer << " streamStartPointer "
                << pointersAndCounts[4] << " openingWindows "
                << pointersAndCounts[5] << " closingWindows "
                << pointersAndCounts[6] << " pendingWindows "
                << pointersAndCounts[7] << " completeWindows " << std::endl;

      //if (task.getTaskId() == 128) {
      //  exit(0);
      //}
    }
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
    if (!hasAggregation())
      throw std::runtime_error("error: aggregation operator hasn't been set up");

    if (!hasGroupBy() || !m_usePtrs) { // use the actual values stored sequentially in a buffer to aggregate
      if (pack) {
        windowsPos = aggregate(openingWindows->getBufferRaw(),
                               openingWindows->getStartPointers().data(),
                               (int) openingWindows->getPosition(),
                               closingOrPendingWindows->getBufferRaw(),
                               closingOrPendingWindows->getStartPointers().data(),
                               (int) closingOrPendingWindows->getPosition(),
                               0,
                               numOfWindows,
                               pack,
                               completeWindows->getBufferRaw(),
                               completeWindows->getPosition(),
                               tupleSize);
      } else {
        windowsPos = aggregate(openingWindows->getBufferRaw(),
                               openingWindows->getStartPointers().data(),
                               (int) openingWindows->getPosition(),
                               closingOrPendingWindows->getBufferRaw(),
                               closingOrPendingWindows->getStartPointers().data(),
                               (int) closingOrPendingWindows->getPosition(),
                               openingWindows->numberOfWindows() - numOfWindows,
                               numOfWindows,
                               pack,
                               completeWindows->getBufferRaw(),
                               completeWindows->getPosition(),
                               tupleSize);
      }
    } else { // use pointers to hashtables to aggregate
      if (pack) {
        windowsPos = aggregateWithPtrs
            //aggregate2
            (openingWindows->getBufferPtrs().data(),
             openingWindows->getStartPointers().data(),
             (int) openingWindows->getPosition(),
             closingOrPendingWindows->getBufferPtrs().data(),
             closingOrPendingWindows->getStartPointers().data(),
             (int) closingOrPendingWindows->getPosition(),
             0,
             numOfWindows,
             pack,
             completeWindows->getBufferRaw(),
             completeWindows->getPosition(),
             tupleSize);
      } else {
        windowsPos = aggregateWithPtrs
            //aggregate2
            (openingWindows->getBufferPtrs().data(),
             openingWindows->getStartPointers().data(),
             (int) openingWindows->getPosition(),
             closingOrPendingWindows->getBufferPtrs().data(),
             closingOrPendingWindows->getStartPointers().data(),
             (int) closingOrPendingWindows->getPosition(),
             openingWindows->numberOfWindows() - numOfWindows,
             numOfWindows,
             pack,
             completeWindows->getBufferRaw(),
             completeWindows->getPosition(),
             tupleSize);
      }
    }
  }

  void aggregateSinglePartial(std::shared_ptr<PartialWindowResults> completeWindows,
                              int completeWindow,
                              int completeWindowsStartPos,
                              std::shared_ptr<PartialWindowResults> partialWindows,
                              int window,
                              int &startPos,
                              int &endPos,
                              int &tupleSize,
                              bool pack) override {
    if (!hasAggregation() || !m_useParallelMerge || !hasGroupBy())
      throw std::runtime_error("error: aggregation operator hasn't been set up for parallel merge");

    if (!hasGroupBy() || !m_usePtrs) { // use the actual values stored sequentially in a buffer to aggregate
      throw std::runtime_error("error: this aggregation mode is not supported without pointers to hashtables");
    } else { // use pointers to hashtables to aggregate
      aggregateSingleHashTableWithPtrs
          (completeWindows->getBufferRaw(),
           completeWindow,
           completeWindowsStartPos,
           (partialWindows != nullptr) ? partialWindows->getBufferPtrs().data() : nullptr,
           window,
           startPos,
           endPos,
           tupleSize,
           pack);
    }
  }

  WindowDefinition &getWindowDefinition() override {
    if (!hasAggregation())
      throw std::runtime_error("error: aggregation operator hasn't been set up");
    return *m_windowDefinition;
  }

  std::vector<AggregationType> &getAggregationTypes() override {
    if (!hasAggregation())
      throw std::runtime_error("error: aggregation operator hasn't been set up");
    return *m_aggregationTypes;
  }

  std::vector<ColumnReference *> &getAggregationAttributes() override {
    if (!hasAggregation())
      throw std::runtime_error("error: aggregation operator hasn't been set up");
    return *m_aggregationAttributes;
  }

  std::vector<Expression *> &getGroupByAttributes() override {
    if (!hasAggregation())
      throw std::runtime_error("error: aggregation operator hasn't been set up");
    return *m_groupByAttributes;
  }

  int getKeyLength() override { return m_keyLength; }

  int getValueLength() override { return m_valueLength; }

  int numberOfValues() override {
    if (!hasAggregation())
      throw std::runtime_error("error: aggregation operator hasn't been set up");
    return (int) (*m_aggregationAttributes).size();
  }

  bool hasProjection() const {
    return (m_expressions != nullptr);
  }

  bool hasSelection() const override {
    return (m_predicate != nullptr);
  }

  bool hasHavingPredicate() const {
    return (m_havingPredicate != nullptr) || (!m_postWindowPredicate.empty());
  }

  bool hasPostWindowOperation() const {
    return !m_postWindowOp.empty();
  }

  bool hasAggregation() const {
    if (m_aggregationTypes != nullptr && m_aggregationAttributes != nullptr
        && m_aggregationTypes->size() != m_aggregationAttributes->size())
      throw std::runtime_error("error: the number  of aggregation types should ne equal to the aggregation attributes");
    return (m_aggregationTypes != nullptr || m_aggregationAttributes != nullptr);
  }

  bool hasStaticHashJoin() const override {
    return (m_staticBuffer != nullptr && m_staticJoinPredicate != nullptr);
  }

  std::string getSelectionExpr() override {
    std::string s;
    if (hasSelection()) {
      s.append("if ( ");
      s.append(m_predicate->toSExprForCodeGen());
      s.append(" )\n");
    }
    return s;
  }

  std::string getHashTableExpr() override {
    std::string s;
    if (!hasGroupBy()
        //|| ((*m_groupByAttributes)[0]->getBasicType() != BasicType::LongLong &&
        //m_numberOfKeyAttributes == 1)
        ) {
      return s;
    }
    // create Key
    if (m_numberOfKeyAttributes == 1) {
      s.append("using Key = ");
      for (int idx = 1; idx <= m_numberOfKeyAttributes; ++idx) {
        auto e = (*m_groupByAttributes)[idx - 1];
        if (e->getBasicType() == BasicType::Integer) {
          s.append("int");
        } else if (e->getBasicType() == BasicType::Float) { ;
          s.append("float");
        } else if (e->getBasicType() == BasicType::Long) {
          s.append("long");
        } else if (e->getBasicType() == BasicType::LongLong) {
          s.append("__uint128_t");
        } else
          throw std::invalid_argument("error: invalid group-by attribute");
      }
      s.append(";\n");
    } else {
      s.append("struct Key {\n");
      for (int idx = 1; idx <= m_numberOfKeyAttributes; ++idx) {
        s.append("\t");
        auto e = (*m_groupByAttributes)[idx - 1];
        if (e->getBasicType() == BasicType::Integer) {
          s.append("int");
        } else if (e->getBasicType() == BasicType::Float) { ;
          s.append("float");
        } else if (e->getBasicType() == BasicType::Long) {
          s.append("long");
        } else if (e->getBasicType() == BasicType::LongLong) {
          s.append("__uint128_t");
        } else
          throw std::invalid_argument("error: invalid group-by attribute");
        s.append(" _" + std::to_string(idx - 1) + ";\n");
      }
      s.append("};\n");
    }
    s.append("using KeyT = Key;\n\n");
    // get hash and equality functions
    if (m_customHashTable.empty()) {
      if (m_numberOfKeyAttributes == 1 &&
          (*m_groupByAttributes)[0]->getBasicType() != BasicType::LongLong) {
        s.append(
            "struct HMEqualTo {\n"
            "    constexpr bool operator()(const KeyT& lhs, const KeyT& rhs) "
            "const {\n"
            "        return lhs == rhs;\n"
            "    }\n"
            "};\n"
            "\n"
            "struct MyHash{\n"
            "    std::size_t operator()(KeyT m) const {\n"
            "        std::hash<KeyT> hashVal;\n"
            "        return hashVal(m);\n"
            "    }\n"
            "};\n"
            "\n");
      } else {
        if (m_numberOfKeyAttributes == 1 &&
            (*m_groupByAttributes)[0]->getBasicType() == BasicType::LongLong) {
          s.append(
              "struct HMEqualTo {\n"
              "    constexpr bool operator()(const KeyT& lhs, const KeyT& rhs) "
              "const {\n"
              "        return lhs == rhs;\n"
              "    }\n"
              "};\n"
              "struct UInt128Hash {\n"
              "    UInt128Hash() = default;\n"
              "    inline std::size_t operator()(__uint128_t data) const {\n"
              "        const __uint128_t __mask = "
              "static_cast<std::size_t>(-1);\n"
              "        const std::size_t __a = (std::size_t)(data & __mask);\n"
              "        const std::size_t __b = (std::size_t)((data & (__mask "
              "<< 64)) >> 64);\n"
              "        auto hasher = std::hash<size_t>();\n"
              "        return hasher(__a) + hasher(__b);\n"
              "    }\n"
              "};\n"
              "using MyHash = UInt128Hash;\n"
              "\n");
        } else {
          s.append(
              "struct HMEqualTo {\n"
              "    constexpr bool operator()(const KeyT& lhs, const KeyT& rhs) "
              "const {\n"
              "        return");
          for (auto i = 0; i < m_numberOfKeyAttributes; ++i) {
            s.append(" lhs._" + std::to_string(i) + " == rhs._" +
                     std::to_string(i));
            if (i != m_numberOfKeyAttributes - 1) s.append(" &&");
          }
          s.append(
              ";\n"
              "    }\n"
              "};\n");
          s.append(
              "\n"
              ""
              "#include <iterator>\n"
              "#include <x86intrin.h>\n"
              "\n"
              "#define CRCPOLY 0x82f63b78 // reversed 0x1EDC6F41\n"
              "#define CRCINIT 0xFFFFFFFF\n"
              "uint32_t Crc32Lookup [256] = {\n"
              "        0x00000000,0x77073096,0xEE0E612C,0x990951BA,0x076DC419,0x706AF48F,0xE963A535,0x9E6495A3,\n"
              "        0x0EDB8832,0x79DCB8A4,0xE0D5E91E,0x97D2D988,0x09B64C2B,0x7EB17CBD,0xE7B82D07,0x90BF1D91,\n"
              "        0x1DB71064,0x6AB020F2,0xF3B97148,0x84BE41DE,0x1ADAD47D,0x6DDDE4EB,0xF4D4B551,0x83D385C7,\n"
              "        0x136C9856,0x646BA8C0,0xFD62F97A,0x8A65C9EC,0x14015C4F,0x63066CD9,0xFA0F3D63,0x8D080DF5,\n"
              "        0x3B6E20C8,0x4C69105E,0xD56041E4,0xA2677172,0x3C03E4D1,0x4B04D447,0xD20D85FD,0xA50AB56B,\n"
              "        0x35B5A8FA,0x42B2986C,0xDBBBC9D6,0xACBCF940,0x32D86CE3,0x45DF5C75,0xDCD60DCF,0xABD13D59,\n"
              "        0x26D930AC,0x51DE003A,0xC8D75180,0xBFD06116,0x21B4F4B5,0x56B3C423,0xCFBA9599,0xB8BDA50F,\n"
              "        0x2802B89E,0x5F058808,0xC60CD9B2,0xB10BE924,0x2F6F7C87,0x58684C11,0xC1611DAB,0xB6662D3D,\n"
              "        0x76DC4190,0x01DB7106,0x98D220BC,0xEFD5102A,0x71B18589,0x06B6B51F,0x9FBFE4A5,0xE8B8D433,\n"
              "        0x7807C9A2,0x0F00F934,0x9609A88E,0xE10E9818,0x7F6A0DBB,0x086D3D2D,0x91646C97,0xE6635C01,\n"
              "        0x6B6B51F4,0x1C6C6162,0x856530D8,0xF262004E,0x6C0695ED,0x1B01A57B,0x8208F4C1,0xF50FC457,\n"
              "        0x65B0D9C6,0x12B7E950,0x8BBEB8EA,0xFCB9887C,0x62DD1DDF,0x15DA2D49,0x8CD37CF3,0xFBD44C65,\n"
              "        0x4DB26158,0x3AB551CE,0xA3BC0074,0xD4BB30E2,0x4ADFA541,0x3DD895D7,0xA4D1C46D,0xD3D6F4FB,\n"
              "        0x4369E96A,0x346ED9FC,0xAD678846,0xDA60B8D0,0x44042D73,0x33031DE5,0xAA0A4C5F,0xDD0D7CC9,\n"
              "        0x5005713C,0x270241AA,0xBE0B1010,0xC90C2086,0x5768B525,0x206F85B3,0xB966D409,0xCE61E49F,\n"
              "        0x5EDEF90E,0x29D9C998,0xB0D09822,0xC7D7A8B4,0x59B33D17,0x2EB40D81,0xB7BD5C3B,0xC0BA6CAD,\n"
              "        0xEDB88320,0x9ABFB3B6,0x03B6E20C,0x74B1D29A,0xEAD54739,0x9DD277AF,0x04DB2615,0x73DC1683,\n"
              "        0xE3630B12,0x94643B84,0x0D6D6A3E,0x7A6A5AA8,0xE40ECF0B,0x9309FF9D,0x0A00AE27,0x7D079EB1,\n"
              "        0xF00F9344,0x8708A3D2,0x1E01F268,0x6906C2FE,0xF762575D,0x806567CB,0x196C3671,0x6E6B06E7,\n"
              "        0xFED41B76,0x89D32BE0,0x10DA7A5A,0x67DD4ACC,0xF9B9DF6F,0x8EBEEFF9,0x17B7BE43,0x60B08ED5,\n"
              "        0xD6D6A3E8,0xA1D1937E,0x38D8C2C4,0x4FDFF252,0xD1BB67F1,0xA6BC5767,0x3FB506DD,0x48B2364B,\n"
              "        0xD80D2BDA,0xAF0A1B4C,0x36034AF6,0x41047A60,0xDF60EFC3,0xA867DF55,0x316E8EEF,0x4669BE79,\n"
              "        0xCB61B38C,0xBC66831A,0x256FD2A0,0x5268E236,0xCC0C7795,0xBB0B4703,0x220216B9,0x5505262F,\n"
              "        0xC5BA3BBE,0xB2BD0B28,0x2BB45A92,0x5CB36A04,0xC2D7FFA7,0xB5D0CF31,0x2CD99E8B,0x5BDEAE1D,\n"
              "        0x9B64C2B0,0xEC63F226,0x756AA39C,0x026D930A,0x9C0906A9,0xEB0E363F,0x72076785,0x05005713,\n"
              "        0x95BF4A82,0xE2B87A14,0x7BB12BAE,0x0CB61B38,0x92D28E9B,0xE5D5BE0D,0x7CDCEFB7,0x0BDBDF21,\n"
              "        0x86D3D2D4,0xF1D4E242,0x68DDB3F8,0x1FDA836E,0x81BE16CD,0xF6B9265B,0x6FB077E1,0x18B74777,\n"
              "        0x88085AE6,0xFF0F6A70,0x66063BCA,0x11010B5C,0x8F659EFF,0xF862AE69,0x616BFFD3,0x166CCF45,\n"
              "        0xA00AE278,0xD70DD2EE,0x4E048354,0x3903B3C2,0xA7672661,0xD06016F7,0x4969474D,0x3E6E77DB,\n"
              "        0xAED16A4A,0xD9D65ADC,0x40DF0B66,0x37D83BF0,0xA9BCAE53,0xDEBB9EC5,0x47B2CF7F,0x30B5FFE9,\n"
              "        0xBDBDF21C,0xCABAC28A,0x53B39330,0x24B4A3A6,0xBAD03605,0xCDD70693,0x54DE5729,0x23D967BF,\n"
              "        0xB3667A2E,0xC4614AB8,0x5D681B02,0x2A6F2B94,0xB40BBE37,0xC30C8EA1,0x5A05DF1B,0x2D02EF8D\n"
              "};\n"
              "// Hardware-accelerated CRC-32C (using CRC32 instruction)\n"
              "inline size_t CRC_Hardware(const void* data, size_t length) {\n"
              "    size_t crc = CRCINIT;\n"
              "\n"
              "    unsigned char* current = (unsigned char*) data;\n"
              "    // Align to DWORD boundary\n"
              "    size_t align = (sizeof(unsigned int) - (__int64_t)current) "
              "& (sizeof(unsigned int) - 1);\n"
              "    align = std::min(align, length);\n"
              "    length -= align;\n"
              "    for (; align; align--)\n"
              "        crc = Crc32Lookup[(crc ^ *current++) & 0xFF] ^ (crc >> "
              "8);\n"
              "\n"
              "    size_t ndwords = length / sizeof(unsigned int);\n"
              "    for (; ndwords; ndwords--) {\n"
              "        crc = _mm_crc32_u32(crc, *(unsigned int*)current);\n"
              "        current += sizeof(unsigned int);\n"
              "    }\n"
              "\n"
              "    length &= sizeof(unsigned int) - 1;\n"
              "    for (; length; length--)\n"
              "        crc = _mm_crc32_u8(crc, *current++);\n"
              "    return ~crc;\n"
              "}\n"
              "struct Crc32Hash {\n"
              "    std::size_t operator()(KeyT t) const {\n"
              "        return CRC_Hardware(&t, KEY_SIZE);\n"
              "    }\n"
              "};\n"
              "using MyHash = Crc32Hash;\n"
              "\n");
        }
      }
      //if (m_collisionBarrier > 0) {
      //  //s.append("    int       _barrier     = " + std::to_string(m_collisionBarrier) + ";\n");
      //  throw std::runtime_error("error: the collisionBarrier is not supported yet");
      //}
    } else {
      //throw std::runtime_error("error: custom hashtables are not supported yet");
    }
    s.append("\n");

    return s;
  }

 private:
  std::string tab = "\t";
  std::string newline = "\n";
  std::string getCombineFunction(AggregationType type, std::string leftArg, std::string rightArg) {
    if (type == CNT || type == SUM)
      return leftArg + "+" + rightArg;
    else if (type == MIN)
      return leftArg + "<" + rightArg + "?" + leftArg + ":" + rightArg;
    else if (type == MAX)
      return leftArg + ">" + rightArg + "?" + leftArg + ":" + rightArg;
    else if (type == AVG)
      return "std::make_pair(" + leftArg + ".first+" + rightArg + ".first," + leftArg + ".second+" + rightArg
          + ".second)";
    else {
      throw std::runtime_error("error: unsupported type");
    }
  }
  std::string addTabs(std::string &input, int numOfTabs) {
    std::string s;
    std::string tabs;
    for (int i = 0; i < numOfTabs; ++i) {
      tabs.append("\t");
    }
    std::istringstream f(input);
    std::string line;
    while (std::getline(f, line)) {
      s.append(tabs).append(line).append("\n");
    }
    return s;
  }
  std::string addPostWindowOperation(int numOfTabs) {
    std::string s;
    std::string tabs;
    for (int i = 0; i < numOfTabs; ++i) {
      tabs.append("\t");
    }
    if (hasPostWindowOperation()) {
      s.append(tabs).append(m_postWindowOp);
    }
    return s;
  }
  std::string addPostMergeOp() {
      std::string s;
      if (hasPostWindowOperation()) {
        s.append(m_postMergeOperation);
      }
      return s;
  }

  std::string getC_Definitions(std::string computation) {
    std::string s;
    s.append(
        "extern \"C\" {\n"
        "void process (int pid, char *inputBuffer, long startPointer, long endPointer,\n"
        "                              char *outputBuffer, long &pos) {\n"
        "    // Input Buffer\n"
        "    input_tuple_t *data= (input_tuple_t *) inputBuffer;\n"
        "\n"
        "    // Output Buffers\n"
        "    output_tuple_t *output = (output_tuple_t *) outputBuffer;\n"
        "\n"
        "    int tupleSize = sizeof(input_tuple_t);\n"
        "    long bufferPtr = startPointer / tupleSize;\n"
        "    startPointer = startPointer / tupleSize;\n"
        "    endPointer = endPointer / tupleSize;\n"
        "\n"
        "    for (;bufferPtr < endPointer; ++bufferPtr) {\n" +
            addTabs(computation, 2) +
            "    }\n"
            "    pos = pos * sizeof(output_tuple_t);\n"
            "};\n"
            "}\n"
    );
    return s;
  }

  std::string getAggregate_C_Definitions() {
    std::string s;
    std::string ptr;
    std::string getSize;
    if (hasGroupBy() && m_usePtrs) {
      ptr.append("*");
      getSize.append(getHashTableSizeInBytesString());
    }
    s.append(
        "extern \"C\" {\n"
        "    void process (int pid, char *inputBuffer, size_t inputBufferSize, long startPointer, long endPointer, long timestampFromPrevBatch,\n"
        "                              long *windowStartPointers, long *windowEndPointers, char *" + ptr
            + "openingWindowsBuffer, char *" + ptr + "closingWindowsBuffer,\n"
                                                     "                              char *" + ptr
            + "pendingWindowsBuffer, char *completeWindowsBuffer,\n"
              "                              int *openingStartPointers, int *closingStartPointers, int *pendingStartPointers, int *completeStartPointers,\n"
              //"                              long *openingWindowIds, long *closingWindowIds, long *pendingWindowIds, long *completeWindowIds,\n"
              "                              long streamStartPointer, int *pointersAndCounts, char *staticBuffer) {\n"
              "            processData (pid, inputBuffer, inputBufferSize, startPointer, endPointer, timestampFromPrevBatch,\n"
              "            windowStartPointers, windowEndPointers, openingWindowsBuffer, closingWindowsBuffer,\n"
              "            pendingWindowsBuffer, completeWindowsBuffer,\n"
              "            openingStartPointers, closingStartPointers, pendingStartPointers, completeStartPointers,\n"
              //"            openingWindowIds, closingWindowIds, pendingWindowIds, completeWindowIds,\n"
              "            streamStartPointer, pointersAndCounts, staticBuffer);\n"
              "    };\n"
              "\n");
    if (!m_useParallelMerge) {
      s.append(
          "    long aggregate (char *" + ptr + "openingBuffer, int *openingStartPointers, int openingEndPointer,\n"
                                               "                            char *" + ptr
              + "closingOrPendingBuffer, int *copStartPointers, int copEndPointer,\n"
                "                            int startingWindow, int numOfWindows, bool pack,\n"
                "                            char* completeBuffer, long completeBufferPtr, int &tupleSize) {\n"
                "        return aggregatePartials (openingBuffer, openingStartPointers, openingEndPointer,\n"
                "                closingOrPendingBuffer, copStartPointers, copEndPointer,\n"
                "                startingWindow, numOfWindows, pack, completeBuffer, completeBufferPtr, tupleSize);\n"
                "    };\n" +
              getSize +
              "}\n"
      );
    } else {
      s.append(
          "    long aggregate (char *completeBuffer, int completeWindow, int completeStartPos,\n"
          "                            char **partialBuffer, int partialWindow,\n"
          "                            int &startPos, int &endPos,\n"
          "                            int &tupleSize, bool pack) {\n"
          "        return aggregatePartials (completeBuffer, completeWindow, completeStartPos, partialBuffer,\n"
          "                partialWindow, startPos, endPos,\n"
          "                tupleSize, pack);\n"
          "    };\n" +
              getSize +
              "}\n"
      );
    }
    return s;
  }

  std::string getAggregationString() const {
    std::string s;
    if (hasAggregation()) {
      s.append("[Partial window u-aggregation] ");
      for (int i = 0; i < (int) (*m_aggregationTypes).size(); ++i)
        s.append(AggregationTypes::toString((*m_aggregationTypes)[i])).append("(").append((*m_aggregationAttributes)[i]->toSExpr()).append(
            ")").append(" ");
      s.append("(group-by ?").append(" ").append(std::to_string(m_groupBy)).append(") ");
      s.append("(incremental ?").append(" ").append(std::to_string(m_processIncremental)).append(")");
    }
    s.append("\n");
    return s;
  }

  std::string getSelectionString() const {
    std::string s;
    if (hasSelection()) {
      s.append("Selection (");
      s.append(m_predicate->toSExpr());
      s.append(")");
    }
    s.append("\n");
    return s;
  }

  std::string getProjectionString() const {
    std::string s;
    if (hasProjection()) {
      s.append("Projection (");
      int i = 0;
      for (auto e: *m_expressions) {
        s.append(e->toSExpr());
        if (i != (int) (*m_expressions).size() - 1)
          s.append(", ");
        i++;
      }
      s.append(")");
    }
    s.append("\n");
    return s;
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

  std::string getInputSchemaString() override {
    if (m_inputSchema == nullptr)
      throw std::runtime_error("error: m_inputSchema hasn't been set up");
    std::string s;
    s.append("struct alignas(16) input_tuple_t {\n");
    /* The first attribute is always a timestamp */
    s.append("\tlong timestamp;\n");
    for (int i = 1; i < m_inputSchema->numberOfAttributes(); i++) {
      auto type = m_inputSchema->getAttributeType(i);
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
    if (m_outputSchema == nullptr)
      throw std::runtime_error("error: outputSchema hasn't been set up");
    std::string s;
    s.append("struct alignas(16) output_tuple_t {\n");
    int i = 0;
    if (m_outputSchema->hasTime()) {
      s.append("\tlong timestamp;\n");
      i++;
    }
    for (; i < m_outputSchema->numberOfAttributes(); i++) {
      auto type = m_outputSchema->getAttributeType(i);
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

  std::string getIntermediateSchemaString() {
    if (!hasProjection())
      throw std::runtime_error("error: projection hasn't been set up");
    auto schema = ExpressionUtils::getTupleSchemaFromExpressions(*m_expressions, "IntermediateStream");
    std::string s;
    s.append("struct alignas(16) interm_tuple_t {\n");
    int i = 0;
    if (schema.hasTime()) {
      s.append("\tlong timestamp;\n");
      i++;
    }
    for (; i < schema.numberOfAttributes(); i++) {
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

  std::string getQueryDefinitionString() {
    std::string s;
    if (m_windowDefinition->isRangeBased())
      s.append("#define RANGE_BASED\n");
    else
      s.append("#define COUNT_BASED\n");
    s.append("#define WINDOW_SIZE      " + std::to_string(m_windowDefinition->getSize()) + "L\n");
    s.append("#define WINDOW_SLIDE     " + std::to_string(m_windowDefinition->getSlide()) + "L\n");
    s.append("#define PANES_PER_WINDOW " + std::to_string(m_windowDefinition->numberOfPanes()) + "L\n");
    s.append("#define PANES_PER_SLIDE  " + std::to_string(m_windowDefinition->panesPerSlide()) + "L\n");
    s.append("#define PANE_SIZE        " + std::to_string(m_windowDefinition->getPaneSize()) + "L\n");
    //s.append("#define PARTIAL_WINDOWS  "+std::to_string(SystemConf::getInstance().PARTIAL_WINDOWS)+"L\n");
    s.append("#define BUFFER_SIZE      " + std::to_string(m_config ? m_config->getCircularBufferSize() :
                                                                     SystemConf::getInstance().CIRCULAR_BUFFER_SIZE) + "L\n");
    s.append("#define UNBOUNDED_SIZE   " + std::to_string(SystemConf::getInstance().UNBOUNDED_BUFFER_SIZE) + "L\n");
    if (!hasGroupBy()) {
      s.append("#define MAP_SIZE         " + std::to_string(1) + "L\n");
    } else {
      s.append("#define MAP_SIZE         " + std::to_string(m_config ? m_config->getHashtableSize() :
                                                 SystemConf::getInstance().HASH_TABLE_SIZE) + "L\n");
      s.append("#define KEY_SIZE         " + std::to_string(m_keyLength) + "L\n");
    }
    s.append("\n");
    return s;
  }

  std::string getSingleKeyDataStructureString() {
    std::string s;
    // TODO: test that they produce equivalent results
    AbstractTreeRepresentation
        tree(*m_windowDefinition, *m_aggregationTypes, hasNonInvertible());
    //GeneralAggregationGraph gag (m_windowDefinition, m_aggregationTypes);
    s.append(tree.generateAggregationTreeNode());
    //s.append(gag.generateAggregationTreeNode());
    if (hasIncremental()) {
      s.append(tree.generateCode());
      //s.append(gag.generateCode(true));
    }
    return s;
  }

  std::string getHashTableBucketString() {
    if (m_outputSchema == nullptr)
      throw std::runtime_error("error: outputSchema hasn't been set up");
    std::string s;
    s.append(
        "struct Value {\n");
    for (unsigned long i = 0; i < (*m_aggregationTypes).size(); ++i) {
      switch ((*m_aggregationTypes)[i]) {
        case SUM:s.append("\tfloat _" + std::to_string((i + 1)) + ";\n");
          break;
        case AVG:s.append("\tfloat _" + std::to_string((i + 1)) + ";\n");
          //s.append("\t\t_c"+std::to_string((i+1))+" = 0.0f;\n");
          break;
        case CNT:s.append("\tfloat _" + std::to_string((i + 1)) + ";\n");
          break;
        case MIN:s.append("\tfloat _" + std::to_string((i + 1)) + ";\n");
          break;
        case MAX:s.append("\tfloat _" + std::to_string((i + 1)) + ";\n");
          break;
        default:throw std::runtime_error("error: invalid aggregation type");
      }
    }
    s.append("\tValue () {\n");
    for (unsigned long i = 0; i < (*m_aggregationTypes).size(); ++i) {
      switch ((*m_aggregationTypes)[i]) {
        case SUM:s.append("\t\t_" + std::to_string((i + 1)) + " = 0.0f;\n");
          break;
        case AVG:s.append("\t\t_" + std::to_string((i + 1)) + " = 0.0f;\n");
          //s.append("\t\t_c"+std::to_string((i+1))+" = 0.0f;\n");
          break;
        case CNT:s.append("\t\t_" + std::to_string((i + 1)) + " = 0.0f;\n");
          break;
        case MIN:s.append("\t\t_" + std::to_string((i + 1)) + " = FLT_MAX;\n");
          break;
        case MAX:s.append("\t\t_" + std::to_string((i + 1)) + " = FLT_MIN;\n");
          break;
        default:throw std::runtime_error("error: invalid aggregation type");
      }
    }
    s.append("\t}\n");
    s.append("};\n");
    s.append("\n");
    return s;
  }

  std::string getHashTableSizeInBytesString() {
    std::string s;
    s.append(
        "    long getHashTableSizeInBytes () {\n"
        "        return sizeof(Bucket) * MAP_SIZE;\n"
        "    }\n"
    );
    return s;
  }

  std::string getHashTableString(std::string key) {
    std::string s;
    if (m_customHashTable.empty()) {
      if (m_numberOfKeyAttributes == 1 && (*m_groupByAttributes)[0]->getBasicType() != BasicType::LongLong)
        s.append(
            "using KeyT = " + key + ";\n"
                                    "using ValueT = Value;\n"
                                    "\n"
                                    "struct HashMapEqualTo {\n"
                                    "    constexpr bool operator()(const KeyT& lhs, const KeyT& rhs) const {\n"
                                    "        return lhs == rhs;\n"
                                    "    }\n"
                                    "};\n"
                                    "\n"
                                    "struct MyHash{\n"
                                    "    std::size_t operator()(KeyT m) const {\n"
                                    "        std::hash<KeyT> hashVal;\n"
                                    "        return hashVal(m);\n"
                                    "    }\n"
                                    "};\n"
                                    "\n");
      else {
        if (m_numberOfKeyAttributes == 1 && (*m_groupByAttributes)[0]->getBasicType() == BasicType::LongLong) {
          s.append("using KeyT = " + key + ";\n"
                                           "using ValueT = Value;\n"
                                           "\n"
                                           "struct HashMapEqualTo {\n"
                                           "    constexpr bool operator()(const KeyT& lhs, const KeyT& rhs) const {\n"
                                           "        return lhs == rhs;\n"
                                           "    }\n"
                                           "};\n"
                                           "struct UInt128Hash {\n"
                                           "    UInt128Hash() = default;\n"
                                           "    inline std::size_t operator()(__uint128_t data) const {\n"
                                           "        const __uint128_t __mask = static_cast<std::size_t>(-1);\n"
                                           "        const std::size_t __a = (std::size_t)(data & __mask);\n"
                                           "        const std::size_t __b = (std::size_t)((data & (__mask << 64)) >> 64);\n"
                                           "        auto hasher = std::hash<size_t>();\n"
                                           "        return hasher(__a) + hasher(__b);\n"
                                           "    }\n"
                                           "};\n"
                                           "using MyHash = UInt128Hash;\n"
                                           "\n");
        } else {
          s.append(key + ";\n"
                         "using KeyT = Key;\n"
                         "using ValueT = Value;\n"
                         "\n"
                         "struct HashMapEqualTo {\n"
                         "    constexpr bool operator()(const KeyT& lhs, const KeyT& rhs) const {\n"
                         "        return"
          );

          for (auto i = 0; i < m_numberOfKeyAttributes; ++i) {
            s.append(" lhs._" + std::to_string(i) + " == rhs._" + std::to_string(i));
            if (i != m_numberOfKeyAttributes - 1)
              s.append(" &&");
          }
          s.append(
              ";\n"
              "    }\n"
              "};\n");
          s.append(
              "\n"
              ""
              "#include <iterator>\n"
              "#include <x86intrin.h>\n"
              "\n"
              "#define CRCPOLY 0x82f63b78 // reversed 0x1EDC6F41\n"
              "#define CRCINIT 0xFFFFFFFF\n"
              "uint32_t Crc32Lookup [256] = {\n"
              "        0x00000000,0x77073096,0xEE0E612C,0x990951BA,0x076DC419,0x706AF48F,0xE963A535,0x9E6495A3,\n"
              "        0x0EDB8832,0x79DCB8A4,0xE0D5E91E,0x97D2D988,0x09B64C2B,0x7EB17CBD,0xE7B82D07,0x90BF1D91,\n"
              "        0x1DB71064,0x6AB020F2,0xF3B97148,0x84BE41DE,0x1ADAD47D,0x6DDDE4EB,0xF4D4B551,0x83D385C7,\n"
              "        0x136C9856,0x646BA8C0,0xFD62F97A,0x8A65C9EC,0x14015C4F,0x63066CD9,0xFA0F3D63,0x8D080DF5,\n"
              "        0x3B6E20C8,0x4C69105E,0xD56041E4,0xA2677172,0x3C03E4D1,0x4B04D447,0xD20D85FD,0xA50AB56B,\n"
              "        0x35B5A8FA,0x42B2986C,0xDBBBC9D6,0xACBCF940,0x32D86CE3,0x45DF5C75,0xDCD60DCF,0xABD13D59,\n"
              "        0x26D930AC,0x51DE003A,0xC8D75180,0xBFD06116,0x21B4F4B5,0x56B3C423,0xCFBA9599,0xB8BDA50F,\n"
              "        0x2802B89E,0x5F058808,0xC60CD9B2,0xB10BE924,0x2F6F7C87,0x58684C11,0xC1611DAB,0xB6662D3D,\n"
              "        0x76DC4190,0x01DB7106,0x98D220BC,0xEFD5102A,0x71B18589,0x06B6B51F,0x9FBFE4A5,0xE8B8D433,\n"
              "        0x7807C9A2,0x0F00F934,0x9609A88E,0xE10E9818,0x7F6A0DBB,0x086D3D2D,0x91646C97,0xE6635C01,\n"
              "        0x6B6B51F4,0x1C6C6162,0x856530D8,0xF262004E,0x6C0695ED,0x1B01A57B,0x8208F4C1,0xF50FC457,\n"
              "        0x65B0D9C6,0x12B7E950,0x8BBEB8EA,0xFCB9887C,0x62DD1DDF,0x15DA2D49,0x8CD37CF3,0xFBD44C65,\n"
              "        0x4DB26158,0x3AB551CE,0xA3BC0074,0xD4BB30E2,0x4ADFA541,0x3DD895D7,0xA4D1C46D,0xD3D6F4FB,\n"
              "        0x4369E96A,0x346ED9FC,0xAD678846,0xDA60B8D0,0x44042D73,0x33031DE5,0xAA0A4C5F,0xDD0D7CC9,\n"
              "        0x5005713C,0x270241AA,0xBE0B1010,0xC90C2086,0x5768B525,0x206F85B3,0xB966D409,0xCE61E49F,\n"
              "        0x5EDEF90E,0x29D9C998,0xB0D09822,0xC7D7A8B4,0x59B33D17,0x2EB40D81,0xB7BD5C3B,0xC0BA6CAD,\n"
              "        0xEDB88320,0x9ABFB3B6,0x03B6E20C,0x74B1D29A,0xEAD54739,0x9DD277AF,0x04DB2615,0x73DC1683,\n"
              "        0xE3630B12,0x94643B84,0x0D6D6A3E,0x7A6A5AA8,0xE40ECF0B,0x9309FF9D,0x0A00AE27,0x7D079EB1,\n"
              "        0xF00F9344,0x8708A3D2,0x1E01F268,0x6906C2FE,0xF762575D,0x806567CB,0x196C3671,0x6E6B06E7,\n"
              "        0xFED41B76,0x89D32BE0,0x10DA7A5A,0x67DD4ACC,0xF9B9DF6F,0x8EBEEFF9,0x17B7BE43,0x60B08ED5,\n"
              "        0xD6D6A3E8,0xA1D1937E,0x38D8C2C4,0x4FDFF252,0xD1BB67F1,0xA6BC5767,0x3FB506DD,0x48B2364B,\n"
              "        0xD80D2BDA,0xAF0A1B4C,0x36034AF6,0x41047A60,0xDF60EFC3,0xA867DF55,0x316E8EEF,0x4669BE79,\n"
              "        0xCB61B38C,0xBC66831A,0x256FD2A0,0x5268E236,0xCC0C7795,0xBB0B4703,0x220216B9,0x5505262F,\n"
              "        0xC5BA3BBE,0xB2BD0B28,0x2BB45A92,0x5CB36A04,0xC2D7FFA7,0xB5D0CF31,0x2CD99E8B,0x5BDEAE1D,\n"
              "        0x9B64C2B0,0xEC63F226,0x756AA39C,0x026D930A,0x9C0906A9,0xEB0E363F,0x72076785,0x05005713,\n"
              "        0x95BF4A82,0xE2B87A14,0x7BB12BAE,0x0CB61B38,0x92D28E9B,0xE5D5BE0D,0x7CDCEFB7,0x0BDBDF21,\n"
              "        0x86D3D2D4,0xF1D4E242,0x68DDB3F8,0x1FDA836E,0x81BE16CD,0xF6B9265B,0x6FB077E1,0x18B74777,\n"
              "        0x88085AE6,0xFF0F6A70,0x66063BCA,0x11010B5C,0x8F659EFF,0xF862AE69,0x616BFFD3,0x166CCF45,\n"
              "        0xA00AE278,0xD70DD2EE,0x4E048354,0x3903B3C2,0xA7672661,0xD06016F7,0x4969474D,0x3E6E77DB,\n"
              "        0xAED16A4A,0xD9D65ADC,0x40DF0B66,0x37D83BF0,0xA9BCAE53,0xDEBB9EC5,0x47B2CF7F,0x30B5FFE9,\n"
              "        0xBDBDF21C,0xCABAC28A,0x53B39330,0x24B4A3A6,0xBAD03605,0xCDD70693,0x54DE5729,0x23D967BF,\n"
              "        0xB3667A2E,0xC4614AB8,0x5D681B02,0x2A6F2B94,0xB40BBE37,0xC30C8EA1,0x5A05DF1B,0x2D02EF8D\n"
              "};\n"
              "// Hardware-accelerated CRC-32C (using CRC32 instruction)\n"
              "inline size_t CRC_Hardware(const void* data, size_t length) {\n"
              "    size_t crc = CRCINIT;\n"
              "\n"
              "    unsigned char* current = (unsigned char*) data;\n"
              "    // Align to DWORD boundary\n"
              "    size_t align = (sizeof(unsigned int) - (__int64_t)current) & (sizeof(unsigned int) - 1);\n"
              "    align = std::min(align, length);\n"
              "    length -= align;\n"
              "    for (; align; align--)\n"
              "        crc = Crc32Lookup[(crc ^ *current++) & 0xFF] ^ (crc >> 8);\n"
              "\n"
              "    size_t ndwords = length / sizeof(unsigned int);\n"
              "    for (; ndwords; ndwords--) {\n"
              "        crc = _mm_crc32_u32(crc, *(unsigned int*)current);\n"
              "        current += sizeof(unsigned int);\n"
              "    }\n"
              "\n"
              "    length &= sizeof(unsigned int) - 1;\n"
              "    for (; length; length--)\n"
              "        crc = _mm_crc32_u8(crc, *current++);\n"
              "    return ~crc;\n"
              "}\n"
              "struct Crc32Hash {\n"
              "    std::size_t operator()(KeyT t) const {\n"
              "        return CRC_Hardware(&t, KEY_SIZE);\n"
              "    }\n"
              "};\n"
              "using MyHash = Crc32Hash;\n"
              "\n"
          );
        }
      }
      s.append(
          "struct alignas(16) Bucket {\n"
          "    char state;\n"
          "    char dirty;\n"
          "    long timestamp;\n"
          "    KeyT key;\n"
          "    ValueT value;\n"
          "    int counter;\n"
          "};\n"
          "\n"
          "using BucketT = Bucket;\n"
          "\n"
          "class alignas(64) HashTable {\n"
          "private:\n"
          "    using HashT = MyHash; //std::hash<KeyT>;\n"
          "    using EqT = HashMapEqualTo;\n"
      );
      if (hasIncremental())
        s.append(
            "    using AggrT = Aggregator;\n"
        );
      s.append(
          "\n"
          "    HashT     _hasher;\n"
          "    EqT       _eq;\n"
          "    BucketT*  _buckets     = nullptr;\n"
      );
      if (hasIncremental())
        s.append(
            "    AggrT*    _aggrs       = nullptr;\n"
        );
      s.append(
          "    size_t    _num_buckets = MAP_SIZE;\n"
          "    size_t    _num_filled  = 0;\n"
          "    size_t    _mask        = MAP_SIZE-1;\n");
      if (m_collisionBarrier > 0)
        s.append("    int       _barrier     = " + std::to_string(m_collisionBarrier) + ";\n");
      s.append(
          "public:\n"
          "    HashTable ();\n"
          "    HashTable (Bucket*nodes);\n"
          "    void init ();\n"
          "    void reset ();\n"
          "    void clear ();\n"
          "    void insert (KeyT &key, ValueT &value, long timestamp);\n"
          "    void insert_or_modify (KeyT &key, ValueT &value, long timestamp);\n"
          "    bool evict (KeyT &key);\n"
          "    void insertSlices ();\n"
          "    void evictSlices ();\n"
          "    void setValues ();\n"
          "    void setIntermValues (int pos, long timestamp);\n"
          "    bool get_value (const KeyT &key, ValueT &result);\n"
          "    bool get_result (const KeyT &key, ValueT &result);\n"
          "    bool get_index (const KeyT &key, int &index);\n"
          "    void deleteHashTable();\n"
          "    BucketT* getBuckets ();\n"
          "    size_t getSize() const;\n"
          "    bool isEmpty() const;\n"
          "    size_t getNumberOfBuckets() const;\n"
          "    float load_factor() const;\n"
          "};\n"
          "\n"
          "HashTable::HashTable () {}\n"
          "\n"
          "HashTable::HashTable (Bucket *nodes) : _buckets(nodes) {\n"
          "    if (!(_num_buckets && !(_num_buckets & (_num_buckets - 1)))) {\n"
          "        throw std::runtime_error (\"error: the size of the hash table has to be a power of two\\n\");\n"
          "    }\n"
          "}\n"
          "\n"
          "void HashTable::init () {\n"
          "    if (!(_num_buckets && !(_num_buckets & (_num_buckets - 1)))) {\n"
          "        throw std::runtime_error (\"error: the size of the hash table has to be a power of two\\n\");\n"
          "    }\n"
          "\n"
          "    _buckets  = (BucketT*)_mm_malloc(_num_buckets * sizeof(BucketT), 64);\n"
      );
      if (hasIncremental())
        s.append(
            "    _aggrs  = (AggrT*)_mm_malloc(_num_buckets * sizeof(AggrT), 64);\n"
        );
      s.append(
          "    if (!_buckets /*|| !_aggrs*/) {\n"
          "        free(_buckets);\n"
          "        /*free(_aggrs);*/\n"
          "        throw std::bad_alloc();\n"
          "    }\n"
          "\n"
          "    for (auto i = 0; i < _num_buckets; ++i) {\n"
          "        _buckets[i].state = 0;\n"
          "        _buckets[i].dirty = 0;\n"
      );
      if (hasIncremental())
        s.append(
            "        _aggrs[i] = AggrT (); // maybe initiliaze this on insert\n"
            "        _aggrs[i].initialise();\n"
        );
      s.append(
          "    }\n"
          "}\n"
          "\n"
          "void HashTable::reset () {\n"
          "    for (auto i = 0; i < _num_buckets; ++i) {\n"
          "        _buckets[i].state = 0;\n"
          "        //_aggrs[i].initialise();\n"
          "    }\n"
          "    _num_filled = 0;\n"
          "}\n"
          "\n"
          "void HashTable::clear () {\n"
          "    for (auto i = 0; i < _num_buckets; ++i) {\n"
          "        _buckets[i].state = 0;\n"
          "        _buckets[i].dirty = 0;\n"
          "        //_buckets[i].counter = 0;\n"
      );
      if (hasIncremental())
        s.append(
            "        _aggrs[i].initialise();\n"
        );
      s.append(
          "    }\n"
          "    _num_filled = 0;\n"
          "}\n"
          "\n"
          "void HashTable::insert (KeyT &key, ValueT &value, long timestamp) {\n"
          "    size_t ind = _hasher(key) & _mask, i = ind;\n"
          "    for (; i < _num_buckets; i++) {\n"
          "        if (!_buckets[i].state || _eq(_buckets[i].key, key)) {\n"
          "            _buckets[i].state = 1;\n"
          "            _buckets[i].timestamp = timestamp;\n"
          "            _buckets[i].key = key; //std::memcpy(&_buckets[i].key, key, KEY_SIZE);\n"
          "            _buckets[i].value = value;\n"
          "            return;\n"
          "        }\n"
          "    }\n"
          "    for (i = 0; i < ind; i++) {\n"
          "        if (!_buckets[i].state || _eq(_buckets[i].key, key)) {\n"
          "            _buckets[i].state = 1;\n"
          "            _buckets[i].timestamp = timestamp;\n"
          "            _buckets[i].key = key;\n"
          "            _buckets[i].value = value;\n"
          "            return;\n"
          "        }\n"
          "    }\n"
          "    throw std::runtime_error (\"error: the hashtable is full \\n\");\n"
          "}\n"
          "\n"
          "void HashTable::insert_or_modify (KeyT &key, ValueT &value, long timestamp) {\n"
          "    size_t ind = _hasher(key) & _mask, i = ind;\n"
          "    char tempState;\n");
      if (m_collisionBarrier > 0)
        s.append("    int steps = 0;\n");
      s.append(
          "    for (; i < _num_buckets; i++) {\n"
          "        tempState = _buckets[i].state;\n"
          "        if (tempState && _eq(_buckets[i].key, key)) {\n");
      for (unsigned long i = 0; i < m_aggregationTypes->size(); ++i) {
        if ((*m_aggregationTypes)[i] == AVG) {
          s.append("\t\t\t_buckets[i].value._" + std::to_string((i + 1)) + " = " +
              getCombineFunction(SUM, "_buckets[i].value._" + std::to_string((i + 1)),
                                 "value._" + std::to_string((i + 1))) + ";\n");
        } else
          s.append("\t\t\t_buckets[i].value._" + std::to_string((i + 1)) + " = " +
              getCombineFunction((*m_aggregationTypes)[i], "_buckets[i].value._" + std::to_string((i + 1)),
                                 "value._" + std::to_string((i + 1))) + ";\n");
      }
      s.append(
          "            _buckets[i].counter++;\n"
          "            return;\n"
          "        }\n"
          "        if (!tempState && (_eq(_buckets[i].key, key) || _buckets[i].dirty == 0)) { // first insert -- keep track of previous inserted value\n"
          "            _buckets[i].state = 1;\n"
          "            _buckets[i].dirty = 1;\n"
          "            _buckets[i].timestamp = timestamp;\n"
          "            _buckets[i].key = key;\n"
          "            _buckets[i].value = value;\n"
          "            _buckets[i].counter = 1;\n"
          "            return;\n"
          "        }\n");
      if (m_collisionBarrier > 0)
        s.append("        steps++;\n"
                 "        if (steps == _barrier ) {\n"
                 "            printf(\"Too many collisions, increase the size...\\n\");\n"
                 "            exit(1);\n"
                 "        };\n");
      s.append(
          "    }\n"
          "    for (i = 0; i < ind; i++) {\n"
          "        tempState = _buckets[i].state;\n"
          "        if (tempState && _eq(_buckets[i].key, key)) {\n");
      for (unsigned long i = 0; i < m_aggregationTypes->size(); ++i) {
        if ((*m_aggregationTypes)[i] == AVG) {
          s.append("\t\t\t\t_buckets[i].value._" + std::to_string((i + 1)) + " = " +
              getCombineFunction(SUM, "_buckets[i].value._" + std::to_string((i + 1)),
                                 "value._" + std::to_string((i + 1))) + ";\n");
        } else
          s.append("\t\t\t\t_buckets[i].value._" + std::to_string((i + 1)) + " = " +
              getCombineFunction((*m_aggregationTypes)[i], "_buckets[i].value._" + std::to_string((i + 1)),
                                 "value._" + std::to_string((i + 1))) + ";\n");
      }
      s.append(
          "            _buckets[i].counter++;\n"
          "            return;\n"
          "        }\n"
          "        if (!tempState && (_eq(_buckets[i].key, key) || _buckets[i].dirty == 0)) { // first insert -- keep track of previous inserted value\n"
          "            _buckets[i].state = 1;\n"
          "            _buckets[i].dirty = 1;\n"
          "            _buckets[i].timestamp = timestamp;\n"
          "            _buckets[i].key = key;\n"
          "            _buckets[i].value = value;\n"
          "            _buckets[i].counter = 1;\n"
          "            return;\n"
          "        }\n");
      if (m_collisionBarrier > 0)
        s.append("        steps++;\n"
                 "        if (steps == _barrier ) {\n"
                 "            printf(\"Too many collisions, increase the size...\\n\");\n"
                 "            exit(1);\n"
                 "        };\n");
      s.append(
          "    }\n"
          "    throw std::runtime_error (\"error: the hashtable is full \\n\");\n"
          "}\n"
          "\n"
          "bool HashTable::evict (KeyT &key) {\n"
          "    size_t ind = _hasher(key) & _mask, i = ind;\n"
          "    for (; i < _num_buckets; i++) {\n"
          "        if (_buckets[i].state && _eq(_buckets[i].key, key)) {\n"
          "            _buckets[i].state = 0;\n"
          "            return true;\n"
          "        }\n"
          "    }\n"
          "    for (i = 0; i < ind; i++) {\n"
          "        if (_buckets[i].state && _eq(_buckets[i].key, key)) {\n"
          "            _buckets[i].state = 0;\n"
          "            return true;\n"
          "        }\n"
          "    }\n"
          "    printf (\"error: entry not found \\n\");\n"
          "    return false;\n"
          "}\n"
          "\n"
      );
      if (hasIncremental()) {
        s.append(
            "void HashTable::insertSlices () {\n"
            "    int maxNumOfSlices = INT_MIN;\n"
            "    for (auto i = 0; i < _num_buckets; ++i) {\n"
            "        int temp = _aggrs[i].addedElements - _aggrs[i].removedElements;\n"
            "        if (_buckets[i].state) {\n"
            "                node n;\n");
        for (unsigned long i = 0; i < m_aggregationTypes->size(); ++i) {
          s.append("\t\t\t\tn._" + std::to_string((i + 1)) + " = _buckets[i].value._" +
              std::to_string((i + 1)) + ";\n");
          if ((*m_aggregationTypes)[i] == AVG) {
            s.append("\t\t\t\tn._c" + std::to_string((i + 1)) + " = _buckets[i].counter;\n");
          }
        }
        s.append(
            "                _aggrs[i].insert(n);\n"
            //        "            }\n"
            "            _buckets[i].state = 0;\n"
            "            //_buckets[i].value = ValueT();\n"
            "        } else if (temp > 0) {\n"
            "            ValueT val;\n"
            "            node n;\n");
        for (unsigned long i = 0; i < m_aggregationTypes->size(); ++i) {
          s.append("\t\t\tn._" + std::to_string((i + 1)) + " = val._" + std::to_string((i + 1)) + ";\n");
          if ((*m_aggregationTypes)[i] == AVG) {
            s.append("\t\t\tn._c" + std::to_string((i + 1)) + " = 0;\n");
          }
        }
        s.append(
            "            _aggrs[i].insert(n);\n"
            "        }\n"
            "    }\n"
            "}\n"
            "\n"
            "void HashTable::evictSlices () {\n"
            "    for (auto i = 0; i < _num_buckets; ++i) {\n"
            "        if (_aggrs[i].addedElements - _aggrs[i].removedElements > 0) {\n"
            "            _aggrs[i].evict();\n"
            "        }\n"
            "    }\n"
            "}\n"
            "\n"
            "void HashTable::setValues () {\n"
            "    for (auto i = 0; i < _num_buckets; ++i) {\n"
            "        if (_aggrs[i].addedElements - _aggrs[i].removedElements > 0) {\n"
            "            auto res = _aggrs[i].query();\n"
            "            _buckets[i].state = 1;\n");
        for (unsigned long i = 0; i < m_aggregationTypes->size(); ++i) {
          s.append("\t\t\t_buckets[i].value._" + std::to_string((i + 1)) + " = res._" +
              std::to_string((i + 1)) + ";\n");
        }
        s.append(
            "            _buckets[i].counter = 1;\n"
            "        }\n"
            "    }\n"
            "}\n"
            "\n"
            "void HashTable::setIntermValues (int pos, long timestamp) {\n"
            "    for (auto i = 0; i < _num_buckets; ++i) {\n"
            "        if (_aggrs[i].addedElements - _aggrs[i].removedElements > 0) {\n"
            "            auto res = _aggrs[i].queryIntermediate (pos);\n"
            "            _buckets[i].state = 1;\n"
            "            _buckets[i].timestamp = timestamp;\n");
        for (unsigned long i = 0; i < m_aggregationTypes->size(); ++i) {
          s.append("\t\t\t_buckets[i].value._" + std::to_string((i + 1)) + " = res._" +
              std::to_string((i + 1)) + ";\n");
          if ((*m_aggregationTypes)[i] == AVG) {
            s.append("\t\t\t_buckets[i].counter = res._c" + std::to_string((i + 1)) + ";\n");
          }
        }
        s.append(
            "        }\n"
            "    }\n"
            "}\n"
            "\n"
        );
      }
      s.append(
          "bool HashTable::get_value (const KeyT &key, ValueT &result) {\n"
          "    size_t ind = _hasher(key) & _mask, i = ind;\n"
          "    for (; i < _num_buckets; i++) {\n"
          "        if (_buckets[i].state && _eq(_buckets[i].key, key)) {\n"
          "            result = _buckets[i].value;\n"
          "            return true;\n"
          "        }\n"
          "    }\n"
          "    for (i = 0; i < ind; i++) {\n"
          "        if (_buckets[i].state && _eq(_buckets[i].key, key)) {\n"
          "            result = _buckets[i].value;\n"
          "            return true;\n"
          "        }\n"
          "    }\n"
          "    return false;\n"
          "}\n"
          "\n"
          "bool HashTable::get_index (const KeyT &key, int &index) {\n"
          "    size_t ind = _hasher(key) & _mask, i = ind;\n"
          "    index = -1;\n");
      if (m_collisionBarrier > 0)
        s.append("    int steps = 0;\n");
      s.append(
          "    for (; i < _num_buckets; i++) {\n"
          "        if (_buckets[i].state && _eq(_buckets[i].key, key)) {\n"
          "            index = i;\n"
          "            return true;\n"
          "        }\n"
          "        if (_buckets[i].state == 0 && index == -1) {\n"
          "            index = i;\n"
          "        }\n");
      if (m_collisionBarrier > 0)
        s.append("        steps++;\n"
                 "        if (steps == _barrier ) {\n"
                 "            return false;;\n"
                 "        };\n");
      s.append(
          "    }\n"
          "    for (i = 0; i < ind; i++) {\n"
          "        if (_buckets[i].state && _eq(_buckets[i].key, key)) {\n"
          "            index = i;\n"
          "            return true;\n"
          "        }\n"
          "        if (_buckets[i].state == 0 && index == -1) {\n"
          "            index = i;\n"
          "        }\n");
      if (m_collisionBarrier > 0)
        s.append("        steps++;\n"
                 "        if (steps == _barrier ) {\n"
                 "            return false;\n"
                 "        };\n");
      s.append(
          "    }\n"
          "    return false;\n"
          "}\n"
          "\n"
          "void HashTable::deleteHashTable() {\n"
          "    for (size_t bucket=0; bucket<_num_buckets; ++bucket) {\n"
          "        _buckets[bucket].~BucketT();\n"
      );
      if (hasIncremental())
        s.append(
            "        _aggrs->~AggrT();\n"
        );
      s.append(
          "    }\n"
          "    free(_buckets);\n"
      );
      if (hasIncremental())
        s.append(
            "    free(_aggrs);\n"
        );
      s.append(
          "}\n"
          "\n"
          "BucketT* HashTable::getBuckets () {\n"
          "    return _buckets;\n"
          "}\n"
          "\n"
          "size_t HashTable::getSize() const {\n"
          "    return _num_filled;\n"
          "}\n"
          "\n"
          "bool HashTable::isEmpty() const {\n"
          "    return _num_filled==0;\n"
          "}\n"
          "\n"
          "size_t HashTable::getNumberOfBuckets() const {\n"
          "    return _num_buckets;\n"
          "}\n"
          "\n"
          "float HashTable::load_factor() const {\n"
          "    return static_cast<float>(_num_filled) / static_cast<float>(_num_buckets);\n"
          "}"
      );
    } else {
      s.append(m_customHashTable);
    }
    s.append("\n");
    return s;
  }

  std::string getComputeString() {
    std::string s;
    if (hasAggregation()) {
      std::string outputBuffers;
      std::string initialiseAggrs, computeAggrs, resetAggrs, insertAggrs, evictAggrs, setValues, filter;
      std::string openingWindows, closingWindows, pendingWindows, completeWindows;
      std::string resultPointers;
      if (m_windowDefinition->isRangeBased())
        filter.append(getSelectionExpr());
      if (!hasGroupBy()) {
        outputBuffers = getAggregationVarsOutputBuffers();
        initialiseAggrs = getAggregationVarsInitialization();
        computeAggrs = getAggregationVarsComputation(m_windowDefinition->isRangeBased());
        if (hasIncremental()) {
          resetAggrs = "aggrs.reset();\n";
          insertAggrs = "aggrStructures[pid].insert(aggrs);\n";
          evictAggrs = "aggrStructures[pid].evict();\n";
          setValues = "aggrs = aggrStructures[pid].query();\n";
          closingWindows = "aggrs = aggrStructures[pid].queryIntermediate(PARENTS_SIZE-2);\n";
          openingWindows = "aggrs = aggrStructures[pid].queryIntermediate(numberOfOpeningWindows);\n";
          pendingWindows = "aggrs = aggrStructures[pid].queryIntermediate(-1);\n";
        } else {
          resetAggrs = getAggregationVarsReset();
        }
        closingWindows.append(getWriteIntermediateResults(0));
        openingWindows.append(getWriteIntermediateResults(1));
        pendingWindows.append(getWriteIntermediateResults(2));
        completeWindows = getWriteCompleteResults();
        resultPointers = getAggregationVarsResultPointers();
      } else {
        //std::string key = getKeyType();
        outputBuffers = getGroupByOutputBuffers();
        initialiseAggrs = getGroupByInitialization();
        computeAggrs = getGroupByComputation(m_windowDefinition->isRangeBased());
        resetAggrs = getGroupByReset();
        if (hasIncremental()) {
          //resetAggrs = "aggrStructures[pid].clear();\n";
          insertAggrs = "aggrStructures[pid].insertSlices();\n";
          evictAggrs = "aggrStructures[pid].evictSlices();\n";
          setValues = "aggrStructures[pid].setValues();\n";
          closingWindows = "aggrStructures[pid].setIntermValues(PARENTS_SIZE-2, (prevClosePane+1) * panesPerSlide + panesPerWindow);\n";
          openingWindows = "aggrStructures[pid].insertSlices();\n"
                           "aggrStructures[pid].setIntermValues(numberOfOpeningWindows, prevOpenPane * paneSize);\n"
                           "prevOpenPane += panesPerSlide;\n";
          pendingWindows = "aggrStructures[pid].setIntermValues(-1, prevPane * paneSize);\n";
        }
        closingWindows.append(getWriteIntermediateResultsGroupBy(0));
        openingWindows.append(getWriteIntermediateResultsGroupBy(1));
        pendingWindows.append(getWriteIntermediateResultsGroupBy(2));
        completeWindows = getWriteCompleteResultsGroupBy();
        resultPointers = getGroupByResultPointers();
      }

      // overwrite if there is a static hashJoin
      if (hasStaticHashJoin()) {
        computeAggrs = m_staticComputation;
        initialiseAggrs += m_staticInitialization;
      }

      if (m_windowDefinition->isRowBased()) {
        if (m_windowDefinition->isTumbling()) {
          s.append(getTumblingWindowRows(outputBuffers,
                                         initialiseAggrs,
                                         computeAggrs,
                                         resetAggrs,
                                         openingWindows,
                                         closingWindows,
                                         pendingWindows,
                                         completeWindows,
                                         resultPointers));
        } else {
          s.append(getSlidingWindowRows(outputBuffers,
                                        initialiseAggrs,
                                        computeAggrs,
                                        insertAggrs,
                                        evictAggrs,
                                        resetAggrs,
                                        setValues,
                                        openingWindows,
                                        closingWindows,
                                        pendingWindows,
                                        completeWindows,
                                        resultPointers));
        }
      } else {
        if (m_windowDefinition->isTumbling()) {
          s.append(getFillEmptyTumblingWindows(resetAggrs, closingWindows, completeWindows));
          s.append(getTumblingWindowRange(outputBuffers,
                                          initialiseAggrs,
                                          computeAggrs,
                                          resetAggrs,
                                          openingWindows,
                                          closingWindows,
                                          pendingWindows,
                                          completeWindows,
                                          resultPointers,
                                          filter));
        } else {
          s.append(getFillEmptySlidingWindows(insertAggrs,
                                              evictAggrs,
                                              resetAggrs,
                                              setValues,
                                              closingWindows,
                                              completeWindows));
          s.append(getSlidingWindowRange(outputBuffers,
                                         initialiseAggrs,
                                         computeAggrs,
                                         insertAggrs,
                                         evictAggrs,
                                         resetAggrs,
                                         setValues,
                                         openingWindows,
                                         closingWindows,
                                         pendingWindows,
                                         completeWindows,
                                         resultPointers,
                                         filter));
        }
      }
    } else {
      std::string projection, filter;

      if (hasSelection()) {
        s.append(getSelectionExpr() + "{\n");
      }
      if (hasProjection()) {
        s.append(getProjectionExpr());
      } else {
        s.append("\toutput[pos] = data[bufferPtr];\n");
      }
      s.append("\tpos++;\n");
      if (hasSelection()) {
        s.append("}\n");
      }
    }
    return s;
  }

  std::string getHashTableStaticDeclaration() {
    std::string s;
    s.append("static HashTable aggrStructures[" + std::to_string(SystemConf::getInstance().WORKER_THREADS) + "];\n");

    s.append("bool isFirst [" + std::to_string(SystemConf::getInstance().WORKER_THREADS) + "] = {");
    for (auto i = 0; i < SystemConf::getInstance().WORKER_THREADS; ++i) {
      s.append("true");
      if (i != SystemConf::getInstance().WORKER_THREADS - 1)
        s.append(", ");
    }
    s.append("};\n");
    s.append("\n");
    return s;
  }

  std::string getHashTableMergeString() {
    std::string s;
    std::string completeResPredicate;
    if (!hasHavingPredicate())
      completeResPredicate = "resultIndex++;\n";
    else
      completeResPredicate = "resultIndex += " + getHavingExpr(false) + ";\n";
    s.append(
        "long aggregatePartials (char *openingBuffer, int *openingStartPointers, int openingEndPointer,\n"
        "                      char *closingOrPendingBuffer, int *copStartPointers, int copEndPointer,\n"
        "                      int startingWindow, int numOfWindows, bool pack,\n"
        "                      char* completeBuffer, long completeBufferPtr, int &tupleSize) {\n"
        "    tupleSize = sizeof(Bucket);\n"
        "    int mapSize = MAP_SIZE;\n"
        "    // Input and Output Buffers\n"
        "    Bucket *openingWindowsRes= (Bucket *) openingBuffer;\n"
        "    Bucket *partialRes= (Bucket *) closingOrPendingBuffer;\n"
        "    output_tuple_t *completeWindowsRes = (output_tuple_t *) completeBuffer; // the results here are packed\n"
        "\n"
        "    // Temp variables for the merging\n"
        "    int resultIndex = (pack) ? completeBufferPtr/sizeof(output_tuple_t) : startingWindow*mapSize;\n"
        "    int posInB2;\n"
        "    bool isFound;\n"
        "    int posInRes = 0;\n"
        "\n"
        "    int start1, end1, start2, end2;\n"
        "    start1 = end1 = start2 = end2 = 0;\n"
        "    for (int wid = startingWindow; wid < numOfWindows; ++wid) {\n"
        "\n"
        "        start1 = openingStartPointers[wid];\n"
        "        start2 = (pack) ? copStartPointers[wid] : 0;\n"
        "        end1 = openingStartPointers[wid+1];\n"
        "        end2 = (pack) ? copStartPointers[wid+1] : copEndPointer/tupleSize;\n"
        "        if (end1 < 0)\n"
        "            end1 = openingEndPointer/tupleSize;\n"
        "        if (end2 < 0)\n"
        "            end2 = copEndPointer/tupleSize;\n"
        "        if (start1 == end1) {\n"
        "            printf (\"error: empty opening window partial result\");\n"
        "            exit(1);\n"
        "        }\n"
        "        if (start2 == end2) {\n"
        "            printf (\"error: empty closing/pending window partial result\");\n"
        "            exit(1);\n"
        "        }\n"
        "        // search in the correct hashtables by moving the respective pointers\n"
        "        HashTable map1 (&openingWindowsRes[resultIndex]);\n"
        "        HashTable map2 (&partialRes[start2]);\n"
        "\n"
        "        if (pack) {\n"
        "            /* Iterate over tuples in first table. Search for key in the hash table.\n"
        "             * If found, merge the two entries. */\n"
        "            for (int idx = start1; idx < end1; idx++) {\n"
        "                if (openingWindowsRes[idx].state != 1) /* Skip empty slot */\n"
        "                    continue;\n"
        "                isFound = map2.get_index(openingWindowsRes[idx].key, posInB2);\n"
        "                if (posInB2 < 0) {\n"
        "                    printf(\"error: open-adress hash table is full \\n\");\n"
        "                    exit(1);\n"
        "                }\n"
        "                posInB2 += start2; // get the correct index;\n"
        "                if (!isFound) {                        \n"
        "                    /* Copy tuple based on output schema */\n"
        "                    /* Put timestamp */\n"
        "                    completeWindowsRes[resultIndex].timestamp = openingWindowsRes[idx].timestamp;\n"
        "                    /* Put key */\n");
    if (m_numberOfKeyAttributes == 1) {
      s.append("                    completeWindowsRes[resultIndex]._1 = openingWindowsRes[idx].key;\n");
    } else {
      for (auto i = 0; i < m_numberOfKeyAttributes; ++i) {
        s.append("                    completeWindowsRes[resultIndex]._" + std::to_string(i + 1)
                     + " = openingWindowsRes[idx].key._" + std::to_string(i) + ";\n");
      }
    }

    int completeResIndex = m_numberOfKeyAttributes;
    s.append("                    /* Put value(s) */\n");
    for (unsigned long i = 0; i < m_aggregationTypes->size(); ++i) {
      if ((*m_aggregationTypes)[i] == AVG) {
        s.append("\t\t\t\t\tcompleteWindowsRes[resultIndex]._" + std::to_string((completeResIndex + 1)) + " = "
                     + "openingWindowsRes[idx].value._" + std::to_string((i + 1))
                     + "/openingWindowsRes[idx].counter;\n");
      } else
        s.append("\t\t\t\t\tcompleteWindowsRes[resultIndex]._" + std::to_string((completeResIndex + 1)) + " = "
                     + "openingWindowsRes[idx].value._" + std::to_string((i + 1)) + ";\n");
      completeResIndex++;
    }
    s.append(
        //"                    resultIndex++;\n"
        addTabs(completeResPredicate, 5) +
            "                    // Do I need padding here ???\n"
            "                } else { // merge values based on the number of aggregated values and their types!            \n"
            "                    /* Copy tuple based on output schema */\n"
            "                    /* Put timestamp */\n"
            "                    completeWindowsRes[resultIndex].timestamp = std::max(completeWindowsRes[resultIndex].timestamp, openingWindowsRes[idx].timestamp);\n"
            "                    /* Put key */\n");
    if (m_numberOfKeyAttributes == 1) {
      s.append("                    completeWindowsRes[resultIndex]._1 = openingWindowsRes[idx].key;\n");
    } else {
      for (auto i = 0; i < m_numberOfKeyAttributes; ++i) {
        s.append("                    completeWindowsRes[resultIndex]._" + std::to_string(i + 1)
                     + " = openingWindowsRes[idx].key._" + std::to_string(i) + ";\n");
      }
    }
    s.append("                    /* Put value(s) */\n");
    completeResIndex = m_numberOfKeyAttributes;
    for (unsigned long i = 0; i < m_aggregationTypes->size(); ++i) {
      if ((*m_aggregationTypes)[i] == AVG) {
        s.append("\t\t\t\t\tcompleteWindowsRes[resultIndex]._" + std::to_string((completeResIndex + 1)) + " = " +
            "(openingWindowsRes[idx].value._" + std::to_string((i + 1)) + "+" + "partialRes[posInB2].value._"
                     + std::to_string((i + 1)) + ")/" +
            "(openingWindowsRes[idx].counter+partialRes[posInB2].counter);\n");
      } else
        s.append("\t\t\t\t\tcompleteWindowsRes[resultIndex]._" + std::to_string((completeResIndex + 1)) + " = " +
            getCombineFunction((*m_aggregationTypes)[i], "openingWindowsRes[idx].value._" + std::to_string((i + 1)),
                               "partialRes[posInB2].value._" + std::to_string((i + 1))) + ";\n");
      completeResIndex++;
    }
    s.append(
        //"                    resultIndex++;\n"
        addTabs(completeResPredicate, 5) +
            "                    // Do I need padding here ???\n"
            "                    \n"
            "                    // Unmark occupancy in second buffer\n"
            "                    partialRes[posInB2].state = 0;\n"
            "                }\n"
            "            }\n"
            "\n"
            "            /* Iterate over the remaining tuples in the second table. */\n"
            "            for (int idx = start2; idx < end2; idx++) {\n"
            "                if (partialRes[idx].state != 1) /* Skip empty slot */\n"
            "                    continue;                    \n"
            "                /* Copy tuple based on output schema */\n"
            "                /* Put timestamp */\n"
            "                completeWindowsRes[resultIndex].timestamp = partialRes[idx].timestamp;\n"
            "                /* Put key */\n");
    if (m_numberOfKeyAttributes == 1) {
      s.append("                completeWindowsRes[resultIndex]._1 = partialRes[idx].key;\n");
    } else {
      for (auto i = 0; i < m_numberOfKeyAttributes; ++i) {
        s.append(
            "                completeWindowsRes[resultIndex]._" + std::to_string(i + 1) + " = partialRes[idx].key._"
                + std::to_string(i) + ";\n");
      }
    }
    s.append("                /* Put value(s) */\n");
    completeResIndex = m_numberOfKeyAttributes;
    for (unsigned long i = 0; i < m_aggregationTypes->size(); ++i) {
      if ((*m_aggregationTypes)[i] == AVG) {
        s.append("\t\t\t\tcompleteWindowsRes[resultIndex]._" + std::to_string((completeResIndex + 1)) + " = "
                     + "partialRes[idx].value._" + std::to_string((i + 1)) + "/partialRes[idx].counter;\n");
      } else
        s.append("\t\t\t\tcompleteWindowsRes[resultIndex]._" + std::to_string((completeResIndex + 1)) + " = "
                     + "partialRes[idx].value._" + std::to_string((i + 1)) + ";\n");
      completeResIndex++;
    }
    s.append(
        //"                resultIndex++;\n"
        addTabs(completeResPredicate, 4) +
            "                // Do I need padding here ???                \n"
            "            }\n"
            "        } else {\n"
            "            /* Iterate over the second table. */\n"
            "            for (int idx = start2; idx < end2; idx++) {\n"
            "                if (partialRes[idx].state != 1) /* Skip empty slot */\n"
            "                    continue;\n"
            "\n"
            "                /* Create a new hash table entry */\n"
            "                isFound = map1.get_index(partialRes[idx].key, posInRes); //isFound = map2.get_index(&openingWindowsResults[resultIndex], &buffer2[idx].key, posInRes);\n"
            //"                if (posInRes < 0 || isFound) {\n"
            //"                    printf(\"error: failed to insert new key in intermediate hash table \\n\");\n"
            //"                    exit(1);\n"
            //"                }\n"
            "                if (!isFound) {\n"
            "                    /* Mark occupancy */\n"
            "                    openingWindowsRes[posInRes + resultIndex].state = 1;\n"
            "                    /* Put timestamp */\n"
            "                    openingWindowsRes[posInRes + resultIndex].timestamp = partialRes[idx].timestamp;\n"
            "                    /* Put key and value(s) */\n"
            "                    openingWindowsRes[posInRes + resultIndex].key = partialRes[idx].key;\n"
            "                    openingWindowsRes[posInRes + resultIndex].value = partialRes[idx].value;\n"
            "                    openingWindowsRes[posInRes + resultIndex].counter = partialRes[idx].counter;\n"
            "                } else {\n"
            "                    /* Mark occupancy */\n"
            "                    openingWindowsRes[posInRes + resultIndex].state = 1;\n"
            "                    /* Put timestamp */\n"
            "                    openingWindowsRes[posInRes + resultIndex].timestamp = std::max(openingWindowsRes[posInRes + resultIndex].timestamp, partialRes[idx].timestamp);\n"
            "                    /* Put key and value(s) */\n"
            "                    openingWindowsRes[posInRes + resultIndex].key = openingWindowsRes[posInRes + resultIndex].key;\n");
    for (unsigned long i = 0; i < m_aggregationTypes->size(); ++i) {
      if ((*m_aggregationTypes)[i] == AVG) {
        s.append("\t\t\t\t\topeningWindowsRes[posInRes + resultIndex].value._" + std::to_string((i + 1)) + " = " +
            "(openingWindowsRes[posInRes + resultIndex].value._" + std::to_string((i + 1)) + "+"
                     + "partialRes[idx].value._" + std::to_string((i + 1)) + "); // /" +
            "(openingWindowsRes[posInRes + resultIndex].counter+partialRes[idx].counter);\n");
      } else
        s.append("\t\t\t\t\topeningWindowsRes[posInRes + resultIndex].value._" + std::to_string((i + 1)) + " = " +
            getCombineFunction((*m_aggregationTypes)[i],
                               "openingWindowsRes[posInRes + resultIndex].value._" + std::to_string((i + 1)),
                               "partialRes[idx].value._" + std::to_string((i + 1))) + ";\n");
    }
    s.append(
        "                    /* Put count */\n"
        "                    openingWindowsRes[posInRes + resultIndex].counter = openingWindowsRes[posInRes + resultIndex].counter + partialRes[idx].counter;\n"

        "                }\n"
        "                \n"
        "            }\n"
        "            resultIndex += mapSize;        \n"
        "        }\n"
        "    }\n"
        "    // return the pointer required for appending or prepending the results\n"
        "    return (pack) ? resultIndex*sizeof(output_tuple_t) : (numOfWindows*mapSize)*sizeof(Bucket);\n"
        "}\n"
    );
    s.append("\n");
    return s;
  }

  std::string getHashTableMergeWithPtrsString() {
    std::string s;
    std::string completeResPredicate;
    if (!hasHavingPredicate())
      completeResPredicate = "resultIndex++;\n";
    else
      completeResPredicate = "resultIndex += " + getHavingExpr(false) + ";\n";
    s.append(
        "long aggregatePartials (char **openingBuffer, int *openingStartPointers, int openingEndPointer,\n"
        "                      char **closingOrPendingBuffer, int *copStartPointers, int copEndPointer,\n"
        "                      int startingWindow, int numOfWindows, bool pack,\n"
        "                      char* completeBuffer, long completeBufferPtr, int &tupleSize) {\n"
        "    tupleSize = sizeof(Bucket);\n"
        "    int mapSize = MAP_SIZE;\n"
        "    // Input and Output Buffers\n"
        "    Bucket **openingWindowsRes= (Bucket **) openingBuffer;\n"
        "    Bucket **partialRes= (Bucket **) closingOrPendingBuffer;\n"
        "    output_tuple_t *completeWindowsRes = (output_tuple_t *) completeBuffer; // the results here are packed\n"
        "\n"
        "    // Temp variables for the merging\n"
        "    int resultIndex = (pack) ? completeBufferPtr/sizeof(output_tuple_t) : startingWindow*mapSize;\n"
        "    int posInB2;\n"
        "    bool isFound;\n"
        "    int posInRes = 0;\n"
        "\n"
        "    int start1, end1, start2, end2, wid2;\n"
        "    start1 = end1 = start2 = end2 = 0;\n"
        "    for (int wid = startingWindow; wid < numOfWindows; ++wid) {\n"
        "\n"
        "        start1 = openingStartPointers[wid];\n"
        "        start2 = (pack) ? copStartPointers[wid] : 0;\n"
        "        wid2 = (pack) ? wid : 0;"
        "        end1 = openingStartPointers[wid+1];\n"
        "        end2 = (pack) ? copStartPointers[wid+1] : copEndPointer/tupleSize;\n"
        "        if (end1 < 0)\n"
        "            end1 = openingEndPointer/tupleSize;\n"
        "        if (end2 < 0)\n"
        "            end2 = copEndPointer/tupleSize;\n"
        "        if (start1 == end1) {\n"
        "            printf (\"error: empty opening window partial result\");\n"
        "            exit(1);\n"
        "        }\n"
        "        if (start2 == end2) {\n"
        "            printf (\"error: empty closing/pending window partial result\");\n"
        "            exit(1);\n"
        "        }\n"
        "        // search in the correct hashtables by moving the respective pointers\n"
        "        HashTable map1 (openingWindowsRes[wid]);\n"
        "        HashTable map2 (partialRes[wid2]);\n"
        "\n"
        "        if (pack) {\n"
        "            /* Iterate over tuples in first table. Search for key in the hash table.\n"
        "             * If found, merge the two entries. */\n"
        + addPostMergeOp() +
        "            for (int idx = 0; idx < mapSize; idx++) {\n"
        "                if (openingWindowsRes[wid][idx].state != 1) /* Skip empty slot */\n"
        "                    continue;\n"
        "                isFound = map2.get_index(openingWindowsRes[wid][idx].key, posInB2);\n"
        "                if (posInB2 < 0) {\n"
        "                    printf(\"error: open-adress hash table is full \\n\");\n"
        "                    exit(1);\n"
        "                }\n"
        //"                posInB2 += start2; // get the correct index;\n"
        "                if (!isFound) {                        \n"
        "                    /* Copy tuple based on output schema */\n"
        "                    /* Put timestamp */\n"
        "                    completeWindowsRes[resultIndex].timestamp = openingWindowsRes[wid][idx].timestamp;\n"
        "                    /* Put key */\n");
    if (m_numberOfKeyAttributes == 1) {
      s.append("                    completeWindowsRes[resultIndex]._1 = openingWindowsRes[wid][idx].key;\n");
    } else {
      for (auto i = 0; i < m_numberOfKeyAttributes; ++i) {
        s.append("                    completeWindowsRes[resultIndex]._" + std::to_string(i + 1)
                     + " = openingWindowsRes[wid][idx].key._" + std::to_string(i) + ";\n");
      }
    }

    int completeResIndex = m_numberOfKeyAttributes;
    s.append("                    /* Put value(s) */\n");
    for (unsigned long i = 0; i < m_aggregationTypes->size(); ++i) {
      if ((*m_aggregationTypes)[i] == AVG) {
        s.append("\t\t\t\t\tcompleteWindowsRes[resultIndex]._" + std::to_string((completeResIndex + 1)) + " = "
                     + "openingWindowsRes[wid][idx].value._" + std::to_string((i + 1))
                     + "/openingWindowsRes[wid][idx].counter;\n");
      } else
        s.append("\t\t\t\t\tcompleteWindowsRes[resultIndex]._" + std::to_string((completeResIndex + 1)) + " = "
                     + "openingWindowsRes[wid][idx].value._" + std::to_string((i + 1)) + ";\n");
      completeResIndex++;
    }
    s.append(
        //"                    resultIndex++;\n"
        addTabs(completeResPredicate, 5) +
            "                    // Do I need padding here ???\n"
            "                } else { // merge values based on the number of aggregated values and their types!            \n"
            "                    /* Copy tuple based on output schema */\n"
            "                    /* Put timestamp */\n"
            "                    completeWindowsRes[resultIndex].timestamp = std::max(completeWindowsRes[resultIndex].timestamp, openingWindowsRes[wid][idx].timestamp);\n"
            "                    /* Put key */\n");
    if (m_numberOfKeyAttributes == 1) {
      s.append("                    completeWindowsRes[resultIndex]._1 = openingWindowsRes[wid][idx].key;\n");
    } else {
      for (auto i = 0; i < m_numberOfKeyAttributes; ++i) {
        s.append("                    completeWindowsRes[resultIndex]._" + std::to_string(i + 1)
                     + " = openingWindowsRes[wid][idx].key._" + std::to_string(i) + ";\n");
      }
    }
    s.append("                    /* Put value(s) */\n");
    completeResIndex = m_numberOfKeyAttributes;
    for (unsigned long i = 0; i < m_aggregationTypes->size(); ++i) {
      if ((*m_aggregationTypes)[i] == AVG) {
        s.append("\t\t\t\t\tcompleteWindowsRes[resultIndex]._" + std::to_string((completeResIndex + 1)) + " = " +
            "(openingWindowsRes[wid][idx].value._" + std::to_string((i + 1)) + "+" + "partialRes[wid2][posInB2].value._"
                     + std::to_string((i + 1)) + ")/" +
            "(openingWindowsRes[wid][idx].counter+partialRes[wid2][posInB2].counter);\n");
      } else
        s.append("\t\t\t\t\tcompleteWindowsRes[resultIndex]._" + std::to_string((completeResIndex + 1)) + " = " +
            getCombineFunction((*m_aggregationTypes)[i],
                               "openingWindowsRes[wid][idx].value._" + std::to_string((i + 1)),
                               "partialRes[wid2][posInB2].value._" + std::to_string((i + 1))) + ";\n");
      completeResIndex++;
    }
    s.append(
        //"                    resultIndex++;\n"
        addTabs(completeResPredicate, 5) +
            "                    // Do I need padding here ???\n"
            "                    \n"
            "                    // Unmark occupancy in second buffer\n"
            "                    partialRes[wid2][posInB2].state = 0;\n"
            "                }\n"
            "            }\n"
            "\n"
            "            /* Iterate over the remaining tuples in the second table. */\n"
            "            for (int idx = 0; idx < mapSize; idx++) {\n"
            "                if (partialRes[wid2][idx].state != 1) /* Skip empty slot */\n"
            "                    continue;                    \n"
            "                /* Copy tuple based on output schema */\n"
            "                /* Put timestamp */\n"
            "                completeWindowsRes[resultIndex].timestamp = partialRes[wid2][idx].timestamp;\n"
            "                /* Put key */\n");
    if (m_numberOfKeyAttributes == 1) {
      s.append("                completeWindowsRes[resultIndex]._1 = partialRes[wid2][idx].key;\n");
    } else {
      for (auto i = 0; i < m_numberOfKeyAttributes; ++i) {
        s.append("                completeWindowsRes[resultIndex]._" + std::to_string(i + 1)
                     + " = partialRes[wid2][idx].key._" + std::to_string(i) + ";\n");
      }
    }
    s.append("                /* Put value(s) */\n");
    completeResIndex = m_numberOfKeyAttributes;
    for (unsigned long i = 0; i < m_aggregationTypes->size(); ++i) {
      if ((*m_aggregationTypes)[i] == AVG) {
        s.append("\t\t\t\tcompleteWindowsRes[resultIndex]._" + std::to_string((completeResIndex + 1)) + " = "
                     + "partialRes[wid2][idx].value._" + std::to_string((i + 1)) + "/partialRes[wid2][idx].counter;\n");
      } else
        s.append("\t\t\t\tcompleteWindowsRes[resultIndex]._" + std::to_string((completeResIndex + 1)) + " = "
                     + "partialRes[wid2][idx].value._" + std::to_string((i + 1)) + ";\n");
      completeResIndex++;
    }
    s.append(
        //"                resultIndex++;\n"
        addTabs(completeResPredicate, 4) +
            "                // Do I need padding here ???                \n"
            "            }\n"
            "        } else {\n"
            "            /* Iterate over the second table. */\n"
            "            for (int idx = 0; idx < mapSize; idx++) {\n"
            "                if (partialRes[wid2][idx].state != 1) /* Skip empty slot */\n"
            "                    continue;\n"
            "\n"
            "                /* Create a new hash table entry */\n"
            "                isFound = map1.get_index(partialRes[wid2][idx].key, posInRes); //isFound = map2.get_index(&openingWindowsResults[resultIndex], &buffer2[idx].key, posInRes);\n"
            //"                if (posInRes < 0 || isFound) {\n"
            //"                    printf(\"error: failed to insert new key in intermediate hash table \\n\");\n"
            //"                    exit(1);\n"
            //"                }\n"
            "                if (!isFound) {\n"
            "                    /* Mark occupancy */\n"
            "                    openingWindowsRes[wid][posInRes].state = 1;\n"
            "                    /* Put timestamp */\n"
            "                    openingWindowsRes[wid][posInRes].timestamp = partialRes[wid2][idx].timestamp;\n"
            "                    /* Put key and value(s) */\n"
            "                    openingWindowsRes[wid][posInRes].key = partialRes[wid2][idx].key;\n"
            "                    openingWindowsRes[wid][posInRes].value = partialRes[wid2][idx].value;\n"
            "                    openingWindowsRes[wid][posInRes].counter = partialRes[wid2][idx].counter;\n"
            "                } else {\n"
            "                    /* Mark occupancy */\n"
            "                    openingWindowsRes[wid][posInRes].state = 1;\n"
            "                    /* Put timestamp */\n"
            "                    openingWindowsRes[wid][posInRes].timestamp = std::max(openingWindowsRes[wid][posInRes].timestamp, partialRes[wid2][idx].timestamp);\n"
            "                    /* Put key and value(s) */\n"
            "                    openingWindowsRes[wid][posInRes].key = openingWindowsRes[wid][posInRes].key;\n");
    for (unsigned long i = 0; i < m_aggregationTypes->size(); ++i) {
      if ((*m_aggregationTypes)[i] == AVG) {
        s.append("\t\t\t\t\topeningWindowsRes[wid][posInRes].value._" + std::to_string((i + 1)) + " = " +
            "(openingWindowsRes[wid][posInRes].value._" + std::to_string((i + 1)) + "+"
                     + "partialRes[wid2][idx].value._" + std::to_string((i + 1)) + "); // /" +
            "(openingWindowsRes[wid][posInRes].counter+partialRes[wid2][idx].counter);\n");
      } else
        s.append("\t\t\t\t\topeningWindowsRes[wid][posInRes].value._" + std::to_string((i + 1)) + " = " +
            getCombineFunction((*m_aggregationTypes)[i],
                               "openingWindowsRes[wid][posInRes].value._" + std::to_string((i + 1)),
                               "partialRes[wid2][idx].value._" + std::to_string((i + 1))) + ";\n");
    }
    s.append(
        "                    /* Put count */\n"
        "                    openingWindowsRes[wid][posInRes].counter = openingWindowsRes[wid][posInRes].counter + partialRes[wid2][idx].counter;\n"
        "                }\n"
        "                \n"
        "            }\n"
        "            resultIndex += mapSize;        \n"
        "        }\n"
        "    }\n"
        "    // return the pointer required for appending or prepending the results\n"
        "    return (pack) ? resultIndex*sizeof(output_tuple_t) : (numOfWindows*mapSize)*sizeof(Bucket);\n"
        "}\n"
    );
    s.append("\n");
    return s;
  }

  std::string getHashTableParallelMergeWithPtrsString() {
    std::string s;
    std::string completeResPredicate;
    if (!hasHavingPredicate())
      completeResPredicate = "resultIndex++;\n";
    else
      completeResPredicate = "resultIndex += " + getHavingExpr(false) + ";\n";
    s.append(
        "long aggregatePartials (char *completeBuffer, int completeWindow, int completeStartPos,\n"
        "                      char **partialBuffer, int partialWindow,\n"
        "                      int &startPos, int &endPos,\n"
        "                      int &tupleSize, bool pack) {\n"
        "    tupleSize = sizeof(Bucket);\n"
        "    int mapSize = MAP_SIZE;\n"
        "    int hashTableSpacing = mapSize * sizeof(Bucket);\n"
        "    int currentWindowSpacing = completeStartPos + completeWindow * hashTableSpacing;\n"
        "\n"
        "    // Input and Output Buffers\n"
        "    Bucket **partialRes= (partialBuffer!=nullptr) ? (Bucket **) partialBuffer : nullptr;\n"
        "    Bucket *tempCompleteWindowsRes = (Bucket *) (completeBuffer+currentWindowSpacing);\n"
        "    output_tuple_t *completeWindowsRes = (output_tuple_t *) (completeBuffer+currentWindowSpacing); // the results here are packed\n"
        "\n"
        "    // Temp variables for the merging\n"
        "    bool isFound;\n"
        "    int resultIndex = 0;\n"
        "    int posInLeft = 0;\n"
        "\n"
        "\n"
        "    // check boundaries\n"
        "    if (currentWindowSpacing+hashTableSpacing >= UNBOUNDED_SIZE) {\n"
        "        throw std::runtime_error (\"error: resize unbounded buffer: \" + std::to_string(currentWindowSpacing+hashTableSpacing) + \" - \" + std::to_string(UNBOUNDED_SIZE) + \"\\n\");\n"
        "    }\n"
        "\n"
        "    if (startPos == -1) {\n"
        "        // memcpy the first opening buffer to the correct result slot\n"
        "        memcpy(completeBuffer+currentWindowSpacing, partialRes[partialWindow], mapSize * tupleSize * sizeof(char));\n"
        "        startPos = 0;\n"
        "        endPos = 0;\n"
        "        return 0;\n"
        "    }\n"
        "\n"
        "    if (!pack) {\n"
        "        HashTable leftMap (tempCompleteWindowsRes);\n"
        "        HashTable rightMap (partialRes[partialWindow]);\n"
        "        /* Iterate over the second table. */\n"
        "        for (int idx = 0; idx < mapSize; idx++) {\n"
        "            if (partialRes[partialWindow][idx].state != 1) /* Skip empty slot */\n"
        "                continue;\n"
        "            isFound = leftMap.get_index(partialRes[partialWindow][idx].key, posInLeft);\n"
        "            if (posInLeft < 0) {\n"
        "                    printf(\"error: open-adress hash table is full \\n\");\n"
        "                    exit(1);\n"
        "            }\n"
        "            /* Create a new hash table entry */\n"
        "            if (!isFound) {\n"
        "                /* Mark occupancy */\n"
        "                tempCompleteWindowsRes[posInLeft].state = 1;\n"
        "                /* Put timestamp */\n"
        "                tempCompleteWindowsRes[posInLeft].timestamp = partialRes[partialWindow][idx].timestamp;\n"
        "                /* Put key and value(s) */\n"
        "                tempCompleteWindowsRes[posInLeft].key = partialRes[partialWindow][idx].key;\n"
        "                tempCompleteWindowsRes[posInLeft].value = partialRes[partialWindow][idx].value;\n"
        "                tempCompleteWindowsRes[posInLeft].counter = partialRes[partialWindow][idx].counter;\n"
        "            } else {\n"
        "                /* Mark occupancy */\n"
        "                tempCompleteWindowsRes[posInLeft].state = 1;\n"
        "                /* Put timestamp */\n"
        "                tempCompleteWindowsRes[posInLeft].timestamp = std::max(tempCompleteWindowsRes[posInLeft].timestamp, partialRes[partialWindow][idx].timestamp);\n"
        "                /* Put key and value(s) */\n"
        "                tempCompleteWindowsRes[posInLeft].key = tempCompleteWindowsRes[posInLeft].key;\n");
    for (unsigned long i = 0; i < m_aggregationTypes->size(); ++i) {
      if ((*m_aggregationTypes)[i] == AVG) {
        s.append("\t\t\t\ttempCompleteWindowsRes[posInLeft].value._" + std::to_string((i + 1)) + " = " +
            "(tempCompleteWindowsRes[posInLeft].value._" + std::to_string((i + 1)) + "+"
                     + "partialRes[partialWindow][idx].value._" + std::to_string((i + 1)) + "); // /" +
            "(tempCompleteWindowsRes[posInLeft].counter+partialRes[partialWindow][idx].counter);\n");
      } else
        s.append("\t\t\t\ttempCompleteWindowsRes[posInLeft].value._" + std::to_string((i + 1)) + " = " +
            getCombineFunction((*m_aggregationTypes)[i],
                               "tempCompleteWindowsRes[posInLeft].value._" + std::to_string((i + 1)),
                               "partialRes[partialWindow][idx].value._" + std::to_string((i + 1))) + ";\n");
    }
    s.append(
        "                /* Put count */\n"
        "                tempCompleteWindowsRes[posInLeft].counter = tempCompleteWindowsRes[posInLeft].counter + partialRes[partialWindow][idx].counter;\n"
        "            }\n"
        "            \n"
        "        }  \n"
        "    } else {\n"
        + addPostMergeOp() +
        "        for (int idx = 0; idx < mapSize; idx++) {\n"
        "            if (tempCompleteWindowsRes[idx].state != 1) /* Skip empty slot */\n"
        "                continue;\n"
        "            /* Copy tuple based on output schema */\n"
        "            /* Put timestamp */\n"
        "            completeWindowsRes[resultIndex].timestamp = tempCompleteWindowsRes[idx].timestamp;\n"
        "            /* Put key */\n");
    if (m_numberOfKeyAttributes == 1) {
      s.append("            completeWindowsRes[resultIndex]._1 = tempCompleteWindowsRes[idx].key;\n");
    } else {
      for (auto i = 0; i < m_numberOfKeyAttributes; ++i) {
        s.append("            completeWindowsRes[resultIndex]._" + std::to_string(i + 1)
                     + " = tempCompleteWindowsRes[idx].key._" + std::to_string(i) + ";\n");
      }
    }
    s.append("            /* Put value(s) */\n");
    auto completeResIndex = m_numberOfKeyAttributes;
    for (unsigned long i = 0; i < m_aggregationTypes->size(); ++i) {
      if ((*m_aggregationTypes)[i] == AVG) {
        s.append("\t\t\tcompleteWindowsRes[resultIndex]._" + std::to_string((completeResIndex + 1)) + " = "
                     + "tempCompleteWindowsRes[idx].value._" + std::to_string((i + 1))
                     + "/tempCompleteWindowsRes[idx].counter;\n");
      } else
        s.append("\t\t\tcompleteWindowsRes[resultIndex]._" + std::to_string((completeResIndex + 1)) + " = "
                     + "tempCompleteWindowsRes[idx].value._" + std::to_string((i + 1)) + ";\n");
      completeResIndex++;
    }
    s.append(
        addTabs(completeResPredicate, 3) +
            "            // Do I need padding here ???                \n"
            "        }\n"
            "\n"
            "        // set result positions\n"
            "        startPos = currentWindowSpacing;\n"
            "        endPos = currentWindowSpacing + sizeof(output_tuple_t)*resultIndex;\n"
            "    }\n"
            "    return 0;\n"
            "}\n"
    );
    s.append("\n");
    return s;
  }

  std::string getSingleBucketString() {
    if (m_outputSchema == nullptr)
      throw std::runtime_error("error: outputSchema hasn't been set up");
    std::string s;
    s.append("struct alignas(16) interm_tuple_t {\n");
    int i = 0;
    if (m_outputSchema->hasTime()) {
      s.append("\tlong timestamp;\n");
      i++;
    }
    for (; i < m_outputSchema->numberOfAttributes(); i++) {
      auto type = m_outputSchema->getAttributeType(i);
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

  std::string getSingleStaticDeclaration() {
    std::string s;
    s.append("static Aggregator aggrStructures[" + std::to_string(SystemConf::getInstance().WORKER_THREADS) + "];\n\n");
    return s;
  }

  std::string getSingleKeyMergeString() {
    std::string s;
    std::string completeResPredicate;
    if (!hasHavingPredicate())
      completeResPredicate = "resultIndex++;\n";
    else
      completeResPredicate = "resultIndex += " + getHavingExpr(false) + ";\n";
    s.append(
        "long aggregatePartials (char *openingBuffer, int *openingStartPointers, int openingEndPointer,\n"
        "                      char *closingOrPendingBuffer, int *copStartPointers, int copEndPointer,\n"
        "                      int startingWindow, int numOfWindows, bool pack,\n"
        "                      char* completeBuffer, long completeBufferPtr, int &tupleSize) {\n"
        "    tupleSize = sizeof(interm_tuple_t);\n"
        "    // Input and Output Buffers\n"
        "    interm_tuple_t *openingWindowsRes= (interm_tuple_t *) openingBuffer;\n"
        "    interm_tuple_t *partialRes= (interm_tuple_t *) closingOrPendingBuffer;\n"
        "    output_tuple_t *completeWindowsRes = (output_tuple_t *) completeBuffer; // the results here are packed\n"
        "\n"
        "    // Temp variables for the merging\n"
        "    int resultIndex = (pack) ? completeBufferPtr/sizeof(output_tuple_t) : startingWindow;\n"
        "\n"
        "    int start1, end1, start2, end2;\n"
        "    for (int wid = startingWindow; wid < numOfWindows; ++wid) {\n"
        "        start1 = openingStartPointers[wid];\n"
        "        start2 = (pack) ? copStartPointers[wid] : 0;\n"
        "        end1 = openingStartPointers[wid+1];\n"
        "        end2 = (pack) ? copStartPointers[wid+1] : copEndPointer/tupleSize;\n"
        "        if (end1 < 0)\n"
        "            end1 = openingEndPointer/tupleSize;\n"
        "        if (end2 < 0)\n"
        "            end2 = copEndPointer/tupleSize;\n"
        "        if (start1 == end1) {\n"
        "            printf (\"error: empty opening window partial result\");\n"
        "            exit(1);\n"
        "        }\n"
        "        if (start2 == end2) {\n"
        "            printf (\"error: empty closing/pending window partial result\");\n"
        "            exit(1);\n"
        "        }\n"
        "        // merge values based on the number of aggregated values and their types!\n"
        "        if (pack) {\n"
        "            /* Copy tuple based on output schema */\n"
        "            /* Put timestamp */\n"
        "            completeWindowsRes[resultIndex].timestamp = std::max(openingWindowsRes[start1].timestamp, partialRes[start2].timestamp);\n"
        "            /* Put value(s) */\n");
    for (unsigned long i = 0; i < m_aggregationTypes->size(); ++i) {
      if ((*m_aggregationTypes)[i] == AVG) {
        s.append("\t\t\tcompleteWindowsRes[resultIndex]._" + std::to_string((i + 1)) + " = " +
            "(openingWindowsRes[start1]._" + std::to_string((i + 1)) + "+" + "partialRes[start2]._"
                     + std::to_string((i + 1)) + ")/" +
            "(openingWindowsRes[start1]._" + std::to_string((m_outputSchema->numberOfAttributes() - 1)) + "+" +
            "partialRes[start2]._" + std::to_string((m_outputSchema->numberOfAttributes() - 1)) + ");\n");
      } else
        s.append("\t\t\tcompleteWindowsRes[resultIndex]._" + std::to_string((i + 1)) + " = " +
            getCombineFunction((*m_aggregationTypes)[i], "openingWindowsRes[start1]._" + std::to_string((i + 1)),
                               "partialRes[start2]._" + std::to_string((i + 1))) + ";\n");
    }
    s.append(
        //"            resultIndex++;\n"
        addTabs(completeResPredicate, 3) +
            "        } else {\n"
            "            openingWindowsRes[start1].timestamp = partialRes[start2].timestamp;\n"
            "            /* Put value(s) */\n"
    );
    for (unsigned long i = 0; i < m_aggregationTypes->size(); ++i) {
      if ((*m_aggregationTypes)[i] == AVG) {
        s.append("\t\t\topeningWindowsRes[start1]._" + std::to_string((i + 1)) + " = " +
            "openingWindowsRes[start1]._" + std::to_string((i + 1)) + "+" + "partialRes[start2]._"
                     + std::to_string((i + 1)) + ";\n");
        s.append(
            "\t\t\topeningWindowsRes[start1]._" + std::to_string((m_outputSchema->numberOfAttributes() - 1)) + " = " +
                "openingWindowsRes[start1]._" + std::to_string((m_outputSchema->numberOfAttributes() - 1)) + "+" +
                "partialRes[start2]._" + std::to_string((m_outputSchema->numberOfAttributes() - 1)) + ";\n");
      } else
        s.append("\t\t\topeningWindowsRes[start1]._" + std::to_string((i + 1)) + " = " +
            getCombineFunction((*m_aggregationTypes)[i], "openingWindowsRes[start1]._" + std::to_string((i + 1)),
                               "partialRes[start2]._" + std::to_string((i + 1))) + ";\n");
    }
    s.append(
        "        }\n"
        "    }\n"
        "    // return the pointer required for appending or prepending the results\n"
        "    return (pack) ? resultIndex*sizeof(output_tuple_t) : (numOfWindows)*sizeof(interm_tuple_t);\n"
        "}\n"
    );
    s.append("\n");
    return s;
  }

  std::string getHavingExpr(bool isCompute) {
    std::string s;
    if (!hasHavingPredicate() && m_postWindowPredicate.empty()) {
      // do nothing
    } else if (hasHavingPredicate() && m_postWindowPredicate.empty()) {
      s.append("( ");
      s.append(m_havingPredicate->toSExprForCodeGen());
      s.append(" )");
      std::string str = "data[bufferPtr]";
      if (isCompute)
        s.replace(s.find(str), str.length(), "completeWindowsResults[completeWindowsPointer]");
      else
        s.replace(s.find(str), str.length(), "completeWindowsRes[resultIndex]");
    } else {
      s.append(m_postWindowPredicate);
      std::string str = "completeWindowsResults[completeWindowsPointer]";
      if (!isCompute)
        s.replace(s.find(str), str.length(), "completeWindowsRes[resultIndex]");
    }
    return s;
  }

  std::string getProjectionExpr() {
    std::string s;
    if (hasProjection() && m_expressions != nullptr && !m_expressions->empty()) {
      s.append("\toutput[pos].timestamp = data[bufferPtr].timestamp;\n");
      for (unsigned long i = 1; i < m_expressions->size(); ++i) {
        s.append("\toutput[pos]._" + std::to_string(i) + " = " + (*m_expressions)[i]->toSExprForCodeGen() + ";\n");
      }
    }
    return s;
  }

  std::string getAggregationVarsOutputBuffers() {
    std::string s;
    s.append(
        "interm_tuple_t *openingWindowsResults = (interm_tuple_t *) openingWindowsBuffer; // the results here are in the\n"
        "interm_tuple_t *closingWindowsResults = (interm_tuple_t *) closingWindowsBuffer; // form of the hashtable\n"
        "interm_tuple_t *pendingWindowsResults = (interm_tuple_t *) pendingWindowsBuffer;\n"
    );
    return s;
  }

  std::string getAggregationVarsInitialization() {
    if (!hasAggregation())
      throw std::runtime_error("error: aggregation operator hasn't been set up");
    std::string s;
    if (hasIncremental())
      s.append("aggrStructures[pid].initialise();\n");
    s.append("node aggrs;\n"
             "aggrs.reset();\n");
    return s;
  }

  std::string getAggregationVarsComputation(bool isRangeBased) {
    std::string s;
    if (!isRangeBased && hasSelection()) {
      s.append(getSelectionExpr());
      s.append("{\n");
    }
    if (hasAggregation()) {
      for (unsigned long i = 0; i < m_aggregationTypes->size(); ++i) {
        auto col = (*m_aggregationAttributes)[i]->getColumn();
        std::string input;
        if (col == 0)
          input.append("data[bufferPtr].timestamp");
        else
          input.append("data[bufferPtr]._" + std::to_string(col));
        switch ((*m_aggregationTypes)[i]) {
          case SUM:s.append("aggrs._" + std::to_string(i + 1) + " += " + input + ";\n");
            break;
          case AVG:s.append("aggrs._" + std::to_string(i + 1) + " += " + input + ";\n");
            s.append("aggrs._c" + std::to_string(i + 1) + "++;\n");
            break;
          case CNT:s.append("aggrs._c" + std::to_string(i + 1) + "++;\n");
            break;
          case MIN:
            s.append("aggrs._" + std::to_string(i + 1) + " = "
                         + getCombineFunction(MIN, "aggrs._" + std::to_string(i + 1), input) + ";\n");
            break;
          case MAX:
            s.append("aggrs._" + std::to_string(i + 1) + " = "
                         + getCombineFunction(MAX, "aggrs._" + std::to_string(i + 1), input) + ";\n");
            break;
          default:throw std::runtime_error("error: invalid aggregation type");
        }
      }
    }
    if (hasProjection()) {

    }
    if (!isRangeBased && hasSelection()) {
      s.append("}\n");
    }
    return s;
  }

  std::string getAggregationVarsReset() {
    if (!hasAggregation())
      throw std::runtime_error("error: aggregation operator hasn't been set up");
    std::string s;
    s.append("aggrs.reset();\n");
    //if (hasIncremental()) {}
    return s;
  }

  std::string getWriteIntermediateResults(int bufferType) {
    if (!hasAggregation())
      throw std::runtime_error("error: aggregation operator hasn't been set up");
    if (m_outputSchema == nullptr)
      throw std::runtime_error("error: outputSchema hasn't been set up");
    if ((int) m_aggregationTypes->size() > m_outputSchema->numberOfAttributes())
      throw std::runtime_error(
          "error: the number of aggregation types should be <= to the attributes of the output schema");
    std::string s;
    std::string buffer;
    if (bufferType == 0)
      buffer = "closingWindowsResults[closingWindowsPointer]";
    else if (bufferType == 1)
      buffer = "openingWindowsResults[openingWindowsPointer]";
    else
      buffer = "pendingWindowsResults[pendingWindowsPointer]";
    s.append("" + buffer + ".timestamp = data[bufferPtr-1].timestamp;\n");
    for (unsigned long i = 0; i < m_aggregationTypes->size(); ++i) {
      if ((*m_aggregationTypes)[i] == AVG) {
        s.append("" + buffer + "._" + std::to_string((i + 1)) + " = aggrs._" + std::to_string((i + 1)) + ";\n");
        s.append("" + buffer + "._" + std::to_string((m_outputSchema->numberOfAttributes() - 1)) + " = aggrs._c"
                     + std::to_string((i + 1)) + ";\n");
      } else
        s.append("" + buffer + "._" + std::to_string((i + 1)) + " = aggrs._" + std::to_string((i + 1)) + ";\n");
    }
    return s;
  }

  std::string getWriteCompleteResults() {
    if (!hasAggregation())
      throw std::runtime_error("error: aggregation operator hasn't been set up");
    if (m_outputSchema == nullptr)
      throw std::runtime_error("error: outputSchema hasn't been set up");
    if ((int) m_aggregationTypes->size() > m_outputSchema->numberOfAttributes())
      throw std::runtime_error(
          "error: the number of aggregation types should be <= to the attributes of the output schema");
    std::string s;
    s.append("completeWindowsResults[completeWindowsPointer].timestamp = prevCompletePane * paneSize;\n"); //data[bufferPtr-1].timestamp;\n");
    for (unsigned long i = 0; i < m_aggregationTypes->size(); ++i) {
      if ((*m_aggregationTypes)[i] == AVG) {
        s.append("completeWindowsResults[completeWindowsPointer]._" + std::to_string((i + 1)) + " = aggrs._"
                     + std::to_string((i + 1)) + "/aggrs._c" + std::to_string((i + 1)) + ";\n");
      } else
        s.append("completeWindowsResults[completeWindowsPointer]._" + std::to_string((i + 1)) + " = aggrs._"
                     + std::to_string((i + 1)) + ";\n");
    }
    std::string completeResPredicate;
    if (!hasHavingPredicate())
      completeResPredicate = "completeWindowsPointer++;\n";
    else
      completeResPredicate = "completeWindowsPointer += " + getHavingExpr(true) + ";\n";
    s.append(completeResPredicate); //s.append("completeWindowsPointer++;\n");
    return s;
  }

  std::string getKeyType() {
    std::string s;
    if (m_numberOfKeyAttributes == 1) {
      for (int idx = 1; idx <= m_numberOfKeyAttributes; ++idx) {
        auto e = (*m_groupByAttributes)[idx - 1];
        if (e->getBasicType() == BasicType::Integer) {
          s.append("int");
        } else if (e->getBasicType() == BasicType::Float) { ;
          s.append("float");
        } else if (e->getBasicType() == BasicType::Long) {
          s.append("long");
        } else if (e->getBasicType() == BasicType::LongLong) {
          s.append("__uint128_t");
        } else
          throw std::invalid_argument("error: invalid group-by attribute");
      }
    } else {
      s.append("struct Key {\n");
      for (int idx = 1; idx <= m_numberOfKeyAttributes; ++idx) {
        s.append("\t");
        auto e = (*m_groupByAttributes)[idx - 1];
        if (e->getBasicType() == BasicType::Integer) {
          s.append("int");
        } else if (e->getBasicType() == BasicType::Float) { ;
          s.append("float");
        } else if (e->getBasicType() == BasicType::Long) {
          s.append("long");
        } else if (e->getBasicType() == BasicType::LongLong) {
          s.append("__uint128_t");
        } else
          throw std::invalid_argument("error: invalid group-by attribute");
        s.append(" _" + std::to_string(idx - 1) + ";\n");
      }
      s.append("};\n");
    }
    return s;
  }

  std::string getAggregationVarsResultPointers() {
    std::string s;
    s.append(
        "pointersAndCounts[0] = openingWindowsPointer * sizeof(interm_tuple_t);\n"
        "pointersAndCounts[1] = closingWindowsPointer * sizeof(interm_tuple_t);\n"
        "pointersAndCounts[2] = pendingWindowsPointer * sizeof(interm_tuple_t);\n"
        "pointersAndCounts[3] = completeWindowsPointer * sizeof(output_tuple_t);\n"
    );
    return s;
  }

  std::string getGroupByOutputBuffers() {
    std::string s;
    if (!m_usePtrs) {
      s.append(
          "Bucket *openingWindowsResults = (Bucket *) openingWindowsBuffer; // the results here are in the\n"
          "Bucket *closingWindowsResults = (Bucket *) closingWindowsBuffer; // form of the hashtable\n"
          "Bucket *pendingWindowsResults = (Bucket *) pendingWindowsBuffer;\n"
      );
    } else {
      s.append(
          "Bucket **openingWindowsResults = (Bucket **) openingWindowsBuffer; // the results here are in the\n"
          "Bucket **closingWindowsResults = (Bucket **) closingWindowsBuffer; // form of the hashtable\n"
          "Bucket **pendingWindowsResults = (Bucket **) pendingWindowsBuffer;\n"
      );
    }

    return s;
  }

  std::string getGroupByInitialization() {
    if (!hasAggregation())
      throw std::runtime_error("error: aggregation operator hasn't been set up");
    std::string s;
    s.append("if (isFirst[pid]) {\n"
             "    aggrStructures[pid].init();\n"
             "    isFirst[pid] = false;\n"
             "}\n"
             "aggrStructures[pid].clear();\n"
             "Value curVal;\n");
    return s;
  }

  std::string getGroupByComputation(bool isRangeBased) {
    if (!hasAggregation())
      throw std::runtime_error("error: aggregation operator hasn't been set up");
    std::string s;
    if (!isRangeBased && hasSelection()) {
      s.append(getSelectionExpr());
      s.append("{\n");
    }
    if (hasAggregation()) {
      for (unsigned long i = 0; i < m_aggregationTypes->size(); ++i) {
        auto col = (*m_aggregationAttributes)[i]->getColumn();
        if (col == 0)
          s.append("curVal._" + std::to_string(i + 1) + " = data[bufferPtr].timestamp;\n");
        else
          s.append("curVal._" + std::to_string(i + 1) + " = data[bufferPtr]._" + std::to_string(col) + ";\n");
      }

      if (m_numberOfKeyAttributes == 1) {
        std::string col;
        if (auto cRef = dynamic_cast<ColumnReference *>((*m_groupByAttributes)[0])) {
          if (cRef->getColumn() == 0)
            col = "timestamp";
          else
            col = "_" + std::to_string(cRef->getColumn());
          s.append("aggrStructures[pid].insert_or_modify(data[bufferPtr]." + col +
              ", curVal, data[bufferPtr].timestamp);\n");
        } else {
          s.append("aggrStructures[pid].insert_or_modify(" + (*m_groupByAttributes)[0]->toSExprForCodeGen() +
              ", curVal, data[bufferPtr].timestamp);\n");
        }

      } else {
        s.append("Key tempKey = {");
        std::string col;
        for (int i = 0; i < m_numberOfKeyAttributes; ++i) {
          if (auto cRef = dynamic_cast<ColumnReference *>((*m_groupByAttributes)[i])) {
            if (cRef->getColumn() == 0)
              col = "timestamp";
            else
              col = "_" + std::to_string(cRef->getColumn());
            s.append("data[bufferPtr]." + col);
          } else {
            s.append((*m_groupByAttributes)[i]->toSExprForCodeGen());
          }
          if (i != m_numberOfKeyAttributes - 1)
            s.append(", ");
        }
        s.append("};\n");
        s.append("aggrStructures[pid].insert_or_modify(tempKey, curVal, data[bufferPtr].timestamp);\n");
      }
    }
    if (hasProjection()) {

    }
    if (!isRangeBased && hasSelection()) {
      s.append("}\n");
    }
    return s;
  }

  std::string getGroupByReset() {
    if (!hasAggregation())
      throw std::runtime_error("error: aggregation operator hasn't been set up");
    std::string s;
    s.append("aggrStructures[pid].reset();\n");
    if (hasIncremental()) {

    }
    return s;
  }

  std::string getWriteIntermediateResultsGroupBy(int bufferType) {
    if (!hasAggregation())
      throw std::runtime_error("error: aggregation operator hasn't been set up");
    if (m_outputSchema == nullptr)
      throw std::runtime_error("error: outputSchema hasn't been set up");
    if ((int) m_aggregationTypes->size() > m_outputSchema->numberOfAttributes())
      throw std::runtime_error(
          "error: the number of aggregation types should be <= to the attributes of the output schema");
    std::string s;
    std::string buffer;
    if (!m_usePtrs) {
      if (bufferType == 0)
        s.append(
            "std::memcpy(closingWindowsResults + closingWindowsPointer, aggrStructures[pid].getBuckets(), mapSize * sizeof(Bucket));\n");
      else if (bufferType == 1)
        s.append(
            "std::memcpy(openingWindowsResults + openingWindowsPointer, aggrStructures[pid].getBuckets(), mapSize * sizeof(Bucket));\n");
      else
        s.append(
            "std::memcpy(pendingWindowsResults + pendingWindowsPointer, aggrStructures[pid].getBuckets(), mapSize * sizeof(Bucket));\n");
    } else {
      if (bufferType == 0)
        s.append(
            "std::memcpy(closingWindowsResults[numberOfClosingWindows], aggrStructures[pid].getBuckets(), mapSize * sizeof(Bucket));\n");
      else if (bufferType == 1)
        s.append(
            "std::memcpy(openingWindowsResults[numberOfOpeningWindows], aggrStructures[pid].getBuckets(), mapSize * sizeof(Bucket));\n");
      else
        s.append("std::memcpy(pendingWindowsResults[0], aggrStructures[pid].getBuckets(), mapSize * sizeof(Bucket));\n");
    }

    return s;
  }

  std::string getWriteCompleteResultsGroupBy() {
    if (!hasAggregation())
      throw std::runtime_error("error: aggregation operator hasn't been set up");
    if (m_outputSchema == nullptr)
      throw std::runtime_error("error: outputSchema hasn't been set up");
    if ((int) m_aggregationTypes->size() > m_outputSchema->numberOfAttributes())
      throw std::runtime_error(
          "error: the number of aggregation types should be <= to the attributes of the output schema");
    std::string s;
    s.append(
        "completeWindowsResults[completeWindowsPointer].timestamp = prevCompletePane * paneSize;\n"); //aggrStructures[pid].getBuckets()[i].timestamp;\n");

    // Write Key
    if (m_numberOfKeyAttributes == 1) {
      s.append("completeWindowsResults[completeWindowsPointer]._" + std::to_string((1))
                   + " = aggrStructures[pid].getBuckets()[i].key;\n");
    } else {
      for (auto i = 0; i < m_numberOfKeyAttributes; ++i) {
        s.append("completeWindowsResults[completeWindowsPointer]._" + std::to_string((i + 1))
                     + " = aggrStructures[pid].getBuckets()[i].key._" + std::to_string(i) + ";\n");
      }
    }

    // Write Values
    int completeResIndex = m_numberOfKeyAttributes;
    for (unsigned long i = 0; i < m_aggregationTypes->size(); ++i) {
      if ((*m_aggregationTypes)[i] == AVG) {
        s.append("completeWindowsResults[completeWindowsPointer]._" + std::to_string((m_numberOfKeyAttributes + 1))
                     + " = aggrStructures[pid].getBuckets()[i].value._" + std::to_string((i + 1))
                     + "/aggrStructures[pid].getBuckets()[i].counter;\n");
      } else
        s.append("completeWindowsResults[completeWindowsPointer]._" + std::to_string((m_numberOfKeyAttributes + 1))
                     + " = aggrStructures[pid].getBuckets()[i].value._" + std::to_string((i + 1)) + ";\n");
      completeResIndex++;
    }
    std::string completeResPredicate = "";
    if (hasHavingPredicate())
      completeResPredicate = " && " + getHavingExpr(true);
    s.append("completeWindowsPointer += aggrStructures[pid].getBuckets()[i].state" + completeResPredicate + ";\n");
    return s;
  }

  std::string getGroupByResultPointers() {
    std::string s;
    s.append(
        "pointersAndCounts[0] = openingWindowsPointer * sizeof(Bucket);\n"
        "pointersAndCounts[1] = closingWindowsPointer * sizeof(Bucket);\n"
        "pointersAndCounts[2] = pendingWindowsPointer * sizeof(Bucket);\n"
        "pointersAndCounts[3] = completeWindowsPointer * sizeof(output_tuple_t);\n"
    );
    return s;
  }

  std::string getTumblingWindowRows(std::string outputBuffers,
                                    std::string initialiseAggrs,
                                    std::string computeAggrs,
                                    std::string resetAggrs,
                                    std::string openingWindows,
                                    std::string closingWindows,
                                    std::string pendingWindows,
                                    std::string completeWindows,
                                    std::string resultPointers) {
    std::string s;
    std::string ptr;
    if (hasGroupBy() && m_usePtrs) {
      ptr.append("*");
    }
    s.append(
        "void processData (int pid, char *inputBuffer, size_t inputBufferSize, long startPointer, long endPointer, long timestampFromPrevBatch,\n"
        "                  long *windowStartPointers, long *windowEndPointers, char *" + ptr
            + "openingWindowsBuffer, char *" + ptr + "closingWindowsBuffer,\n"
                                                     "                  char *" + ptr
            + "pendingWindowsBuffer, char *completeWindowsBuffer,\n"
              "                  int *openingStartPointers, int *closingStartPointers, int *pendingStartPointers, int *completeStartPointers,\n"
              //"                  long *openingWindowIds, long *closingWindowIds, long *pendingWindowIds, long *completeWindowIds,\n"
              "                  long streamStartPointer, int *pointersAndCounts, char *staticBuffer) {"
              "    // Input Buffer\n"
              "    input_tuple_t *data= (input_tuple_t *) inputBuffer;\n"
              "\n"
              "    // Output Buffers\n" +
            addTabs(outputBuffers, 1) +
            "    output_tuple_t *completeWindowsResults = (output_tuple_t *) completeWindowsBuffer; // the results here are packed\n"
            "\n"
            //"    for (int i = 0; i < PARTIAL_WINDOWS; i++) {\n"
            //"        windowStartPointers[i] = -1;\n"
            //"        windowEndPointers[i] = -1;\n"
            //"    }\n"
            //"\n"
            "    int tupleSize = sizeof(input_tuple_t);\n"
            "    int mapSize = MAP_SIZE;\n"
            "    long paneSize = PANE_SIZE;\n"
            "    long panesPerSlide = PANES_PER_SLIDE;\n"
            "    long panesPerWindow = PANES_PER_WINDOW;\n"
            "    long windowSlide = WINDOW_SLIDE;\n"
            "    long windowSize = WINDOW_SIZE;\n"
            "    int openingWindowsPointer = 0, closingWindowsPointer = 0;\n"
            "    int pendingWindowsPointer = 0, completeWindowsPointer = 0;\n"
            "\n"
            "    // Set the first pointer for all types of windows\n"
            "    openingStartPointers[0] = openingWindowsPointer;\n"
            "    closingStartPointers[0] = closingWindowsPointer;\n"
            "    pendingStartPointers[0] = pendingWindowsPointer;\n"
            "    completeStartPointers[0] = completeWindowsPointer;\n"
            "\n"
            "    // initialize aggregation data structures\n" +
            addTabs(initialiseAggrs, 1) +
            "\n"
            "    // Slicing based on panes logic\n"
            "    // Previous, next, and current pane ids\n"
            "    long prevClosePane, currPane, prevCompletePane, prevOpenPane, startPane;\n"
            "    int numberOfOpeningWindows = 0;\n"
            "    int numberOfClosingWindows = 0;\n"
            "    int numberOfPendingWindows = 0;\n"
            "    int numberOfCompleteWindows = 0;\n"
            "    int currentSlide = 0;\n"
            "    int currentWindow = 0;\n"
            "    long step = 1; //tupleSize;\n"
            "    long streamPtr = streamStartPointer / tupleSize;\n"
            "    long bufferPtr = startPointer / tupleSize;\n"
            "    startPointer = startPointer / tupleSize;\n"
            "    endPointer = endPointer / tupleSize;\n"
            "    long diff = streamPtr - bufferPtr;\n"
            "    long tempStartPos, tempEndPos;\n"
            "\n"
            "    //windowStartPointers[currentSlide++] = bufferPtr;\n"
            "    bool completeStartsFromPane = startPane==prevCompletePane;\n"
            "    bool hasComplete = ((endPointer - startPointer) >= windowSize);\n"
            "    startPane = (streamPtr / paneSize);\n"
            "    prevClosePane = prevOpenPane = (streamPtr / paneSize) - panesPerSlide;\n"
            "    prevCompletePane = streamPtr / paneSize;\n"
            "    if (streamStartPointer!=0) {\n"
            "        long tmpPtr = streamPtr;\n"
            "        tmpPtr = tmpPtr/windowSlide;\n"
            "        tmpPtr = tmpPtr * windowSlide;\n"
            "        if (streamPtr%windowSlide!=0) {\n"
            "            prevOpenPane = tmpPtr / paneSize;\n"
            "            prevCompletePane = (tmpPtr+windowSlide) / paneSize;\n"
            "        }\n"
            "        if (streamPtr%windowSlide==0 && hasComplete) {\n"
            "            prevClosePane = tmpPtr / paneSize;\n"
            "        } else {\n"
            "            while (streamPtr-tmpPtr + windowSlide <= windowSize) {\n"
            "                tmpPtr -= windowSlide;\n"
            "            }\n"
            "            prevClosePane = tmpPtr / paneSize;\n"
            "            if (prevClosePane < 0)\n"
            "                prevClosePane = 0;\n"
            "        }\n"
            "    }\n"
            "\n"
            "\n"
            "    // The beginning of the stream. Check if we have at least one complete window.\n"
            "    if (streamPtr == 0) {\n"
            "        // check for opening windows until finding the first complete\n"
            "        while (bufferPtr < endPointer) {\n"
            "            currPane = streamPtr / paneSize;\n"
            "            if (currPane - prevOpenPane == panesPerSlide) {\n"
            "                prevOpenPane += panesPerSlide;\n"
            "                windowStartPointers[currentSlide++] = bufferPtr;\n"
            "            }\n"
            "            if (currPane - prevCompletePane == panesPerWindow) {\n"
            "                windowEndPointers[currentWindow++] = bufferPtr;\n"
            "                numberOfCompleteWindows++;\n"
            "                break;\n"
            "            }\n"
            "            // filter, project + aggregate here\n" +
            addTabs(computeAggrs, 3) +
            "            streamPtr += step;\n"
            "            bufferPtr += step;\n"
            "        }\n"
            "    }\n"
            "        // Check for closing and opening windows, until we have a complete window.\n"
            "    else {\n"
            "        while (bufferPtr < endPointer) {\n"
            "            currPane = streamPtr / paneSize;\n"
            "            if (currPane - prevOpenPane == panesPerSlide) { // new slide and possible opening windows\n"
            "                prevOpenPane += panesPerSlide;\n"
            "                windowStartPointers[currentSlide++] = bufferPtr;\n"
            "            }\n"
            "            if (hasComplete && currPane - prevCompletePane == panesPerWindow) { // complete window\n"
            "                windowEndPointers[currentWindow++] = bufferPtr;\n"
            "                numberOfCompleteWindows++;\n"
            "                break;\n"
            "            }\n"
            "            if (/*prevClosePane >= panesPerWindow &&*/ prevClosePane <= startPane && currPane - prevClosePane == panesPerWindow) { // closing window\n"
            "                // write result to the closing windows\n" +
            addTabs(closingWindows, 5) +
            "                prevClosePane += panesPerSlide;\n"
            "                closingWindowsPointer += mapSize;\n"
            "                numberOfClosingWindows++;\n"
            "                closingStartPointers[numberOfClosingWindows] = closingWindowsPointer;\n"
            //"                closingWindowIds[numberOfClosingWindows-1] = prevClosePane - panesPerSlide;\n"
            "                // reset values\n" +
            addTabs(resetAggrs, 4) +
            "            }\n"
            "            // filter, project + aggregate here\n" +
            addTabs(computeAggrs, 3) +
            "            streamPtr += step;\n"
            "            bufferPtr += step;\n"
            "        }\n"
            "\n"
            "        // check for pending windows\n"
            "        if ((numberOfClosingWindows == 0 || windowSize!=windowSlide) && numberOfCompleteWindows == 0) {\n"
            +
                //"            currPane = streamPtr / paneSize;\n"
                //"            if (/*prevClosePane >= panesPerWindow &&*/ prevClosePane <= startPane && currPane - prevClosePane == panesPerWindow) { // closing window\n"
                //"                // write result to the closing windows\n" +
                //addTabs(closingWindows, 5) +
                //"                closingWindowsPointer += mapSize;\n"
                //"                numberOfClosingWindows++;\n"
                //"                closingStartPointers[numberOfClosingWindows] = closingWindowsPointer;\n"
                //"                // reset values\n" +
                //addTabs(resetAggrs, 4) +
                //"            }\n"
                "            // write result to pending windows\n" +
            addTabs(pendingWindows, 3) +
            "            pendingWindowsPointer += mapSize;\n"
            "            numberOfPendingWindows++;\n"
            "            pendingStartPointers[numberOfPendingWindows] = pendingWindowsPointer;\n"
            "            // reset values\n" +
            addTabs(resetAggrs, 3) +
            "        }\n"
            "    }\n"
            "\n"
            "    if (numberOfCompleteWindows == 0 && (streamStartPointer == 0 || currentSlide >= 1)) { // We only have one opening window so far...\n"
            "        // write results\n" +
            addTabs(openingWindows, 2) +
            "        openingWindowsPointer += mapSize;\n"
            "        numberOfOpeningWindows++;\n"
            "        openingStartPointers[numberOfOpeningWindows] = openingWindowsPointer;\n"
            //"        openingWindowIds[numberOfOpeningWindows-1] = prevOpenPane;\n"
            "\n"
            "    } else if (numberOfCompleteWindows > 0) {\n"
            "        // write results and pack them for the first complete window in the batch\n"
            + addPostWindowOperation(2) +
            "        for (int i = 0; i < mapSize; i++) {\n" +
            addTabs(completeWindows, 3) +
            //"            completeWindowsPointer++;\n"
            "        }\n"
            "        // reset values\n" +
            addTabs(resetAggrs, 2) +
            "        // write in the correct slot, as the value has already been incremented!\n"
            "        completeStartPointers[numberOfCompleteWindows] = completeWindowsPointer;\n"
            //"        completeStartWindowIds[numberOfCompleteWindows-1] = prevCompleteWindow;\n"
            "        // filter, project + aggregate here\n" +
            addTabs(computeAggrs, 2) +
            "\n"
            "        bufferPtr = windowEndPointers[0];\n"
            "        prevOpenPane = (windowStartPointers[currentSlide-1] +diff) / paneSize;\n"
            "        int idx = 1;\n"
            "        prevCompletePane = (windowStartPointers[idx++]+diff) / paneSize;\n"
            "        int removalIndex = currentWindow; //(startingFromPane) ? currentWindow : currentWindow + 1;\n"
            "        bool foundComplete = false;\n"
            "        while (bufferPtr < endPointer) {\n"
            "            // add elements from the next slide\n"
            "            bufferPtr = windowEndPointers[currentWindow - 1] + 1; // take the next position, as we have already computed this value\n"
            "            foundComplete = false;\n"
            "            while (true) {\n"
            "                currPane = (bufferPtr+diff) / paneSize;\n"
            "                if (currPane - prevOpenPane == panesPerSlide) {\n"
            "                    prevOpenPane = currPane;\n"
            "                    windowStartPointers[currentSlide++] = bufferPtr;\n"
            "                }\n"
            "                // complete windows\n"
            "                if (currPane - prevCompletePane == panesPerWindow) {\n"
            "                    //prevPane = currPane;\n"
            "                    prevCompletePane = (windowStartPointers[idx++]+diff) / paneSize;\n"
            "\n"
            "                    windowEndPointers[currentWindow++] = bufferPtr;\n"
            "                    // write and pack the complete window result\n"
            "                    //map.setValues();\n"
            + addPostWindowOperation(5) +
            "                    for (int i = 0; i < mapSize; i++) {\n" +
            addTabs(completeWindows, 6) +
            //"                        completeWindowsPointer++;\n"
            "                    }\n"
            "                    numberOfCompleteWindows++;\n"
            "                    completeStartPointers[numberOfCompleteWindows] = completeWindowsPointer;\n"
            //"                    completeWindowIds[numberOfCompleteWindows-1] = prevCompletePane - 1;\n"
            "                    foundComplete = true;\n"
            "                    // reset values\n" +
            addTabs(resetAggrs, 5) +
            "                }\n"
            "                if (bufferPtr >= endPointer) {\n"
            "                    break;\n"
            "                }\n"
            "                // filter, project + aggregate here\n" +
            addTabs(computeAggrs, 4) +
            "                bufferPtr += step;\n"
            "                if (foundComplete) {\n"
            "                    break;\n"
            "                }\n"
            "            }\n"
            "            removalIndex++;\n"
            "        }\n"
            "\n"
            "        if (!foundComplete) {  // we have reached the first open window after all the complete ones\n"
            "            // write the first open window if we have already computed the result\n" +
            addTabs(openingWindows, 3) +
            "            openingWindowsPointer += mapSize;\n"
            "            numberOfOpeningWindows++;\n"
            "            openingStartPointers[numberOfOpeningWindows] = openingWindowsPointer;\n"
            //"            openingWindowIds[numberOfOpeningWindows-1] = prevCompletePane++;\n"
            "        }\n"
            "    }\n"
            "\n" +
            addTabs(resultPointers, 1) +
            "    pointersAndCounts[4] = numberOfOpeningWindows;\n"
            "    pointersAndCounts[5] = numberOfClosingWindows;\n"
            "    pointersAndCounts[6] = numberOfPendingWindows;\n"
            "    pointersAndCounts[7] = numberOfCompleteWindows;\n"
            "}\n");
    return s;
  }

  std::string getSlidingWindowRows(std::string outputBuffers,
                                   std::string initialiseAggrs,
                                   std::string computeAggrs,
                                   std::string insertAggrs,
                                   std::string evictAggrs,
                                   std::string resetAggrs,
                                   std::string setValues,
                                   std::string openingWindows,
                                   std::string closingWindows,
                                   std::string pendingWindows,
                                   std::string completeWindows,
                                   std::string resultPointers) {
    std::string s;
    std::string ptr;
    if (hasGroupBy() && m_usePtrs) {
      ptr.append("*");
    }
    s.append(
        "void processData (int pid, char *inputBuffer, size_t inputBufferSize, long startPointer, long endPointer, long timestampFromPrevBatch,\n"
        "                  long *windowStartPointers, long *windowEndPointers, char *" + ptr
            + "openingWindowsBuffer, char *" + ptr + "closingWindowsBuffer,\n"
                                                     "                  char *" + ptr
            + "pendingWindowsBuffer, char *completeWindowsBuffer,\n"
              "                  int *openingStartPointers, int *closingStartPointers, int *pendingStartPointers, int *completeStartPointers,\n"
              //"                  long *openingWindowIds, long *closingWindowIds, long *pendingWindowIds, long *completeWindowIds,\n"
              "                  long streamStartPointer, int *pointersAndCounts, char *staticBuffer) {\n"
              "\n"
              "    // Input Buffer\n"
              "    input_tuple_t *data= (input_tuple_t *) inputBuffer;\n"
              "\n"
              "    // Output Buffers\n" +
            addTabs(outputBuffers, 1) +
            "    output_tuple_t *completeWindowsResults = (output_tuple_t *) completeWindowsBuffer; // the results here are packed\n"
            "\n"
            //"    for (int i = 0; i < PARTIAL_WINDOWS; i++) {\n"
            //"        windowStartPointers[i] = -1;\n"
            //"        windowEndPointers[i] = -1;\n"
            //"    }\n"
            //"\n"
            "    int tupleSize = sizeof(input_tuple_t);\n"
            "    int mapSize = MAP_SIZE;\n"
            "    long paneSize = PANE_SIZE;\n"
            "    long panesPerSlide = PANES_PER_SLIDE;\n"
            "    long panesPerWindow = PANES_PER_WINDOW;\n"
            "    long windowSlide = WINDOW_SLIDE;\n"
            "    long windowSize = WINDOW_SIZE;\n"
            "    int openingWindowsPointer = 0, closingWindowsPointer = 0;\n"
            "    int pendingWindowsPointer = 0, completeWindowsPointer = 0;\n"
            "\n"
            "    // Set the first pointer for all types of windows\n"
            "    openingStartPointers[0] = openingWindowsPointer;\n"
            "    closingStartPointers[0] = closingWindowsPointer;\n"
            "    pendingStartPointers[0] = pendingWindowsPointer;\n"
            "    completeStartPointers[0] = completeWindowsPointer;\n"
            "\n"
            "    // initialize aggregation data structures\n" +
            addTabs(initialiseAggrs, 1) +
            "\n"
            "    // Slicing based on panes logic\n"
            "    // Previous, next, and current pane ids\n"
            "    long prevClosePane, currPane, prevCompletePane, prevOpenPane, startPane;\n"
            "    int numberOfOpeningWindows = 0;\n"
            "    int numberOfClosingWindows = 0;\n"
            "    int numberOfPendingWindows = 0;\n"
            "    int numberOfCompleteWindows = 0;\n"
            "    int currentSlide = 0;\n"
            "    int currentWindow = 0;\n"
            "    long step = 1; //tupleSize;\n"
            "    long streamPtr = streamStartPointer / tupleSize;\n"
            "    long bufferPtr = startPointer / tupleSize;\n"
            "    startPointer = startPointer / tupleSize;\n"
            "    endPointer = endPointer / tupleSize;\n"
            "    long diff = streamPtr - bufferPtr;\n"
            "    long tempStartPos, tempEndPos;\n"
            "\n"
            "    //windowStartPointers[currentSlide++] = bufferPtr;\n"
            "    startPane = (streamPtr / paneSize);\n"
            "    prevClosePane = prevOpenPane = (streamPtr / paneSize) - panesPerSlide;\n"
            "    prevCompletePane = streamPtr / paneSize;\n"
            "    if (streamStartPointer!=0) {\n"
            "        long tmpPtr = streamPtr;\n"
            "        tmpPtr = tmpPtr/windowSlide;\n"
            "        tmpPtr = tmpPtr * windowSlide;\n"
            "        if (streamPtr%windowSlide!=0) {\n"
            "            prevOpenPane = tmpPtr / paneSize;\n"
            "            prevCompletePane = (tmpPtr+windowSlide) / paneSize;\n"
            "        }\n"
            "        while (streamPtr-tmpPtr + windowSlide < windowSize) {\n"
            "            tmpPtr -= windowSlide;\n"
            "        }\n"
            "        prevClosePane = tmpPtr / paneSize;\n"
            "        if (prevClosePane < 0)\n"
            "            prevClosePane = 0;\n"
            "    }\n"
            "\n"
            "    bool completeStartsFromPane = startPane==prevCompletePane;\n"
            "    bool hasComplete = ((endPointer - startPointer) >= windowSize);\n"
            "\n"
            "    // The beginning of the stream. Check if we have at least one complete window.\n"
            "    if (streamPtr == 0) {\n"
            "        // check for opening windows until finding the first complete\n"
            "        while (bufferPtr < endPointer) {\n"
            "            currPane = streamPtr / paneSize;\n"
            "            if (currPane - prevOpenPane == panesPerSlide) {\n"
            "                prevOpenPane += panesPerSlide;\n"
            "                windowStartPointers[currentSlide++] = bufferPtr; // TODO: maybe store bPtr*tupleSize\n"
            "                if (bufferPtr!=0) {\n" +
            addTabs(insertAggrs, 5) +
            addTabs(resetAggrs, 5) +
            //"                    aggrStructures[pid].insert(aggrs);\n"
            //"                    aggrs.reset();\n"
            "                }\n"
            "            }\n"
            "            if (currPane - prevCompletePane == panesPerWindow) {\n"
            "                windowEndPointers[currentWindow++] = bufferPtr;\n"
            "                numberOfCompleteWindows++;\n"
            "                break;\n"
            "            }\n"
            "            // filter, project + aggregate here\n" +
            addTabs(computeAggrs, 3) +
            "            streamPtr += step;\n"
            "            bufferPtr += step;\n"
            "        }\n"
            "    }\n"
            "        // Check for closing and opening windows, until we have a complete window.\n"
            "    else {\n"
            "        auto prevPane = streamPtr / paneSize;\n"
            "        int numOfPartials = 0;\n"
            "        while (bufferPtr < endPointer) {\n"
            "            currPane = streamPtr / paneSize;\n"
            "            if (currPane-prevPane==1) {\n"
            "                prevPane = currPane;\n"
            "                if (numOfPartials==BUCKET_SIZE) // remove the extra values so that we have the first complete window\n"
            +
                addTabs(evictAggrs, 5) +
            addTabs(insertAggrs, 4) +
            addTabs(resetAggrs, 4) +
            //"                    aggrStructures[pid].evict();\n"
            //"                aggrStructures[pid].insert(aggrs);\n"
            //"                aggrs.reset();\n"
            "                numOfPartials++;\n"
            "            }\n"
            "            if (currPane - prevOpenPane == panesPerSlide) { // new slide and possible opening windows\n"
            "                prevOpenPane += panesPerSlide;\n"
            "                windowStartPointers[currentSlide++] = bufferPtr;\n"
            "            }\n"
            "            if (hasComplete && currPane - prevCompletePane == panesPerWindow) { // complete window\n"
            "                windowEndPointers[currentWindow++] = bufferPtr;\n"
            "                numberOfCompleteWindows++;\n"
            "                break;\n"
            "            }\n"
            "            if (prevClosePane <= startPane && currPane - prevClosePane == panesPerWindow) { // closing window\n"
            "                // write result to the closing windows\n" +
            //"                aggrs = aggrStructures[pid].queryIntermediate(PARENTS_SIZE-2);\n" +
            addTabs(closingWindows, 4) +
            "                prevClosePane += panesPerSlide;\n"
            "                closingWindowsPointer += mapSize;\n"
            "                numberOfClosingWindows++;\n"
            "                closingStartPointers[numberOfClosingWindows] = closingWindowsPointer;\n" +
            //"                closingWindowIds[numberOfClosingWindows-1] = prevClosePane - panesPerSlide;\n" +
            addTabs(resetAggrs, 4) +
            //"                aggrs.reset();\n"
            "            }\n"
            "            // filter, project + aggregate here\n" +
            addTabs(computeAggrs, 3) +
            "            streamPtr += step;\n"
            "            bufferPtr += step;\n"
            "        }\n"
            "\n"
            "        // check for pending windows\n"
            "        if (numberOfCompleteWindows == 0) {\n" +
            addTabs(insertAggrs, 3) +
            "            currPane = streamPtr / paneSize;\n"
            "            if (prevClosePane <= startPane && currPane - prevClosePane == panesPerWindow) { // closing window\n"
            "                // write result to the closing windows\n" +
            addTabs(closingWindows, 5) +
            "                closingWindowsPointer += mapSize;\n"
            "                numberOfClosingWindows++;\n"
            "                closingStartPointers[numberOfClosingWindows] = closingWindowsPointer;\n"
            //"                closingWindowIds[numberOfClosingWindows-1] = prevClosePane;\n"
            "                // reset values\n" +
            addTabs(resetAggrs, 4) +
            "            }\n"
            "            // write result to pending windows\n" +
            //"            aggrs = aggrStructures[pid].queryIntermediate(-1);\n" +
            addTabs(pendingWindows, 3) +
            "            pendingWindowsPointer += mapSize;\n"
            "            numberOfPendingWindows++;\n"
            "            pendingStartPointers[numberOfPendingWindows] = pendingWindowsPointer;\n" +
            addTabs(resetAggrs, 3) +
            //"            aggrs.reset();\n"
            "        }\n"
            "    }\n"
            "\n"
            "    if (numberOfCompleteWindows == 0 && (streamStartPointer == 0 || currentSlide >= 1)) { // We only have one opening window so far...\n"
            "        if (streamPtr%windowSlide!=0 && streamStartPointer!=0) {\n" +
            addTabs(evictAggrs, 3) +
            //"            aggrStructures[pid].evict();\n"
            "        }\n"
            "        // write results\n" +
            //"        aggrs = aggrStructures[pid].queryIntermediate(numberOfOpeningWindows);\n" +
            addTabs(openingWindows, 2) +
            "        openingWindowsPointer += mapSize;\n"
            "        numberOfOpeningWindows++;\n"
            "        openingStartPointers[numberOfOpeningWindows] = openingWindowsPointer;\n" +
            //"        openingWindowIds[numberOfOpeningWindows-1] = prevOpenPane++;\n" +
            addTabs(resetAggrs, 2) +
            //"        aggrs.reset();\n"
            "    } else if (numberOfCompleteWindows > 0) {\n"
            "        // write results and pack them for the first complete window in the batch\n" +
            addTabs(setValues, 2)
            //"        aggrs = aggrStructures[pid].query();\n" +
            + addPostWindowOperation(2) +
            "        for (int i = 0; i < mapSize; i++) {\n" +
            addTabs(completeWindows, 3) +
            //"            completeWindowsPointer++;\n"
            "        }\n" +
            addTabs(resetAggrs, 2) +
            //"        aggrs.reset();\n"
            "        // write in the correct slot, as the value has already been incremented!\n"
            "        completeStartPointers[numberOfCompleteWindows] = completeWindowsPointer;\n"
            //"        completeWindowIds[numberOfCompleteWindows-1] = prevCompletePane - 1;\n"
            "        // filter, project + aggregate here\n" +
            addTabs(computeAggrs, 2) +
            "        bufferPtr = windowEndPointers[0];\n"
            "        prevOpenPane = (windowStartPointers[currentSlide-1] +diff) / paneSize;\n"
            "        int idx = 1;\n"
            "        prevCompletePane = (windowStartPointers[idx++]+diff) / paneSize;\n"
            "        int removalIndex = currentWindow; //(startingFromPane) ? currentWindow : currentWindow + 1;\n"
            "        bool foundComplete = false;\n"
            "        while (bufferPtr < endPointer) {\n"
            "            // remove previous slide \n" +
            addTabs(evictAggrs, 3) +
            //"            aggrStructures[pid].evict();\n"
            "\n"
            "            // add elements from the next slide\n"
            "            bufferPtr = windowEndPointers[currentWindow - 1] + 1; // take the next position, as we have already computed this value\n"
            "            foundComplete = false;\n"
            "            auto prevPane = (bufferPtr+diff) / paneSize;\n"
            "            while (true) {\n"
            "                currPane = (bufferPtr+diff) / paneSize;\n"
            "                if (currPane-prevPane==1) {\n"
            "                    prevPane = currPane;\n" +
            addTabs(insertAggrs, 4) +
            addTabs(resetAggrs, 4) +
            //"                    aggrStructures[pid].insert(aggrs);\n"
            //"                    aggrs.reset();\n"
            "                }\n"
            "                if (currPane - prevOpenPane == panesPerSlide) {\n"
            "                    prevOpenPane = currPane;\n"
            "                    windowStartPointers[currentSlide++] = bufferPtr;\n"
            "                }\n"
            "                // complete windows\n"
            "                if (currPane - prevCompletePane == panesPerWindow) {\n"
            "                    //prevPane = currPane;\n"
            "                    prevCompletePane = (windowStartPointers[idx++]+diff) / paneSize;\n"
            "\n"
            "                    windowEndPointers[currentWindow++] = bufferPtr;\n"
            "                    // write and pack the complete window result\n" +
            addTabs(setValues, 5)
            //"                    aggrs = aggrStructures[pid].query();\n"
            + addPostWindowOperation(5) +
            "                    for (int i = 0; i < mapSize; i++) {\n" +
            addTabs(completeWindows, 6) +
            //"                        completeWindowsPointer++;\n"
            "                    }\n" +
            addTabs(resetAggrs, 5) +
            //"                    aggrs.reset();\n"
            "                    numberOfCompleteWindows++;\n"
            "                    completeStartPointers[numberOfCompleteWindows] = completeWindowsPointer;\n"
            //"                    completeWindowIds[numberOfCompleteWindows-1] = prevCompletePane - 1;\n"
            "                    foundComplete = true;\n"
            "                }\n"
            "                if (bufferPtr >= endPointer) {\n"
            "                    break;\n"
            "                }\n"
            "\n"
            "                // filter, project + aggregate here\n" +
            addTabs(computeAggrs, 4) +
            "                bufferPtr += step;\n"
            "                if (foundComplete) {\n"
            "                    break;\n"
            "                }\n"
            "            }\n"
            "            removalIndex++;\n"
            "        }\n"
            "\n"
            "        if (!foundComplete) {  // we have reached the first open window after all the complete ones\n"
            "            // write the first open window if we have already computed the result\n" +
            //addTabs(insertAggrs, 3) +
            //"            aggrStructures[pid].insert(aggrs);\n" +
            //"            aggrs = aggrStructures[pid].queryIntermediate(numberOfOpeningWindows);\n" +
            addTabs(openingWindows, 3) +
            "            openingWindowsPointer += mapSize;\n"
            "            numberOfOpeningWindows++;\n"
            "            openingStartPointers[numberOfOpeningWindows] = openingWindowsPointer;\n"
            //"            openingWindowIds[numberOfOpeningWindows-1] = prevCompletePane++;\n"
            "        } else { // otherwise remove the respective tuples for the first opening window\n"
            "            currentWindow--;\n"
            "        }\n"
            "    }\n"
            "\n"
            "    // compute the rest opening windows\n"
            "    while (currentWindow < currentSlide - 1) {\n"
            //"    while (currentWindow < currentSlide - 1) {\n"
            "        // remove previous slide\n"
            "        tempStartPos = windowStartPointers[currentWindow];\n"
            "        tempEndPos = windowStartPointers[currentWindow + 1];\n"
            "        currentWindow++;\n"
            "        if (tempStartPos == tempEndPos || tempEndPos==endPointer) continue;\n" +
            addTabs(evictAggrs, 2) +
            //"        aggrStructures[pid].evict();            \n"
            "        // write result to the opening windows\n" +
            //"        aggrs = aggrStructures[pid].queryIntermediate(numberOfOpeningWindows);\n" +
            addTabs(openingWindows, 2) +
            "        openingWindowsPointer += mapSize;\n"
            "        numberOfOpeningWindows++;\n"
            "        openingStartPointers[numberOfOpeningWindows] = openingWindowsPointer;\n"
            //"        openingWindowIds[numberOfOpeningWindows-1] = prevCompletePane++;\n"
            "    }\n"
            "\n" +
            addTabs(resultPointers, 1) +
            "    pointersAndCounts[4] = numberOfOpeningWindows;\n"
            "    pointersAndCounts[5] = numberOfClosingWindows;\n"
            "    pointersAndCounts[6] = numberOfPendingWindows;\n"
            "    pointersAndCounts[7] = numberOfCompleteWindows;\n"
            "}\n"
    );
    return s;
  }

  std::string getFillEmptyTumblingWindows(std::string resetAggrs,
                                          std::string closingWindows,
                                          std::string completeWindows) {
    std::string s;
    std::string ptr;
    if (hasGroupBy() && m_usePtrs) {
      ptr.append("*");
    }
    std::string closingDef;
    std::string singleKey = ";";
    if (hasGroupBy()) {
      closingDef = "Bucket";
    } else {
      closingDef = "interm_tuple_t";
      std::string str = "data[bufferPtr-1].timestamp";
      closingWindows.replace(closingWindows.find(str), str.length(), "prevPane * paneSize;");
      completeWindows.replace(completeWindows.find(str), str.length(), "prevPane * paneSize;");
      singleKey = "node aggrs;\n";
    }
    s.append(
        "void fillEmptyWindows (int pid, int phase, int numOfSlices, int numOfOpening, int numOfClosing,\n"
        "                          long *windowStartPointers, long *windowEndPointers,\n"
        "                          int &numberOfOpeningWindows, int &numberOfClosingWindows, int &numberOfCompleteWindows,\n"
        "                          int &currentSlide, int &currentWindow,\n"
        "                          int &completeWindowsPointer, output_tuple_t *completeWindowsResults, int *completeStartPointers,\n"
        "                          int &closingWindowsPointer, " + closingDef + " *" + ptr
            + "closingWindowsResults, int *closingStartPointers,\n"
              //"                           long *openingWindowIds, long *closingWindowIds, long *pendingWindowIds, long *completeWindowIds,\n"
              "                          long &prevCompletePane, long &prevClosePane, long &prevOpenPane, long &currPane) {\n"
              "\n"
              "    auto mapSize = MAP_SIZE;\n"
              "    auto paneSize = PANE_SIZE;\n"
              "    auto panesPerWindow = PANES_PER_WINDOW;\n"
              "    auto panesPerSlide = PANES_PER_SLIDE;\n" +
            addTabs(singleKey, 1) +
            "    if (phase == 1) {\n"
            "        // case 1 -- opening at the beginning\n"
            "        auto prevPane = prevOpenPane + 1;\n"
            "        for (int ptr = 0; ptr < numOfSlices; ++ptr) {\n"
            "            // fillBuckets with empty;\n"
            "            //aggrStructs[pid].insertSlices();\n"
            "            if (prevPane - prevOpenPane == panesPerSlide) {\n"
            "                prevOpenPane += panesPerSlide;\n"
            "                windowStartPointers[currentSlide++] = -1;\n"
            "            }\n"
            "            prevPane++;\n"
            "        }\n"
            "        // add more complete\n"
            "    } else if (phase == 2) {\n"
            "        // case 2 - opening and complete at the beginning of tumbling\n"
            "        auto prevPane = prevOpenPane + 1;\n"
            "        for (int ptr = 0; ptr < numOfSlices; ++ptr) {\n"
            "            //aggrStructs[pid].evictSlices();\n"
            "            // add elements from the next slide\n"
            "            //if (currPane-prevPane==1) {\n"
            "            //    prevPane = currPane;\n"
            "                //aggrStructs[pid].insertSlices();\n"
            "            //}\n"
            "            if (prevPane - prevOpenPane == panesPerSlide) {\n"
            "                prevOpenPane += panesPerSlide;\n"
            "                windowStartPointers[currentSlide++] = -1;\n"
            "            }\n"
            "            // complete windows\n"
            "            if (prevPane - prevCompletePane == panesPerWindow) {\n"
            "                prevCompletePane += panesPerSlide; //data[(windowStartPointers[idx++])].timestamp / paneSize;\n"
            "                windowEndPointers[currentWindow++] = -1;\n"
            "                // write and pack the complete window result\n"
            + addPostWindowOperation(4) +
            "                for (int i = 0; i < mapSize; i++) {\n" +
            addTabs(completeWindows, 5) +
            "                }\n"
            "                // reset values\n" +
            addTabs(resetAggrs, 4) +
            "                numberOfCompleteWindows++;\n"
            "                completeStartPointers[numberOfCompleteWindows] = completeWindowsPointer;\n"
            //"                completeWindowIds[numberOfCompleteWindows-1] = prevCompletePane - 1;\n"
            "            }\n"
            "            prevPane++;\n"
            "        }\n"
            "    } else if (phase == 3) {\n"
            "        // case 3 - closing/opening and complete for tumbling\n"
            "        int ptr = 0;\n"
            "        auto prevPane = prevOpenPane + 1;\n"
            "        for (ptr = 0; ptr < numOfClosing; ++ptr) {\n"
            "            if (prevPane - prevClosePane == panesPerWindow) { // closing window\n"
            "                // write result to the closing windows\n" +
            addTabs(closingWindows, 4) +
            "                prevClosePane += panesPerSlide;\n"
            "                closingWindowsPointer += mapSize;\n"
            "                numberOfClosingWindows++;\n"
            "                closingStartPointers[numberOfClosingWindows] = closingWindowsPointer;\n"
            //"                closingWindowIds[numberOfClosingWindows-1] = prevClosePane - panesPerSlide;\n"
            "                // reset values\n" +
            addTabs(resetAggrs, 4) +
            "            }\n"
            "            prevPane++;\n"
            "        }\n"
            "\n"
            "        while (ptr < numOfSlices) {\n"
            "            // fillBuckets with empty;\n"
            "            if (prevPane - prevOpenPane == panesPerSlide) { // new slide and possible opening windows\n"
            "                prevOpenPane += panesPerSlide;\n"
            "                windowStartPointers[currentSlide++] = -1;\n"
            "            }\n"
            "            if (prevPane - prevCompletePane == panesPerWindow) { // complete window\n"
            "                windowEndPointers[currentWindow++] = -1;\n"
            "                numberOfCompleteWindows++;\n"
            "                break;\n"
            "            }\n"
            "            if (prevPane - prevClosePane == panesPerWindow) { // closing window\n"
            "                // write result to the closing windows\n" +
            addTabs(closingWindows, 4) +
            "                prevClosePane += panesPerSlide;\n"
            "                closingWindowsPointer += mapSize;\n"
            "                numberOfClosingWindows++;\n"
            "                closingStartPointers[numberOfClosingWindows] = closingWindowsPointer;\n"
            //"                closingWindowIds[numberOfClosingWindows-1] = prevClosePane - panesPerSlide;\n"
            "                // reset values\n" +
            addTabs(resetAggrs, 4) +
            "            }\n"
            "            ptr++;\n"
            "            prevPane++;\n"
            "        }\n"
            "\n"
            "\n"
            "        for (; ptr < numOfSlices; ++ptr) {\n" +
            "            // add elements from the next slide\n"
            "            //if (currPane-prevPane==1) {\n"
            "            //    prevPane = currPane;\n"
            "            //    aggrStructs[pid].insertSlices();\n"
            "            //}\n"
            "            if (prevPane - prevOpenPane == panesPerSlide) {\n"
            "                prevOpenPane = currPane;\n"
            "                windowStartPointers[currentSlide++] = -1;\n"
            "            }\n"
            "            // complete windows\n"
            "            if (prevPane - prevCompletePane == panesPerWindow) {\n"
            "                prevCompletePane += panesPerWindow; //data[(windowStartPointers[idx++])].timestamp / paneSize;\n"
            "                windowEndPointers[currentWindow++] = -1;\n"
            + addPostWindowOperation(4) +
            "                // write and pack the complete window result\n" +
            "                for (int i = 0; i < mapSize; i++) {\n" +
            addTabs(completeWindows, 5) +
            "                }\n"
            "                // reset values\n" +
            addTabs(resetAggrs, 4) +
            "                numberOfCompleteWindows++;\n"
            "                completeStartPointers[numberOfCompleteWindows] = completeWindowsPointer;\n"
            //"                completeWindowIds[numberOfCompleteWindows-1] = prevCompletePane - 1;\n"
            "            }\n"
            "            prevPane++;\n"
            "        }\n"
            "    }\n"
            "}\n"
            "\n"
    );
    return s;
  }

  std::string getTumblingWindowRange(std::string outputBuffers,
                                     std::string initialiseAggrs,
                                     std::string computeAggrs,
                                     std::string resetAggrs,
                                     std::string openingWindows,
                                     std::string closingWindows,
                                     std::string pendingWindows,
                                     std::string completeWindows,
                                     std::string resultPointers,
                                     std::string filter) {
    std::string s;
    std::string ptr;
    if (hasGroupBy() && m_usePtrs) {
      ptr.append("*");
    }
    s.append(
        "void processData (int pid, char *inputBuffer, size_t inputBufferSize, long startPointer, long endPointer, long timestampFromPrevBatch,\n"
        "                  long *windowStartPointers, long *windowEndPointers, char *" + ptr
            + "openingWindowsBuffer, char *" + ptr + "closingWindowsBuffer,\n"
                                                     "                  char *" + ptr
            + "pendingWindowsBuffer, char *completeWindowsBuffer,\n"
              "                  int *openingStartPointers, int *closingStartPointers, int *pendingStartPointers, int *completeStartPointers,\n"
              //"                  long *openingWindowIds, long *closingWindowIds, long *pendingWindowIds, long *completeWindowIds,\n"
              "                  long streamStartPointer, int *pointersAndCounts, char *staticBuffer) {"
              "    // Input Buffer\n"
              "    input_tuple_t *data= (input_tuple_t *) inputBuffer;\n"
              "\n"
              "    // Output Buffers\n" +
            addTabs(outputBuffers, 1) +
            "    output_tuple_t *completeWindowsResults = (output_tuple_t *) completeWindowsBuffer; // the results here are packed\n"
            "\n"
            //"    for (int i = 0; i < PARTIAL_WINDOWS; i++) {\n"
            //"        windowStartPointers[i] = -1;\n"
            //"        windowEndPointers[i] = -1;\n"
            //"    }\n"
            //"\n"
            "    int tupleSize = sizeof(input_tuple_t);\n"
            "    int mapSize = MAP_SIZE;\n"
            "    long paneSize = PANE_SIZE;\n"
            "    long panesPerSlide = PANES_PER_SLIDE;\n"
            "    long panesPerWindow = PANES_PER_WINDOW;\n"
            "    long windowSlide = WINDOW_SLIDE;\n"
            "    long windowSize = WINDOW_SIZE;\n"
            "    int openingWindowsPointer = 0, closingWindowsPointer = 0;\n"
            "    int pendingWindowsPointer = 0, completeWindowsPointer = 0;\n"
            "\n"
            "    // Set the first pointer for all types of windows\n"
            "    openingStartPointers[0] = openingWindowsPointer;\n"
            "    closingStartPointers[0] = closingWindowsPointer;\n"
            "    pendingStartPointers[0] = pendingWindowsPointer;\n"
            "    completeStartPointers[0] = completeWindowsPointer;\n"
            "\n"
            "    // initialize aggregation data structures\n" +
            addTabs(initialiseAggrs, 1) +
            "\n"
            "    // Slicing based on panes logic\n"
            "    // Previous, next, and current pane ids\n"
            "    long prevClosePane, currPane, prevCompletePane, prevOpenPane, startPane;\n"
            "    int numberOfOpeningWindows = 0;\n"
            "    int numberOfClosingWindows = 0;\n"
            "    int numberOfPendingWindows = 0;\n"
            "    int numberOfCompleteWindows = 0;\n"
            "    int currentSlide = 0;\n"
            "    int currentWindow = 0;\n"
            "    long step = 1; //tupleSize;\n"
            "    long streamPtr = streamStartPointer / tupleSize;\n"
            "    long bufferPtr = startPointer / tupleSize;\n"
            "    startPointer = startPointer / tupleSize;\n"
            "    endPointer = endPointer / tupleSize;\n"
            "    long diff = streamPtr - bufferPtr;\n"
            "    long tempStartPos, tempEndPos;\n"
            "\n"
            "    //windowStartPointers[currentSlide++] = bufferPtr;\n"
            "    bool hasAddedComplete = false;\n"
            #if defined(HAVE_NUMA)
            "    auto bufferSize = (long) inputBufferSize;\n"
            #else
            "    auto bufferSize = (long) BUFFER_SIZE;\n"
            //                "    timestampFromPrevBatch = (bufferPtr != 0) ? data[bufferPtr - 1].timestamp / paneSize :\n"
            //                "            data[bufferSize / sizeof(input_tuple_t) - 1].timestamp / paneSize;"
            #endif
            "    long prevPane =  timestampFromPrevBatch / paneSize;\n"
            "\n"
            "    startPane = (data[bufferPtr].timestamp / paneSize);\n"
            "    prevClosePane = prevOpenPane = (data[bufferPtr].timestamp / paneSize) - panesPerSlide;\n"
            "    prevCompletePane = data[bufferPtr].timestamp / paneSize;\n"
            "    if (streamStartPointer!=0) {\n"
            "        prevOpenPane = timestampFromPrevBatch / paneSize;\n"
            "        long tmpPtr = data[bufferPtr].timestamp;\n"
            "        tmpPtr = tmpPtr/windowSlide;\n"
            "        tmpPtr = tmpPtr * windowSlide;\n"
            "        if (data[bufferPtr].timestamp%windowSlide!=0) {\n"
            "            prevOpenPane = tmpPtr / paneSize;\n"
            "            prevCompletePane = (tmpPtr+windowSlide) / paneSize;\n"
            "        }\n"
            "        prevClosePane = timestampFromPrevBatch / paneSize;\n"
            "        if (prevOpenPane == prevCompletePane)\n"
            "            prevCompletePane += panesPerSlide;\n"
            "        //while (data[bufferPtr].timestamp-tmpPtr + windowSlide <= windowSize) {\n"
            "        //    tmpPtr -= windowSlide;\n"
            "        //}\n"
            "        //prevClosePane = tmpPtr / paneSize;\n"
            "        if (prevClosePane < 0)\n"
            "            prevClosePane = 0;\n"
            "    }\n"
            "\n"
            "    bool completeStartsFromPane = startPane==prevCompletePane;\n"
            "    bool hasComplete = ((data[endPointer-1].timestamp - data[startPointer].timestamp) >= windowSize);\n"
            "\n"
            "    // The beginning of the stream. Check if we have at least one complete window.\n"
            "    if (streamPtr == 0) {\n"
            "        // check for opening windows until finding the first complete\n"
            "        while (bufferPtr < endPointer) {\n");
    if (!filter.empty())
      s.append(
          addTabs(filter, 3) +
              "\t\t\t{\n");
    s.append(
        "            currPane = data[bufferPtr].timestamp / paneSize;\n"
        "            if (currPane - prevOpenPane == panesPerSlide) {\n"
        "                prevOpenPane += panesPerSlide;\n"
        "                windowStartPointers[currentSlide++] = bufferPtr;\n"
        "            }\n"
        "            if (currPane - prevCompletePane == panesPerWindow) {\n"
        "                windowEndPointers[currentWindow++] = bufferPtr;\n"
        "                numberOfCompleteWindows++;\n"
        "                break;\n"
        "            }\n"
        "            if (currPane - prevOpenPane > panesPerSlide || currPane - prevCompletePane > panesPerWindow) {\n"
        "                // fill bubbles\n"
        "                int numOfSlices, numOfComplete, numOfOpening, phase;\n"
        "                phase = 1;\n"
        "                //if (currPane - prevOpenPane > panesPerSlide) {\n"
        "                    numOfSlices = currPane - prevOpenPane; // - panesPerSlide -1;\n"
        "                    numOfComplete = currPane+numOfSlices-panesPerWindow;\n"
        "                //} else {\n"
        "                //    numOfSlices = currPane - prevCompletePane - panesPerWindow -1;\n"
        "                //    numOfComplete = currPane+numOfSlices-panesPerWindow;\n"
        "                //}\n"
        "                if (numOfComplete > 0) {\n"
        "                    numOfOpening =  prevCompletePane + panesPerWindow - prevOpenPane;\n"
        "                    //phase = 2;\n"
        "                    hasAddedComplete = true;\n"
        "                }\n"
        "                fillEmptyWindows(pid, phase, numOfSlices, numOfOpening, 0, windowStartPointers,\n"
        "                        windowEndPointers, numberOfOpeningWindows, numberOfClosingWindows, numberOfCompleteWindows,\n"
        "                                    currentSlide, currentWindow,\n"
        "                                    completeWindowsPointer, completeWindowsResults, completeStartPointers,\n"
        "                                    closingWindowsPointer, closingWindowsResults, closingStartPointers,\n"
        //"                                    openingWindowIds, closingWindowIds, pendingWindowIds, completeWindowIds,\n"
        "                                    prevCompletePane, prevClosePane, prevOpenPane, currPane);\n"
        "            }\n"
        "            // project + aggregate here\n" +
            addTabs(computeAggrs, 3));
    if (!filter.empty())
      s.append("\t\t\t};\n");
    s.append(
        "            streamPtr += step;\n"
        "            bufferPtr += step;\n"
        "        }\n"
        "    }\n"
        "        // Check for closing and opening windows, until we have a complete window.\n"
        "    else {\n"
        "        while (bufferPtr < endPointer) {\n");
    if (!filter.empty())
      s.append(
          addTabs(filter, 3) +
              "\t\t\t{\n");
    s.append(
        "            currPane = data[bufferPtr].timestamp / paneSize;\n"
        "            if (currPane - prevOpenPane == panesPerSlide) { // new slide and possible opening windows\n"
        "                prevOpenPane += panesPerSlide;\n"
        "                windowStartPointers[currentSlide++] = bufferPtr;\n"
        "            }\n"
        "            if (hasComplete && currPane - prevCompletePane == panesPerWindow) { // complete window\n"
        "                windowEndPointers[currentWindow++] = bufferPtr;\n"
        "                numberOfCompleteWindows++;\n"
        "                break;\n"
        "            }\n"
        "            if (/*prevClosePane >= panesPerWindow &&*/ prevClosePane <= startPane && currPane - prevClosePane == panesPerWindow) { // closing window\n"
        "                // write result to the closing windows\n" +
            addTabs(closingWindows, 4) +
            "                prevClosePane += panesPerSlide;\n"
            "                closingWindowsPointer += mapSize;\n"
            "                numberOfClosingWindows++;\n"
            "                closingStartPointers[numberOfClosingWindows] = closingWindowsPointer;\n"
            //"                closingWindowIds[numberOfClosingWindows-1] = prevClosePane - panesPerSlide;\n"
            "                // reset values\n" +
            addTabs(resetAggrs, 4) +
            "            }\n"
            "            if (currPane - prevOpenPane > panesPerSlide || currPane - prevClosePane > panesPerWindow || currPane - prevCompletePane > panesPerWindow) {\n"
            "                // fill bubbles\n"
            "                int numOfSlices, numOfComplete, numOfOpening, numOfClosing,  phase;\n"
            "                numOfClosing = 0;\n"
            "                phase = 3;\n"
            "                if (currPane - prevOpenPane > panesPerSlide) {\n"
            "                    numOfSlices = currPane - prevPane; //prevOpenPane - panesPerSlide -1;\n"
            "                    numOfComplete = prevPane+numOfSlices-(panesPerWindow+prevCompletePane);\n"
            "                } else if (currPane - prevClosePane > panesPerWindow) {\n"
            "                    numOfSlices = currPane - prevClosePane - panesPerWindow -1;\n"
            "                    numOfClosing = currPane - prevPane;\n"
            "                } else {\n"
            "                    numOfSlices = currPane - prevCompletePane - panesPerWindow -1;\n"
            "                    numOfComplete = currPane+numOfSlices-panesPerWindow;\n"
            "                }\n"
            "                if (numOfComplete > 0) {\n"
            "                    hasAddedComplete = true;\n"
            "                }\n"
            "                fillEmptyWindows(pid, phase, numOfSlices, numOfOpening, numOfClosing, windowStartPointers,\n"
            "                                    windowEndPointers, numberOfOpeningWindows, numberOfClosingWindows, numberOfCompleteWindows,\n"
            "                                    currentSlide, currentWindow,\n"
            "                                    completeWindowsPointer, completeWindowsResults, completeStartPointers,\n"
            "                                    closingWindowsPointer, closingWindowsResults, closingStartPointers,\n"
            //"                                    openingWindowIds, closingWindowIds, pendingWindowIds, completeWindowIds,\n"
            "                                    prevCompletePane, prevClosePane, prevOpenPane, currPane);\n"
            "                if (hasAddedComplete)\n"
            "                    break;\n"
            "            }\n"
            "            // project + aggregate here\n" +
            addTabs(computeAggrs, 3));
    if (!filter.empty())
      s.append("\t\t\t}\n");
    s.append(
        "            streamPtr += step;\n"
        "            bufferPtr += step;\n"
        "        }\n"
        "\n"
        "        // check for pending windows\n"
        "        if ((numberOfClosingWindows == 0 || windowSize!=windowSlide) && numberOfCompleteWindows == 0) {\n"
        //"            currPane = data[bufferPtr].timestamp / paneSize;\n"
        //"            if (/*prevClosePane >= panesPerWindow &&*/ prevClosePane <= startPane && currPane - prevClosePane == panesPerWindow) { // closing window\n"
        //"                // write result to the closing windows\n" +
        //addTabs(closingWindows, 5) +
        //"                closingWindowsPointer += mapSize;\n"
        //"                numberOfClosingWindows++;\n"
        //"                closingStartPointers[numberOfClosingWindows] = closingWindowsPointer;\n"
        //"                // reset values\n" +
        //addTabs(resetAggrs, 4) +
        //"            }\n"
        "            // write result to pending windows\n" +
            addTabs(pendingWindows, 3) +
            "            pendingWindowsPointer += mapSize;\n"
            "            numberOfPendingWindows++;\n"
            "            pendingStartPointers[numberOfPendingWindows] = pendingWindowsPointer;\n"
            "            // reset values\n" +
            addTabs(resetAggrs, 3) +
            "        }\n"
            "    }\n"
            "\n"
            "    if (numberOfCompleteWindows == 0 && (streamStartPointer == 0 || currentSlide >= 1)) { // We only have one opening window so far...\n"
            "        // write results\n" +
            addTabs(openingWindows, 2) +
            "        openingWindowsPointer += mapSize;\n"
            "        numberOfOpeningWindows++;\n"
            "        openingStartPointers[numberOfOpeningWindows] = openingWindowsPointer;\n"
            //"        openingWindowIds[numberOfOpeningWindows-1] = prevOpenPane++;\n"
            "\n"
            "    } else if (numberOfCompleteWindows > 0) {\n"
            "        // write results and pack them for the first complete window in the batch\n"
            "        if (!hasAddedComplete) {\n"
            + addPostWindowOperation(2) +
            "        for (int i = 0; i < mapSize; i++) {\n" +
            addTabs(completeWindows, 3) +
            //"            completeWindowsPointer++;\n"
            "        }\n"
            "        // reset values\n" +
            addTabs(resetAggrs, 2) +
            "        // write in the correct slot, as the value has already been incremented!\n"
            "        completeStartPointers[numberOfCompleteWindows] = completeWindowsPointer;\n"
            //"        completeWindowIds[numberOfCompleteWindows-1] = prevCompletePane - 1;\n"
            "        }\n"
            "        // project + aggregate here\n");
    if (!filter.empty())
      s.append(
          addTabs(filter, 2) +
              "\t\t{\n");
    s.append(
        addTabs(computeAggrs, 2));
    if (!filter.empty())
      s.append("\t\t}\n");
    s.append(
        "\n"
        "        bufferPtr = windowEndPointers[0];\n"
        "        //prevOpenPane = (data[windowStartPointers[currentSlide-1]].timestamp) / paneSize;\n"
        "        int idx = 1;\n"
        "        prevCompletePane += panesPerSlide; //(data[windowStartPointers[idx++]].timestamp) / paneSize;\n"
        "        int removalIndex = currentWindow; //(startingFromPane) ? currentWindow : currentWindow + 1;\n"
        "        bool foundComplete = false;\n"
        "        while (bufferPtr < endPointer) {\n"
        "            // add elements from the next slide\n"
        "            //bufferPtr = windowEndPointers[currentWindow - 1] + 1; // take the next position, as we have already computed this value\n"
        "            foundComplete = false;\n"
        "            while (true) {\n");
    if (!filter.empty())
      s.append(
          addTabs(filter, 4) +
              "\t\t\t{\n");
    s.append(
        "                auto dataPtr = bufferPtr;\n"
        "                if (bufferPtr*tupleSize >= bufferSize)\n"
        "                    dataPtr -= bufferSize/tupleSize;\n"
        "                currPane = data[dataPtr].timestamp / paneSize;"
        "                if (currPane - prevOpenPane == panesPerSlide) {\n"
        "                    prevOpenPane = currPane;\n"
        "                    windowStartPointers[currentSlide++] = dataPtr;\n"
        "                }\n"
        "                // complete windows\n"
        "                if (currPane - prevCompletePane == panesPerWindow) {\n"
        "                    //prevPane = currPane;\n"
        "                    prevCompletePane += panesPerSlide; //data[(windowStartPointers[idx++])].timestamp / paneSize;\n"
        "\n"
        "                    windowEndPointers[currentWindow++] = dataPtr;\n"
        "                    // write and pack the complete window result\n"
        "                    //map.setValues();\n"
            + addPostWindowOperation(5) +
        "                    for (int i = 0; i < mapSize; i++) {\n" +
            addTabs(completeWindows, 6) +
            //"                        completeWindowsPointer++;\n"
            "                    }\n"
            "                    numberOfCompleteWindows++;\n"
            "                    completeStartPointers[numberOfCompleteWindows] = completeWindowsPointer;\n"
            //"                    completeWindowIds[numberOfCompleteWindows-1] = prevCompletePane - 1;\n"
            "                    foundComplete = true;\n"
            "                    // reset values\n" +
            addTabs(resetAggrs, 5) +
            "                }\n"
            "                if (currPane - prevOpenPane > panesPerSlide || currPane - prevCompletePane > panesPerWindow) {\n"
            "                    // fill bubbles\n"
            "                    int numOfSlices, numOfComplete, numOfOpening, phase;\n"
            "                    phase = 2;\n"
            "                    //if (currPane - prevOpenPane > panesPerSlide) {\n"
            "                        numOfSlices = currPane - prevPane; //prevOpenPane - panesPerSlide -1;\n"
            "                        numOfComplete = prevPane+numOfSlices-(panesPerWindow+prevCompletePane);\n"
            "                    //} else {\n"
            "                    //    numOfSlices = currPane - prevCompletePane - panesPerWindow -1;\n"
            "                    //    numOfComplete = currPane+numOfSlices-panesPerWindow;\n"
            "                    //}\n"
            "                    if (numOfComplete > 0) {\n"
            "\n"
            "                        numOfOpening =  prevCompletePane + panesPerWindow - prevOpenPane;\n"
            "                        foundComplete = true;\n"
            "                    }\n"
            "                    fillEmptyWindows(pid, phase, numOfSlices, numOfOpening, 0, windowStartPointers,\n"
            "                                        windowEndPointers, numberOfOpeningWindows, numberOfClosingWindows, numberOfCompleteWindows,\n"
            "                                        currentSlide, currentWindow,\n"
            "                                        completeWindowsPointer, completeWindowsResults, completeStartPointers,\n"
            "                                        closingWindowsPointer, closingWindowsResults, closingStartPointers,\n"
            //"                                        openingWindowIds, closingWindowIds, pendingWindowIds, completeWindowIds,\n"
            "                                        prevCompletePane, prevClosePane, prevOpenPane, currPane);\n"
            "                }\n"
            "                if (bufferPtr >= endPointer) {\n"
            "                    break;\n"
            "                }\n"
            "                // project + aggregate here\n" +
            addTabs(computeAggrs, 4));
    if (!filter.empty())
      s.append("\t\t\t}\n");
    s.append(
        "                bufferPtr += step;\n"
        "                if (foundComplete) {\n"
        "                    break;\n"
        "                }\n"
        "            }\n"
        "            removalIndex++;\n"
        "        }\n"
        "\n"
        "        if (!foundComplete) {  // we have reached the first open window after all the complete ones\n"
        "            // write the first open window if we have already computed the result\n" +
            addTabs(openingWindows, 3) +
            "            openingWindowsPointer += mapSize;\n"
            "            numberOfOpeningWindows++;\n"
            "            openingStartPointers[numberOfOpeningWindows] = openingWindowsPointer;\n"
            //"            openingWindowIds[numberOfOpeningWindows-1] = prevCompletePane++;\n"
            "        }\n"
            "    }\n"
            "\n" +
            addTabs(resultPointers, 1) +
            "    pointersAndCounts[4] = numberOfOpeningWindows;\n"
            "    pointersAndCounts[5] = numberOfClosingWindows;\n"
            "    pointersAndCounts[6] = numberOfPendingWindows;\n"
            "    pointersAndCounts[7] = numberOfCompleteWindows;\n"
            "}\n");
    return s;
  }

  std::string getFillEmptySlidingWindows(std::string insertAggrs,
                                         std::string evictAggrs,
                                         std::string resetAggrs,
                                         std::string setValues,
                                         std::string closingWindows,
                                         std::string completeWindows) {
    std::string s;
    std::string ptr;
    if (hasGroupBy() && m_usePtrs) {
      ptr.append("*");
    }
    std::string closingDef;
    std::string singleKey = ";";
    if (hasGroupBy()) {
      closingDef = "Bucket";
    } else {
      closingDef = "interm_tuple_t";
      std::string str1 = "data[bufferPtr-1].timestamp";
      std::string str2 = "prevCompletePane";
      closingWindows.replace(closingWindows.find(str1), str1.length(), "prevPane * paneSize;");
      completeWindows.replace(completeWindows.find(str2), str2.length(), "prevPane");
      singleKey = "node aggrs;\n";
    }
    s.append(
        "void fillEmptyWindows (int pid, int phase, int numOfSlices, int numOfOpening, int numOfClosing,\n"
        "                          long *windowStartPointers, long *windowEndPointers,\n"
        "                          int &numberOfOpeningWindows, int &numberOfClosingWindows, int &numberOfCompleteWindows,\n"
        "                          long bufferPtr, int &currentSlide, int &currentWindow,\n"
        "                          int &completeWindowsPointer, output_tuple_t *completeWindowsResults, int *completeStartPointers,\n"
        "                          int &closingWindowsPointer, " + closingDef + " *" + ptr
            + "closingWindowsResults, int *closingStartPointers,\n"
              //"                          long *openingWindowIds, long *closingWindowIds, long *pendingWindowIds, long *completeWindowIds,\n"
              "                          long &prevCompletePane, long &prevClosePane, long &prevOpenPane, long &currPane) {\n"
              "\n"
              "    auto mapSize = MAP_SIZE;\n"
              "    auto paneSize = PANE_SIZE;\n"
              "    auto panesPerWindow = PANES_PER_WINDOW;\n"
              "    auto panesPerSlide = PANES_PER_SLIDE;\n" +
            addTabs(singleKey, 1) +
            "    // store previous results\n" +
            addTabs(insertAggrs, 1) +
            addTabs(resetAggrs, 1) +
            "    if (phase == 1) {\n"
            "        // case 1 -- opening at the beginning\n"
            "        auto prevPane = prevOpenPane + 1;\n"
            "        int ptr;\n"
            "        for (ptr = 0; ptr < numOfOpening; ++ptr) {\n"
            "            // fillBuckets with empty;\n"
            "            //aggrStructs[pid].insertSlices();\n"
            "            if (prevPane - prevOpenPane == panesPerSlide) {\n"
            "                prevOpenPane += panesPerSlide;\n"
            "                if (ptr == 0) {\n"
            "                    windowStartPointers[currentSlide++] = bufferPtr;\n"
            "                } else {\n"
            "                    windowStartPointers[currentSlide++] = -1;\n"
            "                }\n"
            "            }\n"
            "            prevPane++;\n"
            "        }\n"
            "        if (numOfOpening!=numOfSlices) {\n"
            "            // write results and pack them for the first complete window in the batch\n" +
            addTabs(setValues, 3)
            + addPostWindowOperation(3) +
            "            for (int i = 0; i < mapSize; i++) {\n" +
            addTabs(completeWindows, 4) +
            "            }\n"
            "            // reset values\n" +
            addTabs(resetAggrs, 3) +
            "            // write in the correct slot, as the value has already been incremented!\n"
            "            completeStartPointers[numberOfCompleteWindows] = completeWindowsPointer;\n"
            "            prevCompletePane += panesPerSlide;\n"
            //"            completeWindowIds[numberOfCompleteWindows-1] = prevCompletePane - 1;\n"
            "            numberOfCompleteWindows++;\n "
            "\n"
            "            for (; ptr < numOfSlices; ++ptr) {\n" +
            addTabs(evictAggrs, 4) +
            "                if (prevPane - prevOpenPane == panesPerSlide) {\n"
            "                    prevOpenPane += panesPerSlide;\n"
            "                    windowStartPointers[currentSlide++] = -1;\n"
            "                }\n"
            "                // complete windows\n"
            "                if (prevPane - prevCompletePane == panesPerWindow) {\n"
            "                    prevCompletePane += panesPerSlide; //data[(windowStartPointers[idx++])].timestamp / paneSize;\n"
            "                    windowEndPointers[currentWindow++] = -1;\n"
            "                    // write and pack the complete window result\n" +
            addTabs(setValues, 5)
            + addPostWindowOperation(4) +
            "                    for (int i = 0; i < mapSize; i++) {\n" +
            addTabs(completeWindows, 6) +
            "                    }\n"
            "                    // reset values\n" +
            addTabs(resetAggrs, 5) +
            "                    numberOfCompleteWindows++;\n"
            "                    completeStartPointers[numberOfCompleteWindows] = completeWindowsPointer;\n"
            //"                    completeWindowIds[numberOfCompleteWindows-1] = prevCompletePane - 1;\n"
            "                }\n"
            "                prevPane++;\n"
            "            }\n"
            "\n"
            "        }\n"
            "        // add more complete\n"
            "    } else if (phase == 2) {\n"
            "        // case 2 - opening and complete at the beginning of tumbling\n"
            "        auto prevPane = prevOpenPane + 1;\n"
            "        for (int ptr = 0; ptr < numOfSlices; ++ptr) {\n" +
            addTabs(evictAggrs, 3) +
            "            // add elements from the next slide\n"
            "            //if (currPane-prevPane==1) {\n"
            "            //    prevPane = currPane;\n"
            "                //aggrStructs[pid].insertSlices();\n"
            "            //}\n"
            "            if (prevPane - prevOpenPane == panesPerSlide) {\n"
            "                prevOpenPane += panesPerSlide;\n"
            "                if (ptr == 0) {\n"
            "                    windowStartPointers[currentSlide++] = bufferPtr;\n"
            "                } else {\n"
            "                    windowStartPointers[currentSlide++] = -1;\n"
            "                }\n"
            "            }\n"
            "            // complete windows\n"
            "            if (prevPane - prevCompletePane == panesPerWindow) {\n"
            "                prevCompletePane += panesPerSlide; //data[(windowStartPointers[idx++])].timestamp / paneSize;\n"
            "                windowEndPointers[currentWindow++] = -1;\n"
            "                // write and pack the complete window result\n" +
            addTabs(setValues, 4)
            + addPostWindowOperation(4) +
            "                for (int i = 0; i < mapSize; i++) {\n" +
            addTabs(completeWindows, 5) +
            "                }\n"
            "                // reset values\n" +
            addTabs(resetAggrs, 4) +
            "                numberOfCompleteWindows++;\n"
            "                completeStartPointers[numberOfCompleteWindows] = completeWindowsPointer;\n"
            //"                completeWindowIds[numberOfCompleteWindows-1] = prevCompletePane - 1;\n"
            "            }\n"
            "            prevPane++;\n"
            "        }\n"
            "    } else if (phase == 3) {\n"
            "        // case 3 - closing/opening and complete for tumbling\n"
            "        int ptr = 0;\n"
            "        auto prevPane = prevOpenPane + 1;\n"
            "        for (ptr = 0; ptr < numOfClosing; ++ptr) {\n"
            "            if (prevPane - prevClosePane == panesPerWindow) { // closing window\n"
            "                // write result to the closing windows\n" +
            addTabs(closingWindows, 4) +
            "                prevClosePane += panesPerSlide;\n"
            "                closingWindowsPointer += mapSize;\n"
            "                numberOfClosingWindows++;\n"
            "                closingStartPointers[numberOfClosingWindows] = closingWindowsPointer;\n"
            //"                closingWindowIds[numberOfClosingWindows-1] = prevClosePane - panesPerSlide;\n"
            "                // reset values\n" +
            addTabs(resetAggrs, 4) +
            "            }\n"
            "            prevPane++;\n"
            "        }\n"
            "\n"
            "        while (ptr < numOfSlices) {\n"
            "            // fillBuckets with empty;\n"
            "            if (prevPane - prevOpenPane == panesPerSlide) { // new slide and possible opening windows\n"
            "                prevOpenPane += panesPerSlide;\n"
            "                if (ptr == 0) {\n"
            "                    windowStartPointers[currentSlide++] = bufferPtr;\n"
            "                } else {\n"
            "                    windowStartPointers[currentSlide++] = -1;\n"
            "                }\n"
            "            }\n"
            "            if (prevPane - prevCompletePane == panesPerWindow) { // complete window\n"
            "                windowEndPointers[currentWindow++] = -1;\n"
            "                numberOfCompleteWindows++;\n"
            "                ptr++;\n"
            "                prevPane++;\n"
            "                break;\n"
            "            }\n"
            "            if (prevPane - prevClosePane == panesPerWindow) { // closing window\n"
            "                // write result to the closing windows\n" +
            addTabs(closingWindows, 4) +
            "                prevClosePane += panesPerSlide;\n"
            "                closingWindowsPointer += mapSize;\n"
            "                numberOfClosingWindows++;\n"
            "                closingStartPointers[numberOfClosingWindows] = closingWindowsPointer;\n"
            //"                closingWindowIds[numberOfClosingWindows-1] = prevClosePane - panesPerSlide;\n"
            "                // reset values\n" +
            addTabs(resetAggrs, 4) +
            "            }\n"
            "            ptr++;\n"
            "            prevPane++;\n"
            "        }\n"
            "\n"
            "        if (ptr!=numOfSlices) {\n"
            "            // write results and pack them for the first complete window in the batch\n" +
            addTabs(setValues, 3)
            + addPostWindowOperation(3) +
            "            for (int i = 0; i < mapSize; i++) {\n" +
            addTabs(completeWindows, 4) +
            "            }\n"
            "            // reset values\n" +
            addTabs(resetAggrs, 3) +
            "            // write in the correct slot, as the value has already been incremented!\n"
            "            completeStartPointers[numberOfCompleteWindows] = completeWindowsPointer;\n"
            "            prevCompletePane += panesPerSlide;\n"
            //"            completeWindowIds[numberOfCompleteWindows-1] = prevCompletePane - 1;\n"
            "            numberOfCompleteWindows++;\n"
            "        }\n"
            "\n"
            "        for (; ptr < numOfSlices; ++ptr) {\n" +
            addTabs(evictAggrs, 3) +
            "            if (prevPane - prevOpenPane == panesPerSlide) {\n"
            "                prevOpenPane = currPane;\n"
            "                windowStartPointers[currentSlide++] = -1;\n"
            "            }\n"
            "            // complete windows\n"
            "            if (prevPane - prevCompletePane == panesPerWindow) {\n"
            "                prevCompletePane += panesPerSlide; //data[(windowStartPointers[idx++])].timestamp / paneSize;\n"
            "                windowEndPointers[currentWindow++] = -1;\n"
            "                // write and pack the complete window result\n" +
            addTabs(setValues, 4)
            + addPostWindowOperation(4) +
            "                for (int i = 0; i < mapSize; i++) {\n" +
            addTabs(completeWindows, 5) +
            "                }\n"
            "                // reset values\n" +
            addTabs(resetAggrs, 4) +
            "                numberOfCompleteWindows++;\n"
            "                completeStartPointers[numberOfCompleteWindows] = completeWindowsPointer;\n"
            //"                completeWindowIds[numberOfCompleteWindows-1] = prevCompletePane - 1;\n"
            "            }\n"
            "            prevPane++;\n"
            "        }\n"
            "    }\n"
            "}\n"
            "\n"
    );
    return s;
  }

  std::string getSlidingWindowRange(std::string outputBuffers,
                                    std::string initialiseAggrs,
                                    std::string computeAggrs,
                                    std::string insertAggrs,
                                    std::string evictAggrs,
                                    std::string resetAggrs,
                                    std::string setValues,
                                    std::string openingWindows,
                                    std::string closingWindows,
                                    std::string pendingWindows,
                                    std::string completeWindows,
                                    std::string resultPointers,
                                    std::string filter) {
    std::string s;
    std::string ptr;
    if (hasGroupBy() && m_usePtrs) {
      ptr.append("*");
    }
    s.append(
        "void processData (int pid, char *inputBuffer, size_t inputBufferSize, long startPointer, long endPointer, long timestampFromPrevBatch,\n"
        "                  long *windowStartPointers, long *windowEndPointers, char *" + ptr
            + "openingWindowsBuffer, char *" + ptr + "closingWindowsBuffer,\n"
                                                     "                  char *" + ptr
            + "pendingWindowsBuffer, char *completeWindowsBuffer,\n"
              "                  int *openingStartPointers, int *closingStartPointers, int *pendingStartPointers, int *completeStartPointers,\n"
              //"                  long *openingWindowIds, long *closingWindowIds, long *pendingWindowIds, long *completeWindowIds,\n"
              "                  long streamStartPointer, int *pointersAndCounts, char *staticBuffer) {"
              "\n"
              "    // Input Buffer\n"
              "    input_tuple_t *data= (input_tuple_t *) inputBuffer;\n"
              "\n"
              "    // Output Buffers\n" +
            addTabs(outputBuffers, 1) +
            "    output_tuple_t *completeWindowsResults = (output_tuple_t *) completeWindowsBuffer; // the results here are packed\n"
            "\n"
            //"    for (int i = 0; i < PARTIAL_WINDOWS; i++) {\n"
            //"        windowStartPointers[i] = -1;\n"
            //"        windowEndPointers[i] = -1;\n"
            //"    }\n"
            //"\n"
            "    int tupleSize = sizeof(input_tuple_t);\n"
            "    int mapSize = MAP_SIZE;\n"
            "    long paneSize = PANE_SIZE;\n"
            "    long panesPerSlide = PANES_PER_SLIDE;\n"
            "    long panesPerWindow = PANES_PER_WINDOW;\n"
            "    long windowSlide = WINDOW_SLIDE;\n"
            "    long windowSize = WINDOW_SIZE;\n"
            "    int openingWindowsPointer = 0, closingWindowsPointer = 0;\n"
            "    int pendingWindowsPointer = 0, completeWindowsPointer = 0;\n"
            "\n"
            "    // Set the first pointer for all types of windows\n"
            "    openingStartPointers[0] = openingWindowsPointer;\n"
            "    closingStartPointers[0] = closingWindowsPointer;\n"
            "    pendingStartPointers[0] = pendingWindowsPointer;\n"
            "    completeStartPointers[0] = completeWindowsPointer;\n"
            "\n"
            "    // initialize aggregation data structures\n" +
            addTabs(initialiseAggrs, 1) +
            "\n"
            "    // Slicing based on panes logic\n"
            "    // Previous, next, and current pane ids\n"
            "    long prevClosePane, currPane, prevCompletePane, prevOpenPane, startPane;\n"
            "    int numberOfOpeningWindows = 0;\n"
            "    int numberOfClosingWindows = 0;\n"
            "    int numberOfPendingWindows = 0;\n"
            "    int numberOfCompleteWindows = 0;\n"
            "    int currentSlide = 0;\n"
            "    int currentWindow = 0;\n"
            "    long step = 1; //tupleSize;\n"
            "    long streamPtr = streamStartPointer / tupleSize;\n"
            "    long bufferPtr = startPointer / tupleSize;\n"
            "    startPointer = startPointer / tupleSize;\n"
            "    endPointer = endPointer / tupleSize;\n"
            "    long diff = streamPtr - bufferPtr;\n"
            "    long tempStartPos, tempEndPos;\n"
            "\n"
            "    //windowStartPointers[currentSlide++] = bufferPtr;\n"
            "    bool hasAddedComplete = false;\n"
            #if defined(HAVE_NUMA)
            "    auto bufferSize = (long) inputBufferSize;\n"
            #else
            "    auto bufferSize = (long) BUFFER_SIZE;\n"
            //                "    timestampFromPrevBatch = (bufferPtr != 0) ? data[bufferPtr - 1].timestamp / paneSize :\n"
            //                "            data[bufferSize / sizeof(input_tuple_t) - 1].timestamp / paneSize;"
            #endif
            "    long prevBatchPane = timestampFromPrevBatch / paneSize;\n"
            "    if (streamStartPointer==0)\n"
            "        prevBatchPane = (data[bufferPtr].timestamp / paneSize) - panesPerSlide;\n"
            "    startPane = (data[bufferPtr].timestamp / paneSize);\n"
            "    prevClosePane = prevOpenPane = (data[bufferPtr].timestamp / paneSize) - panesPerSlide;\n"
            "    prevCompletePane = data[bufferPtr].timestamp / paneSize;\n"
            "    if (streamStartPointer!=0) {\n"
            "        prevOpenPane = timestampFromPrevBatch / paneSize;\n"
            "        long tmpPtr = data[bufferPtr].timestamp;\n"
            "        tmpPtr = tmpPtr/windowSlide;\n"
            "        tmpPtr = tmpPtr * windowSlide;\n"
            "        if (data[bufferPtr].timestamp%windowSlide!=0) {\n"
            "            prevOpenPane = tmpPtr / paneSize;\n"
            "            prevCompletePane = (tmpPtr+windowSlide) / paneSize;\n"
            "        }\n"
            "        long tempTimestamp = timestampFromPrevBatch; // / paneSize;\n"
            "        while (tempTimestamp-tmpPtr + windowSlide < windowSize) {\n"
            "            tmpPtr -= windowSlide;\n"
            "        }\n"
            "        prevClosePane = tmpPtr / paneSize;\n"
            "        if (prevClosePane < 0)\n"
            "            prevClosePane = 0;\n"
            "    }\n"
            "\n"
            "    bool completeStartsFromPane = startPane==prevCompletePane;\n"
            "    bool hasComplete = ((data[endPointer-1].timestamp - data[startPointer].timestamp) >= windowSize);"
            "\n"
            "    // The beginning of the stream. Check if we have at least one complete window.\n"
            "    if (streamPtr == 0) {\n"
            "        // check for opening windows until finding the first complete\n"
            "        while (bufferPtr < endPointer) {\n");
    if (!filter.empty())
      s.append(
          addTabs(filter, 3) +
              "\t\t\t{\n");
    s.append(
        "            currPane = data[bufferPtr].timestamp / paneSize;\n"
        "            if (currPane - prevOpenPane == panesPerSlide) {\n"
        "                prevOpenPane += panesPerSlide;\n"
        "                windowStartPointers[currentSlide++] = bufferPtr; // TODO: maybe store bPtr*tupleSize\n"
        "                if (bufferPtr!=0) {\n" +
            addTabs(insertAggrs, 5) +
            addTabs(resetAggrs, 5) +
            //"                    aggrStructures[pid].insert(aggrs);\n"
            //"                    aggrs.reset();\n"
            "                }\n"
            "            }\n"
            "            if (currPane - prevCompletePane == panesPerWindow) {\n"
            "                windowEndPointers[currentWindow++] = bufferPtr;\n"
            "                numberOfCompleteWindows++;\n"
            "                break;\n"
            "            }\n"
            "            if (currPane - prevOpenPane > panesPerSlide || currPane - prevCompletePane > panesPerWindow) {\n"
            "                // fill bubbles\n"
            "                int numOfSlices, numOfComplete, numOfOpening, phase;\n"
            "                phase = 1;\n"
            "                //if (currPane - prevOpenPane > panesPerSlide) {\n"
            "                    numOfSlices = currPane - prevOpenPane;// - panesPerSlide -1;\n"
            "                    numOfOpening = numOfSlices;\n"
            "                    numOfComplete = prevOpenPane + numOfSlices - panesPerWindow;\n"
            "                if (numOfComplete > 0) {\n"
            "                    numOfOpening =  numOfSlices - numOfComplete;\n"
            "                //    phase = 2;\n"
            "                    hasAddedComplete = true;\n"
            "                }\n"
            "                fillEmptyWindows(pid, phase, numOfSlices, numOfOpening, 0, windowStartPointers,\n"
            "                                    windowEndPointers, numberOfOpeningWindows, numberOfClosingWindows, numberOfCompleteWindows,\n"
            "                                    bufferPtr, currentSlide, currentWindow,\n"
            "                                    completeWindowsPointer, completeWindowsResults, completeStartPointers,\n"
            "                                    closingWindowsPointer, closingWindowsResults, closingStartPointers,\n"
            //"                                    openingWindowIds, closingWindowIds, pendingWindowIds, completeWindowIds,\n"
            "                                    prevCompletePane, prevClosePane, prevOpenPane, currPane);\n"
            "                if (numOfComplete > 0)\n"
            "                    break;\n"
            "            }\n"
            "            // project + aggregate here\n" +
            addTabs(computeAggrs, 3));
    if (!filter.empty())
      s.append("\t\t\t};\n");
    s.append(
        "            streamPtr += step;\n"
        "            bufferPtr += step;\n"
        "        }\n"
        "    }\n"
        "        // Check for closing and opening windows, until we have a complete window.\n"
        "    else {\n"
        "        auto prevPane = data[bufferPtr].timestamp / paneSize;\n"
        "        int numOfPartials = 0;\n"
        "        while (bufferPtr < endPointer) {\n");
    if (!filter.empty())
      s.append(
          addTabs(filter, 3) +
              "\t\t\t{\n");
    s.append(
        "            currPane = data[bufferPtr].timestamp / paneSize;\n"
        "            if (currPane-prevPane==1) {\n"
        "                prevPane = currPane;\n"
        "                if (numOfPartials==BUCKET_SIZE) // remove the extra values so that we have the first complete window\n"
            +
                addTabs(evictAggrs, 5) +
            addTabs(insertAggrs, 4) +
            addTabs(resetAggrs, 4) +
            //"                    aggrStructures[pid].evict();\n"
            //"                aggrStructures[pid].insert(aggrs);\n"
            //"                aggrs.reset();\n"
            "                numOfPartials++;\n"
            "            }\n"
            "            if (currPane - prevOpenPane == panesPerSlide) { // new slide and possible opening windows\n"
            "                prevOpenPane += panesPerSlide;\n"
            "                windowStartPointers[currentSlide++] = bufferPtr;\n"
            "            }\n"
            "            if (hasComplete && currPane - prevCompletePane == panesPerWindow) { // complete window\n"
            "                windowEndPointers[currentWindow++] = bufferPtr;\n"
            "                numberOfCompleteWindows++;\n"
            "                break;\n"
            "            }\n"
            "            if (prevClosePane <= startPane && currPane - prevClosePane == panesPerWindow) { // closing window\n"
            "                // write result to the closing windows\n" +
            //"                aggrs = aggrStructures[pid].queryIntermediate(PARENTS_SIZE-2);\n" +
            addTabs(closingWindows, 4) +
            "                prevClosePane += panesPerSlide;\n"
            "                closingWindowsPointer += mapSize;\n"
            "                numberOfClosingWindows++;\n"
            "                closingStartPointers[numberOfClosingWindows] = closingWindowsPointer;\n" +
            //"                closingWindowIds[numberOfClosingWindows-1] = prevClosePane - panesPerSlide;\n" +
            addTabs(resetAggrs, 4) +
            //"                aggrs.reset();\n"
            "            }\n"
            "            if (currPane - prevOpenPane > panesPerSlide || currPane - prevClosePane > panesPerWindow || currPane - prevCompletePane > panesPerWindow) {\n"
            "                    // fill bubbles\n"
            "                    int numOfSlices, numOfComplete, numOfOpening, numOfClosing,  phase;\n"
            "                    numOfClosing = 0;\n"
            "                    phase = 3;\n"
            "                    if (currPane - prevOpenPane >= panesPerSlide) {\n"
            "                        numOfSlices = currPane - prevOpenPane; // - panesPerSlide -1;\n"
            "                        numOfComplete = prevPane + numOfSlices - (panesPerWindow + prevCompletePane);//numOfComplete = currPane+numOfSlices-panesPerWindow;\n"
            "                        if (numOfSlices == 0)\n"
            "                            prevOpenPane = currPane;\n"
            "                    } else if (currPane - prevClosePane >= panesPerWindow) {\n"
            "                        numOfSlices = currPane - prevClosePane; // - panesPerWindow -1;\n"
            "                        numOfClosing = currPane - prevBatchPane;\n"
            "                    } else {\n"
            "                        numOfSlices = currPane - prevCompletePane - panesPerWindow -1;\n"
            "                        numOfComplete = currPane + numOfSlices - panesPerWindow;\n"
            "                    }\n"
            "                    if (numOfComplete > 0) {\n"
            "                        hasAddedComplete = true;\n"
            "                    }\n"
            "                    fillEmptyWindows(pid, phase, numOfSlices, numOfOpening, numOfClosing, windowStartPointers,\n"
            "                                        windowEndPointers, numberOfOpeningWindows, numberOfClosingWindows, numberOfCompleteWindows,\n"
            "                                        bufferPtr, currentSlide, currentWindow,\n"
            "                                        completeWindowsPointer, completeWindowsResults, completeStartPointers,\n"
            "                                        closingWindowsPointer, closingWindowsResults, closingStartPointers,\n"
            //"                                        openingWindowIds, closingWindowIds, pendingWindowIds, completeWindowIds,\n"
            "                                        prevCompletePane, prevClosePane, prevOpenPane, currPane);\n"
            "                    prevPane = currPane;\n"
            "                    if (hasAddedComplete)\n"
            "                        break;\n"
            "                }\n"
            "            // project + aggregate here\n" +
            addTabs(computeAggrs, 3));
    if (!filter.empty())
      s.append("\t\t\t};\n");
    s.append(
        "            streamPtr += step;\n"
        "            bufferPtr += step;\n"
        "        }\n"
        "\n"
        "        // check for pending windows\n"
        "        if (numberOfCompleteWindows == 0) {\n" +
            addTabs(insertAggrs, 3) +
            //"            if (bufferPtr * sizeof(input_tuple_t) == BUFFER_SIZE)\n"
            //"                bufferPtr = 0;\n"
            //"            currPane = data[bufferPtr].timestamp / paneSize;\n"
            //"            if (prevClosePane <= startPane && currPane - prevClosePane == panesPerWindow) { // closing window\n"
            //"                // write result to the closing windows\n" +
            //addTabs(closingWindows, 4) +
            //"                closingWindowsPointer += mapSize;\n"
            //"                numberOfClosingWindows++;\n"
            //"                closingStartPointers[numberOfClosingWindows] = closingWindowsPointer;\n"
            //"                // reset values\n" +
            //addTabs(resetAggrs, 4) +
            //"            }\n"
            "            // write result to pending windows\n" +
            //"            aggrs = aggrStructures[pid].queryIntermediate(-1);\n" +
            addTabs(pendingWindows, 3) +
            "            pendingWindowsPointer += mapSize;\n"
            "            numberOfPendingWindows++;\n"
            "            pendingStartPointers[numberOfPendingWindows] = pendingWindowsPointer;\n" +
            addTabs(resetAggrs, 3) +
            //"            aggrs.reset();\n"
            "        }\n"
            "    }\n"
            "\n"
            "    if (numberOfCompleteWindows == 0 && (streamStartPointer == 0 || currentSlide >= 1)) { // We only have one opening window so far...\n"
            "        if (streamPtr%windowSlide!=0 && streamStartPointer!=0) {\n" +
            addTabs(evictAggrs, 3) +
            //"            aggrStructures[pid].evict();\n"
            "        }\n"
            "        // write results\n" +
            //"        aggrs = aggrStructures[pid].queryIntermediate(numberOfOpeningWindows);\n" +
            addTabs(openingWindows, 2) +
            "        openingWindowsPointer += mapSize;\n"
            "        numberOfOpeningWindows++;\n"
            "        openingStartPointers[numberOfOpeningWindows] = openingWindowsPointer;\n" +
            //"        openingWindowIds[numberOfOpeningWindows-1] = prevOpenPane++;\n" +
            addTabs(resetAggrs, 2) +
            //"        aggrs.reset();\n"
            "    } else if (numberOfCompleteWindows > 0) {\n"
            "        // write results and pack them for the first complete window in the batch\n" +
            "        if (!hasAddedComplete) {\n" +
            addTabs(setValues, 2)
            //"        aggrs = aggrStructures[pid].query();\n" +
            + addPostWindowOperation(2) +
            "        for (int i = 0; i < mapSize; i++) {\n" +
            addTabs(completeWindows, 3) +
            //"            completeWindowsPointer++;\n"
            "        }\n" +
            addTabs(resetAggrs, 2) +
            //"        aggrs.reset();\n"
            "        // write in the correct slot, as the value has already been incremented!\n"
            "        completeStartPointers[numberOfCompleteWindows] = completeWindowsPointer;\n"
            "        prevCompletePane += panesPerSlide;\n"
            //"        completeWindowIds[numberOfCompleteWindows-1] = prevCompletePane - 1;\n"
            "        }\n"
            "        // project + aggregate here\n");
    if (!filter.empty())
      s.append(
          addTabs(filter, 2) +
              "\t\t{\n");
    s.append(
        addTabs(computeAggrs, 2));
    if (!filter.empty())
      s.append("\t\t}\n");
    s.append(
        "        bufferPtr += step; // windowEndPointers[0];\n"
        "        //prevOpenPane = (data[windowStartPointers[currentSlide-1]].timestamp) / paneSize;\n"
        "        int idx = 1;\n"
        //"        prevCompletePane += panesPerSlide; // (data[windowStartPointers[idx++]].timestamp) / paneSize;\n"
        "        int removalIndex = currentWindow; //(startingFromPane) ? currentWindow : currentWindow + 1;\n"
        "        bool foundComplete = false;\n"
        "        while (bufferPtr < endPointer) {\n"
        "            // remove previous slide \n"
        "            tempStartPos = windowStartPointers[removalIndex - 1];\n"
        "            if (tempStartPos!=-1)\n" +
            addTabs(evictAggrs, 4) +
            //"            aggrStructures[pid].evict();\n"
            "\n"
            "            // add elements from the next slide\n"
            "            //bufferPtr = windowEndPointers[currentWindow - 1] + 1; // take the next position, as we have already computed this value\n"
            "            foundComplete = false;\n"
            "            auto prevPane = data[bufferPtr].timestamp / paneSize;\n"
            "            while (true) {\n");
    if (!filter.empty())
      s.append(
          addTabs(filter, 4) +
              "\t\t\t{\n");
    s.append(
        "                auto dataPtr = bufferPtr;\n"
        "                if (bufferPtr*tupleSize >= bufferSize)\n"
        "                    dataPtr -= bufferSize/tupleSize;\n"
        "                currPane = data[dataPtr].timestamp / paneSize;\n"
        "                if (currPane-prevPane==1) {\n"
        "                    prevPane = currPane;\n" +
            addTabs(insertAggrs, 4) +
            addTabs(resetAggrs, 4) +
            //"                    aggrStructures[pid].insert(aggrs);\n"
            //"                    aggrs.reset();\n"
            "                }\n"
            "                if (currPane - prevOpenPane == panesPerSlide) {\n"
            "                    prevOpenPane = currPane;\n"
            "                    windowStartPointers[currentSlide++] = dataPtr;\n"
            "                }\n"
            "                if (bufferPtr >= endPointer) {\n"
            "                    break;\n"
            "                }\n"
            "                // complete windows\n"
            "                if (currPane - prevCompletePane == panesPerWindow) {\n"
            "                    //prevPane = currPane;\n"
            "                    prevCompletePane += panesPerSlide; // data[(windowStartPointers[idx++])].timestamp / paneSize;\n"
            "\n"
            "                    windowEndPointers[currentWindow++] = dataPtr;\n"
            "                    // write and pack the complete window result\n" +
            addTabs(setValues, 5)
            //"                    aggrs = aggrStructures[pid].query();\n"
            + addPostWindowOperation(5) +
            "                    for (int i = 0; i < mapSize; i++) {\n" +
            addTabs(completeWindows, 6) +
            //"                        completeWindowsPointer++;\n"
            "                    }\n" +
            addTabs(resetAggrs, 5) +
            //"                    aggrs.reset();\n"
            "                    numberOfCompleteWindows++;\n"
            "                    completeStartPointers[numberOfCompleteWindows] = completeWindowsPointer;\n"
            //"                    completeWindowIds[numberOfCompleteWindows-1] = prevCompletePane - 1;\n"
            "                    foundComplete = true;\n"
            "                }\n"
            "                if (currPane - prevOpenPane > panesPerSlide || currPane - prevCompletePane > panesPerWindow) {\n"
            "                    // fill bubbles\n"
            "                    int numOfSlices, numOfComplete, numOfOpening, phase;\n"
            "                    phase = 2;\n"
            "                    //if (currPane - prevOpenPane > panesPerSlide) {\n"
            "                        numOfSlices = currPane - prevPane; //prevOpenPane - panesPerSlide -1;\n"
            "                        numOfComplete = prevPane + numOfSlices - (panesPerWindow + prevCompletePane);\n"
            "                    //} else {\n"
            "                    //    numOfSlices = currPane - prevCompletePane - panesPerWindow -1;\n"
            "                    //    numOfComplete = currPane+numOfSlices-panesPerWindow;\n"
            "                    //}\n"
            "                    if (numOfComplete > 0) {\n"
            "\n"
            "                        numOfOpening =  prevCompletePane + panesPerWindow - prevOpenPane;\n"
            "                        foundComplete = true;\n"
            "                    }\n"
            "                    fillEmptyWindows(pid, phase, numOfSlices, numOfOpening, 0, windowStartPointers,\n"
            "                                        windowEndPointers, numberOfOpeningWindows, numberOfClosingWindows, numberOfCompleteWindows,\n"
            "                                        bufferPtr, currentSlide, currentWindow,\n"
            "                                        completeWindowsPointer, completeWindowsResults, completeStartPointers,\n"
            "                                        closingWindowsPointer, closingWindowsResults, closingStartPointers,\n"
            //"                                        openingWindowIds, closingWindowIds, pendingWindowIds, completeWindowIds,\n"
            "                                        prevCompletePane, prevClosePane, prevOpenPane, currPane);\n"
            "                }\n"
            "\n"
            "                // aggregate here\n" +
            addTabs(computeAggrs, 4));
    if (!filter.empty())
      s.append("\t\t\t\t}\n"
               "               if (bufferPtr >= endPointer) {\n"
               "                   break;\n"
               "               }\n");
    s.append(
        "                bufferPtr += step;\n"
        "                if (foundComplete) {\n"
        "                    break;\n"
        "                }\n"
        "            }\n"
        "            removalIndex++;\n"
        "        }\n"
        "\n"
        "        if (!foundComplete) {  // we have reached the first open window after all the complete ones\n"
        "            // write the first open window if we have already computed the result\n" +
            //addTabs(insertAggrs, 3) +
            //"            aggrStructures[pid].insert(aggrs);\n" +
            //"            aggrs = aggrStructures[pid].queryIntermediate(numberOfOpeningWindows);\n" +
            addTabs(openingWindows, 3) +
            "            openingWindowsPointer += mapSize;\n"
            "            numberOfOpeningWindows++;\n"
            "            openingStartPointers[numberOfOpeningWindows] = openingWindowsPointer;\n"
            //"            openingWindowIds[numberOfOpeningWindows-1] = prevCompletePane++;\n"
            "        } else { // otherwise remove the respective tuples for the first opening window\n"
            "            currentWindow--;\n"
            "        }\n"
            "    }\n"
            "\n"
            "    // compute the rest opening windows\n"
            "    if (currentSlide-currentWindow >= panesPerWindow)\t\t\n"
            "        currentWindow = currentSlide-panesPerWindow+1;//+numberOfOpeningWindows;\n"
            "    if (numberOfCompleteWindows!=0 && currentSlide-currentWindow<panesPerWindow-1) {\n"
            "        currentWindow = currentSlide-panesPerWindow+1;\n"
            "    }\n"
            "    while (currentWindow < currentSlide - 1) {\n"
            //"    while (currentWindow < currentSlide - 1) {\n"
            "        // remove previous slide\n"
            "        tempStartPos = windowStartPointers[currentWindow];\n"
            "        tempEndPos = windowStartPointers[currentWindow + 1];\n"
            "        currentWindow++;\n"
            "        if ((tempStartPos == tempEndPos && tempStartPos!=-1) || ((tempEndPos==0 /*|| tempEndPos==endPointer*/) /*&& \n"
            "            data[tempEndPos].timestamp%windowSlide==0*/)) continue;\n"
            "        if (tempStartPos!=-1)\n" +
            addTabs(evictAggrs, 3) +
            //"        aggrStructures[pid].evict();            \n"
            "        // write result to the opening windows\n" +
            //"        aggrs = aggrStructures[pid].queryIntermediate(numberOfOpeningWindows);\n" +
            addTabs(openingWindows, 2) +
            "        openingWindowsPointer += mapSize;\n"
            "        numberOfOpeningWindows++;\n"
            "        openingStartPointers[numberOfOpeningWindows] = openingWindowsPointer;\n"
            //"        openingWindowIds[numberOfOpeningWindows-1] = prevCompletePane++;\n"
            "    }\n"
            "\n"
            "    if (numberOfClosingWindows >= panesPerWindow) {\n");
    if (hasGroupBy())
      s.append("        closingWindowsPointer -= (numberOfClosingWindows - panesPerWindow + 1)*mapSize;\n");
    else
      s.append("        closingWindowsPointer -= (numberOfClosingWindows - panesPerWindow + 1);\n");
    s.append(
        "        numberOfClosingWindows = panesPerWindow - 1;\n"
        "    }\n"
        "\n" +
            addTabs(resultPointers, 1) +
            "    pointersAndCounts[4] = numberOfOpeningWindows;\n"
            "    pointersAndCounts[5] = numberOfClosingWindows;\n"
            "    pointersAndCounts[6] = numberOfPendingWindows;\n"
            "    pointersAndCounts[7] = numberOfCompleteWindows;\n"
            "}\n"
    );
    return s;
  }
};
