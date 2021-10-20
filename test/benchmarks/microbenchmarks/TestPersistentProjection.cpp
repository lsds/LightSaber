#include <iostream>

#include "checkpoint/FileBackedCheckpointCoordinator.h"
#include "compression/Compressor.h"
#include "cql/expressions/ColumnReference.h"
#include "cql/operators/codeGeneration/OperatorKernel.h"
#include "microbenchmarks/RandomDataGenerator.h"
#include "snappy.h"
#include "utils/Query.h"
#include "utils/QueryOperator.h"
#include "utils/WindowDefinition.h"

// Template for implementing hard-coded compression schemes for Scabbard
namespace TPPCompression {
struct input_tuple_t {
  long timestamp;
  int attr1;
  int attr2;
};

std::vector<std::string, tbb::cache_aligned_allocator<std::string>> *metadata;

void compressInput(int pid, char *input, int start, int end, char *output,
                   int &writePos, int length, bool &clear, long latency = -1) {
  if (start == 0 && end == -1) {
    return;
  }

  auto data = (input_tuple_t *)input;
  std::vector<size_t> idxs (2);
  BaseDeltaCompressor<long, uint16_t> bcomp(data[0].timestamp);
  DummyFloatCompressor fcomp(1000);
  struct t_1 {
    uint16_t timestamp : 10;
    uint8_t counter : 6;
  };
  struct t_2 {
    uint16_t attr1 : 10;
  };

  for (auto &i : idxs) {
    i = 0;
  }

  t_1 *buf1 = (t_1 *)(output);
  t_2 *buf2 = (t_2 *)(output + (int) (length*0.5));
  size_t n = (end - start) / sizeof(input_tuple_t);
  uint8_t count_1 = 1;
  for (size_t idx = 0; idx < n; idx++) {
    auto fVal_1 = bcomp.compress(data[idx].timestamp);
    auto sVal_1 = fVal_1;
    if (idx < n - 1 && count_1 < 1023 && fVal_1 ==
                                            (sVal_1 = bcomp.compress(data[idx + 1].timestamp))) {
      count_1++;
    } else {
      buf1[idxs[0]++] = {fVal_1, count_1};
      fVal_1 = sVal_1;
      count_1 = 1;
    }

    buf2[idxs[1]++] = {(uint16_t) data[idx].attr1};
  }

  writePos += idxs[0] * sizeof(t_1);
  (*metadata)[pid] = "c0 RLE BD " + std::to_string(data[0].timestamp) + " {uint16_t:10,uint8_t:6} " + std::to_string(writePos);
  std::memcpy((void *)(output + writePos), (void *)buf2,
              idxs[1] * sizeof(t_2));
  writePos += idxs[1] * sizeof(t_2);
  (*metadata)[pid] += " c1 NS {uint16_t:16} " + std::to_string(writePos);

  // write metadata required for decompression
  if ((*metadata)[pid].size() > 128) {
    throw std::runtime_error("error: increase the metadata size");
  }
  std::memcpy((void *)(output - 128), (*metadata)[pid].data(),
              (*metadata)[pid].size());
  (*metadata)[pid].clear();
}

void decompressInput(int pid, char *input, int start, int end, char *output,
                     int &writePos, int length, bool &copy, long latency = -1) {
  // parse metadata for decompression
  throw std::runtime_error("error: not implemented");
}

void compress(int pid, char *input, int start, int end, char *output, int &writePos,
              bool isComplete, bool &clear) {
  if (start == 0 && end == -1) {
    return;
  }

  size_t output_length;
  auto buf1 = (uint64_t *)input;
  snappy::RawCompress((const char *)(buf1), end, (char *)(output),
                      &output_length);
  writePos += output_length;
  // write metadata required for decompression
  /*(*metadata)[pid] = "snappy " + std::to_string(output_length);
  if ((*metadata)[pid].size() > 128) {
    throw std::runtime_error("error: increase the metadata size");
  }
  std::memcpy((void *)(output - 128), (*metadata)[pid].data(),
              (*metadata)[pid].size());
  (*metadata)[pid].clear();
  }*/
}

};  // namespace TPPCompression

class TestPersistentProjection : public RandomDataGenerator {
 private:
  void createApplication() override {
    SystemConf::getInstance().WORKER_THREADS = 1;
    // Setup input queue and batch size
    SystemConf::getInstance().CIRCULAR_BUFFER_SIZE = 33554432;
    SystemConf::getInstance().UNBOUNDED_BUFFER_SIZE = 524288;
    SystemConf::getInstance().BATCH_SIZE = 524288;
    SystemConf::getInstance().BUNDLE_SIZE = 524288;

    // Configure projection
    std::vector<Expression *> expressions(2);
    // Always project the timestamp
    expressions[0] = new ColumnReference(0);
    expressions[1] = new ColumnReference(1);
    Projection *projection = new Projection(expressions);

    auto window = new WindowDefinition(ROW_BASED, 60, 60);

    bool replayTimestamps = window->isRangeBased();

    // Set up code-generated operator
    OperatorKernel *genCode = new OperatorKernel(true);
    genCode->setInputSchema(getSchema());
    genCode->setProjection(projection);
    genCode->setQueryId(0);
    genCode->setup();
    OperatorCode *cpuCode = genCode;

    // Print operator
    std::cout << cpuCode->toSExpr() << std::endl;

    // Setup checkpointing - no state to checkpoint, only output tuples
    SystemConf::getInstance().CHECKPOINT_ON = true;
    // Set checkpoint interval to 1-sec
    SystemConf::getInstance().CHECKPOINT_INTERVAL = 1000L;
    // Enable compression when checkpointing state/output results
    SystemConf::getInstance().CHECKPOINT_COMPRESSION = true;
    // Set input as persistent
    SystemConf::getInstance().PERSIST_INPUT = true;
    // Set disk block size
    SystemConf::getInstance().BLOCK_SIZE = 1024*1024;
    // Set if we are recovering from previous data
    SystemConf::getInstance().RECOVER = false;
    // Enable dependency tracking
    SystemConf::getInstance().LINEAGE_ON = true;

    // Define the operator as ft-operator
    bool isFaultTolerant = true;
    auto queryOperator = new QueryOperator(*cpuCode, isFaultTolerant);
    std::vector<QueryOperator *> operators;
    operators.push_back(queryOperator);

    long timestampReference =
        std::chrono::system_clock::now().time_since_epoch().count();

    std::vector<std::shared_ptr<Query>> queries(1);
    // Define the input as persistent
    bool persistInput = SystemConf::getInstance().PERSIST_INPUT;
    bool clearPreviousFiles =
        !SystemConf::getInstance().RECOVER;  // set false if you need to recover
                                             // from already persisted files
    queries[0] = std::make_shared<Query>(
        0, operators, *window, m_schema, timestampReference, false, false, true,
        false, 0, persistInput, nullptr, clearPreviousFiles);


    // define hard-coded compression schemes to start compressing
    if (SystemConf::getInstance().CHECKPOINT_COMPRESSION) {
      queries[0]->getBuffer()->setCompressionFP(TPPCompression::compressInput);
      queries[0]->getBuffer()->setDecompressionFP(TPPCompression::decompressInput);
    }

    m_application =
        new QueryApplication(queries, SystemConf::getInstance().CHECKPOINT_ON,
                             !SystemConf::getInstance().RECOVER);
    m_application->setup();
    if (SystemConf::getInstance().CHECKPOINT_COMPRESSION &&
        (SystemConf::getInstance().CHECKPOINT_ON || persistInput)) {
      TPPCompression::metadata = new std::vector<std::string,tbb::cache_aligned_allocator<std::string>>(
              SystemConf::getInstance().WORKER_THREADS, "");
      // Here set hard-coded compression schemes for state/output results compression
      //m_application->getCheckpointCoordinator()->setCompressionFP(0, TPPCompression::compress);
    }
  }

 public:
  TestPersistentProjection(bool inMemory = true) {
    m_name = "TestPersistentProjection";
    createSchema();
    createApplication();
    if (inMemory) loadInMemoryData();
  }
};

int main(int argc, const char **argv) {
  std::unique_ptr<BenchmarkQuery> benchmarkQuery{};

  BenchmarkQuery::parseCommandLineArguments(argc, argv);

  benchmarkQuery = std::make_unique<TestPersistentProjection>();

  return benchmarkQuery->runBenchmark();
}