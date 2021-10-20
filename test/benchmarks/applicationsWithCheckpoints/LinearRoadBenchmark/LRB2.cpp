#include "benchmarks/applications/LinearRoadBenchmark/LinearRoadBenchmark.h"
#include "compression/Compressor.h"
#include "cql/expressions/ColumnReference.h"
#include "cql/expressions/IntConstant.h"
#include "cql/expressions/operations/Division.h"
#include "cql/operators/Aggregation.h"
#include "cql/operators/AggregationType.h"
#include "cql/operators/codeGeneration/OperatorKernel.h"
#include "checkpoint/FileBackedCheckpointCoordinator.h"
#include "utils/Query.h"
#include "utils/QueryOperator.h"
#include "utils/WindowDefinition.h"

namespace LRB2Compress {
struct alignas(16) input_tuple_t {
  long timestamp;
  int _1;
  float _2;
  int _3;
  int _4;
  int _5;
  int _6;
};
struct alignas(16) output_tuple_t {
  long timestamp;
  int _1;
  int _2;
  int _3;
  int _4;
  float _5;
  int _6;
};
struct Value {
  float _1;
};
struct Key {
  int _0;
  int _1;
  int _2;
  int _3;
};
using KeyT = Key;
using ValueT = Value;
struct alignas(16) Bucket {
  char state;
  char dirty;
  long timestamp;
  KeyT key;
  ValueT value;
  int counter;
};
struct hash {
  std::size_t operator()(const Key &key) const {
    uint64_t result = uint16_t(key._0) * 100 + uint16_t(key._2) * 10 +
        uint16_t(key._3);  // todo: is this luck?
    return result;
  }
};
struct Eq {
  constexpr bool operator()(const KeyT& lhs, const KeyT& rhs) const {
    return lhs._0 == rhs._0 && lhs._1 == rhs._1 && lhs._2 == rhs._2 && lhs._3 == rhs._3;
  }
};
std::vector<std::unique_ptr<DictionaryCompressor<Key, uint16_t, hash, Eq>>> *dcomp;
std::vector<std::string, tbb::cache_aligned_allocator<std::string>> *metadata;

struct dBucket {
  Key key;
};
void compressInput(int pid, char *input, int start, int end, char *output, int &writePos, int length, bool &clear, long latency = -1) {
  if (start == 0 && end == -1) {
    auto buf = (dBucket*) (output + writePos);
    auto bucket = (Bucket *)(*dcomp)[pid]->getTable().buckets();
    for (size_t idx = 0; idx < (*dcomp)[pid]->getTable().max_size(); ++idx) {
      if (bucket[idx].state) {
        buf[idx] = dBucket{bucket[idx].key};
      } else {
        buf[idx] = dBucket{ -1, -1, -1, -1};
      }
    }
    auto offset = (*dcomp)[pid]->getTable().max_size() * sizeof(dBucket);
    writePos += offset;
    (*metadata)[pid] += "ht " + std::to_string(offset);
    if ((*metadata)[pid].size() > 128)
      throw std::runtime_error("error: increase the size of the metadata area");
    std::memcpy(output, (void *)(*metadata)[pid].data(), (*metadata)[pid].size());
    (*metadata)[pid].clear();
    return;
  }
  if (clear) {
    (*dcomp)[pid]->clear();
    clear = false;
  }

  auto data = (input_tuple_t *)input;
  std::vector<size_t> idxs (2);
  BaseDeltaCompressor<long, uint16_t> bcomp(data[0].timestamp);
  DummyFloatCompressor fcomp(1000000);
  struct t_1 {
    uint16_t timestamp : 9;
    uint16_t counter   :  7;
  };
  struct t_2 {
    uint16_t groupKey : 12;
    uint16_t counter  : 4;
  };

  //writePos = 0;
  // compress
  for (auto &i : idxs) {
    i = 0;
  }
  t_1 *buf1 = (t_1 *)(output);
  t_2 *buf2 = (t_2 *)(output + (int) (length*0.5));
  size_t n = (end - start) / sizeof(input_tuple_t);
  uint16_t count_1 = 1;
  uint8_t count_2 = 1;
  uint16_t count_3 = 1;
  for (size_t idx = 0; idx < n; idx++) {
    auto fVal_1 = bcomp.compress(data[idx].timestamp);
    auto sVal_1 = fVal_1;
    if (idx < n - 1 && fVal_1 ==
        (sVal_1 = bcomp.compress(data[idx + 1].timestamp))) {
      count_1++;
    } else {
      buf1[idxs[0]++] = {(uint16_t)fVal_1, count_1};
      fVal_1 = sVal_1;
      count_1 = 1;
    }

    Key temp{data[idx]._1, data[idx]._3, data[idx]._5,
                     data[idx]._6 / 5280};
    auto fVal_2 = (*dcomp)[pid]->compress(temp);
    auto sVal_2 = fVal_2;
    if (idx < n - 1) {
      Key temp2{data[idx+1]._1, data[idx+1]._3, data[idx+1]._5,
                        data[idx+1]._6 / 5280};
      sVal_2 = (*dcomp)[pid]->compress(temp2);
      if (sVal_2 == fVal_2) {
        count_2++;
      } else {
        buf2[idxs[1]++] = {fVal_2, count_2};
        fVal_2 = sVal_2;
        count_2 = 1;
      }
    } else {
      buf2[idxs[1]++] = {fVal_2, count_2};
      fVal_2 = sVal_2;
      count_2 = 1;
    }
  }
  std::memcpy((void *)(output + idxs[0] * sizeof(t_1)), (void *)buf2,
              idxs[1] * sizeof(t_2));
  writePos += idxs[0] * sizeof(t_1) + idxs[1] * sizeof(t_2);
  (*metadata)[pid] = "r0 " + std::to_string(idxs[0]) + " r1 " + std::to_string(idxs[1]) + " ";
}

void compressInput_(int pid, char *input, int start, int end, char *output, int &writePos, int length, bool &clear, long latency = -1) {
  if (start == 0 && end == -1) {
    return;
  }
  if (clear) {
    clear = false;
  }

  auto data = (input_tuple_t *)input;
  std::vector<size_t> idxs (6, 0);
  BaseDeltaCompressor<long, uint16_t> bcomp(data[0].timestamp);
  Simple8 simpleComp1;
  Simple8 simpleComp2;
  Simple8 simpleComp3;
  Simple8 simpleComp4;
  //DummyFloatCompressor fcomp(1000000);
  struct t_1 {
    uint16_t timestamp : 9;
    uint16_t counter   :  7;
  };

  //writePos = 0;
  // compress
  size_t n = (end - start) / sizeof(input_tuple_t);
  t_1 *buf1 = (t_1 *)(output);

  // simple 8
  auto buf2 = (uint64_t *)(output + (int) (length*0.2));
  int32_t inOffset = 0;
  int32_t outOffset = 0;
  idxs[1] = simpleComp1.compress(
      inOffset, outOffset, n, &data, &input_tuple_t::_3, 1, buf2);

  // simple 8
  auto buf3 = (uint64_t *)(output + (int) (length*2*0.2));
  inOffset = 0;
  outOffset = 0;
  idxs[2] = simpleComp2.compress(
      inOffset, outOffset, n, &data, &input_tuple_t::_5, 1, buf3);

  // simple 8
  auto buf4 = (uint64_t *)(output + (int) (length*3*0.2));
  inOffset = 0;
  outOffset = 0;
  //for (size_t idx = 0; idx < n; idx++) {
  //  data[idx]._6 /= 5280;
  //}
  idxs[3] = simpleComp3.compress(
      inOffset, outOffset, n, &data, &input_tuple_t::_6, 1, buf4, 5280);

  // simple 8
  auto buf5 = (uint64_t *)(output + (int) (length*4*0.2));
  inOffset = 0;
  outOffset = 0;
  idxs[4] = simpleComp4.compress(
      inOffset, outOffset, n, &data, &input_tuple_t::_1, 1, buf5);


  uint16_t count_1 = 1;
  for (size_t idx = 0; idx < n; idx++) {
    auto fVal_1 = bcomp.compress(data[idx].timestamp);
    auto sVal_1 = fVal_1;
    if (idx < n - 1 && fVal_1 ==
        (sVal_1 = bcomp.compress(data[idx + 1].timestamp))) {
      count_1++;
    } else {
      buf1[idxs[0]++] = {static_cast<uint8_t>(fVal_1), static_cast<uint8_t>(count_1)};
      fVal_1 = sVal_1;
      count_1 = 1;
    }
  }
  writePos += idxs[0] * sizeof(t_1);
  std::memcpy((void *)(output + writePos), (void *)buf2,
              idxs[1] * sizeof(uint64_t));
  writePos += idxs[1] * sizeof(uint64_t);
  std::memcpy((void *)(output + writePos),
              (void *)buf3, idxs[2] * sizeof(uint64_t));
  writePos += idxs[2] * sizeof(uint64_t);
  std::memcpy((void *)(output + writePos),
              (void *)buf4, idxs[3] * sizeof(uint64_t));
  writePos += idxs[3] * sizeof(uint64_t);
  std::memcpy((void *)(output + writePos),
              (void *)buf5, idxs[4] * sizeof(uint64_t));
  writePos += idxs[4] * sizeof(uint64_t);
  (*metadata)[pid] = "r0 " + std::to_string(idxs[0]) + " r1 " + std::to_string(idxs[1]) + " ";
}

void compressGenInput(int pid, char *input, int start, int end, char *output, int &writePos, int length, bool &clear, long latency = -1) {
  if (start == 0 && end == -1) {
    return;
  }
  if (clear) {
    clear = false;
  }

  auto data = (input_tuple_t *)input;
  std::vector<size_t> idxs (6, 0);
  GorillaTimestampCompressor<long, uint64_t> gorillaComp;
  Simple8 simpleComp1;
  Simple8 simpleComp2;
  Simple8 simpleComp3;
  Simple8 simpleComp4;

  //writePos = 0;
  // compress
  size_t n = (end - start) / sizeof(input_tuple_t);
  // gorilla timestamp
  auto buf1 = (uint64_t *)output;
  uint8_t count_1 = 14;  // as the first delta is stored in 14 bits
  // store first timestamp in 64bits + first delta int 14 bits
  buf1[idxs[0]++] = data[0].timestamp;
  int64_t newDelta = data[1].timestamp - data[0].timestamp;
  buf1[idxs[0]] = newDelta << (64 - count_1);

  // simple 8
  auto buf2 = (uint64_t *)(output + (int) (length*0.2));
  int32_t inOffset = 0;
  int32_t outOffset = 0;
  idxs[1] = simpleComp1.compress(
      inOffset, outOffset, n, &data, &input_tuple_t::_3, 1, buf2);

  // simple 8
  auto buf3 = (uint64_t *)(output + (int) (length*2*0.2));
  inOffset = 0;
  outOffset = 0;
  idxs[2] = simpleComp2.compress(
      inOffset, outOffset, n, &data, &input_tuple_t::_5, 1, buf3);

  // simple 8
  auto buf4 = (uint64_t *)(output + (int) (length*3*0.2));
  inOffset = 0;
  outOffset = 0;
  //for (size_t idx = 0; idx < n; idx++) {
  //  data[idx]._6 /= 5280;
  //}
  idxs[3] = simpleComp3.compress(
      inOffset, outOffset, n, &data, &input_tuple_t::_6, 1, buf4, 5280);

  // simple 8
  auto buf5 = (uint64_t *)(output + (int) (length*4*0.2));
  inOffset = 0;
  outOffset = 0;
  idxs[4] = simpleComp4.compress(
      inOffset, outOffset, n, &data, &input_tuple_t::_1, 1, buf5);

  for (size_t idx = 0; idx < n; idx++) {
    if (idx > 1) {
      auto [deltaD, deltaLength] = gorillaComp.compress(
          data[idx].timestamp, data[idx - 1].timestamp,
          data[idx - 2].timestamp);
      if (count_1 + deltaLength > 64) {
        uint8_t split = (64 - count_1);
        if (deltaLength > 1) {
          buf1[idxs[0]] |= deltaD >> (deltaLength - split);
        }
        ++idxs[0];
        count_1 = deltaLength - split;
      } else {
        count_1 += deltaLength;
      }
      buf1[idxs[0]] |= deltaD << (64 - count_1);
    }
  }
  writePos += idxs[0] * sizeof(uint64_t);
  std::memcpy((void *)(output + writePos), (void *)buf2,
              idxs[1] * sizeof(uint64_t));
  writePos += idxs[1] * sizeof(uint64_t);
  std::memcpy((void *)(output + writePos),
              (void *)buf3, idxs[2] * sizeof(uint64_t));
  writePos += idxs[2] * sizeof(uint64_t);
  std::memcpy((void *)(output + writePos),
              (void *)buf4, idxs[3] * sizeof(uint64_t));
  writePos += idxs[3] * sizeof(uint64_t);
  std::memcpy((void *)(output + writePos),
              (void *)buf5, idxs[4] * sizeof(uint64_t));
  writePos += idxs[4] * sizeof(uint64_t);
}

void noCompressInput(int pid, char *input, int start, int end, char *output, int &writePos, int length, bool &clear, long latency = -1) {
  if (start == 0 && end == -1) {
    auto offset = 0;
    writePos += offset;
    return;
  }
  auto data = (input_tuple_t *)input;
  struct res {
    long timestamp;
    int vehicle;
    int highway;
    int direction;
    int segment;
    float speed;
  };
  auto out = (res*) output;
  size_t n = (end - start) / sizeof(input_tuple_t);
  for (size_t idx = 0; idx < n; idx++) {
    out[idx] = {data[idx].timestamp, data[idx]._1, data[idx]._3, data[idx]._5,
                data[idx]._6 / 5280, data[idx]._2};
  }
  writePos = n * sizeof(res);
}

void decompressInput(int pid, char *input, int start, int end, char *output, int &writePos, int length, bool &copy, long latency = -1) {
  throw std::runtime_error("error: the decompression function is not implemented");
}

void compress(int pid, char *input, int start, int end, char *output, int &writePos,
              bool isComplete, bool &clear) {
  if (start == 0 && end == -1) {
    auto offset = (*dcomp)[pid]->getTable().max_size() *
                  (*dcomp)[pid]->getTable().bucket_size();
    std::memcpy(output + writePos, (void *)(*dcomp)[pid]->getTable().buckets(), offset);
    writePos += offset;
    return;
  }

  if (clear) {
    (*dcomp)[pid]->clear();
    clear = false;
  }
  start = start / sizeof(Bucket);
  end = end / sizeof(Bucket);
  struct res {
    uint16_t timestamp : 9;
    uint16_t groupKey : 12;
    uint16_t counter : 7;
  };
  auto outputBuf = (res *)output;
  auto outIdx = writePos / sizeof(res);

  if (!isComplete) {
    auto inputBuf = (Bucket *)input;
    BaseDeltaCompressor<long, uint16_t> bcomp(inputBuf[0].timestamp);
    for (size_t idx = start; idx < end; ++idx) {
      if (inputBuf[idx].state) {
        outputBuf[outIdx++] = {bcomp.compress(inputBuf[idx].timestamp),
                               (*dcomp)[pid]->compress(inputBuf[idx].key),
                               static_cast<uint16_t>(inputBuf[idx].counter)};
      }
    }
    writePos = outIdx * sizeof(res);
  } else {
    auto inputBuf = (output_tuple_t *)input;
    BaseDeltaCompressor<long, uint16_t> bcomp(inputBuf[0].timestamp);
    for (size_t idx = start; idx < end; ++idx) {
      Key temp{inputBuf[idx]._1, inputBuf[idx]._2, inputBuf[idx]._3,
               inputBuf[idx]._4};
      outputBuf[outIdx++] = {bcomp.compress(inputBuf[idx].timestamp),
                             (*dcomp)[pid]->compress(temp),
                             static_cast<uint16_t>(inputBuf[idx]._6)};
    }
    writePos = outIdx * sizeof(res);
  }
}

void decompress(int pid, char *input, int start, int end, char *output, int &writePos,
              bool isComplete, bool &clear) {
  struct res {
    uint32_t timestamp : 9;
    uint32_t groupKey : 12;
    uint32_t counter : 7;
  };
  auto inputBuf = (res *) &input[start];
  start = start / sizeof(res);
  end = end / sizeof(res);
  BaseDeltaCompressor<long, uint16_t> bcomp(1);
  auto hTable = (Bucket *) &input[writePos];
  auto outIdx = 0;

  if (!isComplete) {
    auto outputBuf = (Bucket *)output;
    for (size_t idx = start; idx < end; ++idx) {
      if (inputBuf[idx].groupKey >=  (*dcomp)[pid]->getTable().max_size())
        throw std::runtime_error("error: the group key is greater than the hashtable size");
      Key key = hTable[inputBuf[idx].groupKey].key;
      outputBuf[inputBuf[idx].groupKey] = {1, 1, inputBuf[idx].timestamp, key,
                           {(float)inputBuf[idx].counter}, inputBuf[idx].counter};
    }
  } else {
    auto outputBuf = (output_tuple_t *)output;
    for (size_t idx = start; idx < end; ++idx) {
      Key key = hTable[inputBuf[idx].groupKey].key;
      outputBuf[outIdx] = {inputBuf[idx].timestamp, key._0, key._1,
                           key._2, key._3, (float)inputBuf[idx].counter, inputBuf[idx].counter};
      outIdx++;
    }
  }
}
};

class LRB2 : public LinearRoadBenchmark {
 private:
  void createApplication() override {
    SystemConf::getInstance().SLOTS = 512;
    SystemConf::getInstance().PARTIAL_WINDOWS = 544;
    SystemConf::getInstance().HASH_TABLE_SIZE = 2 * 1024;

    bool useParallelMerge = SystemConf::getInstance().PARALLEL_MERGE_ON;
    bool persistInput = SystemConf::getInstance().PERSIST_INPUT;

    // Configure first query
    auto segmentExpr = new Division(new ColumnReference(6, BasicType::Integer), new IntConstant(5280));

    // Configure second query
    std::vector<AggregationType> _aggregationTypes(1);
    _aggregationTypes[0] = AggregationTypes::fromString("cnt");

    std::vector<ColumnReference *> _aggregationAttributes(1);
    _aggregationAttributes[0] = new ColumnReference(2, BasicType::Float);

    std::vector<Expression *> _groupByAttributes(4);
    _groupByAttributes[0] = new ColumnReference(1, BasicType::Integer);
    _groupByAttributes[1] = new ColumnReference(3, BasicType::Integer);
    _groupByAttributes[2] = new ColumnReference(5, BasicType::Integer);
    _groupByAttributes[3] = segmentExpr;

    auto _window = new WindowDefinition(RANGE_BASED, 30, 1); //(ROW_BASED, 30*1000, 1*1000);
    Aggregation
        *_aggregation = new Aggregation(*_window, _aggregationTypes, _aggregationAttributes, _groupByAttributes);

#if defined(TCP_INPUT)
    bool replayTimestamps = false;
#elif defined(RDMA_INPUT)
    bool replayTimestamps = false;
#else
    bool replayTimestamps = _window->isRangeBased();
#endif

    // Set up code-generated operator
    OperatorKernel *_genCode = new OperatorKernel(true, true, useParallelMerge);
    _genCode->setInputSchema(getSchema());
    _genCode->setAggregation(_aggregation);
    _genCode->setCustomHashTable(buildCustomHashTable());
    _genCode->setQueryId(0);
    _genCode->setup();
    OperatorCode *_cpuCode = _genCode;

    // Print operator
    std::cout << _cpuCode->toSExpr() << std::endl;
    // Define an ft-operator
    auto _queryOperator = new QueryOperator(*_cpuCode, true);
    std::vector<QueryOperator *> _operators;
    _operators.push_back(_queryOperator);

    m_timestampReference = std::chrono::system_clock::now().time_since_epoch().count();
    std::vector<std::shared_ptr<Query>> queries(1);
    queries[0] = std::make_shared<Query>(0,
                                         _operators,
                                         *_window,
                                         m_schema,
                                         m_timestampReference,
                                         true,
                                         replayTimestamps,
                                         !replayTimestamps, //false,
                                         useParallelMerge,
                                         0, persistInput, nullptr, !SystemConf::getInstance().RECOVER);

    if (SystemConf::getInstance().CHECKPOINT_COMPRESSION) {
      if (true) {
        queries[0]->getBuffer()->setCompressionFP(LRB2Compress::compressInput_);
        queries[0]->getBuffer()->setDecompressionFP(LRB2Compress::decompressInput);
      } else {
        std::cout << "No compression is used in the input" << std::endl;
      }
    }

    m_application = new QueryApplication(queries, SystemConf::getInstance().CHECKPOINT_ON, !SystemConf::getInstance().RECOVER);
    m_application->setup();
    if (SystemConf::getInstance().CHECKPOINT_COMPRESSION && (SystemConf::getInstance().CHECKPOINT_ON || persistInput)) {
      LRB2Compress::metadata = new std::vector<std::string, tbb::cache_aligned_allocator<std::string>>(SystemConf::getInstance().WORKER_THREADS, "");
      LRB2Compress::dcomp = new std::vector<std::unique_ptr<
          DictionaryCompressor<LRB2Compress::Key, uint16_t, LRB2Compress::hash,
                               LRB2Compress::Eq>>>();
      for (int w = 0; w < SystemConf::getInstance().WORKER_THREADS; ++w) {
        LRB2Compress::dcomp->emplace_back(
            std::make_unique<DictionaryCompressor<LRB2Compress::Key, uint16_t,
                                                  LRB2Compress::hash, LRB2Compress::Eq>>(
                SystemConf::getInstance().HASH_TABLE_SIZE));
      }
    }
    if (SystemConf::getInstance().CHECKPOINT_ON && SystemConf::getInstance().CHECKPOINT_COMPRESSION) {
      m_application->getCheckpointCoordinator()->setCompressionFP(0, LRB2Compress::compress);
      m_application->getCheckpointCoordinator()->setDecompressionFP(0, LRB2Compress::decompress);
    }
  }

  std::string buildCustomHashTable() {
    std::string barrier = (SystemConf::getInstance().PARALLEL_MERGE_ON) ? "8" : "220";
    return
        "struct Key {\n"
        "    int _0;\n"
        "    int _1;\n"
        "    int _2;\n"
        "    int _3;\n"
        "};\n"
        "using KeyT = Key;\n"
        "using ValueT = Value;\n"
        "\n"
        "struct HashMapEqualTo {\n"
        "    constexpr bool operator()(const KeyT& lhs, const KeyT& rhs) const {\n"
        "        return lhs._0 == rhs._0 && lhs._1 == rhs._1 && lhs._2 == rhs._2 && lhs._3 == rhs._3;\n"
        "    }\n"
        "};\n"
        "\n"
        "struct CustomHash {\n"
        "    std::size_t operator()(KeyT t) const {\n"
        "        std::hash<int> _h;\n"
        "        return _h(t._0);\n"
        "    }\n"
        "};\n"
        "using MyHash = CustomHash;\n"
        "\n"
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
        "    using AggrT = Aggregator;\n"
        "\n"
        "    HashT     _hasher;\n"
        "    EqT       _eq;\n"
        "    BucketT*  _buckets     = nullptr;\n"
        "    AggrT*    _aggrs       = nullptr;\n"
        "    size_t    _num_buckets = MAP_SIZE;\n"
        "    size_t    _num_filled  = 0;\n"
        "    size_t    _mask        = MAP_SIZE-1;\n"
        "    int       _barrier     = " + barrier + ";\n"
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
                                                    "    _buckets  = (BucketT*)malloc(_num_buckets * sizeof(BucketT));\n"
                                                    "    _aggrs  = (AggrT*)malloc(_num_buckets * sizeof(AggrT));\n"
                                                    "    if (!_buckets /*|| !_aggrs*/) {\n"
                                                    "        free(_buckets);\n"
                                                    "        /*free(_aggrs);*/\n"
                                                    "        throw std::bad_alloc();\n"
                                                    "    }\n"
                                                    "\n"
                                                    "    for (auto i = 0; i < _num_buckets; ++i) {\n"
                                                    "        _buckets[i].state = 0;\n"
                                                    "        _buckets[i].dirty = 0;\n"
                                                    "        _aggrs[i] = AggrT (); // maybe initiliaze this on insert\n"
                                                    "        _aggrs[i].initialise();\n"
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
                                                    "        _aggrs[i].initialise();\n"
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
                                                    "    char tempState;\n"
                                                    "    int steps = 0;\n"
                                                    "    for (; i < _num_buckets; i++) {\n"
                                                    "        tempState = _buckets[i].state;\n"
                                                    "        if (tempState && _eq(_buckets[i].key, key)) {\n"
                                                    "\t\t\t_buckets[i].value._1 = _buckets[i].value._1+value._1;\n"
                                                    "            _buckets[i].counter++;\n"
                                                    "            return;\n"
                                                    "        }\n"
                                                    "        if (!tempState && (_buckets[i].key._0 == key._0 || _eq(_buckets[i].key, key) || _buckets[i].dirty == 0)) { // first insert -- keep track of previous inserted value\n"
                                                    "            _buckets[i].state = 1;\n"
                                                    "            _buckets[i].dirty = 1;\n"
                                                    "            _buckets[i].timestamp = timestamp;\n"
                                                    "            _buckets[i].key = key;\n"
                                                    "            _buckets[i].value = value;\n"
                                                    "            _buckets[i].counter = 1;\n"
                                                    "            _aggrs[i].initialise();\n"
                                                    "            return;\n"
                                                    "        }\n"
                                                    "        steps++;\n"
                                                    "        if (steps == _barrier ) {\n"
                                                    "            printf(\"Too many collisions, increase the size...\\n\");\n"
                                                    "            exit(1);\n"
                                                    "        };\n"
                                                    "    }\n"
                                                    "    for (i = 0; i < ind; i++) {\n"
                                                    "        tempState = _buckets[i].state;\n"
                                                    "        if (tempState && _eq(_buckets[i].key, key)) {\n"
                                                    "\t\t\t\t_buckets[i].value._1 = _buckets[i].value._1+value._1;\n"
                                                    "            _buckets[i].counter++;\n"
                                                    "            return;\n"
                                                    "        }\n"
                                                    "        if (!tempState && (_buckets[i].key._0 == key._0 || _eq(_buckets[i].key, key) || _buckets[i].dirty == 0)) { // first insert -- keep track of previous inserted value\n"
                                                    "            _buckets[i].state = 1;\n"
                                                    "            _buckets[i].dirty = 1;\n"
                                                    "            _buckets[i].timestamp = timestamp;\n"
                                                    "            _buckets[i].key = key;\n"
                                                    "            _buckets[i].value = value;\n"
                                                    "            _buckets[i].counter = 1;\n"
                                                    "            _aggrs[i].initialise();\n"
                                                    "            return;\n"
                                                    "        }\n"
                                                    "        steps++;\n"
                                                    "        if (steps == _barrier ) {\n"
                                                    "            printf(\"Too many collisions, increase the size...\\n\");\n"
                                                    "            exit(1);\n"
                                                    "        };\n"
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
                                                    "void HashTable::insertSlices () {\n"
                                                    "    int maxNumOfSlices = INT_MIN;\n"
                                                    "    for (auto i = 0; i < _num_buckets; ++i) {\n"
                                                    "        int temp = _aggrs[i].addedElements - _aggrs[i].removedElements;\n"
                                                    "        if (_buckets[i].state) {\n"
                                                    "                node n;\n"
                                                    "\t\t\t\tn._1 = _buckets[i].value._1;\n"
                                                    "                _aggrs[i].insert(n);\n"
                                                    "            _buckets[i].state = 0;\n"
                                                    "            //_buckets[i].value = ValueT();\n"
                                                    "        } else if (temp > 0) {\n"
                                                    "            ValueT val;\n"
                                                    "            node n;\n"
                                                    "\t\t\tn._1 = val._1;\n"
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
                                                    "            _buckets[i].state = 1;\n"
                                                    "\t\t\t_buckets[i].value._1 = res._1;\n"
                                                    "            _buckets[i].counter = 1;\n"
                                                    "        }\n"
                                                    "    }\n"
                                                    "}\n"
                                                    "\n"
                                                    "void HashTable::setIntermValues (int pos, long timestamp) {\n"
                                                    "    for (auto i = 0; i < _num_buckets; ++i) {\n"
                                                    "        if (_aggrs[i].addedElements - _aggrs[i].removedElements > 0) {\n"
                                                    "            auto res = _aggrs[i].queryIntermediate (pos);\n"
                                                    "            _buckets[i].timestamp = timestamp;\n"
                                                    "            _buckets[i].state = 1;\n"
                                                    "\t\t\t_buckets[i].value._1 = res._1;\n"
                                                    "        }\n"
                                                    "    }\n"
                                                    "}\n"
                                                    "\n"
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
                                                    "    int steps = 0;\n"
                                                    "    index = -1; \n"
                                                    "    for (; i < _num_buckets; i++) {\n"
                                                    "        if (_buckets[i].state && _eq(_buckets[i].key, key)) {\n"
                                                    "            index = i;\n"
                                                    "            return true;\n"
                                                    "        }\n"
                                                    "        if (_buckets[i].state == 0 && index == -1) {\n"
                                                    "            index = i;\n"
                                                    "        }\n"
                                                    "        steps++;\n"
                                                    "        if (steps == _barrier ) {\n"
                                                    "            return false;\n"
                                                    "        };\n"
                                                    "    }\n"
                                                    "    for (i = 0; i < ind; i++) {\n"
                                                    "        if (_buckets[i].state && _eq(_buckets[i].key, key)) {\n"
                                                    "            index = i;\n"
                                                    "            return true;\n"
                                                    "        }\n"
                                                    "        if (_buckets[i].state == 0 && index == -1) {\n"
                                                    "            index = i;\n"
                                                    "        }\n"
                                                    "        steps++;\n"
                                                    "        if (steps == _barrier ) {\n"
                                                    "            return false;\n"
                                                    "        };\n"
                                                    "    }\n"
                                                    "    return false;\n"
                                                    "}\n"
                                                    "\n"
                                                    "void HashTable::deleteHashTable() {\n"
                                                    "    for (size_t bucket=0; bucket<_num_buckets; ++bucket) {\n"
                                                    "        _buckets[bucket].~BucketT();\n"
                                                    "        _aggrs->~AggrT();\n"
                                                    "    }\n"
                                                    "    free(_buckets);\n"
                                                    "    free(_aggrs);\n"
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
                                                    "}\n";
  }

 public:
  LRB2(bool inMemory = true) {
    m_name = "LRB2";
    createSchema();
    createApplication();
    m_fileName = "lrb-data-small-ht.txt";
    if (inMemory)
      loadInMemoryData();
  }
};
