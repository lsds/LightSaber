#include "benchmarks/applications/LinearRoadBenchmark/LinearRoadBenchmark.h"
#include "compression/Compressor.h"
#include "cql/expressions/ColumnReference.h"
#include "cql/expressions/IntConstant.h"
#include "cql/expressions/operations/Division.h"
#include "cql/operators/Aggregation.h"
#include "cql/operators/AggregationType.h"
#include "cql/operators/codeGeneration/OperatorKernel.h"
#include "cql/predicates/ComparisonPredicate.h"
#include "checkpoint/FileBackedCheckpointCoordinator.h"
#include "utils/Query.h"
#include "utils/QueryOperator.h"
#include "utils/WindowDefinition.h"

namespace LRB1Compress {
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
  float _4;
  int _5;
};
struct Value {
  float _1;
};
struct Key {
  int _0;
  int _1;
  int _2;
};
using KeyT = Key;
using ValueT = Value;
struct alignas(16) BucketComp {
  char state;
  char dirty;
  long timestamp;
  KeyT key;
  ValueT value;
  int counter;
};
struct hash {
  std::size_t operator()(const Key &key) const {
    uint64_t result = uint16_t(key._0) * 100 + uint16_t(key._1) * 10 +
        uint16_t(key._2);  // todo: is this luck?
    return result;
  }
};
struct Eq {
  constexpr bool operator()(const KeyT& lhs, const KeyT& rhs) const {
    return lhs._0 == rhs._0 && lhs._1 == rhs._1 && lhs._2 == rhs._2;
  }
};
std::vector<std::unique_ptr<DictionaryCompressor<Key, uint16_t, hash, Eq>>> *dcomp;
std::vector<std::string, tbb::cache_aligned_allocator<std::string>> *metadata;

void compressInput(int pid, char *input, int start, int end, char *output, int &writePos, int length, bool &clear, long latency = -1) {
  if (start == 0 && end == -1) {
    auto offset = (*dcomp)[pid]->getTable().max_size() *
        (*dcomp)[pid]->getTable().bucket_size();
    std::memcpy(output + writePos, (void *)(*dcomp)[pid]->getTable().buckets(), offset);
    writePos += offset;
    (*metadata)[pid] += "ht " + std::to_string(offset);
    if (latency != -1) {
      if (latency == 0) {
        auto value = ((long*)input)[0];
        latency = (int) (value >> 32);
      }
      (*metadata)[pid] += " " + std::to_string(latency) + " ";
    }
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
  //DummyFloatCompressor fcomp(1000000);
  struct t_1 {
    uint16_t timestamp : 10;
    uint16_t counter   :  6;
  };
  struct t_2 {
    uint16_t groupKey : 10;
    uint16_t value  : 6;
  };

  //writePos = 0;
  // compress
  for (auto &i : idxs) {
    i = 0;
  }
  t_1 *buf1 = (t_1 *)(output);
  t_2 *buf2 = (t_2 *)(output + (int) (length*0.33));
  size_t n = (end - start) / sizeof(input_tuple_t);
  uint16_t count_1 = 1;
  for (size_t idx = 0; idx < n; idx++) {
    auto fVal_1 = bcomp.compress(data[idx].timestamp);
    auto sVal_1 = fVal_1;
    if (idx < n - 1 && fVal_1 ==
        (sVal_1 = bcomp.compress(data[idx + 1].timestamp))) {
      count_1++;
    } else {
      buf1[idxs[0]++] = {fVal_1, count_1};
      fVal_1 = sVal_1;
      count_1 = 1;
    }

    Key temp{data[idx]._3, data[idx]._5,
                     data[idx]._6 / 5280};
    auto fVal_2 = (*dcomp)[pid]->compress(temp);
    auto fVal_3 = (uint16_t) data[idx]._2;//fcomp.compress(data[idx]._2);
    buf2[idxs[1]++] = {fVal_2, fVal_3};
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
  std::vector<size_t> idxs (5, 0);
  BaseDeltaCompressor<long, uint16_t> bcomp(data[0].timestamp);
  Simple8 simpleComp1;
  Simple8 simpleComp2;
  Simple8 simpleComp3;
  //DummyFloatCompressor fcomp(1000000);
  struct t_1 {
    uint16_t timestamp : 10;
    uint8_t counter   :  6;
  };
  struct t_5 {
    uint8_t value  : 8;
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
  auto buf3 = (uint64_t *)(output + (int) (length*0.4));
  inOffset = 0;
  outOffset = 0;
  idxs[2] = simpleComp2.compress(
      inOffset, outOffset, n, &data, &input_tuple_t::_5, 1, buf3);

  // simple 8
  auto buf4 = (uint64_t *)(output + (int) (length*0.6));
  inOffset = 0;
  outOffset = 0;
  //for (size_t idx = 0; idx < n; idx++) {
    //data[idx]._6 /= 5280;
  //}
  idxs[3] = simpleComp3.compress(
      inOffset, outOffset, n, &data, &input_tuple_t::_6, 1, buf4, 5280);

  t_5 *buf5 = (t_5 *)(output + (int) (length*0.8));
  uint16_t count_1 = 1;
  for (size_t idx = 0; idx < n; idx++) {
    auto fVal_1 = bcomp.compress(data[idx].timestamp);
    auto sVal_1 = fVal_1;
    if (count_1 < 63 && idx < n - 1 && fVal_1 ==
        (sVal_1 = bcomp.compress(data[idx + 1].timestamp))) {
      count_1++;
    } else {
      buf1[idxs[0]++] = {static_cast<uint16_t>(fVal_1), static_cast<uint8_t>(count_1)};
      fVal_1 = sVal_1;
      count_1 = 1;
    }
    buf5[idxs[4]++] = {static_cast<uint8_t>(data[idx]._2)};
  }

  writePos += idxs[0] * sizeof(t_1);
  (*metadata)[pid] = "c0 RLE BD " + std::to_string(data[0].timestamp) + " {uint16_t:10,uint8_t:6} " + std::to_string(writePos);
  std::memcpy((void *)(output + writePos), (void *)buf2,
              idxs[1] * sizeof(uint64_t));
  writePos += idxs[1] * sizeof(uint64_t);
  (*metadata)[pid] += " c3 S8 " + std::to_string(writePos);
  std::memcpy((void *)(output + writePos),
              (void *)buf3, idxs[2] * sizeof(uint64_t));
  writePos += idxs[2] * sizeof(uint64_t);
  (*metadata)[pid] += " c5 S8 " + std::to_string(writePos);
  std::memcpy((void *)(output + writePos),
              (void *)buf4, idxs[3] * sizeof(uint64_t));
  writePos += idxs[3] * sizeof(uint64_t);
  (*metadata)[pid] += " c6 S8 " + std::to_string(writePos);
  std::memcpy((void *)(output + writePos),
              (void *)buf5, idxs[4] * sizeof(t_5));
  writePos += idxs[4] * sizeof(t_5);
  (*metadata)[pid] += " c2 NS {uint8_t:8} " + std::to_string(writePos);
  if (SystemConf::getInstance().LATENCY_ON) {
    auto value = data[0].timestamp;
    latency = (int) (value >> 32);
    (*metadata)[pid] += " " + std::to_string(latency) + " ";
  }
  //(*metadata)[pid] = "r0 " + std::to_string(idxs[0]) + " r1 " + std::to_string(idxs[1]) + " ";
  if ((*metadata)[pid].size() > 128) {
    throw std::runtime_error("error: increase the metadata size");
  }
  std::memcpy((void *)(output - 128), (*metadata)[pid].data(), (*metadata)[pid].size());
  (*metadata)[pid].clear();

  /*std::cout << "===========compress===========" << std::endl;
  for (int i = 0; i <n; i++) {
    std::cout << i << " " << data[i].timestamp << " " << data[i]._2 << " " <<
    data[i]._3 << " " << data[i]._5 << " " << data[i]._6 << std::endl;
  }
  std::cout << "======================" << std::endl;*/
}

void decompressInput_(int pid, char *input, int start, int end, char *output, int &writePos, int length, bool &copy, long latency = -1) {
  // parse metadata
  std::string meta;
  if (copy) {
    std::memcpy(output, input, end);
    for (size_t i = 0; i < 128; i++) {
      meta += input[i];
    }
  } else {
    for (size_t i = 0; i < 128; i++) {
      meta += output[i];
    }
  }

  std::istringstream iss(meta);
  std::vector<std::string> words{std::istream_iterator<std::string>{iss},
                                 std::istream_iterator<std::string>{}};

  auto base = std::stoi(words[3]);
  std::vector<size_t> idxs (5);
  idxs[0] = std::stoi(words[5]);
  idxs[1] = std::stoi(words[8]);
  idxs[2] = std::stoi(words[11]);
  idxs[3] = std::stoi(words[14]);
  idxs[4] = std::stoi(words[18]);
  if (SystemConf::getInstance().LATENCY_ON) {
    latency = std::stoi(words[19]);
  }


  //BaseDeltaCompressor<long, uint16_t> bcomp(base);
  Simple8 simpleComp1;
  Simple8 simpleComp2;
  Simple8 simpleComp3;
  struct t_1 {
    uint16_t timestamp : 10;
    uint8_t counter   :  6;
  };
  struct t_5 {
    uint8_t value  : 8;
  };

  auto res = (input_tuple_t*) input;
  t_1 *col0 = (t_1 *)(output + 128);
  auto *col3 = (uint64_t *)(output + 128 + idxs[0]);
  auto *col5 = (uint64_t *)(output + 128 + idxs[1]);
  auto *col6 = (uint64_t *)(output + 128 + idxs[2]);
  t_5 *col2 = (t_5 *)(output + 128 + idxs[3]);
  auto wPos = 0;
  auto dataSize = end / sizeof(input_tuple_t);
  auto col1Size = idxs[0] / sizeof(t_1);
  for (int idx = 0; idx < col1Size; ++idx) {
    auto temp = col0[idx];
    for (int it = 0; it < temp.counter; ++it) {
      res[wPos++].timestamp = temp.timestamp + base;
      if (wPos * sizeof(input_tuple_t) > SystemConf::getInstance().BATCH_SIZE) {
        throw std::runtime_error("error: the write position exceeds the batch size");
      }
    }
  }

  if (SystemConf::getInstance().LATENCY_ON) {
    res[0].timestamp = Utils::pack(latency, (int)res[0].timestamp);
  }

  // c3
  auto retVal = (idxs[1]-idxs[0])/sizeof(uint64_t);
  simpleComp1.decompress(retVal, 0, dataSize, &res, &input_tuple_t::_3, 1, col3);

  // c5
  retVal = (idxs[2]-idxs[1])/sizeof(uint64_t);
  simpleComp1.decompress(retVal, 0, dataSize, &res, &input_tuple_t::_5, 1, col5);

  // c6
  retVal = (idxs[3]-idxs[2])/sizeof(uint64_t);
  simpleComp1.decompress(retVal, 0, dataSize, &res, &input_tuple_t::_6, 1, col6, 5280);

  wPos = 0;
  for (int idx = 0; idx < dataSize; ++idx) {
    auto temp = (int) col2[idx].value;
    res[wPos++]._2 = temp;
    if (wPos * sizeof(input_tuple_t) > SystemConf::getInstance().BATCH_SIZE) {
      throw std::runtime_error("error: the write position exceeds the batch size");
    }
  }
  writePos = wPos * sizeof(input_tuple_t);

  /*std::cout << "===========decompress===========" << std::endl;
  auto n = dataSize;
  for (int i = 0; i <n; i++) {
    std::cout << i << " " << res[i].timestamp << " " << res[i]._2 << " " <<
        res[i]._3 << " " << res[i]._5 << " " << res[i]._6 << std::endl;
  }
  std::cout << "======================" << std::endl;*/
}

void compressGenInput(int pid, char *input, int start, int end, char *output, int &writePos, int length, bool &clear, long latency = -1) {
  if (start == 0 && end == -1) {
    return;
  }
  if (clear) {
    clear = false;
  }

  auto data = (input_tuple_t *)input;
  std::vector<size_t> idxs (5);
  GorillaTimestampCompressor<long, uint64_t> gorillaComp;
  Simple8 simpleComp1;
  Simple8 simpleComp2;
  Simple8 simpleComp3;
  GorillaValuesCompressor xorComp;


  //writePos = 0;
  // compress
  for (auto &i : idxs) {
    i = 0;
  }
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
  auto buf3 = (uint64_t *)(output + (int) (length*0.4));
  inOffset = 0;
  outOffset = 0;
  idxs[2] = simpleComp2.compress(
      inOffset, outOffset, n, &data, &input_tuple_t::_5, 1, buf3);

  // simple 8
  auto buf4 = (uint64_t *)(output + (int) (length*0.6));
  inOffset = 0;
  outOffset = 0;
  //for (size_t idx = 0; idx < n; idx++) {
  //  data[idx]._6 /= 5280;
  //}
  idxs[3] = simpleComp3.compress(
      inOffset, outOffset, n, &data, &input_tuple_t::_6, 1, buf4, 5280);

  // gorilla float
  // store first float in 64 bits
  auto buf5 = (uint64_t *)(output + (int) (length*0.8));
  buf5[idxs[4]++] = data[0]._2;
  // calculate trailing and leading zeros for first float
  uint64_t *firstV = (uint64_t *)&data[0]._2;
  int prevLeadingZeros = __builtin_clzll(*firstV);
  int prevTrailingZeros = __builtin_ctzll(*firstV);
  uint16_t count_5 = 1;

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
    if (idx > 0) {
      auto prev = (float)data[idx - 1]._2;
      auto current = (float)data[idx]._2;
      uint64_t *a = (uint64_t *)&prev;
      uint64_t *b = (uint64_t *)&current;
      uint64_t xorValue = *a ^ *b;
      auto [appendedValue, appendedValueLength, leadingZeros,
      trailingZeros] = xorComp.compress(xorValue, prevLeadingZeros, prevTrailingZeros);
      prevLeadingZeros = leadingZeros;
      prevTrailingZeros = trailingZeros;
      if (count_5 + appendedValueLength > 64) {
        uint8_t split = (64 - count_5);
        if (appendedValueLength > 1) {
          buf5[idxs[4]] |=
              appendedValue >> (appendedValueLength - split);
        }
        ++idxs[4];
        count_5 = appendedValueLength - split;
      } else {
        count_5 += appendedValueLength;
      }
      buf5[idxs[4]] |= appendedValue << (64 - count_5);
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
    int highway;
    int direction;
    int segment;
    float speed;
  };
  auto out = (res*) output;
  size_t n = (end - start) / sizeof(input_tuple_t);
  for (size_t idx = 0; idx < n; idx++) {
    out[idx] = {data[idx].timestamp, data[idx]._3, data[idx]._5,
                data[idx]._6 / 5280, data[idx]._2};
  }
  writePos = n * sizeof(res);
}

void decompressInput(int pid, char *input, int start, int end, char *output, int &writePos, int length, bool &copy, long latency = -1) {
  // parse metadata
  std::string meta;
  if (copy) {
    std::memcpy(output, input, end);
    for (size_t i = 0; i < 128; i++) {
      meta += input[i];
    }
  } else {
    for (size_t i = 0; i < 128; i++) {
      meta += output[i];
    }
  }

  std::istringstream iss(meta);
  std::vector<std::string> words{std::istream_iterator<std::string>{iss},
                                 std::istream_iterator<std::string>{}};

  std::vector<size_t> idxs (2);
  idxs[0] = std::stoi(words[1]);
  idxs[1] = std::stoi(words[3]);
  auto hashTableSize = std::stoi(words[5]);
  if (latency != -1) {
    latency = std::stoi(words[6]);
  }

  int bDecomp = 1;
  float fDecomp = (float) 1;
  struct t_1 {
    uint32_t timestamp : 24;
    uint16_t counter   :  8;
  };
  struct t_2 {
    uint16_t groupKey : 10;
    uint16_t value  : 6;
  };

  auto res = (input_tuple_t*) input;
  t_1 *col1 = (t_1 *)(output + 128);
  t_2 *col2 = (t_2 *)(output + 128 + idxs[0] * sizeof(t_1));
  auto buckets = (Bucket<Key, uint16_t> *)(output + 128 + idxs[0] * sizeof(t_1) + idxs[1] * sizeof(t_2));
  auto wPos = 0;
  for (int idx = 0; idx < idxs[0]; ++idx) {
    auto temp = col1[idx];
    for (int it = 0; it < temp.counter; ++it) {
      res[wPos++].timestamp = temp.timestamp * bDecomp;
      if (wPos * sizeof(input_tuple_t) > SystemConf::getInstance().BATCH_SIZE) {
        throw std::runtime_error("error: the write position exceeds the batch size");
      }
    }
  }

  if (latency != -1) {
    res[0].timestamp = Utils::pack(latency, (int)res[0].timestamp);
  }

  wPos = 0;
  for (int idx = 0; idx < idxs[1]; ++idx) {
    auto temp = col2[idx];
    auto pos = (int)temp.groupKey;
    Key key = buckets[pos].key;
    auto val = (float)((float)temp.value / fDecomp);
    //res[wPos]._1 = 0;
    res[wPos]._2 = val;
    res[wPos]._3 = key._0;
    //res[wPos]._4 = 0;
    res[wPos]._5 = key._1;
    res[wPos]._6 = key._2 * 5280;
    wPos++;
    if (wPos * sizeof(input_tuple_t) > SystemConf::getInstance().BATCH_SIZE) {
      throw std::runtime_error("error: the write position exceeds the batch size");
    }
  }
  writePos = wPos * sizeof(input_tuple_t);
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
  start = start / sizeof(BucketComp);
  end = end / sizeof(BucketComp);
  DummyFloatCompressor fcomp(1000000);

  if (!isComplete) {
    auto inputBuf = (BucketComp *)input;
    BaseDeltaCompressor<long, uint16_t> bcomp(inputBuf[0].timestamp);
    struct res {
      uint16_t timestamp : 9;
      uint16_t groupKey : 8;
      uint16_t speed : 7;
      uint16_t counter : 8;
    };
    auto outputBuf = (res *)output;
    auto outIdx = writePos / sizeof(res);
    for (size_t idx = start; idx < end; ++idx) {
      if (inputBuf[idx].state) {
        outputBuf[outIdx++] = {bcomp.compress(inputBuf[idx].timestamp),
                               (*dcomp)[pid]->compress(inputBuf[idx].key),
                               fcomp.compress(inputBuf[idx].value._1),
                               static_cast<uint16_t>(inputBuf[idx].counter)};
      }
    }
    writePos = outIdx * sizeof(res);
  } else {
    auto inputBuf = (output_tuple_t *)input;
    BaseDeltaCompressor<long, uint16_t> bcomp(inputBuf[0].timestamp);
    struct res {
      uint16_t timestamp : 9;
      uint16_t groupKey : 8;
      uint16_t speed : 7;
    };
    auto outputBuf = (res *)output;
    auto outIdx = writePos / sizeof(res);
    for (size_t idx = start; idx < end; ++idx) {
      Key temp{inputBuf[idx]._1, inputBuf[idx]._2, inputBuf[idx]._3};
      outputBuf[outIdx++] = {bcomp.compress(inputBuf[idx].timestamp),
                             (*dcomp)[pid]->compress(temp),
                             fcomp.compress(inputBuf[idx]._4)};
    }
    writePos = outIdx * sizeof(res);
  }
}

void decompress(int pid, char *input, int start, int end, char *output, int &writePos,
                bool isComplete, bool &clear) {

  BaseDeltaCompressor<long, uint16_t> bcomp(1);
  auto fdecomp = 1000000;
  auto hTable = (BucketComp *) &input[writePos];
  auto outIdx = 0;
  auto base = 1;

  if (!isComplete) {
    auto outputBuf = (BucketComp *)output;
    struct res {
      uint16_t timestamp : 9;
      uint16_t groupKey : 8;
      uint16_t speed : 7;
      uint16_t counter : 8;
    };
    auto inputBuf = (res *) &input[start];
    start = start / sizeof(res);
    end = end / sizeof(res);
    for (size_t idx = start; idx < end; ++idx) {
      if (inputBuf[idx].groupKey >=  (*dcomp)[pid]->getTable().max_size())
        throw std::runtime_error("error: the group key is greater than the hashtable size");
      Key key = hTable[inputBuf[idx].groupKey].key;
      outputBuf[inputBuf[idx].groupKey] = {1, 1, (long)inputBuf[idx].timestamp + base, key,
                                           {(float)inputBuf[idx].speed/fdecomp}, inputBuf[idx].counter};
    }
  } else {
    auto outputBuf = (output_tuple_t *)output;
    struct res {
      uint16_t timestamp : 9;
      uint16_t groupKey : 8;
      uint16_t speed : 7;
    };
    auto inputBuf = (res *) &input[start];
    start = start / sizeof(res);
    end = end / sizeof(res);
    for (size_t idx = start; idx < end; ++idx) {
      Key key = hTable[inputBuf[idx].groupKey].key;
      outputBuf[outIdx] = {static_cast<long>(inputBuf[idx].timestamp) + base, key._0, key._1,
                           key._2, (float)inputBuf[idx].speed * fdecomp, 0};
      outIdx++;
    }
  }
}
};

class LRB1 : public LinearRoadBenchmark {
 private:
  void createApplication() override {
    SystemConf::getInstance().SLOTS = 128;
    SystemConf::getInstance().PARTIAL_WINDOWS = 550; //320;
    //SystemConf::getInstance().HASH_TABLE_SIZE = 256;

    bool useParallelMerge = SystemConf::getInstance().PARALLEL_MERGE_ON;
    bool persistInput = SystemConf::getInstance().PERSIST_INPUT;

    // Configure first query
    auto segmentExpr = new Division(new ColumnReference(6, BasicType::Integer), new IntConstant(5280));

    // Configure second query
    std::vector<AggregationType> aggregationTypes(1);
    aggregationTypes[0] = AggregationTypes::fromString("avg");

    std::vector<ColumnReference *> aggregationAttributes(1);
    aggregationAttributes[0] = new ColumnReference(2, BasicType::Float);

    std::vector<Expression *> groupByAttributes(3);
    groupByAttributes[0] = new ColumnReference(3, BasicType::Integer);
    groupByAttributes[1] = new ColumnReference(5, BasicType::Integer);
    groupByAttributes[2] = segmentExpr;

    auto window = new WindowDefinition(RANGE_BASED, 300, 1); //(ROW_BASED, 300*80, 1*80);
    Aggregation *aggregation = new Aggregation(*window, aggregationTypes, aggregationAttributes, groupByAttributes);

    // Configure third query
    auto predicate = new ComparisonPredicate(LESS_OP, new ColumnReference(4), new IntConstant(40));
    Selection *selection = new Selection(predicate);

#if defined(TCP_INPUT)
    bool replayTimestamps = false;
#elif defined(RDMA_INPUT)
    bool replayTimestamps = false;
#else
    bool replayTimestamps = window->isRangeBased();
#endif

    // Set up code-generated operator
    OperatorKernel *genCode = new OperatorKernel(true, true, useParallelMerge);
    genCode->setInputSchema(getSchema());
    genCode->setAggregation(aggregation);
    genCode->setCollisionBarrier(8);
    genCode->setHaving(selection);
    genCode->setQueryId(0);
    genCode->setup();
    OperatorCode *cpuCode = genCode;

    // Print operator
    std::cout << cpuCode->toSExpr() << std::endl;

    // Define an ft-operator
    auto queryOperator = new QueryOperator(*cpuCode, true);
    std::vector<QueryOperator *> operators;
    operators.push_back(queryOperator);

    // this is used for latency measurements
    m_timestampReference = std::chrono::system_clock::now().time_since_epoch().count();

    std::vector<std::shared_ptr<Query>> queries(1);
    queries[0] = std::make_shared<Query>(0,
                                         operators,
                                         *window,
                                         m_schema,
                                         m_timestampReference,
                                         true,
                                         replayTimestamps,
                                         !replayTimestamps,
                                         useParallelMerge,
                                         0, persistInput, nullptr, !SystemConf::getInstance().RECOVER);

    if (SystemConf::getInstance().CHECKPOINT_COMPRESSION/* && false*/) {
      if (true) {
        queries[0]->getBuffer()->setCompressionFP(LRB1Compress::compressInput_);
        queries[0]->getBuffer()->setDecompressionFP(
            LRB1Compress::decompressInput_);
      } else {
        std::cout << "No compression is used in the input" << std::endl;
      }
    }

    m_application = new QueryApplication(queries, SystemConf::getInstance().CHECKPOINT_ON, !SystemConf::getInstance().RECOVER);
    m_application->setup();
    if (SystemConf::getInstance().CHECKPOINT_COMPRESSION && (SystemConf::getInstance().CHECKPOINT_ON || persistInput)) {
      LRB1Compress::metadata = new std::vector<std::string, tbb::cache_aligned_allocator<std::string>>(SystemConf::getInstance().WORKER_THREADS, "");
      LRB1Compress::dcomp = new std::vector<std::unique_ptr<
          DictionaryCompressor<LRB1Compress::Key, uint16_t, LRB1Compress::hash,
                               LRB1Compress::Eq>>>();
      for (int w = 0; w < SystemConf::getInstance().WORKER_THREADS; ++w) {
        LRB1Compress::dcomp->emplace_back(
            std::make_unique<DictionaryCompressor<LRB1Compress::Key, uint16_t,
                                                  LRB1Compress::hash, LRB1Compress::Eq>>(
            SystemConf::getInstance().HASH_TABLE_SIZE));
      }
    }
    if (SystemConf::getInstance().CHECKPOINT_ON && SystemConf::getInstance().CHECKPOINT_COMPRESSION) {
      m_application->getCheckpointCoordinator()->setCompressionFP(0, LRB1Compress::compress);
      m_application->getCheckpointCoordinator()->setDecompressionFP(0, LRB1Compress::decompress);
    }
  }

 public:
  LRB1(bool inMemory = true) {
    m_name = "LRB1";
    createSchema();
    createApplication();
    m_fileName = "lrb-data-small-ht.txt";
    if (inMemory)
      loadInMemoryData();
  }
};
