#include <cql/operators/ThetaJoin.h>

#include "benchmarks/applications/SmartGrid/SmartGrid.h"
#include "cql/expressions/ColumnReference.h"
#include "cql/operators/Aggregation.h"
#include "cql/operators/AggregationType.h"
#include "cql/operators/codeGeneration/OperatorKernel.h"
#include "cql/predicates/ComparisonPredicate.h"
#include "utils/Query.h"
#include "utils/QueryConfig.h"
#include "utils/QueryOperator.h"
#include "utils/WindowDefinition.h"

namespace SG1Compress_ {
struct alignas(16) input_tuple_t {
  long timestamp;
  float value;
  int property;
  int plug;
  int household;
  int house;
  int padding;
};

void compressInput(int pid, char *input, int start, int end, char *output, int &writePos, int length, bool &clear, long latency = -1) {
  if (start == 0 && end == -1) {
    auto offset = 0;
    writePos += offset;
    return;
  }
  auto data = (input_tuple_t *)input;
  std::vector<size_t> idxs (2);
  BaseDeltaCompressor<long, uint8_t> bcomp(data[0].timestamp);
  DummyFloatCompressor fcomp(1000000);
  struct t_1 {
    uint16_t timestamp : 8;
    uint16_t counter   :  8;
  };
  struct t_2 {
    uint32_t value   : 22;
    uint16_t counter : 10;
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
  for (size_t idx = 0; idx < n; idx++) {
    auto fVal_1 = (uint16_t) bcomp.compress(data[idx].timestamp);
    auto sVal_1 = fVal_1;
    if (idx < n - 1 && fVal_1 ==
        (sVal_1 = bcomp.compress(data[idx + 1].timestamp))) {
      count_1++;
    } else {
      buf1[idxs[0]++] = {fVal_1, count_1};
      fVal_1 = sVal_1;
      count_1 = 1;
    }

    auto fVal_2 = fcomp.compress(data[idx].value);
    auto sVal_2 = fVal_2;
    if (idx < n - 1 &&
        fVal_2 == (sVal_2 = fcomp.compress(data[idx + 1].value))) {
      count_2++;
    } else {
      buf2[idxs[1]++] = {(uint16_t)fVal_2, count_2};
      fVal_2 = sVal_2;
      count_2 = 1;
    }
  }
  std::memcpy((void *)(output + idxs[0] * sizeof(t_1)), (void *)buf2,
              idxs[1] * sizeof(t_2));
  writePos += idxs[0] * sizeof(t_1) + idxs[1] * sizeof(t_2);
}

void compressInput_(int pid, char *input, int start, int end, char *output, int &writePos, int length, bool &clear, long latency = -1) {
  if (start == 0 && end == -1) {
    auto offset = 0;
    writePos += offset;
    return;
  }
  auto data = (input_tuple_t *)input;
  std::vector<size_t> idxs (2);
  BaseDeltaCompressor<long, uint8_t> bcomp(data[0].timestamp);
  struct t_1 {
    uint16_t timestamp : 8;
    uint16_t counter   :  8;
  };
  struct t_2 {
    //uint16_t value   : 10;
    //uint16_t counter : 6;
    uint32_t value   : 20;
    uint16_t counter : 12;
  };
  /*struct t_2 {
    uint8_t value   : 4;
    uint8_t counter : 4;
  };*/

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
  for (size_t idx = 0; idx < n; idx++) {
    auto fVal_1 = (uint16_t) bcomp.compress(data[idx].timestamp);
    auto sVal_1 = fVal_1;
    if (idx < n - 1 && fVal_1 ==
        (sVal_1 = bcomp.compress(data[idx + 1].timestamp))) {
      count_1++;
    } else {
      buf1[idxs[0]++] = {fVal_1, count_1};
      fVal_1 = sVal_1;
      count_1 = 1;
    }

    auto fVal_2 = data[idx].value;//(int)std::round(data[idx].value*100);
    auto sVal_2 = fVal_2;
    if (idx < n - 1 &&
        fVal_2 == (sVal_2 = data[idx].value)) {//(int)std::round(data[idx+1].value*100))) {
      count_2++;
    } else {
      buf2[idxs[1]++] = {(uint8_t)fVal_2, count_2};
      fVal_2 = sVal_2;
      count_2 = 1;
    }
    //buf2[idxs[1]++] = {(uint16_t)std::round(data[idx].value*1)};
  }
  std::memcpy((void *)(output + idxs[0] * sizeof(t_1)), (void *)buf2,
              idxs[1] * sizeof(t_2));
  writePos += idxs[0] * sizeof(t_1) + idxs[1] * sizeof(t_2);
}

struct tempV {
  int _1;
};
std::vector<tempV> tempVec[20];
bool isFirst[20] = {false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false};
void compressGenInput(int pid, char *input, int start, int end, char *output, int &writePos, int length, bool &clear, long latency = -1) {
  if (start == 0 && end == -1) {
    auto offset = 0;
    writePos += offset;
    return;
  }
  auto data = (input_tuple_t *)input;
  std::vector<size_t> idxs (2);
  GorillaTimestampCompressor<long, uint64_t> gorillaComp;
  Simple8 simpleComp;

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

  if (!isFirst[pid]) {
    tempVec[pid].resize(n);
    isFirst[pid] = true;
  }

  uint8_t count_2 = 1;
  uint16_t count_3 = 1;
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
    tempVec[pid][idx]._1 = data[idx].value;
  }

  // simple 8
  auto tempData = tempVec[pid].data();
  auto buf2 = (uint64_t *)(output + (int) (length*0.33));
  int32_t inOffset = 0;
  int32_t outOffset = 0;
  idxs[1] = simpleComp.compress(
      inOffset, outOffset, n, &tempData, &tempV::_1, 1, buf2);

  std::memcpy((void *)(output + idxs[0] * sizeof(uint64_t)), (void *)buf2,
              idxs[1] * sizeof(uint64_t));
  writePos += idxs[0] * sizeof(uint64_t) + idxs[1] * sizeof(uint64_t);
}

void noCompressInput(int pid, char *input, int start, int end, char *output, int &writePos, int length, bool &clear, long latency = -1) {
  if (start == 0 && end == -1) {
    auto offset = 0;
    writePos += offset;
    return;
  }
  auto data = (input_tuple_t *)input;
  struct res1 {
    long timestamp;
  };
  struct res2 {
    float value;
  };
  size_t n = (end - start) / sizeof(input_tuple_t);
  auto out1 = (res1*) output;
  auto out2 = (res2*) (output + (int)(n * sizeof(res1)));
  for (size_t idx = 0; idx < n; idx++) {
    out1[idx] = {data[idx].timestamp};
    out2[idx] = {data[idx].value};
  }
  writePos = n * sizeof(res1) + n * sizeof(res2);
}

void decompressInput(int pid, char *input, int start, int end, char *output, int &writePos, int length, bool &copy, long latency = -1) {
  throw std::runtime_error("error: the decompression function is not implemented");
}
};
namespace SG2Compress_ {
struct alignas(16) input_tuple_t {
  long timestamp;
  float value;
  int property;
  int plug;
  int household;
  int house;
  int padding;
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
    uint64_t result = uint16_t(key._0) * 100 + uint16_t(key._2) * 10 + uint16_t(key._2);
    return result;
  }
};
struct Eq {
  constexpr bool operator()(const KeyT& lhs, const KeyT& rhs) const {
    return lhs._0 == rhs._0 && lhs._1 == rhs._1 && lhs._2 == rhs._2;
  }
};
std::vector<std::unique_ptr<DictionaryCompressor<Key, uint16_t, hash, Eq>>> *dcomp;

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
        buf[idx] = dBucket{ -1, -1, -1};
      }
    }
    writePos += (*dcomp)[pid]->getTable().max_size() * sizeof(dBucket);
    return;
  }
  if (clear) {
    (*dcomp)[pid]->clear();
    clear = false;
  }

  auto data = (input_tuple_t *)input;
  std::vector<size_t> idxs (3);
  BaseDeltaCompressor<long, uint16_t> bcomp(data[0].timestamp);
  DummyFloatCompressor fcomp(1000000);
  struct t_1 {
    uint16_t timestamp : 8;
    uint16_t counter   :  8;
  };
  struct t_2 {
    uint16_t groupKey : 10;
    uint16_t counter  : 6;
  };
  struct t_3 {
    uint32_t value   : 22;
    uint16_t counter : 10;
  };

  //writePos = 0;
  // compress
  for (auto &i : idxs) {
    i = 0;
  }
  t_1 *buf1 = (t_1 *)(output);
  t_2 *buf2 = (t_2 *)(output + (int) (length*0.33));
  t_3 *buf3 = (t_3 *)(output + (int) (length*0.66));
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

    Key temp{data[idx].plug, data[idx].household,
             data[idx].house};
    auto fVal_2 = (*dcomp)[pid]->compress(temp);
    auto sVal_2 = fVal_2;
    if (idx < n - 1) {
      Key temp2{data[idx+1].plug, data[idx+1].household,
                data[idx+1].house};
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

    auto fVal_3 = fcomp.compress(data[idx].value);
    auto sVal_3 = fVal_3;
    if (idx < n - 1 &&
        fVal_3 == (sVal_3 = fcomp.compress(data[idx + 1].value))) {
      count_3++;
    } else {
      buf3[idxs[2]++] = {(uint16_t)fVal_3, count_3};
      fVal_3 = sVal_3;
      count_3 = 1;
    }
  }
  std::memcpy((void *)(output + idxs[0] * sizeof(t_1)), (void *)buf2,
              idxs[1] * sizeof(t_2));
  std::memcpy((void *)(output + idxs[0] * sizeof(t_1) +
                  idxs[1] * sizeof(t_2)),
              (void *)buf3, idxs[2] * sizeof(t_3));
  writePos += idxs[0] * sizeof(t_1) + idxs[1] * sizeof(t_2) +
      idxs[2] * sizeof(t_3);
}

void compressInput_(int pid, char *input, int start, int end, char *output, int &writePos, int length, bool &clear, long latency = -1) {
  if (start == 0 && end == -1) {
    return;
  }
  if (clear) {
    clear = false;
  }

  auto data = (input_tuple_t *)input;
  std::vector<size_t> idxs (5);
  BaseDeltaCompressor<long, uint16_t> bcomp(data[0].timestamp);
  DummyFloatCompressor fcomp(1000000);
  struct t_1 {
    uint16_t timestamp : 8;
    uint16_t counter   :  8;
  };
  struct t_2 {
    uint8_t _1   : 6;
    uint8_t _2 : 6;
    uint8_t _3 : 4;
  };
  struct t_5 {
    uint32_t value   : 22;
    uint16_t counter : 10;
  };
  Simple8 simpleComp1;
  Simple8 simpleComp2;
  Simple8 simpleComp3;
  //writePos = 0;
  // compress
  for (auto &i : idxs) {
    i = 0;
  }
  size_t n = (end - start) / sizeof(input_tuple_t);
  t_1 *buf1 = (t_1 *)(output);
  uint8_t count_1 = 1;  // as the first delta is stored in 14 bits

  // simple 8
  auto buf2 = (t_2 *)(output + (int) (length*0.2));
  /*auto buf2 = (uint64_t *)(output + (int) (length*0.2));
  int32_t inOffset = 0;
  int32_t outOffset = 0;
  idxs[1] = simpleComp1.compress(
      inOffset, outOffset, n, &data, &input_tuple_t::plug, 1, buf2);

  // simple 8
  auto buf3 = (uint64_t *)(output + (int) (length*0.4));
  inOffset = 0;
  outOffset = 0;
  idxs[2] = simpleComp2.compress(
      inOffset, outOffset, n, &data, &input_tuple_t::household, 1, buf2);

  // simple 8
  auto buf4 = (uint64_t *)(output + (int) (length*0.6));
  inOffset = 0;
  outOffset = 0;
  idxs[3] = simpleComp3.compress(
      inOffset, outOffset, n, &data, &input_tuple_t::house, 1, buf2);*/

  // store first float in 64 bits
  t_5 *buf5 = (t_5 *)(output + (int) (length*0.8));
  uint16_t count_5 = 1;

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

    buf2[idxs[1]++] = {static_cast<uint8_t>(data[idx].plug), static_cast<uint8_t>(data[idx].household), static_cast<uint8_t>(data[idx].house)};

    auto fVal_3 = fcomp.compress(data[idx].value);
    auto sVal_3 = fVal_3;
    if (idx < n - 1 &&
        fVal_3 == (sVal_3 = fcomp.compress(data[idx + 1].value))) {
      count_5++;
    } else {
      buf5[idxs[4]++] = {(uint16_t)fVal_3, count_5};
      fVal_3 = sVal_3;
      count_5 = 1;
    }
  }
  writePos += idxs[0] * sizeof(t_1);
  std::memcpy((void *)(output + writePos), (void *)buf2,
              idxs[1] * sizeof(uint64_t));
  writePos += idxs[1] * sizeof(t_2);
  /*std::memcpy((void *)(output + writePos),
              (void *)buf3, idxs[2] * sizeof(uint64_t));
  writePos += idxs[2] * sizeof(uint64_t);
  std::memcpy((void *)(output + writePos),
              (void *)buf4, idxs[3] * sizeof(uint64_t));
  writePos += idxs[3] * sizeof(uint64_t);*/
  std::memcpy((void *)(output + writePos),
              (void *)buf5, idxs[4] * sizeof(t_5));
  writePos += idxs[4] * sizeof(t_5);
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
      inOffset, outOffset, n, &data, &input_tuple_t::plug, 1, buf2);

  // simple 8
  auto buf3 = (uint64_t *)(output + (int) (length*0.4));
  inOffset = 0;
  outOffset = 0;
  idxs[2] = simpleComp2.compress(
      inOffset, outOffset, n, &data, &input_tuple_t::household, 1, buf3);

  // simple 8
  auto buf4 = (uint64_t *)(output + (int) (length*0.6));
  inOffset = 0;
  outOffset = 0;
  idxs[3] = simpleComp3.compress(
      inOffset, outOffset, n, &data, &input_tuple_t::house, 1, buf4);

  // gorilla float
  // store first float in 64 bits
  auto buf5 = (uint64_t *)(output + (int) (length*0.8));
  buf5[idxs[4]++] = data[0].value;
  // calculate trailing and leading zeros for first float
  uint64_t *firstV = (uint64_t *)&data[0].value;
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
      auto prev = (float)data[idx - 1].value;
      auto current = (float)data[idx].value;
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
    int plug;
    int household;
    int house;
    float value;
  };
  auto out = (res*) output;
  size_t n = (end - start) / sizeof(input_tuple_t);
  for (size_t idx = 0; idx < n; idx++) {
    out[idx] = {data[idx].timestamp, data[idx].plug,
                data[idx].household, data[idx].house, data[idx].value};
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
  DummyFloatCompressor fcomp(1000000);

  if (!isComplete) {
    auto inputBuf = (Bucket *)input;
    BaseDeltaCompressor<long, uint16_t> bcomp(inputBuf[0].timestamp);
    struct res {
      uint32_t timestamp : 8;
      uint32_t groupKey : 10;
      uint32_t value : 20;
      uint32_t counter : 10;
    };
    auto outputBuf = (res *)output;
    auto outIdx = writePos / sizeof(res);
    for (size_t idx = start; idx < end; ++idx) {
      if (inputBuf[idx].state) {
        outputBuf[outIdx++] = {bcomp.compress(inputBuf[idx].timestamp),
                               (*dcomp)[pid]->compress(inputBuf[idx].key),
                               fcomp.compress(inputBuf[idx].value._1),
                               static_cast<uint32_t>(inputBuf[idx].counter)};
      }
    }
    writePos = outIdx * sizeof(res);
  } else {
    auto inputBuf = (output_tuple_t *)input;
    BaseDeltaCompressor<long, uint16_t> bcomp(inputBuf[0].timestamp);
    struct res {
      uint32_t timestamp : 8;
      uint32_t groupKey : 10;
      uint32_t value : 20;
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
  throw std::runtime_error("error: the decompression function is not implemented");
}
};
namespace SG3Compress {
struct alignas(16) input_tuple_t_1 {
  long timestamp;
  float _1;
  int _2;
};

struct alignas(16) input_tuple_t_2 {
  long timestamp;
  int _1;
  int _2;
  int _3;
  float _4;
  float _5;
  int _6;
};

void compressInput1(int pid, char *input, int start, int end, char *output, int &writePos, int length, bool &clear, long latency = -1) {
  if (start == 0 && end == -1) {
    auto offset = 0;
    writePos += offset;
    return;
  }
  auto data = (input_tuple_t_1 *)input;
  std::vector<size_t> idxs (2);
  BaseDeltaCompressor<long, uint16_t> bcomp(data[0].timestamp);
  DummyFloatCompressor fcomp(1000000);
  struct t_1 {
    uint16_t timestamp : 8;
    uint16_t counter   :  8;
  };
  struct t_2 {
    uint32_t value   : 22;
    uint16_t counter : 10;
  };

  //writePos = 0;
  // compress
  for (auto &i : idxs) {
    i = 0;
  }
  t_1 *buf1 = (t_1 *)(output);
  t_2 *buf2 = (t_2 *)(output + (int) (length*0.5));
  size_t n = (end - start) / sizeof(input_tuple_t_1);
  uint16_t count_1 = 1;
  uint8_t count_2 = 1;
  for (size_t idx = 0; idx < n; idx++) {
    auto fVal_1 = (uint16_t) bcomp.compress(data[idx].timestamp);
    auto sVal_1 = fVal_1;
    if (idx < n - 1 && fVal_1 ==
        (sVal_1 = bcomp.compress(data[idx + 1].timestamp))) {
      count_1++;
    } else {
      buf1[idxs[0]++] = {fVal_1, count_1};
      fVal_1 = sVal_1;
      count_1 = 1;
    }

    auto fVal_2 = fcomp.compress(data[idx]._1);
    buf2[idxs[1]++] = {(uint16_t)fVal_2, count_2};
  }
  std::memcpy((void *)(output + idxs[0] * sizeof(t_1)), (void *)buf2,
              idxs[1] * sizeof(t_2));
  writePos += idxs[0] * sizeof(t_1) + idxs[1] * sizeof(t_2);
}

void decompressInput1(int pid, char *input, int start, int end, char *output, int &writePos, int length, bool &copy, long latency = -1) {
  throw std::runtime_error("error: the decompression function is not implemented");
}

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
    uint64_t result = uint16_t(key._0) * 100 + uint16_t(key._2) * 10 + uint16_t(key._2);
    return result;
  }
};
struct Eq {
  constexpr bool operator()(const KeyT& lhs, const KeyT& rhs) const {
    return lhs._0 == rhs._0 && lhs._1 == rhs._1 && lhs._2 == rhs._2;
  }
};
std::vector<std::unique_ptr<DictionaryCompressor<Key, uint16_t, hash, Eq>>> *dcomp;

struct dBucket {
  Key key;
};
void compressInput2(int pid, char *input, int start, int end, char *output, int &writePos, int length, bool &clear, long latency = -1) {
  if (start == 0 && end == -1) {
    auto buf = (dBucket*) (output + writePos);
    auto bucket = (Bucket *)(*dcomp)[pid]->getTable().buckets();
    for (size_t idx = 0; idx < (*dcomp)[pid]->getTable().max_size(); ++idx) {
      if (bucket[idx].state) {
        buf[idx] = dBucket{bucket[idx].key};
      } else {
        buf[idx] = dBucket{ -1, -1, -1};
      }
    }
    writePos += (*dcomp)[pid]->getTable().max_size() * sizeof(dBucket);
    return;
  }
  if (clear) {
    (*dcomp)[pid]->clear();
    clear = false;
  }

  auto data = (input_tuple_t_2 *)input;
  std::vector<size_t> idxs (3);
  BaseDeltaCompressor<long, uint16_t> bcomp(data[0].timestamp);
  DummyFloatCompressor fcomp(1000000);
  struct t_1 {
    uint16_t timestamp : 8;
    uint16_t counter   :  8;
  };
  struct t_2 {
    uint16_t groupKey : 10;
    uint16_t counter  : 6;
  };
  struct t_3 {
    uint32_t value   : 22;
    uint16_t counter : 10;
  };

  //writePos = 0;
  // compress
  for (auto &i : idxs) {
    i = 0;
  }
  t_1 *buf1 = (t_1 *)(output);
  t_2 *buf2 = (t_2 *)(output + (int) (length*0.33));
  t_3 *buf3 = (t_3 *)(output + (int) (length*0.66));
  size_t n = (end - start) / sizeof(input_tuple_t_2);
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

    Key temp{data[idx]._1, data[idx]._2, data[idx]._3};
    auto fVal_2 = (*dcomp)[pid]->compress(temp);
    auto sVal_2 = fVal_2;
    if (idx < n - 1) {
      Key temp2{data[idx+1]._1, data[idx+1]._2, data[idx+1]._3};
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

    auto fVal_3 = fcomp.compress(data[idx]._4);
    auto sVal_3 = fVal_3;
    if (idx < n - 1 &&
        fVal_3 == (sVal_3 = fcomp.compress(data[idx + 1]._4))) {
      count_3++;
    } else {
      buf3[idxs[2]++] = {(uint16_t)fVal_3, count_3};
      fVal_3 = sVal_3;
      count_3 = 1;
    }
  }
  std::memcpy((void *)(output + idxs[0] * sizeof(t_1)), (void *)buf2,
              idxs[1] * sizeof(t_2));
  std::memcpy((void *)(output + idxs[0] * sizeof(t_1) +
                  idxs[1] * sizeof(t_2)),
              (void *)buf3, idxs[2] * sizeof(t_3));
  writePos += idxs[0] * sizeof(t_1) + idxs[1] * sizeof(t_2) +
      idxs[2] * sizeof(t_3);
}
void decompressInput2(int pid, char *input, int start, int end, char *output, int &writePos, int length, bool &copy, long latency = -1) {
  throw std::runtime_error("error: the decompression function is not implemented");
}
};

class SG3 : public SmartGrid {
 private:
  void createApplication() override {
    SystemConf::getInstance().PARTIAL_WINDOWS = 3800;

    bool persistInput = SystemConf::getInstance().PERSIST_INPUT;
    bool useParallelMerge = SystemConf::getInstance().PARALLEL_MERGE_ON;

    // Configure first query
    std::vector<AggregationType> aggregationTypes1(1);
    aggregationTypes1[0] = AggregationTypes::fromString("avg");

    std::vector<ColumnReference *> aggregationAttributes1(1);
    aggregationAttributes1[0] = new ColumnReference(1, BasicType::Float);

    std::vector<Expression *> groupByAttributes1;

    auto window1 = new WindowDefinition(RANGE_BASED, 128, 1); //ROW_BASED, 85*400, 1*400);
    Aggregation *aggregation1 = new Aggregation(*window1, aggregationTypes1, aggregationAttributes1, groupByAttributes1);

#if defined(TCP_INPUT)
    bool replayTimestamps1 = false;
#elif defined(RDMA_INPUT)
    bool replayTimestamps1 = false;
#else
    bool replayTimestamps1 = window1->isRangeBased();
#endif

    // Set up code-generated operator
    OperatorKernel *genCode1 = new OperatorKernel(true);
    genCode1->setInputSchema(getSchema());
    genCode1->setAggregation(aggregation1);
    genCode1->setQueryId(0);
    genCode1->setup();
    OperatorCode *cpuCode1 = genCode1;

    // Print operator
    std::cout << genCode1->toSExpr() << std::endl;

    // Define an ft-operator
    auto queryOperator1 = new QueryOperator(*cpuCode1, true);
    std::vector<QueryOperator *> operators1;
    operators1.push_back(queryOperator1);

    // Configure second query
    std::vector<AggregationType> aggregationTypes2(2);
    aggregationTypes2[0] = AggregationTypes::fromString("avg");
    aggregationTypes2[1] = AggregationTypes::fromString("cnt");

    std::vector<ColumnReference *> aggregationAttributes2(2);
    aggregationAttributes2[0] = new ColumnReference(1, BasicType::Float);
    aggregationAttributes2[1] = new ColumnReference(1, BasicType::Float);

    std::vector<Expression *> groupByAttributes2(3);
    groupByAttributes2[0] = new ColumnReference(3, BasicType::Integer);
    groupByAttributes2[1] = new ColumnReference(4, BasicType::Integer);
    groupByAttributes2[2] = new ColumnReference(5, BasicType::Integer);

    auto window2 = new WindowDefinition(RANGE_BASED, 128, 1); //ROW_BASED, 36*1000, 1*1000);
    Aggregation *aggregation2 = new Aggregation(*window2, aggregationTypes2, aggregationAttributes2, groupByAttributes2);

#if defined(TCP_INPUT)
    replayTimestamps1 = false;
    bool replayTimestamps2 = false;
#elif defined(RDMA_INPUT)
    replayTimestamps1 = false;
    bool replayTimestamps2 = false;
#else
    replayTimestamps1 = window1->isRangeBased();
    bool replayTimestamps2 = window2->isRangeBased();
#endif

    // Set up code-generated operator
    OperatorKernel *genCode2 = new OperatorKernel(true, true, useParallelMerge);
    genCode2->setInputSchema(getSchema());
    genCode2->setAggregation(aggregation2);
    genCode2->setCollisionBarrier(28);
    genCode2->setQueryId(1);
    genCode2->setup();
    OperatorCode *cpuCode2 = genCode2;

    // Print operator
    std::cout << genCode2->toSExpr() << std::endl;

    // Define an ft-operator
    auto queryOperator2 = new QueryOperator(*cpuCode2, true);
    std::vector<QueryOperator *> operators2;
    operators2.push_back(queryOperator2);

    // Configure third query
    auto config3 =
        new QueryConfig(128 * SystemConf::getInstance()._MB,
                        256 * SystemConf::getInstance()._KB,
                        256 * SystemConf::getInstance()._KB, 1, 4000);
    bool persistJoinInput = false;
    auto window3 = new WindowDefinition(RANGE_BASED, 1, 1); //ROW_BASED, 1, 1);
    TupleSchema *schema3 = &cpuCode1->getOutputSchema();

    auto window4 = new WindowDefinition(RANGE_BASED, 1, 1); //ROW_BASED, 470, 470);
    TupleSchema *schema4 = &cpuCode2->getOutputSchema();

    auto predicate3 = new ComparisonPredicate(LESS_OP, new ColumnReference(1), new ColumnReference(4));
    auto join = new ThetaJoin(*schema3, *schema4, predicate3);
    join->setQueryId(2);
    join->setup(window3, window4, config3->getCircularBufferSize());

    // Define an ft-operator
    auto queryOperator3 = new QueryOperator(*join, true);
    std::vector<QueryOperator *> operators3;
    operators3.push_back(queryOperator3);

    // this is used for latency measurements
    m_timestampReference = std::chrono::system_clock::now().time_since_epoch().count();

    std::vector<std::shared_ptr<Query>> queries(3);
    queries[0] = std::make_shared<Query>(0,
                                         operators1,
                                         *window1,
                                         m_schema,
                                         m_timestampReference,
                                         true,
                                         replayTimestamps1,
                                         !replayTimestamps1,
                                         false,
                                         0, false, nullptr, !SystemConf::getInstance().RECOVER);
    queries[1] = std::make_shared<Query>(1,
                                         operators2,
                                         *window2,
                                         m_schema,
                                         m_timestampReference,
                                         true,
                                         replayTimestamps2,
                                         !replayTimestamps2, useParallelMerge,
                                         0, persistInput);
    queries[2] = std::make_shared<Query>(2,
                                         operators3,
                                         *window3,
                                         schema3,
                                         *window4,
                                         schema4,
                                         m_timestampReference,
                                         true,
                                         false,
                                         true,
                                         false,
                                         0, persistJoinInput, config3);
    queries[0]->connectTo(queries[2].get());
    queries[1]->connectTo(queries[2].get());

    //queries[0]->markForCheckpoint(false);
    //queries[1]->markForCheckpoint(false);
    //queries[2]->markForCheckpoint(false);

    if (persistInput && SystemConf::getInstance().CHECKPOINT_COMPRESSION) {
      queries[0]->getBuffer()->setCompressionFP(SG1Compress_::compressInput);
      queries[1]->getBuffer()->setCompressionFP(SG2Compress_::compressInput_);
      queries[2]->getBuffer()->setCompressionFP(SG3Compress::compressInput1);
      queries[2]->getSecondBuffer()->setCompressionFP(SG3Compress::compressInput2);

      queries[0]->getBuffer()->setDecompressionFP(SG1Compress_::decompressInput);
      queries[1]->getBuffer()->setDecompressionFP(SG2Compress_::decompressInput);
      queries[2]->getBuffer()->setDecompressionFP(SG3Compress::decompressInput1);
      queries[2]->getSecondBuffer()->setDecompressionFP(SG3Compress::decompressInput2);
    }

    m_application = new QueryApplication(queries, SystemConf::getInstance().CHECKPOINT_ON, !SystemConf::getInstance().RECOVER);
    m_application->setup();
    //std::vector<int> rates {1, 4};
    //m_application->setupRates(rates);
    if (SystemConf::getInstance().CHECKPOINT_COMPRESSION && (SystemConf::getInstance().CHECKPOINT_ON || persistInput)) {
      SG2Compress_::dcomp = new std::vector<std::unique_ptr<
          DictionaryCompressor<SG2Compress_::Key, uint16_t, SG2Compress_::hash,
                               SG2Compress_::Eq>>>();
      for (int w = 0; w < SystemConf::getInstance().WORKER_THREADS; ++w) {
        SG2Compress_::dcomp->emplace_back(
            std::make_unique<DictionaryCompressor<SG2Compress_::Key, uint16_t,
                                                  SG2Compress_::hash, SG2Compress_::Eq>>(
                SystemConf::getInstance().HASH_TABLE_SIZE));
      }
      SG3Compress::dcomp = new std::vector<std::unique_ptr<
          DictionaryCompressor<SG3Compress::Key, uint16_t, SG3Compress::hash,
                               SG3Compress::Eq>>>();
      for (int w = 0; w < SystemConf::getInstance().WORKER_THREADS; ++w) {
        SG3Compress::dcomp->emplace_back(
            std::make_unique<DictionaryCompressor<SG3Compress::Key, uint16_t,
                                                  SG3Compress::hash, SG3Compress::Eq>>(
                SystemConf::getInstance().HASH_TABLE_SIZE));
      }
    }
    if (SystemConf::getInstance().CHECKPOINT_ON && SystemConf::getInstance().CHECKPOINT_COMPRESSION) {
      m_application->getCheckpointCoordinator()->setCompressionFP(1, SG2Compress_::compress);
      m_application->getCheckpointCoordinator()->setDecompressionFP(1, SG2Compress_::decompress);
    }
  }

 public:
  SG3(bool inMemory = true) {
    m_name = "SG3";
    createSchema();
    createApplication();
    if (inMemory)
      loadInMemoryData();
  }
};
