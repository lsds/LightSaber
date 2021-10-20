#include "benchmarks/applications/ClusterMonitoring/ClusterMonitoring.h"
#include "compression/Compressor.h"
#include "cql/expressions/ColumnReference.h"
#include "cql/expressions/IntConstant.h"
#include "cql/operators/Aggregation.h"
#include "cql/operators/AggregationType.h"
#include "cql/operators/Selection.h"
#include "cql/operators/codeGeneration/OperatorKernel.h"
#include "cql/predicates/ComparisonPredicate.h"
#include "snappy.h"
#include "utils/Query.h"
#include "utils/QueryOperator.h"
#include "utils/WindowDefinition.h"

namespace CM2Compress {
struct alignas(16) input_tuple_t {
  long timestamp;
  long jobId;
  long taskId;
  long machineId;
  int eventType;
  int userId;
  int category;
  int priority;
  float cpu;
  float ram;
  float disk;
  int constraints;
};

std::vector<std::string, tbb::cache_aligned_allocator<std::string>> *metadata;

void compressInput(int pid, char *input, int start, int end, char *output, int &writePos, int length, bool &clear, long latency = -1) {
  if (start == 0 && end == -1) {
    auto offset = 0;
    writePos += offset;
    return;
  }
  auto data = (input_tuple_t *)input;
  std::vector<size_t> idxs (3);
  std::vector<bool> first (3, true);
  BaseDeltaCompressor<long, uint16_t> bcomp(data[0].timestamp);
  DummyFloatCompressor fcomp(1000);
  struct t_1 {
    uint16_t timestamp : 9;
    //uint16_t counter   :  8;
  };
  struct t_2 {
    uint16_t jobId   : 16;
    //uint16_t counter : 16;
  };
  struct t_3 {
    uint16_t cpu     : 10;
    //uint16_t counter :  6;
  };

  //writePos = 0;
  // compress
  for (auto &i : idxs) {
    i = 0;
  }
  for (auto &&b: first) {
    b = false;
  }
  t_1 *buf1 = (t_1 *)(output);
  t_2 *buf2 = (t_2 *)(output + (int) (length*0.33));
  t_3 *buf3 = (t_3 *)(output + (int) (length*0.66));
  size_t n = (end - start) / sizeof(input_tuple_t);
  uint8_t fVal_1 = (n > 0) ? bcomp.compress(data[0].timestamp) : 0;
  auto fVal_2 = (n > 0) ? (uint8_t)data[0].jobId : 0;
  auto fVal_3 = (n > 0) ? fcomp.compress(data[0].cpu) : 0;
  uint16_t count_1 = 1;
  uint8_t count_2 = 1;
  uint16_t count_3 = 1;
  for (size_t idx = 0; idx < n; idx++) {
    if (data[idx].eventType == 3) {
      buf1[idxs[0]++] = {bcomp.compress(data[idx].timestamp)};
      buf2[idxs[1]++] = {(uint16_t)data[idx+1].jobId};
      buf3[idxs[2]++] = {fcomp.compress(data[idx].cpu)};
      /*fVal_1 = (first[0]) ? bcomp.compress(data[idx].timestamp) : fVal_1;
      first[0] = false;
      auto sVal_1 = fVal_1;
      if (idx < n - 1 && data[idx+1].eventType == 3 &&
          fVal_1 == (sVal_1 = bcomp.compress(data[idx + 1].timestamp))) {
        count_1++;
      } else {
        buf1[idxs[0]++] = {fVal_1, count_1};
        fVal_1 = sVal_1;
        count_1 = 1;
      }

      fVal_2 = (first[1]) ? (uint8_t)data[idx].jobId : fVal_2;
      first[1] = false;
      auto sVal_2 = fVal_2;
      if (idx < n - 1 && data[idx+1].eventType == 3 &&
          fVal_2 == (sVal_2 = (uint8_t)data[idx+1].jobId)) {
        count_2++;
      } else {
        buf2[idxs[1]++] = {(uint8_t) fVal_2, count_2};
        fVal_2 = sVal_2;
        count_2 = 1;
      }

      fVal_3 = (first[2]) ? fcomp.compress(data[idx].cpu) : fVal_3;
      first[2] = false;
      auto sVal_3 = fVal_3;
      if (idx < n - 1 && data[idx+1].eventType == 3 &&
          fVal_3 == (sVal_3 = fcomp.compress(data[idx+1].cpu))) {
        count_3++;
      } else {
        buf3[idxs[2]++] = {(uint16_t)fVal_3, count_3};
        fVal_3 = sVal_3;
        count_3 = 1;
      }*/
    }
  }

  writePos += idxs[0] * sizeof(t_1);
  (*metadata)[pid] = "c0 BS " + std::to_string(data[0].timestamp)  + " {uint16_t:9} " + std::to_string(writePos);
  std::memcpy((void *)(output + writePos), (void *)buf2,
              idxs[1] * sizeof(t_2));
  writePos += idxs[1] * sizeof(t_2);
  (*metadata)[pid] += "c1 NS {uint16_t:16} " + std::to_string(writePos);
  std::memcpy((void *)(output + writePos),
              (void *)buf3, idxs[2] * sizeof(t_3));
  writePos += idxs[2] * sizeof(t_3);
  (*metadata)[pid] += "c9 FM 1000 {uint16_t:10} " + std::to_string(writePos);

  (*metadata)[pid] += "FLT 3";

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
}

void compressGenInput(int pid, char *input, int start, int end, char *output, int &writePos, int length, bool &clear, long latency = -1) {
  if (start == 0 && end == -1) {
    auto offset = 0;
    writePos += offset;
    return;
  }
  auto data = (input_tuple_t *)input;
  std::vector<size_t> idxs (3);
  std::vector<bool> first (3, true);
  GorillaTimestampCompressor<long, uint64_t> gorillaComp;
  Simple8 simpleComp;
  GorillaValuesCompressor xorComp;
  //writePos = 0;
  // compress
  for (auto &i : idxs) {
    i = 0;
  }
  for (auto &&b: first) {
    b = false;
  }
  size_t n = (end - start) / sizeof(input_tuple_t);
  // gorilla timestamp
  auto buf1 = (uint64_t *)output;
  uint8_t count_1 = 14;  // as the first delta is stored in 14 bits
  // store first timestamp in 64bits + first delta int 14 bits
  buf1[idxs[0]++] = data[0].timestamp;
  int64_t newDelta = data[1].timestamp - data[0].timestamp;
  buf1[idxs[0]] = newDelta << (64 - count_1);


  struct res2 {uint64_t _1;};
  std::vector<res2> r2 (8192);
  auto buf2 = (uint64_t *)(output + (int) (length*0.33));

  // gorilla float
  // store first float in 64 bits
  auto buf3 = (uint64_t *)(output + (int) (length*0.66));
  buf3[idxs[2]++] = data[0].cpu;
  // calculate trailing and leading zeros for first float
  uint64_t *firstV = (uint64_t *)&data[0].cpu;
  int prevLeadingZeros = __builtin_clzll(*firstV);
  int prevTrailingZeros = __builtin_ctzll(*firstV);

  uint8_t count_2 = 1;
  uint16_t count_3 = 1;
  for (size_t idx = 0; idx < n; idx++) {
    if (data[idx].eventType == 3) {
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
      r2[idxs[1]++] = {static_cast<uint64_t>(data[idx].jobId)};
      if (idx > 0) {
        auto prev = (float)data[idx - 1].cpu;
        auto current = (float)data[idx].cpu;
        uint64_t *a = (uint64_t *)&prev;
        uint64_t *b = (uint64_t *)&current;
        uint64_t xorValue = *a ^ *b;
        auto [appendedValue, appendedValueLength, leadingZeros,
        trailingZeros] = xorComp.compress(xorValue, prevLeadingZeros, prevTrailingZeros);
        prevLeadingZeros = leadingZeros;
        prevTrailingZeros = trailingZeros;
        if (count_3 + appendedValueLength > 64) {
          uint8_t split = (64 - count_3);
          if (appendedValueLength > 1) {
            buf3[idxs[2]] |=
                appendedValue >> (appendedValueLength - split);
          }
          ++idxs[2];
          count_3 = appendedValueLength - split;
        } else {
          count_3 += appendedValueLength;
        }
        buf3[idxs[2]] |= appendedValue << (64 - count_3);
      }
    }
  }

  // simple 8
  int32_t inOffset = 0;
  int32_t outOffset = 0;
  idxs[1] = simpleComp.compress(
      inOffset, outOffset, idxs[1], &r2, &res2::_1, 1, buf2);


  std::memcpy((void *)(output + idxs[0] * sizeof(uint64_t)), (void *)buf2,
              idxs[1] * sizeof(uint64_t));
  std::memcpy((void *)(output + idxs[0] * sizeof(uint64_t) +
                  idxs[1] * sizeof(uint64_t)),
              (void *)buf3, idxs[2] * sizeof(uint64_t));
  writePos += idxs[0] * sizeof(uint64_t) + idxs[1] * sizeof(uint64_t) +
      idxs[2] * sizeof(uint64_t);
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
    long jobId;
    float cpu;
  };
  auto out = (res*) output;
  size_t n = (end - start) / sizeof(input_tuple_t);
  size_t resIdx = 0;
  for (size_t idx = 0; idx < n; idx++) {
    if (data[idx].eventType == 3) {
      out[resIdx++] = {data[idx].timestamp, data[idx].jobId, data[idx].cpu};
    }
  }
  writePos = resIdx * sizeof(res);
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

  auto base = std::stoi(words[2]);
  auto mul = std::stoi(words[11]);
  auto flt = std::stoi(words[15]);
  std::vector<size_t> idxs (3);
  idxs[0] = std::stoi(words[4]);
  idxs[1] = std::stoi(words[8]);
  idxs[2] = std::stoi(words[13]);
  if (SystemConf::getInstance().LATENCY_ON) {
    latency = std::stoi(words[16]);
  }


  //BaseDeltaCompressor<long, uint16_t> bcomp(base);
  struct t_1 {
    uint16_t timestamp : 9;
  };
  struct t_2 {
    uint16_t jobId : 16;
  };
  struct t_3 {
    uint16_t cpu : 10;
  };

  auto res = (input_tuple_t*) input;
  t_1 *col0 = (t_1 *)(output + 128);
  auto *col1 = (t_2 *)(output + 128 + idxs[0]);
  auto *col9 = (t_3 *)(output + 128 + idxs[1]);
  auto wPos = 0;
  auto dataSize = end / sizeof(input_tuple_t);
  auto col1Size = idxs[0] / sizeof(t_1);
  for (int idx = 0; idx < col1Size; ++idx) {
    auto temp = col0[idx];
    res[wPos++].timestamp = temp.timestamp + base;
  }

  if (SystemConf::getInstance().LATENCY_ON) {
    res[0].timestamp = Utils::pack(latency, (int)res[0].timestamp);
  }

  // c1
  wPos = 0;
  for (int idx = 0; idx < col1Size; ++idx) {
    auto temp = col1[idx];
    res[wPos++].jobId = temp.jobId;
  }

  // c9
  wPos = 0;
  for (int idx = 0; idx < col1Size; ++idx) {
    auto temp = col9[idx];
    res[wPos++].cpu = temp.cpu * mul;
  }

  // fix filtered out values
  for (; wPos < dataSize; wPos++) {
    res[wPos].eventType = 0;
  }

  writePos = wPos * sizeof(input_tuple_t);

  /*std::cout << "===========decompress===========" << std::endl;
  auto n = dataSize;
  for (int i = 0; i <n; i++) {
    std::cout << i << " " << res[i].timestamp << " " << res[i].jobId <<
    " " << res[i].cpu << std::endl;
  }
  std::cout << "======================" << std::endl;*/
}

void onlyCompressInputLossless(int pid, char *input, int start, int end, char *output, int &writePos, int length, bool &clear, long latency = -1) {
  if (start == 0 && end == -1) {
    auto offset = 0;
    writePos += offset;
    return;
  }
  auto data = (input_tuple_t *)input;
  std::vector<size_t> idxs (12);
  GorillaTimestampCompressor<long, uint64_t> gorillaComp_0;
  Simple8 simpleComp_1;
  Simple8 simpleComp_2;
  Simple8 simpleComp_3;
  Simple8 simpleComp_4;
  Simple8 simpleComp_5;
  Simple8 simpleComp_6;
  Simple8 simpleComp_7;
  GorillaValuesCompressor xorComp_8;
  GorillaValuesCompressor xorComp_9;
  GorillaValuesCompressor xorComp_10;
  Simple8 simpleComp_11;

  //writePos = 0;
  // compress
  for (auto &i : idxs) {
    i = 0;
  }

  size_t output_length;
  auto buf1 = (uint64_t *)input;
  snappy::RawCompress((const char *)(buf1), end, (char*)(output), &output_length);
  writePos += output_length;


  /*size_t n = (end - start) / sizeof(input_tuple_t);
  // gorilla timestamp
  auto buf1 = (uint64_t *)output;
  uint8_t count_1 = 14;  // as the first delta is stored in 14 bits
  // store first timestamp in 64bits + first delta int 14 bits
  buf1[idxs[0]++] = data[0].timestamp;
  int64_t newDelta = data[1].timestamp - data[0].timestamp;
  buf1[idxs[0]] = newDelta << (64 - count_1);

  // _1
  auto buf2 = (uint64_t *)(output + (int) (length*0.08));
  int32_t inOffset = 0;
  int32_t outOffset = 0;
  idxs[1] = simpleComp_1.compress(
      inOffset, outOffset, n, &data, &input_tuple_t::jobId, 1, buf2);
  // _2
  auto buf3 = (uint64_t *)(output + (int) (length*0.08*2));
  inOffset = 0;
  outOffset = 0;
  idxs[2] = simpleComp_2.compress(
      inOffset, outOffset, n, &data, &input_tuple_t::taskId, 1, buf3);
  // _3
  auto buf4 = (uint64_t *)(output + (int) (length*0.08*3));
  inOffset = 0;
  outOffset = 0;
  idxs[3] = simpleComp_3.compress(
      inOffset, outOffset, n, &data, &input_tuple_t::machineId, 1, buf4);
  // _4
  auto buf5 = (uint64_t *)(output + (int) (length*0.08*4));
  inOffset = 0;
  outOffset = 0;
  idxs[4] = simpleComp_4.compress(
      inOffset, outOffset, n, &data, &input_tuple_t::eventType, 1, buf5);
  // _5
  auto buf6 = (uint64_t *)(output + (int) (length*0.08*5));
  inOffset = 0;
  outOffset = 0;
  idxs[5] = simpleComp_5.compress(
      inOffset, outOffset, n, &data, &input_tuple_t::userId, 1, buf6);
  // _6
  auto buf7 = (uint64_t *)(output + (int) (length*0.08*6));
  inOffset = 0;
  outOffset = 0;
  idxs[6] = simpleComp_6.compress(
      inOffset, outOffset, n, &data, &input_tuple_t::category, 1, buf7);
  // _7
  auto buf8 = (uint64_t *)(output + (int) (length*0.08*7));
  inOffset = 0;
  outOffset = 0;
  idxs[7] = simpleComp_7.compress(
      inOffset, outOffset, n, &data, &input_tuple_t::priority, 1, buf8);
  // _11
  auto buf12 = (uint64_t *)(output + (int) (length*0.08*11));
  inOffset = 0;
  outOffset = 0;
  idxs[11] = simpleComp_11.compress(
      inOffset, outOffset, n, &data, &input_tuple_t::constraints, 1, buf12);

  if (!isFirst[pid]) {
    tempVec[pid].resize(n);
    isFirst[pid] = true;
  }
  // gorilla float
  // _8
  auto buf9 = (uint64_t *)(output + (int) (length*0.08*8));
  buf9[idxs[8]++] = data[0].cpu;
  uint64_t *firstV_8 = (uint64_t *)&data[0].cpu;
  int prevLeadingZeros_8 = __builtin_clzll(*firstV_8);
  int prevTrailingZeros_8 = __builtin_ctzll(*firstV_8);
  uint8_t count_8 = 1;
  // _9
  auto buf10 = (uint64_t *)(output + (int) (length*0.08*9));
  buf10[idxs[9]++] = data[0].ram;
  uint64_t *firstV_9 = (uint64_t *)&data[0].ram;
  int prevLeadingZeros_9 = __builtin_clzll(*firstV_9);
  int prevTrailingZeros_9 = __builtin_ctzll(*firstV_9);
  uint8_t count_9 = 1;
  // _10
  auto buf11 = (uint64_t *)(output + (int) (length*0.08*10));
  buf11[idxs[10]++] = data[0].disk;
  uint64_t *firstV_10 = (uint64_t *)&data[0].disk;
  int prevLeadingZeros_10 = __builtin_clzll(*firstV_10);
  int prevTrailingZeros_10 = __builtin_ctzll(*firstV_10);
  uint8_t count_10 = 1;

  uint8_t count_2 = 1;
  for (size_t idx = 0; idx < n; idx++) {
    if (idx > 1) {
      auto [deltaD, deltaLength] = gorillaComp_0.compress(
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
      auto prev = (float)data[idx - 1].cpu;
      auto current = (float)data[idx].cpu;
      uint64_t *a = (uint64_t *)&prev;
      uint64_t *b = (uint64_t *)&current;
      uint64_t xorValue = *a ^ *b;
      auto [appendedValue, appendedValueLength, leadingZeros,
      trailingZeros] = xorComp_8.compress(xorValue, prevLeadingZeros_8, prevTrailingZeros_8);
      prevLeadingZeros_8 = leadingZeros;
      prevTrailingZeros_8 = trailingZeros;
      if (count_8 + appendedValueLength > 64) {
        uint8_t split = (64 - count_8);
        if (appendedValueLength > 1) {
          buf9[idxs[8]] |=
              appendedValue >> (appendedValueLength - split);
        }
        ++idxs[8];
        count_8 = appendedValueLength - split;
      } else {
        count_8 += appendedValueLength;
      }
      buf9[idxs[8]] |= appendedValue << (64 - count_8);
    }
    if (idx > 0) {
      auto prev = (float)data[idx - 1].ram;
      auto current = (float)data[idx].ram;
      uint64_t *a = (uint64_t *)&prev;
      uint64_t *b = (uint64_t *)&current;
      uint64_t xorValue = *a ^ *b;
      auto [appendedValue, appendedValueLength, leadingZeros,
      trailingZeros] = xorComp_9.compress(xorValue, prevLeadingZeros_9, prevTrailingZeros_9);
      prevLeadingZeros_9 = leadingZeros;
      prevTrailingZeros_9 = trailingZeros;
      if (count_9 + appendedValueLength > 64) {
        uint8_t split = (64 - count_9);
        if (appendedValueLength > 1) {
          buf10[idxs[9]] |=
              appendedValue >> (appendedValueLength - split);
        }
        ++idxs[2];
        count_9 = appendedValueLength - split;
      } else {
        count_9 += appendedValueLength;
      }
      buf10[idxs[9]] |= appendedValue << (64 - count_9);
    }
    if (idx > 0) {
      auto prev = (float)data[idx - 1].disk;
      auto current = (float)data[idx].disk;
      uint64_t *a = (uint64_t *)&prev;
      uint64_t *b = (uint64_t *)&current;
      uint64_t xorValue = *a ^ *b;
      auto [appendedValue, appendedValueLength, leadingZeros,
      trailingZeros] = xorComp_10.compress(xorValue, prevLeadingZeros_10, prevTrailingZeros_10);
      prevLeadingZeros_10 = leadingZeros;
      prevTrailingZeros_10 = trailingZeros;
      if (count_10 + appendedValueLength > 64) {
        uint8_t split = (64 - count_10);
        if (appendedValueLength > 1) {
          buf11[idxs[10]] |=
              appendedValue >> (appendedValueLength - split);
        }
        ++idxs[2];
        count_10 = appendedValueLength - split;
      } else {
        count_10 += appendedValueLength;
      }
      buf11[idxs[10]] |= appendedValue << (64 - count_10);
    }
  }

  writePos += idxs[0] * sizeof(uint64_t);
  std::memcpy((void *)(output + writePos), (void *)buf2,
              idxs[1] * sizeof(uint64_t));
  writePos += idxs[1] * sizeof(uint64_t);
  std::memcpy((void *)(output + writePos), (void *)buf3,
              idxs[2] * sizeof(uint64_t));
  writePos += idxs[2] * sizeof(uint64_t);
  std::memcpy((void *)(output + writePos), (void *)buf4,
              idxs[3] * sizeof(uint64_t));
  writePos += idxs[3] * sizeof(uint64_t);
  std::memcpy((void *)(output + writePos), (void *)buf5,
              idxs[4] * sizeof(uint64_t));
  writePos += idxs[4] * sizeof(uint64_t);
  std::memcpy((void *)(output + writePos), (void *)buf6,
              idxs[5] * sizeof(uint64_t));
  writePos += idxs[5] * sizeof(uint64_t);
  std::memcpy((void *)(output + writePos), (void *)buf7,
              idxs[6] * sizeof(uint64_t));
  writePos += idxs[6] * sizeof(uint64_t);
  std::memcpy((void *)(output + writePos), (void *)buf8,
              idxs[7] * sizeof(uint64_t));
  writePos += idxs[7] * sizeof(uint64_t);
  std::memcpy((void *)(output + writePos), (void *)buf9,
              idxs[8] * sizeof(uint64_t));
  writePos += idxs[8] * sizeof(uint64_t);
  std::memcpy((void *)(output + writePos), (void *)buf10,
              idxs[9] * sizeof(uint64_t));
  writePos += idxs[9] * sizeof(uint64_t);
  std::memcpy((void *)(output + writePos), (void *)buf11,
              idxs[10] * sizeof(uint64_t));
  writePos += idxs[10] * sizeof(uint64_t);
  std::memcpy((void *)(output + writePos), (void *)buf12,
              idxs[11] * sizeof(uint64_t));
  writePos += idxs[11] * sizeof(uint64_t);*/
}
};

class CM2 : public ClusterMonitoring {
 private:
  void createApplication() override {

    SystemConf::getInstance().SLOTS = 256;
    // change this depending on the batch size
    if (SystemConf::getInstance().BATCH_SIZE <= 524288) {
      SystemConf::getInstance().PARTIAL_WINDOWS = 256;
    } else if (SystemConf::getInstance().BATCH_SIZE <= 1048576) {
      SystemConf::getInstance().PARTIAL_WINDOWS = 3 * 256;
    } else {
      SystemConf::getInstance().PARTIAL_WINDOWS = 6 * 256;
    }
    SystemConf::getInstance().HASH_TABLE_SIZE = 32;

    bool useParallelMerge = SystemConf::getInstance().PARALLEL_MERGE_ON;
    bool persistInput = SystemConf::getInstance().PERSIST_INPUT;

    // Configure first query
    auto predicate = new ComparisonPredicate(EQUAL_OP, new ColumnReference(4), new IntConstant(3));
    Selection *selection = new Selection(predicate);

    // Configure second query
    std::vector<AggregationType> aggregationTypes(1);
    aggregationTypes[0] = AggregationTypes::fromString("sum");

    std::vector<ColumnReference *> aggregationAttributes(1);
    aggregationAttributes[0] = new ColumnReference(8, BasicType::Float);

    std::vector<Expression *> groupByAttributes(1);
    groupByAttributes[0] = new ColumnReference(1, BasicType::Long);

    auto window = new WindowDefinition(RANGE_BASED, 60, 1); //ROW_BASED, 60*25, 1*25);
    Aggregation *aggregation = new Aggregation(*window, aggregationTypes, aggregationAttributes, groupByAttributes);

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
    genCode->setSelection(selection);
    genCode->setAggregation(aggregation);
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

    if (SystemConf::getInstance().CHECKPOINT_COMPRESSION) {
      queries[0]->getBuffer()->setCompressionFP(CM2Compress::compressInput);
      queries[0]->getBuffer()->setDecompressionFP(CM2Compress::decompressInput);
    }

    m_application = new QueryApplication(queries, SystemConf::getInstance().CHECKPOINT_ON, !SystemConf::getInstance().RECOVER);
    m_application->setup();

    if (SystemConf::getInstance().CHECKPOINT_COMPRESSION && (SystemConf::getInstance().CHECKPOINT_ON || persistInput)) {
      CM2Compress::metadata = new std::vector<std::string, tbb::cache_aligned_allocator<std::string>>(SystemConf::getInstance().WORKER_THREADS, "");
      //m_application->getCheckpointCoordinator()->setCompressionFP(0, CM2Compress::compress);
    }
  }

 public:
  CM2(bool inMemory = true) {
    m_name = "CM2";
    createSchema();
    createApplication();
    if (inMemory)
      loadInMemoryData();
  }
};
