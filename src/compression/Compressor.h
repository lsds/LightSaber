#pragma once

#include <sstream>

#include "compression/Zigzag.h"
#include "cql/operators/HashTable.h"

#define DELTA_7_MASK 0x02 << 7;
#define DELTA_9_MASK 0x06 << 9;
#define DELTA_12_MASK 0x0E << 12;
#define DELTA_28_MASK 0x0F << 28;

/*
 * \biref A set of compression algorithms used for implementing the hardcoded
 * versions of compression.
 *
 * */

template <typename In, typename Out>
class Compressor {
 public:
  virtual Out compress(In &input) = 0;
};

template <typename In, typename Out, typename H, typename Eq>
class DictionaryCompressor : public Compressor<In, Out> {
 private:
  HashSet<In, Out, H, Eq> m_table;

 public:
  DictionaryCompressor(size_t size) : m_table(size) {}
  Out compress(In &input) override {
    Out res;
    if (m_table.find(input, res)) {
      return res;
    }
    m_table.insert(input, res);
    return res;
  }
  HashSet<In, Out, H, Eq> &getTable() { return m_table; }
  void clear() { m_table.clear(); }
};

template <typename In, typename Out>
class BaseDeltaCompressor : public Compressor<In, Out> {
 private:
  In m_base;

 public:
  BaseDeltaCompressor(In base) : m_base(base) {}
  Out compress(In &input) override { return std::abs(m_base - input); }
};

template <typename In, typename Out>
class BucketCompressor : public Compressor<In, Out> {
 private:
  In m_bucket;

 public:
  BucketCompressor(In bucket) : m_bucket(bucket) {}
  Out compress(In &input) override { return Out(input / m_bucket); }
};

class DummyFloatCompressor : public Compressor<float, uint16_t> {
 private:
  const int m_multiplier;

 public:
  DummyFloatCompressor(int multiplier) : m_multiplier(multiplier) {
    // check for precision issues here!
  }
  uint16_t compress(float &input) override {
    return (uint16_t)(input * m_multiplier);
  }
};

template <typename In, typename Out>
class GorillaTimestampCompressor {
 public:
  GorillaTimestampCompressor() {}

  std::tuple<Out, int> compress(In timestamp, In prevTimestamp,
                                In doublePrevTimestamp) {
    int deltaLength = 0;  // The length of the stored value
    uint64_t deltaD = 0;  // The value stored in 64 bits due to shift up to 64
    int64_t deltaOfDelta =
        (timestamp - prevTimestamp) - (prevTimestamp - doublePrevTimestamp);
    if (deltaOfDelta == 0) {
      deltaLength = 1;
      deltaD = 0;
    } else if (deltaOfDelta < 65 && deltaOfDelta > -64) {
      deltaD = zz::encode(deltaOfDelta);
      deltaD |= DELTA_7_MASK;
      deltaLength = 9;
    } else if (deltaOfDelta < 256 && deltaOfDelta > -255) {
      deltaD = zz::encode(deltaOfDelta);
      deltaD |= DELTA_9_MASK;
      deltaLength = 12;
    } else if (deltaOfDelta < 2048 && deltaOfDelta > -2047) {
      deltaD = zz::encode(deltaOfDelta);
      deltaD |= DELTA_12_MASK;
      deltaLength = 16;
    } else {
      deltaD = zz::encode(deltaOfDelta);
      deltaD |= DELTA_28_MASK;
      deltaLength = 32;
    }
    return {deltaD, deltaLength};
  }
};

class GorillaValuesCompressor {
 public:
  std::tuple<uint64_t, int, int, int> compress(uint64_t xorValue,
                                               int prevLeadingZeros,
                                               int prevTrailingZeros) {
    uint64_t appendedValue;
    int appendedValueLength = 0;

    if (xorValue == 0) {
      appendedValue = 0;
    } else {
      int leadingZeros = __builtin_clzll(xorValue);
      int trailingZeros = __builtin_ctzll(xorValue);
      if (leadingZeros >= 32) {
        leadingZeros = 31;
      }

      if (leadingZeros == trailingZeros) {
        xorValue = xorValue >> 1 << 1;
        trailingZeros = 1;
      }
      // Store bit '1'
      appendedValue = 1;
      appendedValueLength++;

      if (leadingZeros >= prevLeadingZeros &&
          trailingZeros >= prevTrailingZeros) {
        appendedValue <<= 1;
        appendedValueLength++;
        int significantBits = 64 - prevLeadingZeros - prevTrailingZeros;
        xorValue >>= prevTrailingZeros;
        appendedValue <<= significantBits;
        appendedValue |= xorValue;
        appendedValueLength += significantBits;
      } else {
        int significantBits = 64 - leadingZeros - trailingZeros;
        // fot_comment: 0x20->0010 0000 to keep the 1 control bit
        // then xor with the leading zeros, keeping the leading
        // zeros after 1 then shifts it for 6 places to enter the
        // significant bits so 1+5 leading zeros+ 6 length of
        // significant = 12 length
        appendedValue <<= 12;
        appendedValue |= ((0x20 ^ leadingZeros) << 6) ^ (significantBits);
        appendedValueLength += 12;
        xorValue >>= trailingZeros;  // Length of meaningful bits in
        // the next 6 bits
        appendedValue <<= significantBits;
        appendedValue |= xorValue;
        appendedValueLength += significantBits;
      }
    }
    return {appendedValue, appendedValueLength, prevLeadingZeros,
            prevTrailingZeros};
  }
};

class VarByte {
 public:
  size_t compression(uint64_t input, uint8_t *buffer, size_t outputSize) {
    while (input > 127) {
      //|128: Set the next byte flag
      buffer[outputSize] = ((uint8_t)(input & 127)) | 128;
      // Remove the seven bits we just wrote
      input >>= 7;
      outputSize++;
    }
    buffer[outputSize++] = ((uint8_t)input) & 127;
    return outputSize;
  }
};

class Simple8 {
  constexpr static const uint32_t bitLength[16] = {
      1, 1, 2, 3, 4, 5, 6, 7, 8, 10, 12, 15, 20, 30, 60, 32};

  static const uint32_t SIMPLE8B_BITSIZE = 60;
  static const uint32_t SIMPLE8B_MAXCODE = 15;
  static const uint32_t SIMPLE8B_MINCODE = 1;

 public:
  template <typename StructName, typename MemberName>
  uint32_t compress(int32_t inOffset, int32_t outOffset, uint32_t n,
                    StructName *st, MemberName mem, int numberForEquation,
                    uint64_t *buf) {
    uint32_t inPos = inOffset;
    uint32_t inEnd = inOffset + n;
    uint32_t outPos = outOffset;

    while (inPos < inEnd) {
      uint32_t remainingCount = inEnd - inPos;
      uint64_t outVal = 0;
      uint32_t code = SIMPLE8B_MINCODE;
      for (; code < SIMPLE8B_MAXCODE; code++) {
        uint32_t intNum = bitLength[SIMPLE8B_MAXCODE - code];
        uint32_t bitLen = bitLength[code];
        intNum = (intNum < remainingCount) ? intNum : remainingCount;

        uint64_t maxVal = (1ULL << bitLen) - 1;
        uint64_t val = static_cast<uint64_t>(code) << SIMPLE8B_BITSIZE;
        uint32_t j = 0;
        for (; j < intNum; j++) {
          uint64_t inputVal = ((*st)[inPos + j].*mem) / numberForEquation;
          /*static_cast<uint64_t>(data[inPos + j].mem);*/
          if (inputVal > maxVal) {
            break;
          }
          val |= inputVal << (j * bitLen);
        }
        if (j == intNum) {
          outVal = val;
          inPos += intNum;
          break;
        }
      }
      // if no bit packing possible, encode just one value
      if (code == SIMPLE8B_MAXCODE) {
        outVal =
            (static_cast<uint64_t>(code) << SIMPLE8B_BITSIZE) |
            ((*st)[inPos++].*mem) / numberForEquation; /*data[inPos++].mem;*/
      }

      buf[outPos++] = outVal;
    }
    return outPos - outOffset;
  }

  template <typename StructName, typename MemberName>
  uint32_t compress(int32_t inOffset, int32_t outOffset, uint32_t n,
                    StructName *st, MemberName mem, int numberForEquation,
                    uint64_t *buf, int divider) {
    uint32_t inPos = inOffset;
    uint32_t inEnd = inOffset + n;
    uint32_t outPos = outOffset;

    while (inPos < inEnd) {
      uint32_t remainingCount = inEnd - inPos;
      uint64_t outVal = 0;
      uint32_t code = SIMPLE8B_MINCODE;
      for (; code < SIMPLE8B_MAXCODE; code++) {
        uint32_t intNum = bitLength[SIMPLE8B_MAXCODE - code];
        uint32_t bitLen = bitLength[code];
        intNum = (intNum < remainingCount) ? intNum : remainingCount;

        uint64_t maxVal = (1ULL << bitLen) - 1;
        uint64_t val = static_cast<uint64_t>(code) << SIMPLE8B_BITSIZE;
        uint32_t j = 0;
        for (; j < intNum; j++) {
          uint64_t inputVal = (((*st)[inPos + j].*mem)/divider) / numberForEquation;
          /*static_cast<uint64_t>(data[inPos + j].mem);*/
          if (inputVal > maxVal) {
            break;
          }
          val |= inputVal << (j * bitLen);
        }
        if (j == intNum) {
          outVal = val;
          inPos += intNum;
          break;
        }
      }
      // if no bit packing possible, encode just one value
      if (code == SIMPLE8B_MAXCODE) {
        outVal =
            (static_cast<uint64_t>(code) << SIMPLE8B_BITSIZE) |
                (((*st)[inPos++].*mem)/divider) / numberForEquation; /*data[inPos++].mem;*/
      }

      buf[outPos++] = outVal;
    }
    return outPos - outOffset;
  }

  template <typename StructName, typename MemberName>
  uint32_t decompress(uint32_t returnVal, uint32_t outOffset, uint32_t n,
                      StructName *st, MemberName mem, int numberForEquation,
                      uint64_t *buf) {
    // REMEMBER!!!! THE FIRST VALUE TO DECODE
    // IS ON THE LSB AND READ INCREMENTAL -> MSB
    uint32_t inPos = 0;
    uint32_t outPos = outOffset;

    for (uint32_t bufferRow = 0; bufferRow < returnVal; bufferRow++) {
      uint32_t remainingCount = n - outPos;
      uint64_t val = buf[inPos++];
      auto code = static_cast<uint32_t>(val >> SIMPLE8B_BITSIZE);

      // optional check for end-of-stream
      if (code == 0) {
        break;  // end of stream
      }

      else {
        // decode bit-packed integers
        uint32_t intNum = bitLength[SIMPLE8B_MAXCODE - code];
        uint32_t bitLen = bitLength[code];
        uint64_t bitMask = (1ULL << bitLen) - 1;
        intNum = (intNum < remainingCount)
                     ? intNum
                     : remainingCount;  // optional buffer end check

        int bufShift = 0;
        for (uint32_t inRow = 0; inRow < intNum; inRow++) {
          // decompressed[outPos++] = (val >> bufShift) & bitMask;
          ((*st)[outPos++].*mem) = (val >> bufShift) & bitMask;
          bufShift += bitLen;
        }
      }
    }
    return outPos;
  }

  template <typename StructName, typename MemberName>
  uint32_t decompress(uint32_t returnVal, uint32_t outOffset, uint32_t n,
                      StructName *st, MemberName mem, int numberForEquation,
                      uint64_t *buf, int multiplier) {
    // REMEMBER!!!! THE FIRST VALUE TO DECODE
    // IS ON THE LSB AND READ INCREMENTAL -> MSB
    uint32_t inPos = 0;
    uint32_t outPos = outOffset;

    for (uint32_t bufferRow = 0; bufferRow < returnVal; bufferRow++) {
      uint32_t remainingCount = n - outPos;
      uint64_t val = buf[inPos++];
      auto code = static_cast<uint32_t>(val >> SIMPLE8B_BITSIZE);

      // optional check for end-of-stream
      if (code == 0) {
        break;  // end of stream
      }

      else {
        // decode bit-packed integers
        uint32_t intNum = bitLength[SIMPLE8B_MAXCODE - code];
        uint32_t bitLen = bitLength[code];
        uint64_t bitMask = (1ULL << bitLen) - 1;
        intNum = (intNum < remainingCount)
                 ? intNum
                 : remainingCount;  // optional buffer end check

        int bufShift = 0;
        for (uint32_t inRow = 0; inRow < intNum; inRow++) {
          // decompressed[outPos++] = (val >> bufShift) & bitMask;
          ((*st)[outPos++].*mem) = ((val >> bufShift) & bitMask) * multiplier;
          bufShift += bitLen;
        }
      }
    }
    return outPos;
  }
};