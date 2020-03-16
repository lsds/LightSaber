#pragma once

/*
 * \brief Different hash functions that can be used for code generation.
 *
 * */

#include <iterator>
#include <x86intrin.h>

#define CRCPOLY 0x82f63b78 // reversed 0x1EDC6F41
#define CRCINIT 0xFFFFFFFF
//static uint32_t Crc32Lookup[8][256];

uint32_t Crc32Lookup[256] = {
    0x00000000, 0x77073096, 0xEE0E612C, 0x990951BA, 0x076DC419, 0x706AF48F, 0xE963A535, 0x9E6495A3,
    0x0EDB8832, 0x79DCB8A4, 0xE0D5E91E, 0x97D2D988, 0x09B64C2B, 0x7EB17CBD, 0xE7B82D07, 0x90BF1D91,
    0x1DB71064, 0x6AB020F2, 0xF3B97148, 0x84BE41DE, 0x1ADAD47D, 0x6DDDE4EB, 0xF4D4B551, 0x83D385C7,
    0x136C9856, 0x646BA8C0, 0xFD62F97A, 0x8A65C9EC, 0x14015C4F, 0x63066CD9, 0xFA0F3D63, 0x8D080DF5,
    0x3B6E20C8, 0x4C69105E, 0xD56041E4, 0xA2677172, 0x3C03E4D1, 0x4B04D447, 0xD20D85FD, 0xA50AB56B,
    0x35B5A8FA, 0x42B2986C, 0xDBBBC9D6, 0xACBCF940, 0x32D86CE3, 0x45DF5C75, 0xDCD60DCF, 0xABD13D59,
    0x26D930AC, 0x51DE003A, 0xC8D75180, 0xBFD06116, 0x21B4F4B5, 0x56B3C423, 0xCFBA9599, 0xB8BDA50F,
    0x2802B89E, 0x5F058808, 0xC60CD9B2, 0xB10BE924, 0x2F6F7C87, 0x58684C11, 0xC1611DAB, 0xB6662D3D,
    0x76DC4190, 0x01DB7106, 0x98D220BC, 0xEFD5102A, 0x71B18589, 0x06B6B51F, 0x9FBFE4A5, 0xE8B8D433,
    0x7807C9A2, 0x0F00F934, 0x9609A88E, 0xE10E9818, 0x7F6A0DBB, 0x086D3D2D, 0x91646C97, 0xE6635C01,
    0x6B6B51F4, 0x1C6C6162, 0x856530D8, 0xF262004E, 0x6C0695ED, 0x1B01A57B, 0x8208F4C1, 0xF50FC457,
    0x65B0D9C6, 0x12B7E950, 0x8BBEB8EA, 0xFCB9887C, 0x62DD1DDF, 0x15DA2D49, 0x8CD37CF3, 0xFBD44C65,
    0x4DB26158, 0x3AB551CE, 0xA3BC0074, 0xD4BB30E2, 0x4ADFA541, 0x3DD895D7, 0xA4D1C46D, 0xD3D6F4FB,
    0x4369E96A, 0x346ED9FC, 0xAD678846, 0xDA60B8D0, 0x44042D73, 0x33031DE5, 0xAA0A4C5F, 0xDD0D7CC9,
    0x5005713C, 0x270241AA, 0xBE0B1010, 0xC90C2086, 0x5768B525, 0x206F85B3, 0xB966D409, 0xCE61E49F,
    0x5EDEF90E, 0x29D9C998, 0xB0D09822, 0xC7D7A8B4, 0x59B33D17, 0x2EB40D81, 0xB7BD5C3B, 0xC0BA6CAD,
    0xEDB88320, 0x9ABFB3B6, 0x03B6E20C, 0x74B1D29A, 0xEAD54739, 0x9DD277AF, 0x04DB2615, 0x73DC1683,
    0xE3630B12, 0x94643B84, 0x0D6D6A3E, 0x7A6A5AA8, 0xE40ECF0B, 0x9309FF9D, 0x0A00AE27, 0x7D079EB1,
    0xF00F9344, 0x8708A3D2, 0x1E01F268, 0x6906C2FE, 0xF762575D, 0x806567CB, 0x196C3671, 0x6E6B06E7,
    0xFED41B76, 0x89D32BE0, 0x10DA7A5A, 0x67DD4ACC, 0xF9B9DF6F, 0x8EBEEFF9, 0x17B7BE43, 0x60B08ED5,
    0xD6D6A3E8, 0xA1D1937E, 0x38D8C2C4, 0x4FDFF252, 0xD1BB67F1, 0xA6BC5767, 0x3FB506DD, 0x48B2364B,
    0xD80D2BDA, 0xAF0A1B4C, 0x36034AF6, 0x41047A60, 0xDF60EFC3, 0xA867DF55, 0x316E8EEF, 0x4669BE79,
    0xCB61B38C, 0xBC66831A, 0x256FD2A0, 0x5268E236, 0xCC0C7795, 0xBB0B4703, 0x220216B9, 0x5505262F,
    0xC5BA3BBE, 0xB2BD0B28, 0x2BB45A92, 0x5CB36A04, 0xC2D7FFA7, 0xB5D0CF31, 0x2CD99E8B, 0x5BDEAE1D,
    0x9B64C2B0, 0xEC63F226, 0x756AA39C, 0x026D930A, 0x9C0906A9, 0xEB0E363F, 0x72076785, 0x05005713,
    0x95BF4A82, 0xE2B87A14, 0x7BB12BAE, 0x0CB61B38, 0x92D28E9B, 0xE5D5BE0D, 0x7CDCEFB7, 0x0BDBDF21,
    0x86D3D2D4, 0xF1D4E242, 0x68DDB3F8, 0x1FDA836E, 0x81BE16CD, 0xF6B9265B, 0x6FB077E1, 0x18B74777,
    0x88085AE6, 0xFF0F6A70, 0x66063BCA, 0x11010B5C, 0x8F659EFF, 0xF862AE69, 0x616BFFD3, 0x166CCF45,
    0xA00AE278, 0xD70DD2EE, 0x4E048354, 0x3903B3C2, 0xA7672661, 0xD06016F7, 0x4969474D, 0x3E6E77DB,
    0xAED16A4A, 0xD9D65ADC, 0x40DF0B66, 0x37D83BF0, 0xA9BCAE53, 0xDEBB9EC5, 0x47B2CF7F, 0x30B5FFE9,
    0xBDBDF21C, 0xCABAC28A, 0x53B39330, 0x24B4A3A6, 0xBAD03605, 0xCDD70693, 0x54DE5729, 0x23D967BF,
    0xB3667A2E, 0xC4614AB8, 0x5D681B02, 0x2A6F2B94, 0xB40BBE37, 0xC30C8EA1, 0x5A05DF1B, 0x2D02EF8D
};

template<typename T, size_t len>
struct Crc32Hash {
  /*static void Crc32LookupInit() {
      for (uint32_t i = 0; i <= 0xFF; i++) {
          uint32_t x = i;
          for (uint32_t j = 0; j < 8; j++)
              x = (x>>1) ^ (CRCPOLY & (-(int)(x & 1)));
          Crc32Lookup[0][i] = x;
      }

      for (uint32_t i = 0; i <= 0xFF; i++) {
          uint32_t c = Crc32Lookup[0][i];
          for (uint32_t j = 1; j < 8; j++) {
              c = Crc32Lookup[0][c & 0xFF] ^ (c >> 8);
              Crc32Lookup[j][i] = c;
          }
      }
  }*/
  // Hardware-accelerated CRC-32C (using CRC32 instruction)
  static inline size_t CRC_Hardware(const void *data, size_t length) {
    size_t crc = CRCINIT;

    unsigned char *current = (unsigned char *) data;
    // Align to DWORD boundary
    size_t align = (sizeof(unsigned int) - (__int64_t) current) & (sizeof(unsigned int) - 1);
    align = std::min(align, length);
    length -= align;
    for (; align; align--)
      crc = Crc32Lookup[(crc ^ *current++) & 0xFF] ^ (crc >> 8);

    size_t ndwords = length / sizeof(unsigned int);
    for (; ndwords; ndwords--) {
      crc = _mm_crc32_u32(crc, *(unsigned int *) current);
      current += sizeof(unsigned int);
    }

    length &= sizeof(unsigned int) - 1;
    for (; length; length--)
      crc = _mm_crc32_u8(crc, *current++);
    return ~crc;
  }
  Crc32Hash() {/*Crc32LookupInit();*/}
  std::size_t operator()(T t) const {
    return CRC_Hardware(&t, len);
  }
};

#if COMPILER == GCCC
#define FALLTHROUGH [[gnu::fallthrough]];
#elif  COMPILER == CLANG
#define FALLTHROUGH
#endif
template<class T, int len>
class CRC32Hash {
 public:
  static const uint64_t seed = 902850234;
  inline auto hashKey(uint64_t k) const { // -> typename std::enable_if<IS_INT_LE(64,T), hash_t>::type{
    uint64_t result1 = _mm_crc32_u64(seed, k);
    uint64_t result2 = _mm_crc32_u64(0x04c11db7, k);
    return ((result2 << 32) | result1) * 0x2545F4914F6CDD1Dull;
  }
  //inline uint64_t hashKey(uint64_t k) const { return hashKey(k, 0); }
  inline uint64_t hashKey(const void *key, int length) const {
    auto data = reinterpret_cast<const uint8_t *>(key);
    uint64_t s = seed;
    while (length >= 8) {
      s = hashKey(*reinterpret_cast<const uint64_t *>(data));
      data += 8;
      length -= 8;
    }
    if (length >= 4) {
      s = hashKey((uint32_t) *reinterpret_cast<const uint32_t *>(data));
      data += 4;
      length -= 4;
    }
    switch (length) {
      case 3: s ^= ((uint64_t) data[2]) << 16;FALLTHROUGH
      case 2: s ^= ((uint64_t) data[1]) << 8;FALLTHROUGH
      case 1: s ^= data[0];FALLTHROUGH
    }
    return s;
  }
  std::size_t operator()(T t) const {
    return hashKey(&t, len);
  }
};

template<typename T, size_t len>
struct MurmurHash {
  static inline uint64_t MurmurHash64(void *key, int length, unsigned int seed) {
    const uint64_t m = 0xc6a4a7935bd1e995;
    const int r = 47;
    uint64_t h = seed ^(length * m);
    const uint64_t *data = (const uint64_t *) key;
    const uint64_t *end = data + (length / 8);
    while (data != end) {
      uint64_t k = *data++;
      k *= m;
      k ^= k >> r;
      k *= m;

      h ^= k;
      h *= m;
    }

    const unsigned char *data2 = (const unsigned char *) data;
    switch (length & 7) {
      case 7: h ^= uint64_t(data2[6]) << 48;
      case 6: h ^= uint64_t(data2[5]) << 40;
      case 5: h ^= uint64_t(data2[4]) << 32;
      case 4: h ^= uint64_t(data2[3]) << 24;
      case 3: h ^= uint64_t(data2[2]) << 16;
      case 2: h ^= uint64_t(data2[1]) << 8;
      case 1: h ^= uint64_t(data2[0]);
        h *= m;
    };
    h ^= h >> r;
    h *= m;
    h ^= h >> r;
    return h;
  }
  std::size_t operator()(T t) const {
    return MurmurHash64(&t, len, 0);
  }
};

template<typename T, size_t len>
struct PythonHash {
#define _PyHASH_MULTIPLIER 1000003UL  /* 0xf4243 */
  static size_t pythonHash(const void *data, size_t length) {
    size_t x;  /* Unsigned for defined overflow behavior. */
    size_t y;
    size_t mult = _PyHASH_MULTIPLIER;
    x = 0x345678UL;
    unsigned char *current = (unsigned char *) data;
    auto hasher = std::hash<char>();
    while (--length >= 0) {
      y = hasher(*current++);
      if (y == -1)
        return -1;
      x = (x ^ y) * mult;
      /* the cast might truncate len; that doesn't change hash stability */
      mult += (size_t) (82520UL + length + length);
    }
    x += 97531UL;
    if (x == (size_t) -1)
      x = -2;
    return x;
  }
  std::size_t operator()(T t) const {
    return pythonHash(&t, len);
  }
};

template<typename T, size_t len>
struct JHash {

  static inline uint32_t rol32(uint32_t word, unsigned int shift) {
    return (word << shift) | (word >> (32 - shift));
  }

/* Best hash sizes are of power of two */
#define jhash_size(n)   ((uint32_t)1<<(n))
/* Mask the hash value, i.e (value & jhash_mask(n)) instead of (value % n) */
#define jhash_mask(n)   (jhash_size(n)-1)

/* __jhash_mix -- mix 3 32-bit values reversibly. */
#define __jhash_mix(a, b, c)            \
{                        \
    a -= c;  a ^= rol32(c, 4);  c += b;    \
    b -= a;  b ^= rol32(a, 6);  a += c;    \
    c -= b;  c ^= rol32(b, 8);  b += a;    \
    a -= c;  a ^= rol32(c, 16); c += b;    \
    b -= a;  b ^= rol32(a, 19); a += c;    \
    c -= b;  c ^= rol32(b, 4);  b += a;    \
}

/* __jhash_final - final mixing of 3 32-bit values (a,b,c) into c */
#define __jhash_final(a, b, c)            \
{                        \
    c ^= b; c -= rol32(b, 14);        \
    a ^= c; a -= rol32(c, 11);        \
    b ^= a; b -= rol32(a, 25);        \
    c ^= b; c -= rol32(b, 16);        \
    a ^= c; a -= rol32(c, 4);        \
    b ^= a; b -= rol32(a, 14);        \
    c ^= b; c -= rol32(b, 24);        \
}

/*
 * Arbitrary initial parameters
 */
#define JHASH_INITVAL    0xdeadbeef
#define LISTEN_SEED    0xfaceb00c
#define WORKER_SEED    0xb00cface

/* jhash2 - hash an array of uint32_t's
 * @k: the key which must be an array of uint32_t's
 * @length: the number of uint32_t's in the key
 * @initval: the previous hash, or an arbitray value
 *
 * Returns the hash value of the key.
 */
  static inline __attribute__((pure)) uint32_t jhash2(uint32_t *k,
                                                      uint32_t length, uint32_t initval) {
    uint32_t a, b, c;

    /* Set up the internal state */
    a = b = c = JHASH_INITVAL + (length << 2) + initval;

    /* Handle most of the key */
    while (length > 3) {
      a += k[0];
      b += k[1];
      c += k[2];
      __jhash_mix(a, b, c);
      length -= 3;
      k += 3;
    }

    /* Handle the last 3 uint32_t's: all the case statements fall through */
    switch (length) {
      case 3: c += k[2];
      case 2: b += k[1];
      case 1: a += k[0];
        __jhash_final(a, b, c);
      case 0:    /* Nothing left to add */
        break;
    }

    return c;
  }

  std::size_t operator()(T t) const {
    return jhash2((uint32_t *) &t, len / 4, 0);
  }
};

template<typename T, size_t len>
struct MurmurHash3 {
#if defined(_MSC_VER)

#define FORCE_INLINE	__forceinline

#include <stdlib.h>

#define ROTL32(x,y)	_rotl(x,y)
#define ROTL64(x,y)	_rotl64(x,y)

#define BIG_CONSTANT(x) (x)

  // Other compilers

#else	// defined(_MSC_VER)

#define    FORCE_INLINE inline __attribute__((always_inline))

  inline uint32_t rotl32(uint32_t x, int8_t r) {
    return (x << r) | (x >> (32 - r));
  }

  inline uint64_t rotl64(uint64_t x, int8_t r) {
    return (x << r) | (x >> (64 - r));
  }

#define    ROTL32(x, y)    rotl32(x,y)
#define ROTL64(x, y)    rotl64(x,y)

#define BIG_CONSTANT(x) (x##LLU)

#endif // !defined(_MSC_VER)

//-----------------------------------------------------------------------------
// Block read - if your platform needs to do endian-swapping or can only
// handle aligned reads, do the conversion here

  FORCE_INLINE uint32_t getblock32(const uint32_t *p, int i) {
    return p[i];
  }

  FORCE_INLINE uint64_t getblock64(const uint64_t *p, int i) {
    return p[i];
  }

//-----------------------------------------------------------------------------
// Finalization mix - force all bits of a hash block to avalanche

  FORCE_INLINE uint32_t fmix32(uint32_t h) {
    h ^= h >> 16;
    h *= 0x85ebca6b;
    h ^= h >> 13;
    h *= 0xc2b2ae35;
    h ^= h >> 16;

    return h;
  }

//----------

  FORCE_INLINE uint64_t fmix64(uint64_t k) {
    k ^= k >> 33;
    k *= BIG_CONSTANT(0xff51afd7ed558ccd);
    k ^= k >> 33;
    k *= BIG_CONSTANT(0xc4ceb9fe1a85ec53);
    k ^= k >> 33;

    return k;
  }

  inline uint32_t MurmurHash3_x86_32(void *key, int length,
                                     uint32_t seed) {
    const uint8_t *data = (const uint8_t *) key;
    const int nblocks = length / 4;

    uint32_t h1 = seed;

    const uint32_t c1 = 0xcc9e2d51;
    const uint32_t c2 = 0x1b873593;

    //----------
    // body

    const uint32_t *blocks = (const uint32_t *) (data + nblocks * 4);

    for (int i = -nblocks; i; i++) {
      uint32_t k1 = getblock32(blocks, i);

      k1 *= c1;
      k1 = ROTL32(k1, 15);
      k1 *= c2;

      h1 ^= k1;
      h1 = ROTL32(h1, 13);
      h1 = h1 * 5 + 0xe6546b64;
    }

    //----------
    // tail

    const uint8_t *tail = (const uint8_t *) (data + nblocks * 4);

    uint32_t k1 = 0;

    switch (length & 3) {
      case 3: k1 ^= tail[2] << 16;
      case 2: k1 ^= tail[1] << 8;
      case 1: k1 ^= tail[0];
        k1 *= c1;
        k1 = ROTL32(k1, 15);
        k1 *= c2;
        h1 ^= k1;
    };

    //----------
    // finalization

    h1 ^= length;

    h1 = fmix32(h1);

    //(uint32_t*)out = h1;
    return h1;
  }

  std::size_t operator()(T t) {
    return MurmurHash3_x86_32(&t, len, 0);
  }
};

struct UInt128Hash {
  std::size_t rotate_by_at_least_1(std::size_t __val, int __shift) const {
    return (__val >> __shift) | (__val << (64 - __shift));
  }

  std::size_t hash_len_16(std::size_t __u, std::size_t __v) const {
    const std::size_t __mul = 0x9ddfea08eb382d69ULL;
    std::size_t __a = (__u ^ __v) * __mul;
    __a ^= (__a >> 47);
    std::size_t __b = (__v ^ __a) * __mul;
    __b ^= (__b >> 47);
    __b *= __mul;
    return __b;
  }
  UInt128Hash() = default;
  inline std::size_t operator()(__uint128_t data) const {
    const __uint128_t __mask = static_cast<std::size_t>(-1);
    const std::size_t __a = (std::size_t) (data & __mask);
    const std::size_t __b = (std::size_t) ((data & (__mask << 64)) >> 64);
    return hash_len_16(__a, rotate_by_at_least_1(__b + 16, 16)) ^ __b;
  }
};