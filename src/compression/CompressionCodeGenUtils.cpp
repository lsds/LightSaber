#include "compression/CompressionCodeGenUtils.h"

#include "utils/Utils.h"

uint32_t getRoundedTypeInt(uint32_t precision) {
  auto roundedType = Utils::getPowerOfTwo(precision);
  if (roundedType <= 8)
    roundedType = 8;
  return roundedType;
}

std::string getRoundedType(uint32_t precision) {
  return getType(Utils::getPowerOfTwo(precision));
}

std::string getType(uint32_t precision) {
  bool powerOfTwo = precision != 0 && !(precision & (precision - 1));
  if (!powerOfTwo) {
    throw std::runtime_error("error: precision must be a power of two");
  }
  switch (precision) {
    case 1:
    case 2:
    case 4:
    case 8:
      return "uint8_t";
    case 16:
      return "uint16_t";
    case 32:
      return "uint32_t";
    case 64:
      return "uint64_t";
    case 128:
      return "__uint128_t";
    default:
      throw std::runtime_error(
          "error: precision must be lower or equal to 128 bits");
  }
}

std::string getIncludesString() {
  std::string s;
  s.append("#include <iostream>\n");
  s.append("#include <stdlib.h>\n");
  s.append("#include <climits>\n");
  s.append("#include <cfloat>\n");
  s.append("#include <cmath>\n");
  s.append("#include <stdexcept>\n");
  s.append("#include <iterator>\n");
  s.append("#include <utility>\n");
  s.append("#include <cstring>\n");
  s.append("#include <mm_malloc.h>\n");
  s.append("#include <sstream>\n");
  s.append("#include <cstdint>\n");
  s.append("#include <vector>\n");
  s.append("\n");
  return s;
}

std::string getInstrumentationMetrics(size_t workers, size_t cols) {
  return "static const int numOfWorkers = " + std::to_string(workers) + ";\n"
         "static const int numOfCols = " + std::to_string(cols) + ";\n"
         "static uint32_t dVals[numOfWorkers][numOfCols];\n"
         "static double cVals[numOfWorkers][numOfCols];\n"
         "static double min[numOfWorkers][numOfCols];\n"
         "static double max[numOfWorkers][numOfCols];\n"
         "static double maxDiff[numOfWorkers][numOfCols];\n"
         "static double temp[numOfWorkers][numOfCols];\n";
}

std::string getCompressionAlgorithms() {
  return "#define DELTA_7_MASK 0x02 << 7;\n"
         "#define DELTA_9_MASK 0x06 << 9;\n"
         "#define DELTA_12_MASK 0x0E << 12;\n"
         "#define DELTA_28_MASK 0x0F << 28;\n"
         "\n"
         "#include <limits>\n"
         "#include <stdint.h>\n"
         "\n"
         "using std::numeric_limits;\n"
         "\n"
         "template <typename T, typename U>\n"
         "    bool CanTypeFitValue(const U value) {\n"
         "        const intmax_t botT = intmax_t(numeric_limits<T>::min() );\n"
         "        const intmax_t botU = intmax_t(numeric_limits<U>::min() );\n"
         "        const uintmax_t topT = uintmax_t(numeric_limits<T>::max() );\n"
         "        const uintmax_t topU = uintmax_t(numeric_limits<U>::max() );\n"
         "        return !( (botT > botU && value < static_cast<U> (botT)) || (topT < topU && value > static_cast<U> (topT)) );        \n"
         "    }\n"
         "\n"
         "template <typename T>\n"
         "struct HashMapEqualTo {\n"
         "  constexpr bool operator()(const T &lhs, const T &rhs) const {\n"
         "    return lhs == rhs;\n"
         "  }\n"
         "};\n"
         "\n"
         "template <typename KeyT>\n"
         "struct alignas(16) Bucket {\n"
         "  char state;\n"
         "  KeyT key;\n"
         "};\n"
         "\n"
         "template <typename KeyT, typename ValueT, typename HashT = "
         "std::hash<KeyT>,\n"
         "          typename EqT = HashMapEqualTo<KeyT>>\n"
         "class alignas(64) HashSet {\n"
         " private:\n"
         "  using BucketT = Bucket<KeyT>;\n"
         "\n"
         "  HashT m_hasher;\n"
         "  EqT m_eq;\n"
         "  BucketT *m_buckets = nullptr;\n"
         "  size_t m_num_buckets = 0;\n"
         "  size_t m_num_filled = 0;\n"
         "  size_t m_mask = 0;\n"
         "\n"
         " public:\n"
         "  HashSet(size_t size = 512)\n"
         "      : m_num_buckets(size), m_mask(size - 1) {\n"
         "    if (!(m_num_buckets && !(m_num_buckets & (m_num_buckets - 1)))) "
         "{\n"
         "      throw std::runtime_error(\n"
         "          \"error: the size of the hash table has to be a power of "
         "two\\n\");\n"
         "    }\n"
         "\n"
         "    m_buckets = (BucketT *)malloc(m_num_buckets * sizeof(BucketT));\n"
         "    if (!m_buckets) {\n"
         "      free(m_buckets);\n"
         "      throw std::bad_alloc();\n"
         "    }\n"
         "\n"
         "    for (auto i = 0; i < m_num_buckets; ++i) {\n"
         "      m_buckets[i].state = 0;\n"
         "    }\n"
         "  }\n"
         "\n"
         "  HashSet(Bucket<KeyT> *nodes,\n"
         "            size_t size = 512)\n"
         "      : m_buckets(nodes), m_num_buckets(size), m_mask(size - 1) {\n"
         "    if (!(m_num_buckets && !(m_num_buckets & (m_num_buckets - 1)))) "
         "{\n"
         "      throw std::runtime_error(\n"
         "          \"error: the size of the hash table has to be a power of "
         "two\\n\");\n"
         "    }\n"
         "  }\n"
         "\n"
         "  void clear() {\n"
         "    for (auto i = 0; i < m_num_buckets; ++i) {\n"
         "      m_buckets[i].state = 0;\n"
         "    }\n"
         "    m_num_filled = 0;\n"
         "  }\n"
         "\n"
         "  void insert(KeyT key, ValueT &pos) {\n"
         "    size_t ind = m_hasher(key) & m_mask, i = ind;\n"
         "    for (; i < m_num_buckets; i++) {\n"
         "      if (!m_buckets[i].state || m_eq(m_buckets[i].key, key)) {\n"
         "        m_buckets[i].state = 1;\n"
         "        m_buckets[i].key = key;\n"
         "        pos = i;\n"
         "        return;\n"
         "      }\n"
         "    }\n"
         "    for (i = 0; i < ind; i++) {\n"
         "      if (!m_buckets[i].state || m_eq(m_buckets[i].key, key)) {\n"
         "        m_buckets[i].state = 1;\n"
         "        m_buckets[i].key = key;\n"
         "        pos = i;\n"
         "        return;\n"
         "      }\n"
         "    }\n"
         "    throw std::runtime_error(\"error: the hashtable is full \\n\");\n"
         "  }\n"
         "\n"
         "  bool find(const KeyT &key, ValueT &pos) {\n"
         "    size_t ind = m_hasher(key) & m_mask, i = ind;\n"
         "    for (; i < m_num_buckets; i++) {\n"
         "      if (m_buckets[i].state && m_eq(m_buckets[i].key, key)) {\n"
         "        pos = i;\n"
         "        return true;\n"
         "      }\n"
         "    }\n"
         "    for (i = 0; i < ind; i++) {\n"
         "      if (m_buckets[i].state && m_eq(m_buckets[i].key, key)) {\n"
         "        pos = i;\n"
         "        return true;\n"
         "      }\n"
         "    }\n"
         "    return false;\n"
         "  }\n"
         "\n"
         "  bool erase(const KeyT &key) {\n"
         "    size_t ind = m_hasher(key) & m_mask, i = ind;\n"
         "    for (; i < m_num_buckets; i++) {\n"
         "      if (m_buckets[i].state && m_eq(m_buckets[i].key, key)) {\n"
         "        m_buckets[i].state = 0;\n"
         "        return true;\n"
         "      }\n"
         "    }\n"
         "    for (i = 0; i < ind; i++) {\n"
         "      if (m_buckets[i].state && m_eq(m_buckets[i].key, key)) {\n"
         "        m_buckets[i].state = 0;\n"
         "        return true;\n"
         "      }\n"
         "    }\n"
         "    printf(\"error: entry not found \\n\");\n"
         "    return false;\n"
         "  }\n"
         "\n"
         "  BucketT *buckets() { return m_buckets; }\n"
         "\n"
         "  size_t size() {\n"
         "    m_num_filled = 0;\n"
         "    for (size_t i = 0; i < m_num_buckets; i++) {\n"
         "      m_num_filled += m_buckets[i].state;\n"
         "    }\n"
         "    return m_num_filled;\n"
         "  }\n"
         "\n"
         "  bool empty() const { return m_num_filled == 0; }\n"
         "\n"
         "  size_t max_size() const { return m_num_buckets; }\n"
         "\n"
         "  size_t bucket_size() const { return sizeof(BucketT); }\n"
         "\n"
         "  float load_factor() {\n"
         "    return static_cast<float>(size()) / "
         "static_cast<float>(m_num_buckets);\n"
         "  }\n"
         "\n"
         "  ~HashSet() {\n"
         "    for (size_t bucket = 0; bucket < m_num_buckets; ++bucket) {\n"
         "      m_buckets[bucket].~BucketT();\n"
         "    }\n"
         "    free(m_buckets);\n"
         "  }\n"
         "};\n"
         "\n"
         "\n"
         "namespace zz {\n"
         "inline uint64_t encode(int64_t i) { return (i >> 63) ^ (i << 1); }\n"
         "\n"
         "inline int64_t decode(uint64_t i) { return (i >> 1) ^ (-(i & 1)); }\n"
         "}  // namespace zz\n"
         "\n"
         "\n"
         "template <typename In, typename Out, typename H, typename Eq>\n"
         "class DictionaryCompressor {\n"
         " private:\n"
         "  HashSet<In, Out, H, Eq> m_table;\n"
         "  Out m_id = 0;\n"
         "\n"
         " public:\n"
         "  DictionaryCompressor(size_t size) : m_table(size) {}\n"
         "  Out compress(In &input) {\n"
         "    Out res;\n"
         "    if (m_table.find(input, res)) {\n"
         "      return res;\n"
         "    }\n"
         "    res = m_id;\n"
         "    m_table.insert(input, m_id);\n"
         "    m_id++;\n"
         "    return res;\n"
         "  }\n"
         "  HashSet<In, Out, H, Eq> &getTable() {\n"
         "    return m_table;\n"
         "  }\n"
         "  void clear() {\n"
         "    m_table.clear();\n"
         "  }\n"
         "};\n"
         "\n"
         "template <typename In, typename Out>\n"
         "class BaseDeltaCompressor {\n"
         " private:\n"
         "  In m_base;\n"
         "\n"
         " public:\n"
         "  BaseDeltaCompressor(In base) : m_base(base) {}\n"
         "  inline Out compress(In &input) { return (Out) std::abs(m_base - input); }\n"
         "  inline bool check(In &input) {\n"
         "    auto res = input - m_base;\n "
         "    return !CanTypeFitValue<Out, In>(res);\n"
         "  }\n"
         "  inline std::string getBase() {\n"
         "    return std::to_string(m_base);\n"
         "  }\n"
         "};\n"
         "\n"
         "template <typename In, typename Out>\n"
         "class BucketCompressor {\n"
         " private:\n"
         "  In m_bucket;\n"
         "\n"
         " public:\n"
         "  BucketCompressor(In bucket) : m_bucket(bucket) {}\n"
         "  inline Out compress(In &input) { return Out(input / m_bucket); }\n"
         "};\n"
         "\n"
         "template <typename Out>\n"
         "class FloatMultCompressor {\n"
         " private:\n"
         "  const int m_multiplier;\n"
         "\n"
         " public:\n"
         "  FloatMultCompressor(int multiplier) : m_multiplier(multiplier) {\n"
         "    // check for precision issues here!\n"
         "  }\n"
         "  inline Out compress(float &input) {\n"
         "    return (Out)(input * m_multiplier);\n"
         "  }\n"
         "  inline bool check(float &input) {\n"
         "    //double intpart;\n"
         "    // does it have a fractional part?\n"
         "    //if (modf(input, &intpart) != 0) {\n"
         "    //  return true;\n"
         "    //}\n"
         "    auto res = (uint64_t)(input * m_multiplier);\n"
         "    return !CanTypeFitValue<Out, uint64_t>(res);\n"
         "  }\n"
         "  inline std::string getMultiplier() {\n"
         "    return std::to_string(m_multiplier);\n"
         "  }\n"
         "};\n";
}

std::string getCompressionVars(bool hasDict, size_t workers, size_t cols) {
  std::string s;
  s.append("#include <memory>\n");
  s.append(
      "static const int numOfWorkers = " + std::to_string(workers) + ";\n" +
      "static const int numOfCols = " + std::to_string(cols) + ";\n");
  if (hasDict)
    s.append("static std::unique_ptr<DictionaryCompressor<Key, uint16_t, MyHash, HMEqualTo>> dcomp[numOfWorkers][numOfCols];\n");
  s.append("static std::string metadata[numOfWorkers][numOfCols];\n");
  s.append("static bool isFirst [numOfWorkers] = {");
  for (size_t i = 0; i < workers; ++i) {
    s.append("true");
    if (i != workers - 1)
      s.append(", ");
  }
  s.append("};\n");
  return s;
}