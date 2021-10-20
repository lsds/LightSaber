#pragma once

#include <climits>
#include <iterator>
#include <utility>

#include "utils/SystemConf.h"

template <typename T>
struct HashMapEqualTo {
  constexpr bool operator()(const T &lhs, const T &rhs) const {
    return lhs == rhs;
  }
};

template <typename KeyT>
struct alignas(16) SimpleBucket {
  char state;
  KeyT key;
};

template <typename KeyT, typename ValueT>
struct alignas(16) Bucket {
  char state = 0;
  KeyT key;
  ValueT value;
  int counter = 0;
  void combine(ValueT v) { Opt(value, v); }
};

template <typename ValueT>
struct DummyAggr {
  unsigned int addedElements = 0;
  unsigned int removedElements = 0;
  void initialise(){};
  void insert(ValueT v){};
  ValueT query() { return 0; };
  void evict(){};
};

/*
 * \brief This class implements a hashtable.
 *
 * It is used for debugging.
 *
 * */

template <typename KeyT, typename ValueT, typename HashT = std::hash<KeyT>,
          typename EqT = HashMapEqualTo<KeyT>,
          typename AggrT = DummyAggr<ValueT>>
class alignas(64) HashTable {
 private:
  using BucketT = Bucket<KeyT, ValueT>;

  HashT m_hasher;
  EqT m_eq;
  BucketT *m_buckets = nullptr;
  AggrT *m_aggrs = nullptr;
  size_t m_num_buckets = 0;
  size_t m_num_filled = 0;
  size_t m_mask = 0;

 public:
  HashTable(size_t size = SystemConf::getInstance().HASH_TABLE_SIZE)
      : m_num_buckets(size), m_mask(size - 1) {
    if (!(m_num_buckets && !(m_num_buckets & (m_num_buckets - 1)))) {
      throw std::runtime_error(
          "error: the size of the hash table has to be a power of two\n");
    }

    m_buckets = (BucketT *)malloc(m_num_buckets * sizeof(BucketT));
    m_aggrs = (AggrT *)malloc(m_num_buckets * sizeof(AggrT));
    if (!m_buckets || !m_aggrs) {
      free(m_buckets);
      free(m_aggrs);
      throw std::bad_alloc();
    }

    for (auto i = 0; i < m_num_buckets; ++i) {
      m_buckets[i].state = 0;
    }
  }

  HashTable(Bucket<KeyT, ValueT> *nodes,
            size_t size = SystemConf::getInstance().HASH_TABLE_SIZE)
      : m_buckets(nodes), m_num_buckets(size), m_mask(size - 1) {
    if (!(m_num_buckets && !(m_num_buckets & (m_num_buckets - 1)))) {
      throw std::runtime_error(
          "error: the size of the hash table has to be a power of two\n");
    }
  }

  void clear() {
    for (auto i = 0; i < m_num_buckets; ++i) {
      m_buckets[i].state = 0;
      m_aggrs[i].initialise();
    }
    m_num_filled = 0;
  }

  void insert(KeyT key, ValueT value) {
    size_t ind = m_hasher(key) & m_mask, i = ind;
    for (; i < m_num_buckets; i++) {
      if (!m_buckets[i].state || m_eq(m_buckets[i].key, key)) {
        m_buckets[i].state = 1;
        m_buckets[i].key = key;
        m_buckets[i].value = value;
        return;
      }
    }
    for (i = 0; i < ind; i++) {
      if (!m_buckets[i].state || m_eq(m_buckets[i].key, key)) {
        m_buckets[i].state = 1;
        m_buckets[i].key = key;
        m_buckets[i].value = value;
        return;
      }
    }
    throw std::runtime_error("error: the hashtable is full \n");
  }

  void insert_or_modify(KeyT key, ValueT value) {
    size_t ind = m_hasher(key) & m_mask, i = ind;
    char tempState;
    for (; i < m_num_buckets; i++) {
      tempState = m_buckets[i].state;
      if (tempState && m_eq(m_buckets[i].key, key)) {
        m_buckets[i].value.combine(value);
        m_buckets[i].counter++;
        return;
      }
      if (!tempState) {
        m_buckets[i].state = 1;
        m_buckets[i].key = key;
        m_buckets[i].value = value;
        m_buckets[i].counter = 1;
        return;
      }
    }
    for (i = 0; i < ind; i++) {
      tempState = m_buckets[i].state;
      if (tempState && m_eq(m_buckets[i].key, key)) {
        m_buckets[i].value.combine(value);
        m_buckets[i].counter++;
        return;
      }
      if (!tempState) {
        m_buckets[i].state = 1;
        m_buckets[i].key = key;
        m_buckets[i].value = value;
        m_buckets[i].counter = 1;
        return;
      }
    }

    throw std::runtime_error("error: the hashtable is full \n");
  }

  bool erase(const KeyT &key) {
    size_t ind = m_hasher(key) & m_mask, i = ind;
    for (; i < m_num_buckets; i++) {
      if (m_buckets[i].state && m_eq(m_buckets[i].key, key)) {
        m_buckets[i].state = 0;
        return true;
      }
    }
    for (i = 0; i < ind; i++) {
      if (m_buckets[i].state && m_eq(m_buckets[i].key, key)) {
        m_buckets[i].state = 0;
        return true;
      }
    }
    printf("error: entry not found \n");
    return false;
  }

  bool find(const KeyT &key, ValueT &result) {
    size_t ind = m_hasher(key) & m_mask, i = ind;
    for (; i < m_num_buckets; i++) {
      if (m_buckets[i].state && m_eq(m_buckets[i].key, key)) {
        result = m_buckets[i].value;
        return true;
      }
    }
    for (i = 0; i < ind; i++) {
      if (m_buckets[i].state && m_eq(m_buckets[i].key, key)) {
        result = m_buckets[i].value;
        return true;
      }
    }
    return false;
  }

  bool find_index(const KeyT &key, size_t &index) {
    size_t ind = m_hasher(key) & m_mask, i = ind;
    int dist = 0;
    for (; i < m_num_buckets; i++) {
      if (m_buckets[i].state && m_eq(m_buckets[i].key, key)) {
        index = i;
        return true;
      }
    }
    for (i = 0; i < ind; i++) {
      if (m_buckets[i].state && _eq(m_buckets[i].key, key)) {
        index = i;
        return true;
      }
    }
    return false;
  }

  BucketT *buckets() { return m_buckets; }

  size_t size() {
    m_num_filled = 0;
    for (size_t i = 0; i < m_num_buckets; i++) {
      m_num_filled += m_buckets[i].state;
    }
    return m_num_filled;
  }

  bool empty() const { return m_num_filled == 0; }

  size_t max_size() const { return m_num_buckets; }

  size_t bucket_size() const { return sizeof(BucketT); }

  float load_factor() {
    return static_cast<float>(size()) / static_cast<float>(m_num_buckets);
  }

  ~HashTable() {
    for (size_t bucket = 0; bucket < m_num_buckets; ++bucket) {
      m_buckets[bucket].~BucketT();
      m_aggrs->~AggrT();
    }
    free(m_buckets);
    free(m_aggrs);
  }
};

template <typename KeyT, typename ValueT, typename HashT = std::hash<KeyT>,
    typename EqT = HashMapEqualTo<KeyT>>
class alignas(64) HashSet {
 private:
  using BucketT = SimpleBucket<KeyT>;

  HashT m_hasher;
  EqT m_eq;
  BucketT *m_buckets = nullptr;
  size_t m_num_buckets = 0;
  size_t m_num_filled = 0;
  size_t m_mask = 0;

 public:
  explicit HashSet(size_t size = SystemConf::getInstance().HASH_TABLE_SIZE)
      : m_num_buckets(size), m_mask(size - 1) {
    if (!(m_num_buckets && !(m_num_buckets & (m_num_buckets - 1)))) {
      throw std::runtime_error(
          "error: the size of the hash table has to be a power of two\n");
    }

    m_buckets = (BucketT *)malloc(m_num_buckets * sizeof(BucketT));
    if (!m_buckets) {
      free(m_buckets);
      throw std::bad_alloc();
    }

    for (auto i = 0; i < m_num_buckets; ++i) {
      m_buckets[i].state = 0;
    }
  }

  explicit HashSet(SimpleBucket<KeyT> *nodes,
            size_t size = SystemConf::getInstance().HASH_TABLE_SIZE)
      : m_buckets(nodes), m_num_buckets(size), m_mask(size - 1) {
    if (!(m_num_buckets && !(m_num_buckets & (m_num_buckets - 1)))) {
      throw std::runtime_error(
          "error: the size of the hash table has to be a power of two\n");
    }
  }

  void clear() {
    for (auto i = 0; i < m_num_buckets; ++i) {
      m_buckets[i].state = 0;
    }
    m_num_filled = 0;
  }

  void insert(KeyT key, ValueT &pos) {
    size_t ind = m_hasher(key) & m_mask, i = ind;
    for (; i < m_num_buckets; i++) {
      if (!m_buckets[i].state || m_eq(m_buckets[i].key, key)) {
        m_buckets[i].state = 1;
        m_buckets[i].key = key;
        pos = i;
        return;
      }
    }
    for (i = 0; i < ind; i++) {
      if (!m_buckets[i].state || m_eq(m_buckets[i].key, key)) {
        m_buckets[i].state = 1;
        m_buckets[i].key = key;
        pos = i;
        return;
      }
    }
    throw std::runtime_error("error: the hashtable is full \n");
  }

  bool find(const KeyT &key, ValueT &pos) {
    size_t ind = m_hasher(key) & m_mask, i = ind;
    for (; i < m_num_buckets; i++) {
      if (m_buckets[i].state && m_eq(m_buckets[i].key, key)) {
        pos = i;
        return true;
      }
    }
    for (i = 0; i < ind; i++) {
      if (m_buckets[i].state && m_eq(m_buckets[i].key, key)) {
        pos = i;
        return true;
      }
    }
    return false;
  }

  bool erase(const KeyT &key) {
    size_t ind = m_hasher(key) & m_mask, i = ind;
    for (; i < m_num_buckets; i++) {
      if (m_buckets[i].state && m_eq(m_buckets[i].key, key)) {
        m_buckets[i].state = 0;
        return true;
      }
    }
    for (i = 0; i < ind; i++) {
      if (m_buckets[i].state && m_eq(m_buckets[i].key, key)) {
        m_buckets[i].state = 0;
        return true;
      }
    }
    printf("error: entry not found \n");
    return false;
  }

  BucketT *buckets() { return m_buckets; }

  size_t size() {
    m_num_filled = 0;
    for (size_t i = 0; i < m_num_buckets; i++) {
      m_num_filled += m_buckets[i].state;
    }
    return m_num_filled;
  }

  bool empty() const { return m_num_filled == 0; }

  size_t max_size() const { return m_num_buckets; }

  size_t bucket_size() const { return sizeof(BucketT); }

  float load_factor() {
    return static_cast<float>(size()) / static_cast<float>(m_num_buckets);
  }

  ~HashSet() {
    for (size_t bucket = 0; bucket < m_num_buckets; ++bucket) {
      m_buckets[bucket].~BucketT();
    }
    free(m_buckets);
  }
};