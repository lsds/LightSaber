#include <random>

#include "buffers/CircularQueryBuffer.h"
#include "cql/operators/HashTable.h"
#include "gtest/gtest.h"

TEST(HashTable, TestFunctions) {
  std::random_device rd;
  std::mt19937_64 eng(rd());
  std::uniform_int_distribution<long> distr(0, 1000000);
  std::vector<int> input (512);
  for (size_t i = 0; i < input.size(); i++) {
    input[i] = distr(eng);
  }

  struct Key {
    long timestamp;
    int id;
  };
  struct Value {
    int _1;
    void combine (Value &v) {
      this->_1 += v._1;
    }
  };
  struct Hasher {
    std::size_t operator()(const Key& key) const {
      std::hash<int> hasher;
      return hasher(key.id);
    }
  };
  struct Eq {
    constexpr bool operator()(const Key& k1, const Key& k2) const {
      return k1.timestamp == k2.timestamp &&
          k1.id == k2.id;
    }
  };
  HashTable<Key, Value, Hasher, Eq> table (1024);

  //  check insertions
  for (auto &t: input) {
    table.insert_or_modify({0, t}, {1});
  }
  for (auto &t: input) {
    table.insert_or_modify({0, t}, {1});
  }
  auto buf = table.buckets();
  for (size_t i = 0; i < table.size(); i++) {
    if (buf[i].state) {
      EXPECT_EQ(buf[i].value._1, 2);
      EXPECT_EQ(buf[i].counter, 2);
    }
  }

  // check erase
  table.erase({0, input[0]});
  Value val;
  auto res = table.find({0, input[0]}, val);
  EXPECT_EQ(res, false);
  EXPECT_LE(table.load_factor(), 0.5);

  // check clearing the hashtable
  table.clear();
  for (size_t i = 0; i < table.size(); i++) {
    EXPECT_EQ(buf[i].state, false);
  }
}

TEST(CircularBuffer, ProcessBytes) {
  CircularQueryBuffer circularBuffer(0, 1024, 32);
  std::vector<char> v(128);
  circularBuffer.put(v.data(), v.size(), -1);
  circularBuffer.free(127);
  EXPECT_EQ(circularBuffer.getBytesProcessed(), 128);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
