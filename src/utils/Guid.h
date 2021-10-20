#pragma once

#include <uuid/uuid.h>

#include <cassert>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <functional>
#include <string>

class Guid {
 public:
  Guid() { uuid_clear(m_uuid); }

 private:
  Guid(const uuid_t uuid) { uuid_copy(m_uuid, uuid); }

 public:
  static Guid Create() {
    uuid_t uuid;
    uuid_generate(uuid);
    return uuid;
  }

  static Guid Parse(const std::string str) {
    uuid_t uuid;
    int result = uuid_parse(const_cast<char*>(str.c_str()), uuid);
    assert(result == 0);
    return uuid;
  }

  std::string ToString() const {
    char buffer[37];
    uuid_unparse(m_uuid, buffer);
    return std::string{buffer};
  }

  bool operator==(const Guid& other) const {
    return uuid_compare(m_uuid, other.m_uuid) == 0;
  }

  uint32_t GetHashCode() const {
    uint32_t Data1;
    uint16_t Data2;
    uint16_t Data3;
    std::memcpy(&Data1, m_uuid, sizeof(Data1));
    std::memcpy(&Data2, m_uuid + 4, sizeof(Data2));
    std::memcpy(&Data3, m_uuid + 6, sizeof(Data3));
    return Data1 ^
           ((static_cast<uint32_t>(Data2) << 16) |
            static_cast<uint32_t>(Data3)) ^
           ((static_cast<uint32_t>(m_uuid[10]) << 24) | m_uuid[15]);
  }

 private:
  uuid_t m_uuid;
};

// Implement std::hash<> for GUIDs.
namespace std {
template <>
struct hash<Guid> {
  size_t operator()(const Guid& val) const { return val.GetHashCode(); }
};
}  // namespace std