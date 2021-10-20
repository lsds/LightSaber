#pragma once

#include <vector>
#include <utility>
#include <cmath>

#include "AttributeType.h"

/*
 * This class is used for defining the schema of a stream.
 *
 * */

class TupleSchema {
 private:
  std::vector<std::pair<std::string, AttributeType>> m_attributes;
  std::string m_name;
  int m_size;
  bool m_hasTimestamp;
  int m_tupleSize;
  int m_padLength = -1;

 public:

  TupleSchema(int size, std::string name, bool hasTimestamp = true) :
      m_attributes(size), m_name(name), m_size(size), m_hasTimestamp(hasTimestamp), m_tupleSize(0) {
    for (int i = 0; i < size; ++i) {
      std::string n = "atr_" + std::to_string(i);
      m_attributes[i].first = n;
    }
  }

  int numberOfAttributes() {
    return m_size;
  }

  bool hasTime() {
    return m_hasTimestamp;
  }

  void setAttributeType(int idx, AttributeType &type) {
    m_attributes[idx].second = type;
  }

  BasicType getAttributeType(int idx) {
    return m_attributes[idx].second.getBasicType();
  }

  void setAttributeName(int idx, std::string &name) {
    m_attributes[idx].first = name;
  }

  std::string &getAttributeName(int idx) {
    return m_attributes[idx].first;
  }

  void setSchemaName(std::string &name) {
    m_name = name;
  }

  std::string &getSchemaName() {
    return m_name;
  }

  std::string getSchema() {
    std::string schema = m_name;
    schema += "(\n";
    for (int i = 0; i < m_size; ++i) {
      schema += m_attributes[i].first + ", " +
          m_attributes[i].second.toSExpr() + "\n";
    }
    schema += ")";
    return schema;
  }

  int getTupleSize() {
    if (m_tupleSize == 0) {
      for (int i = 0; i < m_size; ++i) {
        m_tupleSize += m_attributes[i].second.getWidth();
      }
      /*tupleSize = Utils::getPowerOfTwo(tupleSize);*/
    }
    return m_tupleSize;
  }

  int getPadLength() {
    if (m_padLength == -1) {
      auto tupleSize = getTupleSize();
      m_padLength = 0;
      // Expand size, if needed, to ensure that tuple size is a power of 2
      if ((tupleSize & (tupleSize - 1)) != 0) {
        auto pow2Size = 1;
        while (tupleSize > pow2Size) pow2Size *= 2;
        m_padLength = pow2Size - tupleSize;
      }
    }
    return m_padLength;
  }
};