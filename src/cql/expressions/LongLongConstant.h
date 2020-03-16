#pragma once

#include "Expression.h"

/*
 * \brief A 128-bit constant value
 *
 * */

class LongLongConstant : public Expression {
 public:
  LongLongConstant(__uint128_t val) : Expression(BasicType::LongLong), m_value(val) {};
  long getConstant() { return m_value; }
  std::string toSExpr() const override {
    std::string s;
    s.append("Constant ").append(std::to_string(m_value));
    return s;
  }
  std::string toSExprForCodeGen() const override {
    std::string s;
    s.append(std::to_string(m_value));
    return s;
  }
  explicit operator std::string() const {
    std::string s;
    s.append("Constant ").append(std::to_string(m_value));
    return s;
  }
  ~LongLongConstant() override = default;
 private:
  long m_value;
};