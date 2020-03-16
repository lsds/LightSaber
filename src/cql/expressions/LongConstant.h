#pragma once

#include "Expression.h"

/*
 * \brief A long constant value
 *
 * */

class LongConstant : public Expression {
 public:
  LongConstant(long val) : Expression(BasicType::Long), m_value(val) {};
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
  ~LongConstant() override = default;
 private:
  long m_value;
};