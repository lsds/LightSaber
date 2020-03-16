#pragma once

#include "Expression.h"

/*
 * \brief An int constant value
 *
 * */

class IntConstant : public Expression {
 public:
  IntConstant(int val) : Expression(BasicType::Integer), m_value(val) {};
  int getConstant() { return m_value; }
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
  ~IntConstant() override = default;
 private:
  int m_value;
};