#pragma once

#include "Expression.h"

/*
 * \brief A float constant value
 *
 * */

class FloatConstant : public Expression {
 public:
  FloatConstant(float val) : Expression(BasicType::Float), m_value(val) {};
  float getConstant() { return m_value; }
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
  ~FloatConstant() override = default;
 private:
  float m_value;
};