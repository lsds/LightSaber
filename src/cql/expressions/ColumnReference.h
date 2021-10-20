#pragma once

#include "Expression.h"

/*
 * \brief This class is a column reference.
 *
 * */

class ColumnReference : public Expression {
 public:
  ColumnReference(int col, const BasicType &basicType = BasicReferenceType) : Expression(basicType), m_column(col) {};
  int getColumn() { return m_column; }
  std::string toSExpr() const override {
    std::string s;
    s.append("\"").append(std::to_string(m_column)).append("\"");
    return s;
  }
  std::string toSExprForCodeGen() const override {
    std::string s;
    if (m_column == 0) // assume that timestamp is the first attribute
      s.append("data[bufferPtr].timestamp");
    else
      s.append("data[bufferPtr]._").append(std::to_string(m_column));
    return s;
  }
  explicit operator std::string() const {
    std::string s;
    s.append("\"").append(std::to_string(m_column)).append("\"");
    return s;
  }
  void setExpression(std::string expression) {
    m_expression = std::move(expression);
  }
  std::string getExpression() {
    return m_expression;
  }
  ~ColumnReference() override = default;
 private:
  int m_column;
  std::string m_expression;
};