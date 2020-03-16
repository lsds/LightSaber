#pragma once

#include "cql/expressions/Expression.h"

/*
 * \brief The division operation used for projections.
 *
 * */

class Division : public Expression {
 public:
  Division(Expression *a, Expression *b) : m_first(a), m_second(b) {};
  Expression *getFirstExpression() { return m_first; }
  Expression *getSecondExpression() { return m_second; }
  std::string toSExpr() const override {
    std::string s;
    s.append("( ").append(m_first->toSExpr()).
        append(" / ").
        append(m_second->toSExpr()).
        append(" )");
    return s;
  }
  std::string toSExprForCodeGen() const override {
    std::string s;
    s.append("( ").append(m_first->toSExprForCodeGen()).
        append(" / ").
        append(m_second->toSExprForCodeGen()).
        append(" )");
    return s;
  }
  explicit operator std::string() const {
    std::string s;
    s.append("( ").append(m_first->toSExpr()).
        append(" / ").
        append(m_second->toSExpr()).
        append(" )");
    return s;
  }
  ~Division() override = default;
 private:
  Expression *m_first;
  Expression *m_second;
};