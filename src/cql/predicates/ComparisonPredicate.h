#pragma once

#include "Predicate.h"
#include "cql/expressions/Expression.h"

/*
 * \brief A comparison evaluated as a filter predicate.
 *
 * */

class ComparisonPredicate : public Predicate {
 public:
  ComparisonPredicate(int op, Expression *a, Expression *b) : Predicate(op), m_first(a), m_second(b) {};
  Expression *getFirstExpression() { return m_first; }
  Expression *getSecondExpression() { return m_second; }
  std::string toSExpr() const override {
    std::string s;
    s.append(m_first->toSExpr()).
        append(getComparisonString()).
        append(m_second->toSExpr());
    return s;
  }
  std::string toSExprForCodeGen() const override {
    std::string s;
    s.append(m_first->toSExprForCodeGen()).
        append(getComparisonString()).
        append(m_second->toSExprForCodeGen());
    return s;
  }
  explicit operator std::string() const {
    std::string s;
    s.append(m_first->toSExpr()).
        append(getComparisonString()).
        append(m_second->toSExpr());
    return s;
  }
  int getNumberOfPredicates() const override { return 1; };
  ~ComparisonPredicate() override = default;
 private:
  Expression *m_first;
  Expression *m_second;
};

