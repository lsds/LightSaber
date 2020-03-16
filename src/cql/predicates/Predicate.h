#pragma once

#include <string>

static const int EQUAL_OP = 0;
static const int NONEQUAL_OP = 1;
static const int LESS_OP = 2;
static const int NONLESS_OP = 3;
static const int GREATER_OP = 4;
static const int NONGREATER_OP = 5;

/*
 * \brief The base class for creating predicates used from the filter operator.
 *
 * */

class Predicate {
 public:
  Predicate(int op = EQUAL_OP) : m_op(op) {}
  virtual ~Predicate() = 0;
  virtual std::string toSExpr() const = 0;
  virtual std::string toSExprForCodeGen() const = 0;
  virtual int getNumberOfPredicates() const = 0;
  std::string getComparisonString() const {
    std::string result;
    switch (m_op) {
      case NONEQUAL_OP: result = " != ";
        break;
      case LESS_OP: result = " < ";
        break;
      case NONLESS_OP: result = " >= ";
        break;
      case GREATER_OP: result = " > ";
        break;
      case NONGREATER_OP: result = " <= ";
        break;
      default: result = " == ";
    }
    return result;
  }
 private:
  int m_op;
};

inline Predicate::~Predicate() = default;
