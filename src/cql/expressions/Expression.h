#pragma once

#include <vector>

#include "utils/AttributeType.h"
#include "utils/TupleSchema.h"

/*
 * \brief This base class is used to express projection operations, column references and
 * constant values.
 *
 * */

class Expression {
 public:
  Expression(const BasicType &basicType = BasicReferenceType)
      : m_basicType(basicType) {}
  virtual ~Expression() = 0;
  virtual std::string toSExpr() const = 0;
  virtual std::string toSExprForCodeGen() const = 0;
  const BasicType &getBasicType() const { return m_basicType; }
 private:
  BasicType m_basicType;
};

inline Expression::~Expression() = default;

namespace ExpressionUtils {
TupleSchema getTupleSchemaFromExpressions(std::vector<Expression *> &expressions, std::string name = "Stream");
TupleSchema mergeTupleSchemas(TupleSchema &x, TupleSchema &y);
}