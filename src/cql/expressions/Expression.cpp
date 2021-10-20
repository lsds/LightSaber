#include "cql/expressions/Expression.h"

TupleSchema ExpressionUtils::getTupleSchemaFromExpressions(std::vector<Expression *> &expressions, std::string name) {
  TupleSchema schema((int) expressions.size(), name);
  int idx = 0;
  /* Set types */
  for (auto e : expressions) {
    if (e->getBasicType() == BasicType::Integer) {
      auto attr = AttributeType(BasicType::Integer);
      schema.setAttributeType(idx, attr);
    } else if (e->getBasicType() == BasicType::Float) {
      auto attr = AttributeType(BasicType::Float);
      schema.setAttributeType(idx, attr);
    } else if (e->getBasicType() == BasicType::Long) {
      auto attr = AttributeType(BasicType::Long);
      schema.setAttributeType(idx, attr);
    } else if (e->getBasicType() == BasicType::LongLong) {
      auto attr = AttributeType(BasicType::LongLong);
      schema.setAttributeType(idx, attr);
    }
    idx++;
  }
  return schema;
}

TupleSchema ExpressionUtils::mergeTupleSchemas(TupleSchema &x, TupleSchema &y) {
  TupleSchema schema(x.numberOfAttributes() + y.numberOfAttributes(), "MergedSchema");
  int idx = 0;
  for (int i = 0; i < x.numberOfAttributes(); ++i) {
    auto type = x.getAttributeType(i);
    auto attr = AttributeType(type);
    schema.setAttributeType(idx++, attr);
  }
  for (int i = 0; i < y.numberOfAttributes(); ++i) {
    auto type = y.getAttributeType(i);
    auto attr = AttributeType(type);
    schema.setAttributeType(idx++, attr);
  }
  return schema;
}