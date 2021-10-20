#include "buffers/CircularQueryBuffer.h"
#include "cql/expressions/ColumnReference.h"
#include "cql/expressions/IntConstant.h"
#include "cql/expressions/LongConstant.h"
#include "cql/expressions/operations/Addition.h"
#include "cql/expressions/operations/Division.h"
#include "cql/expressions/operations/Multiplication.h"
#include "cql/expressions/operations/Subtraction.h"
#include "cql/operators/Aggregation.h"
#include "cql/operators/Projection.h"
#include "cql/operators/Selection.h"
#include "cql/operators/ThetaJoin.h"
#include "cql/predicates/ComparisonPredicate.h"
#include "gtest/gtest.h"
#include "monitors/LatencyMonitor.h"
#include "monitors/PerformanceMonitor.h"
#include "result/ResultHandler.h"
#include "tasks/WindowBatchFactory.h"
#include "utils/QueryApplication.h"
#include "utils/QueryOperator.h"

TEST(Expressions, BasicExpressionAndPredicateCreation) {
  ColumnReference ref1(0);
  ColumnReference ref2(3);
  LongConstant const1(2L);
  IntConstant const2(5);
  IntConstant const3(3);
  Addition add(&const2, &const3);
  Multiplication mul(&ref2, &add);
  Division div(&ref1, &const1);
  Subtraction sub(&div, &mul);
  EXPECT_EQ(sub.toSExpr(), "( ( \"0\" / Constant 2 ) - ( \"3\" * ( Constant 5 + Constant 3 ) ) )");

  ColumnReference ref3(1);
  ComparisonPredicate pr1(NONEQUAL_OP, &sub, &ref3);
  EXPECT_EQ(pr1.toSExpr(), "( ( \"0\" / Constant 2 ) - ( \"3\" * ( Constant 5 + Constant 3 ) ) ) != \"1\"");
}

TEST(Selection, OperatorInitialization) {
  Selection selection(new ComparisonPredicate(LESS_OP, new ColumnReference(0), new IntConstant(100)));
  EXPECT_EQ(selection.toSExpr(), "Selection (\"0\" < Constant 100)");
}

TEST(Projection, OperatorInitialization) {
  std::vector<Expression *> expressions(3);
  // Always project the timestamp
  expressions[0] = new ColumnReference(0);
  expressions[1] = new ColumnReference(1);
  expressions[2] = new Division(new Multiplication(new IntConstant(3), new IntConstant(15)), new IntConstant(2));
  Projection projection(expressions);
  EXPECT_EQ(projection.toSExpr(), "Projection (\"0\", \"1\", ( ( Constant 3 * Constant 15 ) / Constant 2 ))");
}

TEST(Aggregation, OperatorInitialization) {
  WindowDefinition windowDefinition(ROW_BASED, 1024, 32);
  std::vector<AggregationType> aggregationTypes(2);
  aggregationTypes[0] = AVG;
  aggregationTypes[1] = MIN;
  std::vector<ColumnReference *> aggregationAttributes(2);
  aggregationAttributes[0] = new ColumnReference(1);
  aggregationAttributes[1] = new ColumnReference(2);
  std::vector<Expression *> groupByAttributes(1);
  groupByAttributes[0] = new ColumnReference(2);
  Aggregation aggregation(windowDefinition, aggregationTypes, aggregationAttributes, groupByAttributes);
  std::cout << aggregation.toSExpr() << std::endl;
  EXPECT_EQ(aggregation.toSExpr(),
            "[Partial window u-aggregation] AVG(\"1\") MIN(\"2\") (group-by ? 1) (incremental ? 1)");
}

TEST(ThetaJoin, OperatorInitialization) {
  int numberOfAttributes = 1;
  std::unique_ptr<WindowDefinition> window1 = std::make_unique<WindowDefinition>(ROW_BASED, 1024, 512);
  std::unique_ptr<TupleSchema> schema1 = std::make_unique<TupleSchema>(2, "Stream1");
  auto attr = AttributeType(BasicType::Long);
  for (int i = 1; i < numberOfAttributes + 1; i++) {
    schema1->setAttributeType(i, attr);
  }
  schema1->setAttributeType(0, attr);
  std::unique_ptr<WindowDefinition> window2 = std::make_unique<WindowDefinition>(ROW_BASED, 1024, 512);
  std::unique_ptr<TupleSchema> schema2 = std::make_unique<TupleSchema>(2, "Stream2");
  for (int i = 1; i < numberOfAttributes + 1; i++) {
    schema1->setAttributeType(i, attr);
  }
  schema1->setAttributeType(0, attr);
  std::unique_ptr<ComparisonPredicate> predicate = std::make_unique<ComparisonPredicate>(EQUAL_OP, new ColumnReference(1), new ColumnReference(1));
  auto join = new ThetaJoin(*schema1, *schema2, predicate.get());
  // TODO: fix OperatorJit::removeAllModules() so that the test passes without heap allocation
  std::cout << join->toSExpr() << std::endl;
  EXPECT_EQ(join->toSExpr(),
            "ThetaJoin (\"1\" == \"1\")");
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
