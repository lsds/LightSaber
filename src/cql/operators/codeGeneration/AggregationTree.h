#pragma once

#include <iostream>
#include <climits>
#include <vector>
#include <fstream>

#include <boost/type_index.hpp>

#include "utils/WindowDefinition.h"
#include "cql/operators/AggregationType.h"

/*
 * \brief This class is used for generating the code for GAGs and incremental computation.
 *
 * TODO: merge this with the GeneralAggregationGraph.h
 *
 * */

class AbstractTreeRepresentation {
 private:
  long m_size;
  long m_numOfLeaves;
  long m_numOfParents;
  int m_numOfFlips;
  bool m_hasNonInvertible;
  WindowDefinition &m_windowDefinition;
  std::vector<AggregationType> &m_aggregationTypes;

 public:
  AbstractTreeRepresentation(WindowDefinition &windowDefinition,
                             std::vector<AggregationType> &aggregationTypes,
                             bool hasNonInvertible) : m_hasNonInvertible(hasNonInvertible),
                                                      m_windowDefinition(windowDefinition),
                                                      m_aggregationTypes(aggregationTypes) {
    // Initialize appropriate variables for generating the abstract tree
    m_size = m_windowDefinition.numberOfPanes();
    // initiliaze variable needed for code generation
    m_numOfFlips = 1;
    m_numOfLeaves = m_size;
    m_numOfParents = 1;
    if (m_hasNonInvertible) {
      m_numOfParents = 1 + m_numOfLeaves;
    }
  };

  std::string generateCode() {
    //std::string aggregationNode = generateAggregationTreeNode ();
    std::string insertFunction = generateInsertFunction(false);
    std::string evictFunction = generateEvictFunction(true);
    std::string queryFunction = generateQueryFunction();
    std::string queryIntermFunction = generateQueryIntermFunction();
    std::string initialiseFunction = generateInitialiseFunction();
    std::string flipFunction = generateFlipFunction();
    std::string fullCode = getCodeTemplate(insertFunction, evictFunction, queryFunction,
                                           queryIntermFunction, initialiseFunction, flipFunction);
    //std::cout << fullCode;
    return fullCode;
  }

  std::string generateAggregationTreeNode() {
    std::string s;
    s.append("struct node {\n");
    for (unsigned long i = 0; i < m_aggregationTypes.size(); ++i) {
      switch (m_aggregationTypes[i]) {
        case SUM:s.append("\tfloat _" + std::to_string((i + 1)) + ";\n");
          break;
        case AVG:s.append("\tfloat _" + std::to_string((i + 1)) + ";\n");
          s.append("\tfloat _c" + std::to_string((i + 1)) + ";\n");
          break;
        case CNT:s.append("\tfloat _" + std::to_string((i + 1)) + ";\n");
          break;
        case MIN:s.append("\tfloat _" + std::to_string((i + 1)) + ";\n");
          break;
        case MAX:s.append("\tfloat _" + std::to_string((i + 1)) + ";\n");
          break;
        default:throw std::runtime_error("error: invalid aggregation type");
      }
    }
    s.append("\tvoid reset () {\n");
    for (unsigned long i = 0; i < m_aggregationTypes.size(); ++i) {
      switch (m_aggregationTypes[i]) {
        case SUM:s.append("\t\t_" + std::to_string((i + 1)) + " = 0.0f;\n");
          break;
        case AVG:s.append("\t\t_" + std::to_string((i + 1)) + " = 0.0f;\n");
          s.append("\t\t_c" + std::to_string((i + 1)) + " = 0.0f;\n");
          break;
        case CNT:s.append("\t\t_" + std::to_string((i + 1)) + " = 0.0f;\n");
          break;
        case MIN:s.append("\t\t_" + std::to_string((i + 1)) + " = FLT_MAX;\n");
          break;
        case MAX:s.append("\t\t_" + std::to_string((i + 1)) + " = FLT_MIN;\n");
          break;
        default:throw std::runtime_error("error: invalid aggregation type");
      }
    }
    s.append("\t};\n");
    s.append("};\n");
    return s;
  }

 private:
  std::string m_tab = "\t";

  bool isInvertible(AggregationType type) {
    return (type == CNT || type == AVG || type == SUM || type == W_AVG);
  }

  bool hasMulitpleFlips() { return m_numOfFlips > 1; }

  /*std::string getLiftFunction(AggregationType type) {
      if (type == CNT || type == MIN || type == SUM || type == MAX)
          return "value";
      else if (type == AVG)
          return "std::make_pair(1,value)";
      else if (type == W_AVG)
          return "std::make_pair(value.first, value.first*value.second)";
      else {
          throw std::runtime_error ("error: Unsupported type");
      }
  }*/

  std::string getCombineFunction(AggregationType type, std::string leftArg, std::string rightArg) {
    if (type == CNT || type == SUM)
      return leftArg + "+" + rightArg;
    else if (type == MIN)
      return leftArg + "<" + rightArg + "?" + leftArg + ":" + rightArg;
    else if (type == MAX)
      return leftArg + ">" + rightArg + "?" + leftArg + ":" + rightArg;
    else if (type == AVG || type == W_AVG)
      return "std::make_pair(" + leftArg + ".first+" + rightArg + ".first," + leftArg + ".second+" + rightArg +
          ".second)";
    else {
      throw std::runtime_error("error: Unsupported type");
    }
  }

  std::string getLowerFunction(AggregationType type, std::string arg) {
    if (type == CNT || type == MIN || type == SUM || type == MAX)
      return arg;
    else if (type == AVG || type == W_AVG)
      return arg + ".second/" + arg + ".first";
    else {
      throw std::runtime_error("error: Unsupported type");
    }
  }

  std::string getInverseFunction(AggregationType type, std::string leftArg, std::string rightArg) {
    if (type == CNT || type == SUM)
      return leftArg + "-" + rightArg;
    else if (type == AVG || type == W_AVG)
      return "std::make(" + leftArg + ".first-" + rightArg + ".first, " + leftArg + ".second-" + rightArg +
          ".second)";
    else {
      throw std::runtime_error("error: Unsupported type");
    }
  }

  std::string getFlipCondition(AggregationType type, std::string leftArg, std::string rightArg) {
    if (type == MIN)
      return leftArg + ">=" + rightArg;
    else if (type == MAX)
      return leftArg + "<=" + rightArg;
    else {
      throw std::runtime_error("error: Unsupported type");
    }
  }

  std::string generateInitialiseFunction() {
    std::string parentsLine;
    parentsLine.append(m_tab + m_tab + "for (int i = 0; i < PARENTS_SIZE; i++) {\n");
    for (unsigned long i = 0; i < m_aggregationTypes.size(); ++i) {
      switch (m_aggregationTypes[i]) {
        case SUM:parentsLine.append(m_tab + m_tab + m_tab + "parents[i]._" + std::to_string((i + 1)) + " = 0.0f;\n");
          break;
        case AVG:parentsLine.append(m_tab + m_tab + m_tab + "parents[i]._" + std::to_string((i + 1)) + " = 0.0f;\n");
          parentsLine.append(m_tab + m_tab + m_tab + "parents[i]._c" + std::to_string((i + 1)) + " = 0.0f;\n");
          break;
        case CNT:parentsLine.append(m_tab + m_tab + m_tab + "parents[i]._" + std::to_string((i + 1)) + " = 0.0f;\n");
          break;
        case MIN:parentsLine.append(m_tab + m_tab + m_tab + "parents[i]._" + std::to_string((i + 1)) + " = FLT_MAX;\n");
          break;
        case MAX:parentsLine.append(m_tab + m_tab + m_tab + "parents[i]._" + std::to_string((i + 1)) + " = FLT_MIN;\n");
          break;
        default:throw std::runtime_error("error: invalid aggregation type");
      }
    }
    parentsLine.append(m_tab + m_tab + "}\n");
    std::string initialiseFunction = m_tab + "void initialise () {\n" +
        m_tab + m_tab + "curPos = 0;\n" +
        m_tab + m_tab + "addedElements = 0;\n" +
        m_tab + m_tab + "removedElements = 0;\n" +
        parentsLine + "\n" +
        m_tab + "}\n";
    return initialiseFunction;
  }

  std::string generateInsertFunction(bool mergedWithEviction = true) {
    std::string insertFunction;
    std::string leavesLine = m_tab + "// lift function \n";
    for (unsigned long i = 0; i < m_aggregationTypes.size(); ++i) {
      leavesLine.append(
          m_tab + m_tab + "leaves[curPos]._" + std::to_string((i + 1)) + " = value._" + std::to_string((i + 1))
              + ";\n");
      switch (m_aggregationTypes[i]) {
        case AVG:
          leavesLine.append(
              m_tab + m_tab + "leaves[curPos]._c" + std::to_string((i + 1)) + " = value._c" + std::to_string((i + 1))
                  + ";\n");
      }
    }

    std::string parent = (m_numOfParents == 1) ? "parents[0] = " : "parents[curPos] = ";
    std::string parentsLine = m_tab + m_tab + "float temp;\n";
    for (unsigned long i = 0; i < m_aggregationTypes.size(); ++i) {
      switch (m_aggregationTypes[i]) {
        case SUM:
          parentsLine.append(m_tab + m_tab + "parents[0]._" + std::to_string((i + 1)) + " = " +
              getCombineFunction(m_aggregationTypes[i],
                                 "value._" + std::to_string((i + 1)),
                                 "parents[0]._" + std::to_string((i + 1))) + ";\n");
          break;
        case AVG:
          parentsLine.append(m_tab + m_tab + "parents[0]._" + std::to_string((i + 1)) + " = " +
              getCombineFunction(SUM, "value._" + std::to_string((i + 1)), "parents[0]._" + std::to_string((i + 1)))
                                 + ";\n");
          parentsLine.append(m_tab + m_tab + "parents[0]._c" + std::to_string((i + 1)) + " = " +
              getCombineFunction(CNT, "value._c" + std::to_string((i + 1)), "parents[0]._c" + std::to_string((i + 1)))
                                 + ";\n");
          break;
        case CNT:
          parentsLine.append(m_tab + m_tab + "parents[0]._" + std::to_string((i + 1)) + " = " +
              getCombineFunction(m_aggregationTypes[i],
                                 "value._" + std::to_string((i + 1)),
                                 "parents[0]._" + std::to_string((i + 1))) + ";\n");
          break;
        case MIN:
          parentsLine.append(
              m_tab + m_tab + "temp = (curPos==0) ? FLT_MAX : parents[0]._" + std::to_string((i + 1)) + ";\n");
          parentsLine.append(m_tab + m_tab + "parents[0]._" + std::to_string((i + 1)) + " = " +
              getCombineFunction(m_aggregationTypes[i], "value._" + std::to_string((i + 1)), "temp") + ";\n");
          break;
        case MAX:
          parentsLine.append(
              m_tab + m_tab + "temp = (curPos==0) ? FLT_MIN : parents[0]._" + std::to_string((i + 1)) + ";\n");
          parentsLine.append(m_tab + m_tab + "parents[0]._" + std::to_string((i + 1)) + " = " +
              getCombineFunction(m_aggregationTypes[i], "value._" + std::to_string((i + 1)), "temp") + ";\n");
          break;
        default:throw std::runtime_error("error: invalid aggregation type");
      }
    }

    insertFunction = m_tab + "void insert (node value) {\n";

    if (mergedWithEviction) {
      insertFunction += generateEvictFunction(false);
    }
    insertFunction +=
        m_tab + m_tab + leavesLine + "\n" +
            parentsLine + "\n" +
            m_tab + m_tab + "curPos++;\n" +
            m_tab + m_tab + "addedElements++;\n" +
            m_tab + m_tab + "if (curPos==BUCKET_SIZE) curPos=0;\n" +
            m_tab + "}\n";

    return insertFunction;
  }

  std::string generateEvictFunction(bool isSeparateFunction = false) {
    std::string flipLine;
    if (m_hasNonInvertible)
      flipLine = m_tab + m_tab + "if (curPos==0) flip();\n";

    for (unsigned long i = 0; i < m_aggregationTypes.size(); ++i) {
      switch (m_aggregationTypes[i]) {
        case SUM:
          flipLine.append(m_tab + m_tab + "parents[0]._" + std::to_string((i + 1)) + " = " +
              getInverseFunction(m_aggregationTypes[i], "parents[0]._" + std::to_string((i + 1)),
                                 "leaves[curPos]._" + std::to_string((i + 1)) + ";\n"));
          break;
        case AVG:
          flipLine.append(m_tab + m_tab + "parents[0]._" + std::to_string((i + 1)) + " = " +
              getInverseFunction(SUM, "parents[0]._" + std::to_string((i + 1)),
                                 "leaves[curPos]._" + std::to_string((i + 1)) + ";\n"));
          flipLine.append(m_tab + m_tab + "parents[0]._c" + std::to_string((i + 1)) + " = " +
              getInverseFunction(CNT, "parents[0]._c" + std::to_string((i + 1)),
                                 "leaves[curPos]._c" + std::to_string((i + 1)) + ";\n"));
          break;
        case CNT:
          flipLine.append(m_tab + m_tab + "parents[0]._" + std::to_string((i + 1)) + " = " +
              getInverseFunction(m_aggregationTypes[i], "parents[0]._" + std::to_string((i + 1)),
                                 "leaves[curPos]._" + std::to_string((i + 1)) + ";\n"));
          break;
      }
    }

    std::string evictFunction;
    if (isSeparateFunction) {
      evictFunction = m_tab + "void evict () {\n" +
          m_tab + m_tab + "if (addedElements<removedElements) {\n" +
          //m_tab+m_tab+m_tab+"std::cout << \"No elements to remove!\" << std::endl;\n"+
          m_tab + m_tab + m_tab + "printf(\"No elements to remove!\\n\");\n" +
          m_tab + m_tab + m_tab + "exit(1);\n" +
          m_tab + m_tab + "}\n" +
          flipLine + "\n" +
          m_tab + m_tab + "leaves[curPos].reset();\n" +
          m_tab + m_tab + "removedElements++;\n" +
          m_tab + "}\n";
    } else {
      evictFunction =
          m_tab + m_tab + "if (addedElements>=BUCKET_SIZE) { // eviction\n" +
              flipLine + "\n" +
              m_tab + m_tab + m_tab + "leaves[curPos].reset();\n" +
              m_tab + m_tab + m_tab + "removedElements++;\n" +
              m_tab + m_tab + "}\n";
    }
    return evictFunction;
  }

  std::string generateQueryFunction() {
    std::string queryFunction;
    queryFunction.append(m_tab + "node query () {\n");
    queryFunction.append(m_tab + m_tab + "node res; // lower functions\n");
    for (unsigned long i = 0; i < m_aggregationTypes.size(); ++i) {
      switch (m_aggregationTypes[i]) {
        case SUM:
          queryFunction.append(m_tab + m_tab + "res._" + std::to_string((i + 1)) + " = " +
              getLowerFunction(m_aggregationTypes[i], "parents[0]._" + std::to_string((i + 1))) + ";\n");
          break;
        case AVG:
          queryFunction.append(m_tab + m_tab + "res._" + std::to_string((i + 1)) + " = " +
              "parents[0]._" + std::to_string((i + 1)) + "/parents[0]._c" + std::to_string((i + 1)) + ";\n");
          break;
        case CNT:
          queryFunction.append(m_tab + m_tab + "res._" + std::to_string((i + 1)) + " = " +
              getLowerFunction(m_aggregationTypes[i], "parents[0]._" + std::to_string((i + 1))) + ";\n");
          break;
        case MIN:
          queryFunction.append(m_tab + m_tab + "if (curPos==0) {\n" +
              m_tab + m_tab + m_tab + "res._" + std::to_string((i + 1)) + " = " + "parents[0]._"
                                   + std::to_string((i + 1))
                                   + ";\n" +
              m_tab + m_tab + "} else {\n" +
              m_tab + m_tab + m_tab + "auto temp = " +
              getCombineFunction(m_aggregationTypes[i], "parents[0]._" + std::to_string((i + 1)),
                                 "parents[PARENTS_SIZE - curPos -1]._" + std::to_string((i + 1))) + ";\n" +
              m_tab + m_tab + m_tab + "res._" + std::to_string((i + 1)) + " = "
                                   + getLowerFunction(m_aggregationTypes[i], "temp") + ";\n" +
              m_tab + m_tab + "}\n");
          break;
        case MAX:
          queryFunction.append(m_tab + m_tab + "if (curPos==0) {\n" +
              m_tab + m_tab + m_tab + "res._" + std::to_string((i + 1)) + " = " + "parents[0]._"
                                   + std::to_string((i + 1))
                                   + ";\n" +
              m_tab + m_tab + "} else {\n" +
              m_tab + m_tab + m_tab + "auto temp = " +
              getCombineFunction(m_aggregationTypes[i], "parents[0]._" + std::to_string((i + 1)),
                                 "parents[PARENTS_SIZE - curPos -1]._" + std::to_string((i + 1))) + ";\n" +
              m_tab + m_tab + m_tab + "res._" + std::to_string((i + 1)) + " = "
                                   + getLowerFunction(m_aggregationTypes[i], "temp") + ";\n" +
              m_tab + m_tab + "}\n");
          break;
        default:throw std::runtime_error("error: invalid aggregation type");
      }
    }
    queryFunction.append(m_tab + m_tab + "return res;\n");
    queryFunction.append(m_tab + "}\n");
    //std::cout << queryFunction << std::endl;
    return queryFunction;
  }

  std::string generateQueryIntermFunction() {
    std::string queryIntermFunction;
    queryIntermFunction.append(m_tab + "node queryIntermediate (int pos) {\n");
    queryIntermFunction.append(m_tab + m_tab + "node res;\n");
    for (unsigned long i = 0; i < m_aggregationTypes.size(); ++i) {
      switch (m_aggregationTypes[i]) {
        case SUM:
          queryIntermFunction.append(m_tab + m_tab + "res._" + std::to_string((i + 1)) + " = " +
              getLowerFunction(m_aggregationTypes[i], "parents[0]._" + std::to_string((i + 1))) + ";\n");
          break;
        case AVG:
          queryIntermFunction.append(m_tab + m_tab + "res._" + std::to_string((i + 1)) + " = " +
              getLowerFunction(SUM, "parents[0]._" + std::to_string((i + 1))) + ";\n");
          queryIntermFunction.append(m_tab + m_tab + "res._c" + std::to_string((i + 1)) + " = " +
              getLowerFunction(CNT, "parents[0]._c" + std::to_string((i + 1))) + ";\n");
          break;
        case CNT:
          queryIntermFunction.append(m_tab + m_tab + "res._" + std::to_string((i + 1)) + " = " +
              getLowerFunction(m_aggregationTypes[i], "parents[0]._" + std::to_string((i + 1))) + ";\n");
          break;
        case MIN:
          queryIntermFunction.append(m_tab + m_tab + "res._" + std::to_string((i + 1)) + " = " +
              getLowerFunction(m_aggregationTypes[i],
                               "parents[0]._" + std::to_string((i + 1)) + " = parents[PARENTS_SIZE - pos -2]._"
                                   + std::to_string((i + 1))) + ";\n");
          break;
        case MAX:
          queryIntermFunction.append(m_tab + m_tab + "res._" + std::to_string((i + 1)) + " = " +
              getLowerFunction(m_aggregationTypes[i],
                               "parents[0]._" + std::to_string((i + 1)) + " = parents[PARENTS_SIZE - pos -2]._"
                                   + std::to_string((i + 1))) + ";\n");
          break;
        default:throw std::runtime_error("error: invalid aggregation type");
      }
    }
    queryIntermFunction.append(m_tab + m_tab + "return res;\n");
    queryIntermFunction.append(m_tab + "}\n");
    //std::cout << queryIntermFunction << std::endl;
    return queryIntermFunction;
  }

  std::string generateFlipFunction() {
    std::string flipFunction;
    if (m_hasNonInvertible) {
      flipFunction.append(m_tab + "inline void flip () {\n" +
          m_tab + m_tab + "parents[1] = leaves[BUCKET_SIZE-1];\n" +
          m_tab + m_tab + "for (int i = 1 ; i < BUCKET_SIZE; i++) {\n");
      for (unsigned long i = 0; i < m_aggregationTypes.size(); ++i) {
        switch (m_aggregationTypes[i]) {
          case MIN:
            flipFunction.append(m_tab + m_tab + m_tab + "parents[i+1]._" + std::to_string((i + 1)) + " = " +
                getCombineFunction(m_aggregationTypes[i], "parents[i]._" + std::to_string((i + 1)),
                                   "leaves[BUCKET_SIZE-i-1]._" + std::to_string((i + 1))) + ";\n");
            break;
          case MAX:
            flipFunction.append(m_tab + m_tab + m_tab + "parents[i+1]._" + std::to_string((i + 1)) + " = " +
                getCombineFunction(m_aggregationTypes[i], "parents[i]._" + std::to_string((i + 1)),
                                   "leaves[BUCKET_SIZE-i-1]._" + std::to_string((i + 1))) + ";\n");
            break;
        }
      }
      flipFunction.append(m_tab + m_tab + "}\n");
      for (unsigned long i = 0; i < m_aggregationTypes.size(); ++i) {
        switch (m_aggregationTypes[i]) {
          case MIN:flipFunction.append(m_tab + m_tab + "parents[0]._" + std::to_string((i + 1)) + " = FLT_MAX;\n");
            break;
          case MAX:flipFunction.append(m_tab + m_tab + "parents[0]._" + std::to_string((i + 1)) + " = FLT_MIN;\n");
            break;
        }
      }
      flipFunction.append(m_tab + "}\n");
    }
    return flipFunction;
  }

  std::string getCodeTemplate(std::string insertFunction,
                              std::string evictFunction,
                              std::string queryFunction,
                              std::string queryIntermFunction,
                              std::string initialiseFunction,
                              std::string flipFunction) {
    std::string parentsLine = "node parents[" + std::to_string(m_numOfParents) + "];";
    std::string leavesLine = "node leaves[" + std::to_string(m_numOfLeaves) + "];";
    std::string s;
    long bucketSize = m_windowDefinition.numberOfPanes();
    s.append(
        "#define PARENTS_SIZE     " + std::to_string(m_numOfParents) + "L\n" +
            "#define BUCKET_SIZE      " + std::to_string(bucketSize) + "L\n" +
            "struct Aggregator {\n" +
            m_tab + "int curPos;\n" +
            m_tab + "unsigned int addedElements;\n" +
            m_tab + "unsigned int removedElements;\n" +
            m_tab + parentsLine + "\n" +
            m_tab + leavesLine + "\n" +
            "" + initialiseFunction + "\n" +
            "" + flipFunction + "\n" +
            "" + insertFunction + "\n" +
            "" + evictFunction + "\n" +
            "" + queryFunction + "\n" +
            "" + queryIntermFunction + "\n" +
            "};\n" +
            "\n"
    );
    return s;
  }
};