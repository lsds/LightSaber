#pragma once

#include <iostream>
#include <functional>
#include <memory>
#include <string>
#include <chrono>
#include <stack>
#include <climits>
#include <vector>
#include <fstream>

#include <boost/type_index.hpp>
#include <regex>
#include <unordered_set>

#include "utils/WindowDefinition.h"
#include "cql/operators/AggregationType.h"

/*
 * \brief GeneralAggregationGraph is used for generating the code for GAGs and incremental computation
 * in the microbenchmarks to split the logic required for the system and the standalone implementation.
 *
 * TODO: merge this with the AggregationTree.h
 *
 * */

struct NodeFactory;
struct Node {
  int m_id;
  bool m_isMonoid = false;
  bool m_isParent = false;
  bool m_isLeftParent = false;
  bool m_isMarked = false;
  bool m_isRoot = false;
  Node *m_leftParent = nullptr;
  Node *m_rightParent = nullptr;
  std::vector<Node *> m_children;

  // variables used for code generation
  int m_numOfLeftParents = 0;
  int m_numOfRightParents = 0;

  void connectLeft(Node *node, bool isLeaf = false) {
    node->m_leftParent = this;
    m_children.push_back(node);
    m_isParent = true;
    if (isLeaf)
      m_isLeftParent = true;
  }
  void connectRight(Node *node) {
    node->m_rightParent = this;
    m_children.push_back(node);
    m_isParent = true;
  }
  void addChild(Node *node) {
    m_children.push_back(node);
  }
  void setRoot(Node *node) {
    m_isRoot = true;
    m_children.push_back(node);
    node->m_isMarked = true;
  }

 private:
  friend NodeFactory;
  Node(int id, bool isMonoid) : m_id(id), m_isMonoid(isMonoid) {}
};

struct NodeFactory {
  static int m_nextId;
  static Node *createNode(bool isMonoid = false) {
    return new Node(m_nextId++, isMonoid);
  }
  static int getCurrentId() {
    return m_nextId;
  };
};
int NodeFactory::m_nextId = 0;

void DFS(Node *root, int numOfNodes, bool debug = false) {
  if (root == nullptr)
    return;

  std::stack<Node *> s1, s2;
  std::unordered_set<Node *> set(2 * numOfNodes);

  s1.push(root);
  Node *node;
  while (!s1.empty()) {
    node = s1.top();
    s1.pop();
    s2.push(node);
    for (auto &child : node->m_children) {
      auto it = set.find(child);
      if ((child->m_isRoot || child->m_isParent) && it != set.end()) {
        s1.push(child);
        set.insert(child);
      }
    }
  }

  while (!s2.empty()) {
    node = s2.top();
    s2.pop();
    // collapse parent nodes that are not marked
    if (node->m_isParent && !node->m_isMarked) {
      Node *parent;
      if (node->m_leftParent) {
        parent = node->m_leftParent;
      } else {
        parent = node->m_rightParent;
      }
      // remove this node from its parent's list
      std::vector<Node *>::iterator iter;
      for (iter = parent->m_children.begin(); iter != parent->m_children.end();) {
        if ((*iter) == node)
          iter = parent->m_children.erase(iter);
        else
          ++iter;
      }
      // add new children
      for (auto child : node->m_children) {
        parent->addChild(child);
      }
      node->m_children.clear();
    } else if (node->m_isRoot) {
      if (node->m_children.size() == 1) {
        //node->hasSingleInvertible = true;
        node->m_numOfLeftParents = 2;
      } else {
        for (auto &child : node->m_children) {
          if (child->m_isLeftParent) {
            node->m_numOfLeftParents++;
          } else {
            node->m_numOfRightParents++;
          }
        }
        if (node->m_numOfLeftParents > 0)
          node->m_numOfLeftParents++;
        if (node->m_numOfRightParents > 0)
          node->m_numOfRightParents++;
      }
    }
  }

  if (debug) {
    std::cout << "Finished the rewriting." << std::endl;
  }
}

class GeneralAggregationGraph {
 private:
  long m_size;
  long m_numOfLeaves;
  int m_numberOfWindows;
  int m_numberOfSlidingWindows;
  int m_flipRange;
  int m_numOfFlips;
  bool m_hasInvertible;
  bool m_hasNonInvertible;
  int m_numOfInvertible = 0;
  int m_numOfNonInvertible = 0;
  bool m_hasMultipleWindows = false;
  Node *m_root;

  // single window definition
  WindowDefinition *m_windowDefinition;
  std::vector<AggregationType> *m_aggregationTypes;
  // multiple window definitions
  std::vector<WindowDefinition> *m_windowDefinitions;
  std::vector<std::vector<AggregationType>> *m_mulAggregationTypes;

 public:
  GeneralAggregationGraph(WindowDefinition *windowDefinition, std::vector<AggregationType> *aggregationTypes) :
      m_hasInvertible(false), m_hasNonInvertible(false),
      m_windowDefinition(windowDefinition), m_aggregationTypes(aggregationTypes) {
    if (windowDefinition == nullptr ||
        aggregationTypes == nullptr //||
      /*(windowDefinition->isTumbling() && windowDefinition->getSize()!=1)*/) {
      throw std::runtime_error("error: the type of the window is wrong");
    }

    // Initialize appropriate variables for generating the abstract graph
    for (auto &type : *aggregationTypes) {
      if (AggregationTypes::isInvertible(type)) {
        m_hasInvertible = true;
      } else {
        m_hasNonInvertible = true;
      }
    }
    m_numberOfWindows = 1;
    m_size = m_windowDefinition->numberOfPanes();
    // initialize variables needed for code generation
    m_flipRange = (int) m_windowDefinition->numberOfPanes();
    m_numberOfSlidingWindows = 1;
    m_numOfFlips = 1;
    m_numOfLeaves = m_size;
  }

  GeneralAggregationGraph(std::vector<WindowDefinition> *windowDefinitions,
                          std::vector<std::vector<AggregationType>> *aggregationTypes) :
      m_hasInvertible(false),
      m_hasNonInvertible(false),
      m_windowDefinition(&(*windowDefinitions)[0]),
      m_aggregationTypes(&(*aggregationTypes)[0]),
      m_windowDefinitions(windowDefinitions),
      m_mulAggregationTypes(aggregationTypes) {
    // Initialize appropriate variables for generating the abstract tree
    if (windowDefinitions == nullptr ||
        aggregationTypes == nullptr ||
        windowDefinitions->size() != aggregationTypes->size()) {
      throw std::runtime_error("error: the number of windows does not match");
    }
    m_numberOfWindows = windowDefinitions->size();

    // initialize variables needed for code generation
    m_hasMultipleWindows = true;
    m_size = -1;
    m_flipRange = INT_MAX;
    m_numberOfSlidingWindows = 0;
    for (int i = 0; i < m_numberOfWindows; ++i) {
      bool invFound = false;
      bool nonInvFound = false;
      m_size = std::max(m_size, (*m_windowDefinitions)[i].numberOfPanes());
      //if (!(*this->windowDefinitions)[i].isTumbling()) {
      m_flipRange = std::min(m_flipRange, (int) (*m_windowDefinitions)[i].numberOfPanes());
      m_numberOfSlidingWindows++;
      //}
      for (auto &type : (*m_mulAggregationTypes)[i]) {
        if (AggregationTypes::isInvertible(type)) {
          m_hasInvertible = true;
          invFound = true;
        } else {
          m_hasNonInvertible = true;
          nonInvFound = true;
        }
      }
      if (invFound)
        m_numOfInvertible++;
      if (nonInvFound)
        m_numOfNonInvertible++;
    }
    m_numOfFlips = 1;
    m_numOfLeaves = m_size;
  }

  std::string generateCode(bool splitEviction = false,
                           bool codeForMicroBenchs = false,
                           bool debug = false,
                           bool measureLatency = false) {
    Node *leaves[m_numOfLeaves];
    Node *monoids[2];
    Node *leftPars[m_numOfLeaves];
    Node *rightPars[m_numOfLeaves];
    monoids[0] = NodeFactory::createNode(true);
    monoids[1] = NodeFactory::createNode(true);
    for (long i = 0; i < m_numOfLeaves; ++i) {
      leaves[i] = NodeFactory::createNode();
    }
    for (long i = 0; i < m_numOfLeaves; ++i) {
      leftPars[i] = NodeFactory::createNode();
      if (i == 0)
        leftPars[i]->connectLeft(monoids[0]);
      else
        leftPars[i]->connectLeft(leftPars[i - 1]);
      leftPars[i]->connectLeft(leaves[i], true);
    }
    for (long i = 0; i < m_numOfLeaves; ++i) {
      rightPars[i] = NodeFactory::createNode();
      if (i == 0)
        rightPars[i]->connectRight(monoids[1]);
      else
        rightPars[i]->connectRight(rightPars[i - 1]);
      rightPars[i]->connectRight(leaves[m_numOfLeaves - i - 1]);
    }

    // TODO: add more roots based on a cost based rule for invertible function
    // mark nodes needed for producing the output
    m_root = NodeFactory::createNode();
    // single invertible
    m_root->setRoot(leftPars[m_numOfLeaves - 1]);
    // single or multiple non-invertible
    if (m_hasNonInvertible || m_numOfInvertible > 0) {
      for (long i = m_numOfLeaves - 1; i >= 0; --i) {
        m_root->setRoot(rightPars[i]);
      }
    }
    // multiple invertible
    if (m_numOfInvertible > 0) {
      for (long i = m_numOfLeaves - 2; i >= 0; --i) {
        m_root->setRoot(leftPars[i]);
      }
    }

    DFS(m_root, NodeFactory::getCurrentId(), debug);

    std::string insertFunction = generateInsertFunction(splitEviction);
    std::string queryFunction = generateQueryFunction();
    std::string queryMultFunction = generateQueryMultFunction();
    std::string queryIntermFunction = generateQueryIntermFunction();
    std::string initialiseFunction = generateInitialiseFunction();
    std::string fullCode = getCodeTemplate(insertFunction, queryFunction, queryMultFunction,
                                           queryIntermFunction, initialiseFunction, codeForMicroBenchs, measureLatency);

    if (debug) {
      std::cout << generateAggregationTreeNode() << std::endl;
      std::cout << fullCode << std::endl;
    }

    // clear memory
    for (long i = 0; i < m_numOfLeaves; ++i) {
      delete (leaves[i]);
      delete (leftPars[i]);
      delete (rightPars[i]);
    }
    delete (monoids[0]);
    delete (monoids[1]);

    return fullCode;
  }

  std::string generateAggregationTreeNode() {
    std::string s;
    s.append("struct node {\n");
    for (unsigned long i = 0; i < m_aggregationTypes->size(); ++i) {
      switch ((*m_aggregationTypes)[i]) {
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
    for (unsigned long i = 0; i < m_aggregationTypes->size(); ++i) {
      switch ((*m_aggregationTypes)[i]) {
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
  std::string newline = "\n";

  static bool isInvertible(AggregationType type) {
    return AggregationTypes::isInvertible(type);
  }

  bool hasMulitpleFlips() { return m_numOfFlips > 1; }

  static std::string getCombineFunction(AggregationType type, std::string leftArg, std::string rightArg) {
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

  static std::string getLowerFunction(AggregationType type, std::string arg) {
    if (type == CNT || type == MIN || type == SUM || type == MAX)
      return arg;
    else if (type == AVG || type == W_AVG)
      return arg + ".second/" + arg + ".first";
    else {
      throw std::runtime_error("error: Unsupported type");
    }
  }

  static std::string getInverseFunction(AggregationType type, std::string leftArg, std::string rightArg) {
    if (type == CNT || type == SUM)
      return leftArg + "-" + rightArg;
    else if (type == AVG || type == W_AVG)
      return "std::make(" + leftArg + ".first-" + rightArg + ".first, " + leftArg + ".second-" + rightArg +
          ".second)";
    else {
      throw std::runtime_error("error: Unsupported type");
    }
  }

  static std::string getFlipCondition(AggregationType type, std::string leftArg, std::string rightArg) {
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
    parentsLine.append(m_tab + m_tab + "for (int i = 0; i < BUCKET_SIZE; i++) {\n");
    parentsLine.append(m_tab + m_tab + m_tab + "leaves[i].reset();\n");
    parentsLine.append(m_tab + m_tab + "}\n");
    if (m_root->m_numOfLeftParents) {
      parentsLine.append(m_tab + m_tab + "for (int i = 0; i < LEFT_PARENTS_SIZE; i++) {\n");
      for (unsigned long i = 0; i < m_aggregationTypes->size(); ++i) {
        switch ((*m_aggregationTypes)[i]) {
          case SUM:parentsLine.append(m_tab + m_tab + m_tab + "lparents[i]._" + std::to_string((i + 1)) + " = 0.0f;\n");
            break;
          case AVG:parentsLine.append(m_tab + m_tab + m_tab + "lparents[i]._" + std::to_string((i + 1)) + " = 0.0f;\n");
            parentsLine.append(m_tab + m_tab + m_tab + "lparents[i]._c" + std::to_string((i + 1)) + " = 0.0f;\n");
            break;
          case CNT:parentsLine.append(m_tab + m_tab + m_tab + "lparents[i]._" + std::to_string((i + 1)) + " = 0.0f;\n");
            break;
          case MIN:
            parentsLine.append(
                m_tab + m_tab + m_tab + "lparents[i]._" + std::to_string((i + 1)) + " = FLT_MAX;\n");
            break;
          case MAX:
            parentsLine.append(
                m_tab + m_tab + m_tab + "lparents[i]._" + std::to_string((i + 1)) + " = FLT_MIN;\n");
            break;
          default:throw std::runtime_error("error: invalid aggregation type");
        }
      }
      parentsLine.append(m_tab + m_tab + "}\n");
    }
    if (m_root->m_numOfRightParents) {
      parentsLine.append(m_tab + m_tab + "for (int i = 0; i < RIGHT_PARENTS_SIZE; i++) {\n");
      for (unsigned long i = 0; i < m_aggregationTypes->size(); ++i) {
        switch ((*m_aggregationTypes)[i]) {
          case SUM:parentsLine.append(m_tab + m_tab + m_tab + "rparents[i]._" + std::to_string((i + 1)) + " = 0.0f;\n");
            break;
          case AVG:parentsLine.append(m_tab + m_tab + m_tab + "rparents[i]._" + std::to_string((i + 1)) + " = 0.0f;\n");
            parentsLine.append(m_tab + m_tab + m_tab + "rparents[i]._c" + std::to_string((i + 1)) + " = 0.0f;\n");
            break;
          case CNT:parentsLine.append(m_tab + m_tab + m_tab + "rparents[i]._" + std::to_string((i + 1)) + " = 0.0f;\n");
            break;
          case MIN:
            parentsLine.append(
                m_tab + m_tab + m_tab + "rparents[i]._" + std::to_string((i + 1)) + " = FLT_MAX;\n");
            break;
          case MAX:
            parentsLine.append(
                m_tab + m_tab + m_tab + "rparents[i]._" + std::to_string((i + 1)) + " = FLT_MIN;\n");
            break;
          default:throw std::runtime_error("error: invalid aggregation type");
        }
      }
      parentsLine.append(m_tab + m_tab + "}\n");
    }
    std::string initialiseFunction = m_tab + "void initialise () {\n" +
        m_tab + m_tab + "curPos = 0;\n" +
        m_tab + m_tab + "addedElements = 0;\n" +
        m_tab + m_tab + "removedElements = 0;\n" +
        parentsLine + "\n" +
        m_tab + "}\n";
    return initialiseFunction;
  }

  std::string generateInsertFunction(bool splitEviction = false) {
    std::string insertFunction;
    std::string leavesLine = "// lift function \n";
    for (unsigned long i = 0; i < m_aggregationTypes->size(); ++i) {
      leavesLine.append(
          m_tab + m_tab + "leaves[curPos]._" + std::to_string((i + 1)) + " = value._" + std::to_string((i + 1))
              + ";\n");
      switch ((*m_aggregationTypes)[i]) {
        case AVG:
          leavesLine.append(
              m_tab + m_tab + "leaves[curPos]._c" + std::to_string((i + 1)) + " = value._c" + std::to_string((i + 1))
                  + ";\n");
        default:break;
      }
    }

    std::string parent = (m_hasMultipleWindows && m_numOfInvertible > 0) ? "lparents[curPos+1]" : "lparents[1]";
    std::string prevParent = (m_hasMultipleWindows && m_numOfInvertible > 0) ? "lparents[curPos]" : "lparents[1]";
    std::string parentsLine = m_tab + m_tab + "float temp;\n";
    for (unsigned long i = 0; i < m_aggregationTypes->size(); ++i) {
      switch ((*m_aggregationTypes)[i]) {
        case SUM:
          parentsLine.append(m_tab + m_tab + parent + "._" + std::to_string((i + 1)) + " = " +
              getCombineFunction((*m_aggregationTypes)[i],
                                 "value._" + std::to_string((i + 1)),
                                 prevParent + "._" + std::to_string((i + 1))) + ";\n");
          break;
        case AVG:
          parentsLine.append(m_tab + m_tab + parent + "._" + std::to_string((i + 1)) + " = " +
              getCombineFunction(SUM, "value._" + std::to_string((i + 1)), prevParent + "._" + std::to_string((i + 1)))
                                 + ";\n");
          parentsLine.append(m_tab + m_tab + parent + "._c" + std::to_string((i + 1)) + " = " +
              getCombineFunction(CNT,
                                 "value._c" + std::to_string((i + 1)),
                                 prevParent + "._c" + std::to_string((i + 1))) + ";\n");
          break;
        case CNT:
          parentsLine.append(m_tab + m_tab + parent + "._" + std::to_string((i + 1)) + " = " +
              getCombineFunction((*m_aggregationTypes)[i],
                                 "value._" + std::to_string((i + 1)),
                                 prevParent + "._" + std::to_string((i + 1))) + ";\n");
          break;
        case MIN:
          parentsLine.append(m_tab + m_tab + "temp = " + prevParent + "._" + std::to_string((i + 1))
                                 + ";\n"); //"temp = (curPos==0) ? FLT_MAX : parents[0]._"+std::to_string((i+1))+";\n");
          parentsLine.append(m_tab + m_tab + parent + "._" + std::to_string((i + 1)) + " = " +
              getCombineFunction((*m_aggregationTypes)[i], "value._" + std::to_string((i + 1)), "temp") + ";\n");
          break;
        case MAX:
          parentsLine.append(m_tab + m_tab + "temp = " + prevParent + "._" + std::to_string((i + 1))
                                 + ";\n"); //"temp = (curPos==0) ? FLT_MIN : parents[0]._"+std::to_string((i+1))+";\n");
          parentsLine.append(m_tab + m_tab + parent + "._" + std::to_string((i + 1)) + " = " +
              getCombineFunction((*m_aggregationTypes)[i], "value._" + std::to_string((i + 1)), "temp") + ";\n");
          break;
        default:throw std::runtime_error("error: invalid aggregation type");
      }
    }

    std::string evictionFunction = generateEvictFunction(splitEviction);

    insertFunction = m_tab + "void insert (node value) {\n";
    if (!splitEviction)
      insertFunction += evictionFunction;
    insertFunction +=
        m_tab + m_tab + leavesLine + "\n" +
            parentsLine + "\n" +
            m_tab + m_tab + "curPos++;\n" +
            m_tab + m_tab + "addedElements++;\n" +
            m_tab + m_tab + "if (curPos==BUCKET_SIZE) curPos=0;\n" +
            m_tab + "}\n";
    if (splitEviction)
      insertFunction += evictionFunction;

    return insertFunction;
  }

  std::string generateEvictFunction(bool splitEviction = false) {
    std::string flipLine;
    if (m_hasNonInvertible || m_numOfInvertible > 0) {
      flipLine += m_tab + m_tab + "if (curPos==0) {\n";
      flipLine += generateFlipFunction("0");
      flipLine += m_tab + m_tab + m_tab + "flipCounter = 0;\n";
      flipLine += m_tab + m_tab + "}\n";

      if (m_numOfNonInvertible > 0) {
        flipLine += m_tab + m_tab + "else if (flipCounter==flipBarrier) {\n";
        flipLine += generateFlipFunction("RIGHT_PARENTS_SIZE-curPos-1");
        flipLine += m_tab + m_tab + m_tab + "flipCounter = 0;\n";
        flipLine += m_tab + m_tab + "}\n";
        flipLine += m_tab + m_tab + "flipCounter++;\n";
      }
    } else {
      for (unsigned long i = 0; i < m_aggregationTypes->size(); ++i) {
        switch ((*m_aggregationTypes)[i]) {
          case SUM:
            flipLine.append(m_tab + m_tab + "lparents[1]._" + std::to_string((i + 1)) + " = " +
                getInverseFunction((*m_aggregationTypes)[i],
                                   "lparents[1]._" + std::to_string((i + 1)),
                                   "leaves[curPos]._" + std::to_string((i + 1)) + ";\n"));
            break;
          case AVG:
            flipLine.append(m_tab + m_tab + "lparents[1]._" + std::to_string((i + 1)) + " = " +
                getInverseFunction(SUM, "lparents[1]._" + std::to_string((i + 1)),
                                   "leaves[curPos]._" + std::to_string((i + 1)) + ";\n"));
            flipLine.append(m_tab + m_tab + "lparents[1]._c" + std::to_string((i + 1)) + " = " +
                getInverseFunction(CNT, "lparents[1]._c" + std::to_string((i + 1)),
                                   "leaves[curPos]._c" + std::to_string((i + 1)) + ";\n"));
            break;
          case CNT:
            flipLine.append(m_tab + m_tab + "lparents[1]._" + std::to_string((i + 1)) + " = " +
                getInverseFunction((*m_aggregationTypes)[i],
                                   "lparents[1]._" + std::to_string((i + 1)),
                                   "leaves[curPos]._" + std::to_string((i + 1)) + ";\n"));
            break;
          default:break;
        }
      }
    }
    std::string evictFunction = (splitEviction) ?
                                newline +
                                    m_tab + "void evict () {\n" +
                                    //m_tab + m_tab + "if (addedElements>=BUCKET_SIZE) { // eviction\n" +
                                    //m_tab + m_tab + m_tab + "removedElements++;\n" +
                                    //m_tab + m_tab + m_tab + "leaves[curPos].reset();\n" +
                                    m_tab + m_tab + "if (addedElements<removedElements) {\n" +
                                    m_tab + m_tab + m_tab + "printf(\"No elements to remove!\\n\");\n" +
                                    m_tab + m_tab + m_tab + "exit(1);\n" +
                                    m_tab + m_tab + "}\n" :
                                "";

    evictFunction += flipLine + "\n"; //+

    evictFunction += (splitEviction) ?
                     m_tab + m_tab + "leaves[curPos].reset();\n" +
                         m_tab + m_tab + "removedElements++;\n" +
                         m_tab + "}\n" :
                     "";

    return evictFunction;
  }

  std::string generateQueryFunction() {
    std::string queryFunction;
    queryFunction.append(m_tab + "node query () {\n");
    queryFunction.append(m_tab + m_tab + "node res; // lower functions\n");
    std::string parent = (m_hasMultipleWindows && m_numOfInvertible > 0) ? "lparents[curPos+1]" : "lparents[1]";
    std::string secondParent = "rparents[BUCKET_SIZE - curPos]";
    for (unsigned long i = 0; i < m_aggregationTypes->size(); ++i) {
      switch ((*m_aggregationTypes)[i]) {
        case SUM:
        case CNT:
          queryFunction.append(m_tab + m_tab + "res._" + std::to_string((i + 1)) + " = "
                                   + getLowerFunction((*m_aggregationTypes)[i],
                                                      parent + "._" + std::to_string((i + 1)) + ";\n"));
          break;
        case AVG:
          queryFunction.append(m_tab + m_tab + "res._" + std::to_string((i + 1)) + " = " +
              "lparents[1]._" + std::to_string((i + 1)) + "/lparents[1]._c" + std::to_string((i + 1)) + ";\n");
          break;
        case MIN:
          queryFunction.append(m_tab + m_tab + "auto temp = " +
              getCombineFunction((*m_aggregationTypes)[i], parent + "._" + std::to_string((i + 1)),
                                 secondParent + "._" + std::to_string((i + 1))) + ";\n" +
              m_tab + m_tab + "res._" + std::to_string((i + 1)) + " = "
                                   + getLowerFunction((*m_aggregationTypes)[i], "temp")
                                   + ";\n");
          break;
        case MAX:
          queryFunction.append(m_tab + m_tab + "auto temp = " +
              getCombineFunction((*m_aggregationTypes)[i], parent + "._" + std::to_string((i + 1)),
                                 secondParent + "._" + std::to_string((i + 1))) + ";\n" +
              m_tab + m_tab + "res._" + std::to_string((i + 1)) + " = "
                                   + getLowerFunction((*m_aggregationTypes)[i], "temp")
                                   + ";\n");
          break;
        default:throw std::runtime_error("error: invalid aggregation type");
      }
    }
    queryFunction.append(m_tab + m_tab + "return res;\n");
    queryFunction.append(m_tab + "}\n");
    //std::cout << queryFunction << std::endl;
    return queryFunction;
  }

  std::string generateQueryMultFunction() {
    std::string queryFunction;
    if (m_hasMultipleWindows) {
      queryFunction.append(m_tab + "node query (int pos) {\n");
      queryFunction.append(m_tab + m_tab + "bool hasWrapped = false;\n" +
          m_tab + m_tab + "auto frontPtr = curPos;\n" +
          m_tab + m_tab + "if (frontPtr <= 0)\n" +
          m_tab + m_tab + m_tab + "frontPtr = BUCKET_SIZE;\n" +
          m_tab + m_tab + "auto backPtr = frontPtr - pos;\n" +
          m_tab + m_tab + "if (backPtr < 0) {\n" +
          m_tab + m_tab + m_tab + "hasWrapped = true;\n" +
          m_tab + m_tab + m_tab + "backPtr += BUCKET_SIZE;\n" +
          m_tab + m_tab + "}\n");

      queryFunction.append(m_tab + m_tab + "node res; // lower functions\n");
      std::string parent = (m_hasMultipleWindows && m_numOfInvertible > 0) ? "lparents[pos]" : "lparents[1]";

      /*
           if (!hasWrapped && curPos == pos) {
              res._x =  lparent[1];
           } else if (hasWrapped) {
              res._x = combine(lparents[1], rparents[BUCKET_SIZE - backPtr]);
           } else {
              res._x = combine(leaves[frontPtr], rparents[BUCKET_SIZE - backPtr]);
           }
       * */
      for (unsigned long i = 0; i < m_aggregationTypes->size(); ++i) {
        switch ((*m_aggregationTypes)[i]) {
          case SUM:
          case CNT:
            queryFunction.append(
                m_tab + m_tab + "auto temp = " + parent + "._" + std::to_string((i + 1)) + ";\n" +
                    //getCombineFunction((*aggregationTypes)[i], parent+"._"+std::to_string((i+1)),
                    //                   secondParent+"._"+std::to_string((i+1))) +";\n" +
                    m_tab + m_tab + "res._" + std::to_string((i + 1)) + " = " +
                    getLowerFunction((*m_aggregationTypes)[i], "temp") + ";\n");
            queryFunction.append(
                m_tab + m_tab + "if (!hasWrapped && backPtr == 0) {\n" +
                    m_tab + m_tab + m_tab + "res._" + std::to_string((i + 1)) + " =  lparents[frontPtr]._"
                    + std::to_string(i + 1) + ";\n" +
                    m_tab + m_tab + "} else if (hasWrapped) {\n" +
                    m_tab + m_tab + m_tab + "res._" + std::to_string((i + 1)) + " = " +
                    getCombineFunction((*m_aggregationTypes)[i],
                                       "lparents[frontPtr]._" + std::to_string((i + 1)),
                                       "rparents[BUCKET_SIZE - backPtr]._" + std::to_string((i + 1))) + ";\n" +
                    m_tab + m_tab + "} else {\n" +
                    m_tab + m_tab + m_tab + "res._" + std::to_string((i + 1)) + " = " +
                    getInverseFunction((*m_aggregationTypes)[i],
                                       "lparents[frontPtr]._" + std::to_string((i + 1)),
                                       "lparents[backPtr]._" + std::to_string((i + 1))) + ";\n" +
                    m_tab + m_tab + "}\n");
            break;
          case AVG:throw std::runtime_error("error: this is not supported yet from the code generation");
            //queryFunction.append(
            //        m_tab + m_tab + "auto temp = " + parent + "._" + std::to_string((i + 1)) + ";\n" +
            //        m_tab + m_tab + "res._" + std::to_string((i + 1)) + " = " +
            //        "temp._" + std::to_string((i + 1)) + "/temp._c" + std::to_string((i + 1)) + ";\n");
            //break;
          case MIN:
          case MAX:
            queryFunction.append(
                m_tab + m_tab + "res._" + std::to_string((i + 1)) + " =  " +
                    getCombineFunction((*m_aggregationTypes)[i],
                                       "lparents[1]._" + std::to_string((i + 1)),
                                       "rparents[BUCKET_SIZE - backPtr]._" + std::to_string((i + 1))) + ";\n"
                /*m_tab + m_tab + "if (!hasWrapped && backPtr == 0) {\n" +
                m_tab + m_tab + m_tab + "res._"+std::to_string((i + 1))+" =  lparents[1]._"+std::to_string((i + 1))+";\n" +
                m_tab + m_tab + "} else if (hasWrapped) {\n" +
                m_tab + m_tab + m_tab + "res._"+std::to_string((i + 1))+" = " +
                getCombineFunction((*aggregationTypes)[i],
                                   "lparents[1]._" + std::to_string((i + 1)),
                                   "rparents[BUCKET_SIZE - backPtr]._" + std::to_string((i + 1))) + ";\n" +
                m_tab + m_tab + "} else {\n" +
                m_tab + m_tab + m_tab + "res._"+std::to_string((i + 1))+" = " +
                getCombineFunction((*aggregationTypes)[i],
                                   "leaves[frontPtr]._" + std::to_string((i + 1)),
                                   "rparents[BUCKET_SIZE - backPtr]._" + std::to_string((i + 1))) + ";\n" +
                m_tab + m_tab + "}\n"*/);
            break;
          default:throw std::runtime_error("error: invalid aggregation type");
        }
      }
      queryFunction.append(m_tab + m_tab + "return res;\n");
      queryFunction.append(m_tab + "}\n");
    }
    //std::cout << queryFunction << std::endl;
    return queryFunction;
  }

  // TODO: generalise this. This assumption now is that we have only functions of the same type
  std::string generateQueryIntermFunction() {
    if (m_hasMultipleWindows && m_numOfInvertible > 0 && m_numOfNonInvertible > 0)
      throw std::runtime_error(
          "error: the combination of multiple invertible and not invertible functions is not supported yet");

    std::string queryIntermFunction;
    queryIntermFunction.append(m_tab + "node queryIntermediate (int pos) {\n");
    queryIntermFunction.append(m_tab + m_tab + "node res;\n");
    std::string parent = (m_hasMultipleWindows && m_numOfInvertible > 0) ? "lparents[pos+1]" : "lparents[1]";
    std::string secondParent = "rparents[BUCKET_SIZE - pos]";
    for (unsigned long i = 0; i < m_aggregationTypes->size(); ++i) {
      switch ((*m_aggregationTypes)[i]) {
        case SUM:
          if ((m_hasMultipleWindows && m_numOfInvertible > 0)) {
            queryIntermFunction.append(m_tab + m_tab + "auto temp" + std::to_string(i) + " = " +
                getCombineFunction((*m_aggregationTypes)[i],
                                   parent + "._" + std::to_string((i + 1)),
                                   secondParent + "._" +
                                       std::to_string((i + 1))) +
                ";\n" +
                m_tab + m_tab + "res._" + std::to_string((i + 1)) + " = " +
                getLowerFunction((*m_aggregationTypes)[i],
                                 "temp" + std::to_string(i)) + ";\n");
          } else {
            queryIntermFunction.append(m_tab + m_tab + "res._" + std::to_string((i + 1)) + " = " +
                getLowerFunction((*m_aggregationTypes)[i], parent + "._" + std::to_string((i + 1))) + ";\n");
          }
          break;
        case AVG:
          if ((m_hasMultipleWindows && m_numOfInvertible > 0)) {
            queryIntermFunction.append(m_tab + m_tab + "auto temp" + std::to_string(i) + "a = " +
                getCombineFunction(SUM,
                                   parent + "._" + std::to_string((i + 1)),
                                   secondParent + "._" + std::to_string((i + 1))) +
                ";\n" +
                m_tab + m_tab + "res._" + std::to_string((i + 1)) + " = " +
                getLowerFunction(SUM, "temp" + std::to_string(i)) + "a;\n");
            queryIntermFunction.append(m_tab + m_tab + "auto temp" + std::to_string(i) + "b = " +
                getCombineFunction(CNT,
                                   parent + "._" + std::to_string((i + 1)),
                                   secondParent + "._" + std::to_string((i + 1))) +
                ";\n" +
                m_tab + m_tab + "res._" + std::to_string((i + 1)) + " = " +
                getLowerFunction(CNT, "temp" + std::to_string(i)) + "b;\n");

          } else {
            queryIntermFunction.append(m_tab + m_tab + "res._" + std::to_string((i + 1)) + " = " +
                getLowerFunction(SUM, parent + "._" + std::to_string((i + 1))) + ";\n");
            queryIntermFunction.append(m_tab + m_tab + "res._c" + std::to_string((i + 1)) + " = " +
                getLowerFunction(CNT, parent + "._" + std::to_string((i + 1))) + ";\n");
          }
          break;
        case CNT:
          if ((m_hasMultipleWindows && m_numOfInvertible > 0)) {
            queryIntermFunction.append(m_tab + m_tab + "auto temp" + std::to_string(i) + " = " +
                getCombineFunction((*m_aggregationTypes)[i],
                                   parent + "._" + std::to_string((i + 1)),
                                   secondParent + "._" + std::to_string((i + 1))) +
                ";\n" +
                m_tab + m_tab + "res._" + std::to_string((i + 1)) + " = " +
                getLowerFunction((*m_aggregationTypes)[i], "temp" + std::to_string(i)) + ";\n");
          } else {
            queryIntermFunction.append(m_tab + m_tab + "res._" + std::to_string((i + 1)) + " = " +
                getLowerFunction((*m_aggregationTypes)[i], parent + "._" + std::to_string((i + 1))) + ";\n");
          }
          break;
        case MIN:
          queryIntermFunction.append(m_tab + m_tab + "auto temp" + std::to_string((i)) + " = " +
              getCombineFunction((*m_aggregationTypes)[i], parent + "._" + std::to_string((i + 1)),
                                 secondParent + "._" + std::to_string((i + 1))) + ";\n" +
              m_tab + m_tab + "res._" + std::to_string((i + 1)) + " = " +
              getLowerFunction((*m_aggregationTypes)[i], "temp" + std::to_string((i)) + ";\n"));
          break;
        case MAX:
          queryIntermFunction.append(m_tab + m_tab + "auto temp" + std::to_string((i)) + " = " +
              getCombineFunction((*m_aggregationTypes)[i], parent + "._" + std::to_string((i + 1)),
                                 secondParent + "._" + std::to_string((i + 1))) + ";\n" +
              m_tab + m_tab + "res._" + std::to_string((i + 1)) + " = " +
              getLowerFunction((*m_aggregationTypes)[i], "temp" + std::to_string((i)) + ";\n"));
          break;
        default:throw std::runtime_error("error: invalid aggregation type");
      }
    }
    queryIntermFunction.append(m_tab + m_tab + "return res;\n");
    queryIntermFunction.append(m_tab + "}\n");
    //std::cout << queryIntermFunction << std::endl;
    return queryIntermFunction;
  }

  std::string generateFlipFunction(std::string startPos) {
    if ((*m_aggregationTypes).size() > 1)
      throw std::runtime_error("error: generating flip function for multiple aggregate functions is not supported yet");
    std::string flipFunction;
    if (m_hasNonInvertible) {
      if (m_hasMultipleWindows)
        flipFunction.append(
            m_tab + m_tab + m_tab + "bool firstTime = true;\n" +
                m_tab + m_tab + m_tab + "node prevVal;\n");
      flipFunction.append(
          m_tab + m_tab + m_tab + "for (int i = " + startPos + " ; i < BUCKET_SIZE; i++) {\n");
      for (unsigned long i = 0; i < m_aggregationTypes->size(); ++i) {
        switch ((*m_aggregationTypes)[i]) {
          case MIN:
          case MAX:
            if (m_hasMultipleWindows)
              flipFunction.append(
                  m_tab + m_tab + m_tab + m_tab + "if (!firstTime && prevVal._1 == rparents[i+1]._1)\n" +
                      m_tab + m_tab + m_tab + m_tab + m_tab + "break;\n" +
                      m_tab + m_tab + m_tab + m_tab + "else if (firstTime) {\n" +
                      m_tab + m_tab + m_tab + m_tab + m_tab + "firstTime = false;\n" +
                      m_tab + m_tab + m_tab + m_tab + m_tab + "rparents[i]._1 = FLT_MAX;\n" +
                      m_tab + m_tab + m_tab + m_tab + "}\n");
            flipFunction.append(m_tab + m_tab + m_tab + m_tab + "rparents[i+1]._" + std::to_string((i + 1)) + " = " +
                getCombineFunction((*m_aggregationTypes)[i], "rparents[i]._" + std::to_string((i + 1)),
                                   "leaves[BUCKET_SIZE-i-1]._" + std::to_string((i + 1))) + ";\n");
            if (m_hasMultipleWindows)
              flipFunction.append(m_tab + m_tab + m_tab + m_tab + "prevVal = rparents[i+1];\n");
            break;
          case SUM:
            flipFunction.append(m_tab + m_tab + m_tab + m_tab + "rparents[i+1]._" + std::to_string((i + 1)) + " = " +
                getCombineFunction((*m_aggregationTypes)[i],
                                   "rparents[i]._" + std::to_string((i + 1)),
                                   "leaves[BUCKET_SIZE-i-1]._" + std::to_string((i + 1)) + ";\n"));
            break;
          case AVG:
            flipFunction.append(m_tab + m_tab + m_tab + m_tab + "rparents[i+1]._" + std::to_string((i + 1)) + " = " +
                getCombineFunction(SUM, "rparents[i]._" + std::to_string((i + 1)),
                                   "leaves[BUCKET_SIZE-i-1]._" + std::to_string((i + 1)) + ";\n"));
            flipFunction.append(m_tab + m_tab + m_tab + m_tab + "rparents[i+1]._c" + std::to_string((i + 1)) + " = " +
                getCombineFunction(CNT, "rparents[i]._c" + std::to_string((i + 1)),
                                   "leaves[BUCKET_SIZE-i-1]._c" + std::to_string((i + 1)) + ";\n"));
            break;
          case CNT:
            flipFunction.append(m_tab + m_tab + m_tab + m_tab + "rparents[i+1]._" + std::to_string((i + 1)) + " = " +
                getCombineFunction((*m_aggregationTypes)[i],
                                   "rparents[i]._" + std::to_string((i + 1)),
                                   "leaves[BUCKET_SIZE-i-1]._" + std::to_string((i + 1)) + ";\n"));
          default:break;
        }
      }
      flipFunction.append(m_tab + m_tab + m_tab + "}\n");
      // TODO: fix this
      for (unsigned long i = 0; i < m_aggregationTypes->size(); ++i) {
        switch ((*m_aggregationTypes)[i]) {
          case MIN:
            flipFunction.append(m_tab + m_tab + m_tab + "lparents[1]._" + std::to_string((i + 1)) + " = FLT_MAX;\n");
            if (startPos == "0")
              flipFunction.append(
                  m_tab + m_tab + m_tab + "rparents[BUCKET_SIZE]._" + std::to_string((i + 1)) + " = FLT_MAX;\n");
            break;
          case MAX:
            flipFunction.append(m_tab + m_tab + m_tab + "lparents[1]._" + std::to_string((i + 1)) + " = FLT_MIN;\n");
            if (startPos == "0")
              flipFunction.append(
                  m_tab + m_tab + m_tab + "rparents[BUCKET_SIZE]._" + std::to_string((i + 1)) + " = FLT_MAX;\n");
            break;
          default:break;
        }
      }
      //flipFunction.append(m_tab + "}\n");
    } else if (m_numOfInvertible > 0) {
      flipFunction.append(
          m_tab + m_tab + m_tab + "for (int i = " + startPos + " ; i < BUCKET_SIZE; i++) {\n");
      for (unsigned long i = 0; i < m_aggregationTypes->size(); ++i) {
        switch ((*m_aggregationTypes)[i]) {
          case MIN:
          case MAX:throw std::runtime_error("error: this is not supported yet from the code generation");
          case SUM:
          case CNT:
            flipFunction.append(m_tab + m_tab + m_tab + m_tab + "rparents[i+1]._" + std::to_string((i + 1)) + " = " +
                getCombineFunction((*m_aggregationTypes)[i],
                                   "rparents[i]._" + std::to_string((i + 1)),
                                   "leaves[BUCKET_SIZE-i-1]._" + std::to_string((i + 1)) + ";\n"));
            break;
          case AVG:
            flipFunction.append(m_tab + m_tab + m_tab + m_tab + "rparents[i+1]._" + std::to_string((i + 1)) + " = " +
                getCombineFunction(SUM, "rparents[i]._" + std::to_string((i + 1)),
                                   "leaves[BUCKET_SIZE-i-1]._" + std::to_string((i + 1)) + ";\n"));
            flipFunction.append(m_tab + m_tab + m_tab + m_tab + "rparents[i+1]._c" + std::to_string((i + 1)) + " = " +
                getCombineFunction(CNT, "rparents[i]._c" + std::to_string((i + 1)),
                                   "leaves[BUCKET_SIZE-i-1]._c" + std::to_string((i + 1)) + ";\n"));
            break;
          default:break;
        }
      }
      flipFunction.append(m_tab + m_tab + m_tab + "}\n");
    }
    return flipFunction;
  }

  std::string getCodeForMicroBenchmarks(std::string &insertFunction,
                                        std::string &queryFunction,
                                        std::string &queryMultFunction,
                                        std::string &queryIntermFunction,
                                        std::string &initialiseFunction,
                                        bool measureLatency = false) {
    std::string parentsLine = m_tab + "static int lparents[LEFT_PARENTS_SIZE];\n" +
        m_tab + "static int rparents[RIGHT_PARENTS_SIZE];\n";
    std::string leavesLine = "static int leaves[" + std::to_string(m_numOfLeaves) + "];";
    std::string s;

    auto numOfParents = (m_root->m_numOfRightParents > 0) ? m_root->m_numOfRightParents - 1 : 0;
    numOfParents += (m_root->m_numOfLeftParents > 0) ? m_root->m_numOfLeftParents - 1 : 0;

    long bucketSize = m_numOfLeaves; //windowDefinition->numberOfPanes();

    if (!queryMultFunction.empty())
      queryFunction = queryMultFunction;

    s.append("#include <stdlib.h>\n"
             "#include <climits>\n"
             //"#include <cfloat>\n"
             "#include <stdexcept>\n");
    if (measureLatency) {
      s.append("#include <chrono>\n"
               "struct InputSchema; \n"
               "void runLatency(InputSchema *input, int start, int end, int *output, double *latencies);\n");
    }
    s.append(
        "#define PARENTS_SIZE     " + std::to_string(numOfParents) + "L\n" +
            "#define LEFT_PARENTS_SIZE     " + std::to_string(m_root->m_numOfLeftParents) + "L\n" +
            "#define RIGHT_PARENTS_SIZE     " + std::to_string(m_root->m_numOfRightParents) + "L\n" +
            "#define BUCKET_SIZE      " + std::to_string(bucketSize) + "L\n" +
            "extern \"C\" {\n" +
            m_tab + "struct alignas(64) InputSchema {\n" +
            m_tab + "    long timestamp;\n" +
            m_tab + "    long messageIndex;\n" +
            m_tab + "    int value;          //Electrical Power Main Phase 1\n" +
            m_tab + "    int mf02;           //Electrical Power Main Phase 2\n" +
            m_tab + "    int mf03;           //Electrical Power Main Phase 3\n" +
            m_tab + "    int pc13;           //Anode Current Drop Detection Cell 1\n" +
            m_tab + "    int pc14;           //Anode Current Drop Detection Cell 2\n" +
            m_tab + "    int pc15;           //Anode Current Drop Detection Cell 3\n" +
            m_tab + "    unsigned int pc25;  //Anode Voltage Drop Detection Cell 1\n" +
            m_tab + "    unsigned int pc26;  //Anode Voltage Drop Detection Cell 2\n" +
            m_tab + "    unsigned int pc27;  //Anode Voltage Drop Detection Cell 3\n" +
            m_tab + "    unsigned int res;\n" +
            m_tab + "    int bm05 = 0;\n" +
            m_tab + "    int bm06 = 0;\n" +
            m_tab + "};\n" +
            "\n" +
            m_tab + "static int curPos;\n" +
            m_tab + "static unsigned int addedElements;\n" +
            m_tab + "static unsigned int removedElements;\n" +
            m_tab + "static int flipCounter = 0;\n" +
            m_tab + "static int flipBarrier = " + std::to_string(m_flipRange) + ";\n" +
            parentsLine +
            m_tab + leavesLine + "\n" +
            "" + initialiseFunction + "\n" +
            "" + insertFunction + "\n" +
            "" + queryFunction + "\n" +
            //"" + queryMultFunction + "\n" +
            "" + queryIntermFunction + "\n" +
            //"};\n" +
            "\n"
    );

    std::string multipleWindowsLines;
    if (m_numberOfWindows > 1) {
      multipleWindowsLines = m_tab + "static int numOfWindows = " + std::to_string(m_numberOfWindows) + ";\n" +
          m_tab + "static int *windowSizes;\n" +
          m_tab + "void setWindowSizes (int *winSizes) { windowSizes = winSizes;} \n";
    }
    std::string driverFunction =
        m_tab + "void run("/* + boost::typeindex::type_id<In>().pretty_name() + */"int *input, int start, int end, ";
    std::string driverLatencyFunction =
        m_tab + "void runLatency(InputSchema *input, int start, int end, ";
    if (measureLatency) {
      driverFunction += "int *output, double *latencies) {\n" +
          m_tab + m_tab + "runLatency(input, start, end, output, latencies); \n" +
          m_tab + "}\n";
      driverLatencyFunction += "int *output, double *latencies) {\n" +
          m_tab + m_tab + "int outIdx = 0; \n" +
          m_tab + m_tab + "auto startT = std::chrono::high_resolution_clock::now(); \n" +
          m_tab + m_tab + "auto endT = std::chrono::high_resolution_clock::now(); \n" +
          m_tab + m_tab
          + "auto timeTaken = (endT - startT).count() * ((double) std::chrono::high_resolution_clock::period::num / std::chrono::high_resolution_clock::period::den); \n"
          +
              m_tab + m_tab + "for (int i = 0; i < BUCKET_SIZE; ++i) { \n" +
          m_tab + m_tab + m_tab + "insert(input[i]); \n" +
          m_tab + m_tab + m_tab + "startT = std::chrono::high_resolution_clock::now(); \n" +
          m_tab + m_tab + "}\n" +
          m_tab + m_tab + "output[0] = query();\n" +
          m_tab + m_tab + "endT = std::chrono::high_resolution_clock::now();\n" +
          m_tab + m_tab + "timeTaken = std::chrono::duration_cast<std::chrono::nanoseconds>(endT-startT).count();\n" +
          m_tab + m_tab + "latencies[0] = timeTaken;\n" +
          m_tab + m_tab + "for (int i = BUCKET_SIZE; i < end; ++i) { \n" +
          m_tab + m_tab + m_tab + "startT = std::chrono::high_resolution_clock::now(); \n" +
          m_tab + m_tab + m_tab + "insert(input[i]); \n" +
          m_tab + m_tab + m_tab + "output[outIdx++] = query(); \n" +
          m_tab + m_tab + m_tab + "endT = std::chrono::high_resolution_clock::now();\n" +
          m_tab + m_tab + m_tab
          + "timeTaken = std::chrono::duration_cast<std::chrono::nanoseconds>(endT-startT).count();\n" +
          m_tab + m_tab + m_tab + "latencies[i-BUCKET_SIZE+1] = timeTaken;\n" +
          m_tab + m_tab + "}\n" +
          m_tab + "}\n";
    } else if (m_numberOfWindows == 1) {
      driverFunction += "int *output) {\n" +
          m_tab + m_tab + "int outIdx = 0; \n" +
          m_tab + m_tab + "for (int i = 0; i < end; ++i) { \n" +
          m_tab + m_tab + m_tab + "insert(input[i]); \n" +
          m_tab + m_tab + m_tab + "if (i >= BUCKET_SIZE) \n" +
          m_tab + m_tab + m_tab + m_tab + "output[outIdx++] = query()._1; \n" +
          m_tab + m_tab + "}\n" +
          m_tab + "}\n";
    } else {
      driverFunction += "int *output) {\n" +
          m_tab + m_tab + "for (int i = 0; i < end; ++i) { \n" +
          m_tab + m_tab + m_tab + "insert(input[i]); \n" +
          m_tab + m_tab + m_tab + "if (i >= BUCKET_SIZE) { \n" +
          m_tab + m_tab + m_tab + m_tab + "for (int w = 0; w < numOfWindows; ++w) \n" +
          m_tab + m_tab + m_tab + m_tab + m_tab + "output[w] = query(windowSizes[w]); \n" +
          m_tab + m_tab + m_tab + "} else { \n" +
          m_tab + m_tab + m_tab + m_tab + "for (int w = 0; w < numOfWindows; ++w) \n" +
          m_tab + m_tab + m_tab + m_tab + "if (i>=windowSizes[w]) \n" +
          m_tab + m_tab + m_tab + m_tab + m_tab + "output[w] = query(windowSizes[w]); \n" +
          m_tab + m_tab + m_tab + "} \n" +
          m_tab + m_tab + "}\n" +
          m_tab + "}\n";
    }

    s.append(multipleWindowsLines +
        driverFunction +
        "};\n");

    if (measureLatency) {
      s.append(driverLatencyFunction);
    }

    s = std::regex_replace(s, std::regex("node"), "int");
    s = std::regex_replace(s, std::regex("._1"), "");
    s = std::regex_replace(s, std::regex(".reset\\(\\);"), " = 0;");
    s = std::regex_replace(s, std::regex("0.0f"), "0");
    s = std::regex_replace(s, std::regex("FLT_MAX"), "INT_MAX");
    s = std::regex_replace(s, std::regex("FLT_MIN"), "INT_MIN");
    s = std::regex_replace(s, std::regex("float"), "int");
    s = std::regex_replace(s, std::regex("int \\*input"), "InputSchema *input");
    s = std::regex_replace(s, std::regex("input\\[i\\]"), "input[i].value");

    return s;
  }

  std::string getCodeTemplate(std::string &insertFunction,
                              std::string &queryFunction,
                              std::string &queryMultFunction,
                              std::string &queryIntermFunction,
                              std::string &initialiseFunction,
                              bool codeForMicroBenchs = false,
                              bool measureLatency = false) {
    std::string parentsLine = m_tab + "node lparents[LEFT_PARENTS_SIZE];\n" +
        m_tab + "node rparents[RIGHT_PARENTS_SIZE];\n";
    std::string leavesLine = "node leaves[" + std::to_string(m_numOfLeaves) + "];";
    std::string s;

    auto numOfParents = (m_root->m_numOfRightParents > 0) ? m_root->m_numOfRightParents - 1 : 0;
    numOfParents += (m_root->m_numOfLeftParents > 0) ? m_root->m_numOfLeftParents - 1 : 0;

    long bucketSize = m_numOfLeaves; //windowDefinition->numberOfPanes();

    if (codeForMicroBenchs) {
      s.append(getCodeForMicroBenchmarks(insertFunction,
                                         queryFunction,
                                         queryMultFunction,
                                         queryIntermFunction,
                                         initialiseFunction,
                                         measureLatency));
    } else {
      s.append(
          "#define PARENTS_SIZE     " + std::to_string(numOfParents) + "L\n" +
              "#define LEFT_PARENTS_SIZE     " + std::to_string(m_root->m_numOfLeftParents) + "L\n" +
              "#define RIGHT_PARENTS_SIZE     " + std::to_string(m_root->m_numOfRightParents) + "L\n" +
              "#define BUCKET_SIZE      " + std::to_string(bucketSize) + "L\n" +
              "struct Aggregator {\n" +
              m_tab + "int curPos;\n" +
              m_tab + "unsigned int addedElements;\n" +
              m_tab + "unsigned int removedElements;\n" +
              m_tab + "int flipCounter = 0;\n" +
              m_tab + "int flipBarrier = " + std::to_string(m_flipRange) + ";\n" +
              parentsLine +
              m_tab + leavesLine + "\n" +
              "" + initialiseFunction + "\n" +
              "" + insertFunction + "\n" +
              "" + queryFunction + "\n" +
              "" + queryMultFunction + "\n" +
              "" + queryIntermFunction + "\n" +
              "};\n" +
              "\n"
      );
    }
    return s;
  }
};