#include "monitors/CompressionMonitor.h"

#include <fcntl.h>

#include "buffers/QueryBuffer.h"
#include "compression/CompressionCodeGenUtils.h"
#include "compression/CompressionStatistics.h"
#include "cql/expressions/ColumnReference.h"
#include "cql/operators/OperatorCode.h"
#include "utils/Query.h"
#include "utils/QueryApplication.h"
#include "utils/QueryOperator.h"
#include "utils/SystemConf.h"

CompressionMonitor::CompressionMonitor(QueryApplication *application) : m_application(application),
                                                                        m_size(application->getQueries().size()),
                                                                        m_codeGenPos(m_size, 0) {
  if (!m_application) {
    throw std::runtime_error("error: the application is not set");
  }
  // assume queries are pre-sorted based on their id
  for (int idx = 0; idx < m_size; ++idx) {
    if (m_application->getQueries()[idx]->getSecondSchema()) {
      throw std::runtime_error(
          "error: enabling adaptive compression to queries with more than one "
          "input streams is not supported");
    }
    // generate instrumentation code for all queries
    auto query = m_application->getQueries()[idx].get();
    m_threads.emplace_back(std::thread([&, query] {
      generateInstrumentation(query);
    }));
  }
}

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wmissing-noreturn"
void CompressionMonitor::operator()() {
  for (int idx = 0; idx < m_size; ++idx) {
    m_threads[idx].join();
  }
  m_threads.clear();
  while (true) {
    try {
      std::this_thread::sleep_for(std::chrono::milliseconds(SystemConf::getInstance().COMPRESSION_MONITOR_INTERVAL));
    } catch (std::exception &e) {
      std::cout << e.what() << std::endl;
    }
    auto t2 = std::chrono::system_clock::now();
    m_time = t2.time_since_epoch() / std::chrono::milliseconds(1); // milliseconds
    m_dt = m_time - m__time;

    // iterate over the statistics of all queries
    for (int idx = 0; idx < m_size; ++idx) {
      // streams
      auto query = m_application->getQueries()[idx].get();
      auto buffer = query->getBuffer();
      if (buffer->hasCompressionPolicyChanged()) {
        std::cout << "[CMON] Q" + std::to_string(query->getId()) + " " + std::to_string(m_time) +
                         " Generating a new compression scheme" << std::endl;
        generateCode(query);
      }

      // state
    }

    m__time = m_time;
  }
}
#pragma clang diagnostic pop

void CompressionMonitor::generateInstrumentation(Query *query) {
  // generate code
  std::string code;
  auto opcode = &query->getOperator()->getCode();
  //if (!opcode) {
  //  throw std::runtime_error("error: wrong operator type");
  //}
  // get headers
  code.append(getIncludesString());
  // get num of workers and cols to generate static vars
  auto cols = opcode->getInputCols();
  code.append(getInstrumentationMetrics(SystemConf::getInstance().WORKER_THREADS, cols->size()));
  // get input schema
  code.append(opcode->getInputSchemaString());
  // generate function header with initializations
  code.append(
      "extern \"C\" {\n"
      "void instrument(int pid, char *inputBuffer, int batchSize, uint32_t *&dv, double *&cv, double *&mn, double *&mx, double *&md) {\n"
      "\tif (batchSize < 0)\n"
      "\t\treturn;\n"
      "\n"
      "\t// Input Buffer\n"
      "    input_tuple_t *data= (input_tuple_t *) inputBuffer;\n"
      "\n"
      "    //Output Buffers\n"
      "    for (int i = 0; i < numOfCols; ++i) {\n"
      "    \tcVals[pid][i] = 0;\n"
      "    \tmin[pid][i] = DBL_MAX;\n"
      "    \tmax[pid][i] = DBL_MIN;\n"
      "    \tmaxDiff[pid][i] = DBL_MIN;\n"
      "    \ttemp[pid][i] = DBL_MIN;\n"
      "    }\n"
      "\n"
      "    int tupleSize = sizeof(input_tuple_t);\n"
      "    int endPtr = batchSize / tupleSize;\n"
      "    int tupleCounter = 0;\n"
      "\n"
      "    for (int i = 0; i < endPtr; ++i) {\n");
  // generate filter and projections
  if (opcode->hasSelection()) {
    auto sel = opcode->getSelectionExpr();
    std::string str = "bufferPtr";
    sel.replace(sel.find(str), str.length(), "i");
    code.append(sel + "{\n");
  }
  // generate per column stats
  int idx = 0;
  for (auto col: *cols) {
    std::string scol = "data[i].";
    std::string colPos = std::to_string(idx++);
    if (col->getColumn() == 0) {
      scol += "timestamp";
    } else if (col->getColumn() == -1) {
      scol = col->getExpression();
      std::string toReplace("bufferPtr");
      size_t pos = scol.find(toReplace);
      scol.replace(pos, toReplace.length(), "i");
    } else {
      scol += "_" + std::to_string(col->getColumn());
    }
    code.append(
        "    \t// check each column\n"
        "\t\tif (temp[pid][" + colPos + "] != " + scol + ") {\n"
        "      \t\tcVals[pid][" + colPos + "]++;\n"
        "    \t}\n"
        "    \tmin[pid][" + colPos + "] = min[pid][" + colPos + "] < " + scol + " ? min[pid][" + colPos + "] : " + scol + ";\n"
        "    \tmax[pid][" + colPos + "] = max[pid][" + colPos + "] > " + scol + " ? max[pid][" + colPos + "] : " + scol + ";\n"
        "    \tif (i != 0) {\n"
        "    \t\tmaxDiff[pid][" + colPos + "] = maxDiff[pid][" + colPos + "] > abs(" + scol + " - min[pid][" + colPos + "]) ? maxDiff[pid][" + colPos + "] : abs(" + scol + " - min[pid][" + colPos + "]);\n"
        "    \t}\n"
        "    \ttemp[pid][" + colPos + "] = " + scol + " ;\n");
  }
  code.append("    \ttupleCounter++;\n");

  // finish loop and finalize results
  if (opcode->hasSelection()) {
    code.append("}\n");
  }
  code.append(
      "    }\n"
      "\n"
      "    for (int i = 0; i < numOfCols; ++i) {\n"
      "    \tcVals[pid][i] = (double)tupleCounter/cVals[pid][i];\n"
      "    }\n"
      "\n"
      "\t// set output pointers\n"
      "    cv = cVals[pid];\n"
      "    mn = min[pid];\n"
      "    mx = max[pid];\n"
      "    md = maxDiff[pid];   \n"
      "}\n"
      "}");

  auto id = query->getId();
  // create file on disk
  //auto path = Utils::getCurrentWorkingDir();
  auto path = SystemConf::getInstance().FILE_ROOT_PATH + "/scabbard";
  std::ofstream out(path + "/InstrumentationCode_" + std::to_string(id) + ".cpp");
  out << code;
  out.close();
  // generate library
  std::string generatedPath = path + "/InstrumentationCode_" + std::to_string(id) + ".cpp";
  std::string libPath = path + "/InstrumentationCode_" + std::to_string(id) + ".so";
  std::string command = "clang -shared -fPIC -O3 -march=native -g -o " + libPath + " " + generatedPath;
  system(command.c_str());
  // load library
  m_dLoader.addLibrary(libPath);
  //auto dl = std::make_unique<Utils::DynamicLoader>(libPath.c_str());

  // load function
  auto fp = m_dLoader.load<void(int, char *, int, uint32_t *&, double *&, double *&,
                            double *&, double *&)>(libPath, "instrument");
  // enable instrumentation
  query->getBuffer()->enableInstrumentation(fp);
}

void CompressionMonitor::generateCode(Query *query) {
  // todo: skip compression if code exists

  // generate code
  std::string code;
  auto opcode = &query->getOperator()->getCode();
  // get headers and compression algorithms
  code.append(getIncludesString());
  code.append(getCompressionAlgorithms());
  // get num of workers and cols to generate static vars
  auto cols = opcode->getInputCols();
  auto hashset = opcode->getHashTableExpr();
  auto compStats = query->getBuffer()->getCompressionStatistics();
  auto compCols = compStats->m_compCols;
  bool hasDict = false;
  for (auto &col: compCols) {
    for(auto &comp: col->m_comps) {
      if (comp == CompressionType::Dictionary) {
        hasDict = true;
      }
    }
  }
  if (hasDict) {
    if (!hashset.empty()) {
      code.append(hashset);
    }
    code.append(getCompressionVars(true, SystemConf::getInstance().WORKER_THREADS, cols->size()));
  } else {
    code.append(getCompressionVars(false, SystemConf::getInstance().WORKER_THREADS, cols->size()));
  }
  // get input schema
  code.append(opcode->getInputSchemaString());
  // generate function header with initializations
  code.append(
      "inline void compressInput(int pid, char *input, int start, int end, char *output, int &writePos, int length, bool &clear, long latency) {\n"
      "\tif (start == 0 && end == -1) {\n");
  if (!hashset.empty()) {
    // todo: reduce hashtable size here
    code.append("    \t// write metadata\n");
  }
  code.append(
      "    \treturn;\n"
      "  \t}\n");

  // initialize variables
  size_t idx = 0;
  code.append("  \tif (isFirst[pid]) {\n");
  for (auto &col: compCols) {
    // do we need to initialize metadata here?
    if (col->m_comps.find(CompressionType::Dictionary) != col->m_comps.end()) {
      auto htSize = opcode->hasStaticHashJoin() ? Utils::getPowerOfTwo(SystemConf::getInstance().CAMPAIGNS_NUM * 10) : SystemConf::getInstance().HASH_TABLE_SIZE;
      code.append(
          "    \tdcomp[pid][" + std::to_string(idx) + "] = std::make_unique<DictionaryCompressor<Key, uint16_t, MyHash, HMEqualTo>>(" + std::to_string(htSize)+ ");\n");
    }
    idx++;
  }
  code.append("    \tisFirst[pid] = false;\n"
      "  \t}\n");
  code.append("  \tif (clear) {\n");
  idx = 0;
  for (auto &col: compCols) {
    if (col->m_comps.find(CompressionType::Dictionary) != col->m_comps.end()) {
      code.append("    \tdcomp[pid][" + std::to_string(idx) + "]->clear();\n");
    }
    idx++;
  }
  code.append(
      "    \tclear = false;\n"
      "  \t}\n"
      "\n"
      "\tif (start == end || end < start) {\n"
      "\t\treturn;\n"
      "\t}\n"
      "\n"
      "\t// Input Buffer\n"
      "    auto data = (input_tuple_t *)input;\n");

  // setup compression: each compression technique get a monotonically increasing _id
  // setup output structs
  idx = 0;
  code.append("\tstd::vector<size_t> idxs (" + std::to_string(cols->size()) + ", 0);\n");
  for (auto &col: compCols) {
    for (const auto& c: col->m_comps) {
      if (c == CompressionType::RLE) {
        continue;
      }
      // todo: fix float compression without a multiplier
      std::string resStruct;
      resStruct.append("\tstruct t_" + std::to_string(idx) + " {\n");
      std::string resType;
      uint32_t precision = 0;
      if (c == CompressionType::BaseDelta) {
        resType = getRoundedType(col->m_diffPrecision);
        resStruct.append("\t\t" + resType + " _" + std::to_string(idx) + " : ");
        resStruct.append(std::to_string(col->m_diffPrecision) + ";\n");
        precision = col->m_diffPrecision;
      } else if (c == CompressionType::Dictionary) {
        resType = getRoundedType(16);
        resStruct.append("\t\t" + resType + " _" + std::to_string(idx) + " : ");
        resStruct.append(std::to_string(16) + ";\n");
        precision = 16;
      } else {
        resType = getRoundedType(col->m_precision);
        resStruct.append("\t\t" + resType + " _" + std::to_string(idx) + " : ");
        resStruct.append(std::to_string(col->m_precision) + ";\n");
        precision = col->m_precision;
      }
      if (col->m_comps.find(CompressionType::RLE) != col->m_comps.end()) {
        auto maxCounter = std::max(col->m_RLEPrecision, getRoundedTypeInt(precision)-precision);
        col->m_RLEPrecision = maxCounter;
        resStruct.append("\t\t"+getRoundedType(col->m_RLEPrecision)+" counter : " + std::to_string(col->m_RLEPrecision) + ";\n");
        // create an RLE counter for that column
        code.append("\t"+getRoundedType(col->m_RLEPrecision)+" count_" + std::to_string(idx) + " = 1;\n");
      }

      std::string base;
      if (col->m_column == 0) {
        base = "data[0].timestamp";
      } else if (col->m_column == -1) {
        base = col->m_expression;
        std::string toReplace("bufferPtr");
        size_t pos = base.find(toReplace);
        base.replace(pos, toReplace.length(), "0");
      } else {
        base = "data[0]._" + std::to_string(col->m_column);
      }
      if (c == CompressionType::BaseDelta) {
        code.append("\tBaseDeltaCompressor<" + col->m_typeString + ", " + getRoundedType(col->m_diffPrecision) + "> comp_"+std::to_string(idx)+"("+base+");\n");
        code.append("\tauto temp_" + std::to_string(idx) + " = comp_"+std::to_string(idx)+".compress("+base+");\n");
      } else if (c == CompressionType::FloatMult) {
        code.append("\tFloatMultCompressor<" + getRoundedType(col->m_precision) + "> comp_"+std::to_string(idx)+"("+std::to_string(col->m_multiplier)+");\n");
        code.append("\tauto temp_" + std::to_string(idx) + " = comp_"+std::to_string(idx)+".compress("+base+");\n");
      } else if (c == CompressionType::None) {
        code.append("\tauto temp_" + std::to_string(idx) + " = ("+getRoundedType(col->m_precision)+")"+base+";\n");
      }
      resStruct.append("\t};\n");
      code.append(resStruct);
    }
    idx++;
  }
  // setup output buffers
  idx = 0;
  float mult = (float) 1 / (float) cols->size();
  code.append("\t// output buffers\n");
  code.append("\tint barriers["+std::to_string(cols->size())+"];\n");
  for (auto &col: compCols) {
    auto bar = (float)(idx * mult);
    code.append("\tbarriers["+std::to_string(idx)+"] = (int)(length*"+std::to_string(bar)+");\n");
    code.append("\tt_"+std::to_string(idx)+" *buf"+std::to_string(idx)+" = (t_"+std::to_string(idx)+" *) (output + barriers["+std::to_string(idx)+"]);\n");
    idx++;
  }

  // iterate over data
  code.append("\tsize_t n = (end - start) / sizeof(input_tuple_t);\n\n");
  code.append("\tfor (size_t idx = 0; idx < n; idx++) {\n");
  // generate filter and projections
  if (opcode->hasSelection()) {
    auto sel = opcode->getSelectionExpr();
    std::string str = "bufferPtr";
    sel.replace(sel.find(str), str.length(), "idx");
    code.append("\t\t" + sel + "\t\t{\n");
  }

  // compression code
  idx = 0;
  for (auto &col: compCols) {
    std::string rleS;
    auto idxS = std::to_string(idx);
    for (const auto &c : col->m_comps) {
      // apply compression
      std::string base;
      if (col->m_column == 0) {
        base = "data[idx].timestamp";
      }  else if (col->m_column == -1) {
        base = col->m_expression;
        std::string toReplace("bufferPtr");
        size_t pos = base.find(toReplace);
        base.replace(pos, toReplace.length(), "idx");
      } else {
        base = "data[idx]._" + std::to_string(col->m_column);
      }
      if (c == CompressionType::BaseDelta || c == CompressionType::FloatMult) {
        code.append("        // apply compression\n");
        code.append(
            "\t\tif (comp_" + idxS + ".check("+base+")) {\n"
            "\t\t\tstd::cout << \"warning: falling back to the original compression scheme\"<< std::endl;\n"
            "\t\t\tclear = true;\n"
            "\t\t\treturn;\n"
            "\t\t}\n");
        code.append("\t\tauto res_" + idxS + " = comp_" + idxS + ".compress("+base+");\n");
      } else if (c == CompressionType::Dictionary) {
        code.append("\t\tauto res_" + idxS + " = dcomp[pid][" + idxS + "]->compress("+base+");\n");
      } else if (c == CompressionType::None) {
        std::string resType = getRoundedType(col->m_precision);
        code.append(
            "\t\tif (!CanTypeFitValue<"+resType+","+col->m_typeString+">("+base+")) {\n"
            "\t\t\tstd::cout << \"warning: falling back to the original compression scheme\"<< std::endl;\n"
            "\t\t\tclear = true;\n"
            "\t\t\treturn;\n"
            "\t\t}\n");
        code.append("\t\t"+resType+" res_" + idxS + " = ("+resType+") "+base+";\n");
      }
      if (c == CompressionType::RLE) {
        auto cntCheck = std::pow(2, col->m_RLEPrecision) - 1;
        rleS.append(
            "        // apply RLE\n"
            "        if (temp_" + idxS + " != res_" + idxS + " || count_" + idxS + " >= " + std::to_string(cntCheck) + ") {\n"
            "\t\t  buf" + idxS + "[idxs[" + idxS + "]++] = {temp_" + idxS + ", count_" + idxS + "};\n"
            "          count_" + idxS + " = 0;\n"
            "          temp_" + idxS + " = res_" + idxS + ";\n"
            "\t\t} else {\n"
            "          count_" + idxS + "++;\n"
            "        }\n");
      }
      // deal with no compression
    }
    if (!rleS.empty()) {
      code.append(rleS);
    } else { // if no rle takes place, just store the compressed result
      code.append("\t\tbuf" + idxS + "[idxs[" + idxS + "]++] = {res_" + idxS + "};\n");
    }
    idx++;
  }

  if (opcode->hasSelection()) {
    code.append("\t\t}\n");
  }
  code.append("\t}\n");

  // after loop result finalization
  idx = 0;
  for (auto &col: compCols) {
    for (const auto &c : col->m_comps) {
      auto idxS = std::to_string(idx);
      std::string base;
      if (c == CompressionType::RLE) {
        auto cntCheck = std::pow(2, col->m_RLEPrecision) - 1;
        code.append(
            "    if (count_" + idxS + " != 0) {\n"
            "\t  buf" + idxS + "[idxs[" + idxS + "]++] = {temp_" + idxS + ", count_" + idxS + "};\n"
            "\t}\n");
      }
    }
    idx++;
  }

  // copy results and set output pointers
  idx = 0;
  code.append("\t// copy results and set output pointers\n");
  for (auto &col: compCols) {
    code.append("\twritePos += idxs["+std::to_string(idx)+"] * sizeof(t_"+std::to_string(idx)+");\n");
    if (idx < cols->size() - 1) {
      code.append("\tif (writePos > barriers["+std::to_string(idx+1)+"]) {throw std::runtime_error(\"error: larger barriers needed\");}\n");
      code.append("\tstd::memcpy((void *)(output + writePos), (void *)buf"+std::to_string(idx+1)+", idxs["+std::to_string(idx+1)+"] * sizeof(t_"+std::to_string(idx+1)+"));\n");
    } else {
      code.append("\tif (writePos > length) {throw std::runtime_error(\"error: larger barriers needed\");}\n");
    }
    idx++;
  }

  // write metadata: col comp schema s-idx e-idx
  if (m_writeMetadata) {
    idx = 0;
    code.append("\t//write metadata\n");
    code.append("\twritePos = 0;\n");
    code.append("\tmetadata[pid][0] = \"\";\n");
    for (auto &col : compCols) {
      std::string comps;
      std::string resStruct;
      for (const auto &c : col->m_comps) {
        if (c == CompressionType::RLE) {
          comps += "\"RLE \"";
          continue;
        }
        resStruct.append("{");
        resStruct.append(col->m_typeString + ":");
        if (c == CompressionType::BaseDelta) {
          comps += "\"BD \"+comp_" + std::to_string(idx) + ".getBase()+\" \"";
          resStruct.append(std::to_string(col->m_diffPrecision) + ";");
        } else {
          if (c == CompressionType::FloatMult) {
            comps += "\"FM \"+comp_" + std::to_string(idx) +
                     ".getMultiplier()+\" \"";
          } else if (c == CompressionType::Dictionary) {
            std::string endPtr = "\tauto endPtr = ";
            size_t tmpIdx = 0;
            for (auto &cl : compCols) {
              endPtr += "idxs[" + std::to_string(tmpIdx) + "] * sizeof(t_" +
                        std::to_string(tmpIdx) + ")";
              tmpIdx++;
              if (tmpIdx != compCols.size()) {
                endPtr += "+";
              } else {
                endPtr += ";\n";
              }
            }
            code.append(endPtr);
            code.append("\tauto dcompSize = dcomp[pid][" + std::to_string(idx) +
                        "]->getTable().bucket_size() * dcomp[pid][" +
                        std::to_string(idx) + "]->getTable().max_size();\n");
            comps +=
                "\"D \"+std::to_string(endPtr)+\" \"+std::to_string(endPtr+dcompSize)+\" \"";
            if (!opcode->hasStaticHashJoin()) {
              code.append(
                  "\tstd::memcpy((void *)(output + writePos), (void *)dcomp[pid][" +
                  std::to_string(idx) +
                  "]->getTable().buckets(), dcompSize);\n"
                  "\twritePos += dcompSize;\n"
                  "\tif (writePos > length) {\n"
                  "\t\tthrow std::runtime_error(\"error: larger barriers needed\");\n"
                  "\t}\n");
            }
          } else {
            comps += "\"N \"";
          }
          resStruct.append(std::to_string(col->m_precision) + ";");
        }
        if (col->m_comps.find(CompressionType::RLE) != col->m_comps.end()) {
          resStruct.append("uint16_t:" + std::to_string(col->m_RLEPrecision) +
                           ";");
        }
        resStruct.append("} ");
      }
      code.append("\tmetadata[pid][0] += \"" + std::to_string(col->m_column) +
                  " \"" + comps + "\"" + resStruct + "\" " +
                  "+ std::to_string(writePos) + \" \";\n");
      // if (idx < cols->size() - 1) {
      code.append("\twritePos += idxs[" + std::to_string(idx) +
                  "] * sizeof(t_" + std::to_string(idx) + ");\n");
      code.append("\tmetadata[pid][0] += std::to_string(writePos) + \" \";\n");
      //}
      idx++;
    }
    // copy string
    code.append(
        "\tif (metadata[pid][0].size() > 128) { throw std::runtime_error(\"error: increase the size of metadata\"); }\n");
    code.append(
        "\tstd::memcpy((void *)(output - 128), (void *)metadata[pid][0].data(), metadata[pid][0].size());\n");
  }
  code.append("}\n");
  // generate c wrapped function
  code.append(
      "extern \"C\" {\n"
      "\tvoid compress(int pid, char *input, int start, int end, char *output, int &writePos, int length, bool &clear, long latency = -1) {\n"
      "\t\tcompressInput(pid, input, start, end, output, writePos, length, clear, latency);\n"
      "\t}\n"
      "}");

  auto id = query->getId();
  // create file on disk
  //auto path = Utils::getCurrentWorkingDir();
  auto path = SystemConf::getInstance().FILE_ROOT_PATH + "/scabbard";
  std::string filePath = path + "/" + "CompressionCode_" + std::to_string(id) + "_" + std::to_string(m_codeGenPos[id]);
  std::ofstream out(filePath + ".cpp");
  out << code;
  out.close();
  // generate library
  std::string generatedPath = filePath + ".cpp";
  std::string libPath = filePath + ".so";
  std::string command = "clang -shared -fPIC -O3 -march=native -g -o " + libPath + " " + generatedPath;
  system(command.c_str());
  // load library
  m_dLoader.addLibrary(libPath);

  // load function
  auto fp = m_dLoader.load<void(int, char *, int, int, char *, int &, int, bool &, long)>(libPath, "compress");
  m_codeGenPos[id]++; // increment compression pointer
  // enable the new compression
  query->getBuffer()->setCompressionFP(fp, m_codeGenPos[id]);
  //query->getBuffer()->setDeCompressionFP(fp);
}