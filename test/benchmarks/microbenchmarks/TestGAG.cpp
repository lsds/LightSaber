#include <iostream>
#include <algorithm>
#include <set>
#include <chrono>
#include <random>

#include <boost/date_time/posix_time/ptime.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>

#include "cql/operators/codeGeneration/GeneralAggregationGraph.h"
#include "cql/operators/codeGeneration/OperatorJit.h"
#include "utils/Utils.h"

int maxWindowSize = 4 * 32 * 1024;
int dataItems = 16 * 1024 * 1024;
const int numOfIterations = 5;
double iterRes[numOfIterations];

int *getWindowSizes(std::vector<WindowDefinition> &windows) {
  int *windowSizes;
  windowSizes = (int *) malloc(sizeof(int) * windows.size());
  int i = 0;
  for (auto &window : windows) {
    windowSizes[i++] = window.getSize();
  }
  return windowSizes;
}

double calculateMean(double *data) {
  double sum = 0.0, mean;
  int i;
  for (i = 0; i < numOfIterations; ++i) {
    sum += data[i];
  }
  mean = sum / numOfIterations;
  return mean;
}

double calculateSTD(double *data) {
  double sum = 0.0, mean, standardDeviation = 0.0;
  int i;
  for (i = 0; i < numOfIterations; ++i) {
    sum += data[i];
  }
  mean = sum / numOfIterations;
  for (i = 0; i < numOfIterations; ++i)
    standardDeviation += pow(data[i] - mean, 2);
  return sqrt(standardDeviation / numOfIterations);
}

long normalisedTimestamp = -1;
struct alignas(64) InputSchema {
  long timestamp;
  long messageIndex;
  int value;          //Electrical Power Main Phase 1
  int mf02;           //Electrical Power Main Phase 2
  int mf03;           //Electrical Power Main Phase 3
  int pc13;           //Anode Current Drop Detection Cell 1
  int pc14;           //Anode Current Drop Detection Cell 2
  int pc15;           //Anode Current Drop Detection Cell 3
  unsigned int pc25;  //Anode Voltage Drop Detection Cell 1
  unsigned int pc26;  //Anode Voltage Drop Detection Cell 2
  unsigned int pc27;  //Anode Voltage Drop Detection Cell 3
  unsigned int res;
  int bm05 = 0;
  int bm06 = 0;

  static void parse(InputSchema &tuple,
                    std::string &line,
                    long &normalisedTimestamp,
                    boost::posix_time::ptime &myEpoch) {
    std::istringstream iss(line);
    std::vector<std::string> words{std::istream_iterator<std::string>{iss},
                                   std::istream_iterator<std::string>{}};
    const std::locale
        loc = std::locale(std::locale::classic(), new boost::posix_time::time_input_facet("%Y-%m-%dT%H:%M:%S%f"));
    std::istringstream is(words[0]);
    is.imbue(loc);
    boost::posix_time::ptime myTime;
    is >> myTime;
    boost::posix_time::time_duration myTimeFromEpoch = myTime - myEpoch;

    tuple.timestamp = myTimeFromEpoch.total_milliseconds() / 1000;
    tuple.messageIndex = std::stol(words[1]);
    tuple.value = std::stoi(words[2]);
    tuple.mf02 = std::stoi(words[3]);
    tuple.mf03 = std::stoi(words[4]);
    tuple.pc13 = std::stoi(words[5]);
    tuple.pc14 = std::stoi(words[6]);
    tuple.pc15 = std::stoi(words[7]);
    tuple.pc25 = std::stoi(words[8]);
    tuple.pc26 = std::stoi(words[9]);
    tuple.pc27 = std::stoi(words[10]);
    tuple.res = std::stoi(words[11]);
  }
};
std::vector<char> *data;
void loadData() {
  size_t len = 32 * 1024;
  data = new std::vector<char>(dataItems * sizeof(InputSchema));
  auto buf = (InputSchema *) data->data();

  const std::string cell = "2012-02-22T16:46:28.9670320+00:00";
  const std::locale
      loc = std::locale(std::locale::classic(), new boost::posix_time::time_input_facet("%Y-%m-%dT%H:%M:%S%f"));
  std::istringstream is(cell);
  is.imbue(loc);
  boost::posix_time::ptime myEpoch;
  is >> myEpoch;

  std::string filePath = Utils::getHomeDir() + "/LightSaber/resources/datasets/manufacturing_equipment/";
  std::ifstream file(filePath + "DEBS2012-small.txt");
  std::string line;
  unsigned long idx = 0;
  while (std::getline(file, line) && idx < len) {
    InputSchema::parse(buf[idx], line, normalisedTimestamp, myEpoch);
    idx++;
  }
  size_t temp = 0;
  for (size_t j = len; j < dataItems; ++j) {
    buf[j].value = buf[temp++].value;
    if (temp == len)
      temp = 0;
  }

  if (false) {
    std::cout << "timestamp messageIndex mf01 mf02 mf03 pc13 pc14 pc15 pc25 pc26 pc27 res bm05 bm06 bm07 bm08 bm09 bm10"
              << std::endl;
    for (unsigned long i = 0; i < data->size() / sizeof(InputSchema); ++i) {
      printf("[DBG] %06d: %09d %09d %6d %6d %6d %6d %6d %6d %6d %6d %6d %6d"
             " %1d %1d \n",
             i, buf[i].timestamp, buf[i].messageIndex, buf[i].value,
             buf[i].mf02, buf[i].mf03, buf[i].pc13, buf[i].pc14, buf[i].pc15, buf[i].pc25,
             buf[i].pc26, buf[i].pc27, buf[i].res, buf[i].bm05, buf[i].bm06
          //buf[i].bm07, buf[i].bm08, buf[i].bm09, buf[i].bm10
      );
    }
  }
}

// Don't free the module before it's not needed anymore!
void generateFunctionsSingle(int argc, const char **argv, std::vector<WindowDefinition> &windows,
                             InputSchema *input, int *output, int idx, int numOfIter, bool invertible) {
  (void) idx;
  std::vector<AggregationType> aggrs;
  if (invertible)
    aggrs.push_back(AggregationType::SUM);
  else
    aggrs.push_back(AggregationType::MIN);
  GeneralAggregationGraph gag(&windows.front(), &aggrs);
  std::string path = Utils::getCurrentWorkingDir() + "/GeneratedCode.cpp";
  auto sourceCode = gag.generateCode(false, true);
  std::ofstream out(path);
  out << sourceCode;
  out.close();

  CodeGenWrapper codeGen;
  codeGen.parseAndCodeGen(argc, argv);
  llvm::Expected<std::function<void()>> initFn = codeGen.getFunction<void()>("initialise");
  auto runFn = codeGen.getFunction<void(InputSchema *, int, int, int *)>("run");
  if (!runFn || !initFn) {
    std::cout << "Failed to fetch the pointers." << std::endl;
    exit(1);
  }
  auto init = *initFn;
  auto run = *runFn;

  auto start = std::chrono::high_resolution_clock::now();
  auto end = std::chrono::high_resolution_clock::now();
  auto timeTaken = (end - start).count()
      * ((double) std::chrono::high_resolution_clock::period::num / std::chrono::high_resolution_clock::period::den);

  for (int r = 0; r < numOfIter; ++r) {
    init();
    start = std::chrono::high_resolution_clock::now();

    run(input, 0, dataItems, output);

    end = std::chrono::high_resolution_clock::now();
    timeTaken = (end - start).count()
        * ((double) std::chrono::high_resolution_clock::period::num / std::chrono::high_resolution_clock::period::den);
    //std::cout << (dataItems/timeTaken) << " ";
    //std::cout << output[std::rand()%idx] << std::endl;
    iterRes[r] = (dataItems / timeTaken);
  }
  std::cout << calculateMean(iterRes) << " " << calculateSTD(iterRes) << std::endl;
}

void generateFunctionsMultiple(int argc, const char **argv, std::vector<WindowDefinition> &windows,
                               InputSchema *input,
                               int *output, //int**output,
                               int idx, int numOfIter, int numOfQueries, bool invertible) {
  (void) idx;
  (void) numOfQueries;
  std::vector<AggregationType> aggrs;
  std::vector<std::vector<AggregationType>> mulAggrs;
  for (auto &w : windows) {
    if (invertible)
      mulAggrs.push_back({AggregationType::SUM});
    else
      mulAggrs.push_back({AggregationType::MIN});
  }
  GeneralAggregationGraph gag(&windows, &mulAggrs);
  std::string path = Utils::getCurrentWorkingDir() + "/GeneratedCode.cpp";
  auto sourceCode = gag.generateCode(false, true);
  std::ofstream out(path);
  out << sourceCode;
  out.close();

  CodeGenWrapper codeGen;
  codeGen.parseAndCodeGen(argc, argv);
  llvm::Expected<std::function<void()>> initFn = codeGen.getFunction<void()>("initialise");
  llvm::Expected<std::function<void(int *)>> windowsFn = codeGen.getFunction<void(int *)>("setWindowSizes");
  auto runFn = codeGen.getFunction<void(InputSchema *, int, int, int *)>("run");
  if (!runFn || !initFn || !windowsFn) {
    std::cout << "Failed to fetch the pointers." << std::endl;
    exit(1);
  }
  auto init = *initFn;
  auto setWindows = *windowsFn;
  auto run = *runFn;
  int *windowSizes = getWindowSizes(windows);

  auto start = std::chrono::high_resolution_clock::now();
  auto end = std::chrono::high_resolution_clock::now();
  auto timeTaken = (end - start).count()
      * ((double) std::chrono::high_resolution_clock::period::num / std::chrono::high_resolution_clock::period::den);
  std::srand(0);
  for (int r = 0; r < numOfIter; ++r) {
    init();
    setWindows(windowSizes);
    start = std::chrono::high_resolution_clock::now();

    run(input, 0, dataItems, output);

    end = std::chrono::high_resolution_clock::now();
    timeTaken = (end - start).count()
        * ((double) std::chrono::high_resolution_clock::period::num / std::chrono::high_resolution_clock::period::den);
    //std::cout << (dataItems/timeTaken) << " ";
    //std::cout << output[std::rand()%numOfQueries] << std::endl; //std::cout << output[std::rand()%numOfQueries][std::rand()%idx] << std::endl;
    iterRes[r] = (dataItems / timeTaken);
  }
  std::cout << calculateMean(iterRes) << " " << calculateSTD(iterRes) << std::endl;
}

void singleInvFunctions(int argc, const char **argv, int windowSize) {

  auto input = (InputSchema *) data->data();
  int *output = (int *) malloc(dataItems * sizeof(int));

  auto start = std::chrono::high_resolution_clock::now();
  auto end = std::chrono::high_resolution_clock::now();
  auto timeTaken = (end - start).count()
      * ((double) std::chrono::high_resolution_clock::period::num / std::chrono::high_resolution_clock::period::den);

  std::vector<int> windowSizes; //{32, 256, 1024, 16*1024, 64*1024, 512*1024};
  windowSizes.push_back(windowSize);
  int idx = 1;
  std::cout << "# Single Invertible Function: " + std::to_string(windowSize) << std::endl;
  for (auto ws : windowSizes) {

    std::cout << "# Generated - Throughput (tuples/sec):" << std::endl;
    std::vector<WindowDefinition> windows;
    windows.emplace_back(WindowDefinition(RANGE_BASED, ws, 1));
    generateFunctionsSingle(argc, argv, windows, input, output, idx, numOfIterations, true);
  }

  free(input);
  free(output);
}

void singleNonInvFunctions(int argc, const char **argv, int windowSize) {

  auto input = (InputSchema *) data->data();
  int *output = (int *) malloc(dataItems * sizeof(int));

  auto start = std::chrono::high_resolution_clock::now();
  auto end = std::chrono::high_resolution_clock::now();
  auto timeTaken = (end - start).count()
      * ((double) std::chrono::high_resolution_clock::period::num / std::chrono::high_resolution_clock::period::den);

  std::vector<int> windowSizes; // {32, 256, 1024, 16*1024, 64*1024, 512*1024};
  windowSizes.push_back(windowSize);
  int idx = 1;
  std::cout << "# Single Non-Invertible Function: " + std::to_string(windowSize) << std::endl;
  for (auto ws : windowSizes) {

    std::cout << "# Generated - Throughput (tuples/sec):" << std::endl;
    std::vector<WindowDefinition> windows;
    windows.emplace_back(WindowDefinition(RANGE_BASED, ws, 1));
    generateFunctionsSingle(argc, argv, windows, input, output, idx, numOfIterations, false);
  }

  free(input);
  free(output);
}

void multInvFunctions(int argc, const char **argv, int numOfQ) {

  auto input = (InputSchema *) data->data();
  auto start = std::chrono::high_resolution_clock::now();
  auto end = std::chrono::high_resolution_clock::now();
  auto timeTaken = (end - start).count()
      * ((double) std::chrono::high_resolution_clock::period::num / std::chrono::high_resolution_clock::period::den);

  std::vector<int> numOfQueries; // {1, 10, 50, 100, 500, 1000};
  numOfQueries.push_back(numOfQ);
  //int maxWindowSize = 1024;
  int outIdx;
  std::cout << "# Multiple Invertible Function (1K): " + std::to_string(numOfQ) << std::endl;
  for (auto num : numOfQueries) {

    int *output = (int *) malloc(num * sizeof(int));
    int tempSize = maxWindowSize;

    std::cout << "# Generated - Throughput (tuples/sec):" << std::endl;
    std::vector<WindowDefinition> windows;
    const int range_from = 1;
    const int range_to = maxWindowSize;
    std::random_device rand_dev;
    std::mt19937 generator(rand_dev());
    std::uniform_int_distribution<int> distr(range_from, range_to);
    for (int i = 0; i < num; i++) {
      tempSize = distr(generator);
      if (tempSize == 0)
        tempSize = 1;
      windows.emplace_back(WindowDefinition(RANGE_BASED, tempSize, 1));
    }
    int *output2 = (int *) malloc(num * sizeof(int));
    generateFunctionsMultiple(argc, argv, windows, input, output2, outIdx, numOfIterations, num, true);
  }

  free(input);
}

void multNonInvFunctions(int argc, const char **argv, int numOfQ) {

  auto input = (InputSchema *) data->data();
  auto start = std::chrono::high_resolution_clock::now();
  auto end = std::chrono::high_resolution_clock::now();
  auto timeTaken = (end - start).count()
      * ((double) std::chrono::high_resolution_clock::period::num / std::chrono::high_resolution_clock::period::den);

  std::vector<int> numOfQueries; // {1, 10, 50, 100, 500, 1000};
  numOfQueries.push_back(numOfQ);
  //int maxWindowSize = 1024;
  int outIdx;
  std::cout << "# Multiple Non-Invertible Function (1K): " + std::to_string(numOfQ) << std::endl;
  for (auto num : numOfQueries) {

    int *output = (int *) malloc(num * sizeof(int));
    int tempSize = maxWindowSize;
    std::vector<WindowDefinition> windows;
    const int range_from = 1;
    const int range_to = maxWindowSize;
    std::random_device rand_dev;
    std::mt19937 generator(rand_dev());
    std::uniform_int_distribution<int> distr(range_from, range_to);
    for (int i = 0; i < num; i++) {
      tempSize = distr(generator);
      if (tempSize == 0)
        tempSize = 1;
      windows.emplace_back(WindowDefinition(RANGE_BASED, tempSize, 1));
    }
    int *output2 = (int *) malloc(num * sizeof(int));
    generateFunctionsMultiple(argc, argv, windows, input, output2, outIdx, numOfIterations, num, false);
  }

  free(input);
}

int main(int argc, const char **argv) {

  Utils::bindProcess(3);

  // give the path of the file...
  //argc++;
  std::string path = Utils::getCurrentWorkingDir() + "/GeneratedCode.cpp";
  argv[1] = path.c_str();
  int type = 3;
  int num = 5;
  if (argc == 3) {
    type = std::stoi(argv[2]);
  } else if (argc == 4) {
    type = std::stoi(argv[2]);
    num = std::stoi(argv[3]);
  }

  loadData();

  argc = 2;
  if (type == 2 && num == 1) {
    type = 0;
    num = maxWindowSize;
  }
  if (type == 3 && num == 1) {
    type = 1;
    num = maxWindowSize;
  }

  if (type == 0)
    singleInvFunctions(argc, argv, num);
  else if (type == 1)
    singleNonInvFunctions(argc, argv, num);
  else if (type == 2)
    multInvFunctions(argc, argv, num);
  else if (type == 3)
    multNonInvFunctions(argc, argv, num);

  return 0;
}