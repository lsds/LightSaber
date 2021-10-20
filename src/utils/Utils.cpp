#include "Utils.h"

#include <pthread.h>
#include <pwd.h>
#include <sys/types.h>
#include <unistd.h>

#include <boost/fiber/numa/pin_thread.hpp>
#include <boost/fiber/numa/topology.hpp>
#include <experimental/filesystem>
#include <iostream>

void Utils::bindProcess(int core_id) {
  if (core_id >= (int) std::thread::hardware_concurrency()) {
    std::cout << "warning: the core id exceeds the number of cores" << std::endl;
    core_id = core_id % (int) std::thread::hardware_concurrency();
  }
  //core_id = core_id + 1;
  pthread_t pid = pthread_self();
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(core_id, &cpuset);
  const int set_result = pthread_setaffinity_np(pid, sizeof(cpu_set_t), &cpuset);
  if (set_result != 0)
    print_error_then_terminate(set_result, "sched_setaffinity");
  if (CPU_ISSET(core_id, &cpuset))
    fprintf(stdout, "Successfully set thread %lu to affinity to CPU %d\n", pid, core_id);
  else
    fprintf(stderr, "Failed to set thread %lu to affinity to CPU %d\n", pid, core_id);
}

void Utils::bindProcess(std::thread &thread, int id) {
  if (id >= (int) std::thread::hardware_concurrency()) {
    std::cout << "warning: the core id exceeds the number of cores" << std::endl;
    id = id % (int) std::thread::hardware_concurrency();
  }
  auto pid = thread.native_handle();
  /* Pin worker to thread */
  /*int min = 1; // +1 dispatcher
  int max = SystemConf::getInstance().THREADS;
  int total = max - min + 1;

  int core_id = ((id - (min - 1)) % total) + min;*/
  int core_id = id; // + 1;
  std::cout << "[DBG] bind worker " + std::to_string(id) + " to core " + std::to_string(core_id) << std::endl;

  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(core_id, &cpuset);
  const int set_result = pthread_setaffinity_np(pid, sizeof(cpu_set_t), &cpuset);
  if (set_result != 0)
    print_error_then_terminate(set_result, "sched_setaffinity");
  if (CPU_ISSET(core_id, &cpuset))
    fprintf(stdout, "Successfully set thread %lu to affinity to CPU %d\n", pid, core_id);
  else
    fprintf(stderr, "Failed to set thread %lu to affinity to CPU %d\n", pid, core_id);
}

long Utils::pack(long systemTimestamp, long tupleTimestamp) {
  return (systemTimestamp << 32) | tupleTimestamp;
}

int Utils::getSystemTimestamp(long value) {
  return (int) (value >> 32);
}

int Utils::getTupleTimestamp(long value) {
  return (int) value;
}

int Utils::getPowerOfTwo(int value) {
  bool powerOfTwo = value != 0 && !(value & (value - 1));
  if (!powerOfTwo) {
    int temp = 0;
    auto num = (double) value;
    while (num > 1) {
      num = num / 2;
      temp++;
    }
    value = (int) std::pow(2, temp);
  }
  return value;
}

bool Utils::_is_pointer_aligned(const void *p, int alignment) {
  return ((((uintptr_t) p) & (alignment - 1)) == 0);
}

bool Utils::_is_length_aligned(int length, int alignment) {
  return ((length & (alignment - 1)) == 0);
}

std::string Utils::getCurrentWorkingDir() {
  char buff[FILENAME_MAX];
  GetCurrentDir(buff, FILENAME_MAX);
  std::string current_working_dir(buff);
  return current_working_dir;
}

std::string Utils::getHomeDir() {
  struct passwd *pw = getpwuid(getuid());
  const char *homedir = pw->pw_dir;
  return std::string(homedir);
}

void Utils::process_mem_usage(double &vm_usage, double &resident_set) {
  using std::ios_base;
  using std::ifstream;
  using std::string;

  vm_usage = 0.0;
  resident_set = 0.0;

  // 'file' stat seems to give the most reliable results
  ifstream stat_stream("/proc/self/stat", ios_base::in);

  // dummy vars for leading entries in stat that we don't care about
  string pid, comm, state, ppid, pgrp, session, tty_nr;
  string tpgid, flags, minflt, cminflt, majflt, cmajflt;
  string utime, stime, cutime, cstime, priority, nice;
  string O, itrealvalue, starttime;

  // the two fields we want
  unsigned long vsize;
  long rss;

  stat_stream >> pid >> comm >> state >> ppid >> pgrp >> session >> tty_nr
              >> tpgid >> flags >> minflt >> cminflt >> majflt >> cmajflt
              >> utime >> stime >> cutime >> cstime >> priority >> nice
              >> O >> itrealvalue >> starttime >> vsize >> rss; // don't care about the rest

  stat_stream.close();

  long page_size_kb = sysconf(_SC_PAGE_SIZE) / 1024; // in case x86-64 is configured to use 2MB pages
  vm_usage = vsize / 1024.0;
  resident_set = rss * page_size_kb;
}

std::string Utils::getStdoutFromCommand(std::string cmd) {
  std::string data;
  FILE *stream;
  const int max_buffer = 1024;
  char buffer[max_buffer];
  cmd.append(" 2>&1");

  stream = popen(cmd.c_str(), "r");
  if (stream) {
    while (!feof(stream))
      if (fgets(buffer, max_buffer, stream) != nullptr) data.append(buffer);
    pclose(stream);
  }
  return data;
}

int Utils::getNumberOfSockets () {
    auto topo = boost::fibers::numa::topology();
    auto sockets = topo.size();
    return sockets;
}

int Utils::getNumberOfCoresPerSocket () {
    auto topo = boost::fibers::numa::topology();
    int coresPerSocket = topo.front().logical_cpus.size();
    return coresPerSocket;
}

void Utils::getOrderedCores (std::vector<int> &orderedCores) {
  auto topo = boost::fibers::numa::topology();
  for (auto &n: topo) {
    for (auto &cpu_id: n.logical_cpus) {
      orderedCores.push_back(cpu_id);
    }
  }
}

int Utils::getFirstCoreFromSocket(size_t socket) {
  auto topo = boost::fibers::numa::topology();
  if (socket > topo.size()) {
    throw std::runtime_error("error: wrong socket number");
  }
  auto &soc = topo[socket];
  for (auto &cpu_id: soc.logical_cpus) {
    return (int) cpu_id;
  }
  return -1;
}

int Utils::fileExists(char const *file) {
  return access(file, F_OK);
}

void Utils::readDirectory(const std::string &name, std::vector<std::string> &v) {
  DIR* dirp = opendir(name.c_str());
  struct dirent * dp;
  while ((dp = readdir(dirp)) != nullptr) {
    v.emplace_back(dp->d_name);
  }
  closedir(dirp);
}

void Utils::tryCreateDirectory(std::string dir) {
  std::experimental::filesystem::path path{dir};
  if (!std::experimental::filesystem::exists(
      std::experimental::filesystem::status(path))) {
    std::experimental::filesystem::create_directories(path);
  }
}

template <typename T>
void wrapArrayInVector(
    T *sourceArray, size_t arraySize,
    std::vector<T, boost::alignment::aligned_allocator<T, 64> > &targetVector) {
  typename std::_Vector_base<
      T, boost::alignment::aligned_allocator<T, 64> >::_Vector_impl *vectorPtr =
      (typename std::_Vector_base<
          T, boost::alignment::aligned_allocator<T, 64> >::_Vector_impl
           *)((void *)&targetVector);
  vectorPtr->_M_start = sourceArray;
  vectorPtr->_M_finish = vectorPtr->_M_end_of_storage =
      vectorPtr->_M_start + arraySize;
}
template void wrapArrayInVector(
    char *sourceArray, size_t arraySize,
    std::vector<char, boost::alignment::aligned_allocator<char, 64> >
        &targetVector);

template <typename T>
void releaseVectorWrapper(
    std::vector<T, boost::alignment::aligned_allocator<T, 64> > &targetVector) {
  typename std::_Vector_base<
      T, boost::alignment::aligned_allocator<T, 64> >::_Vector_impl *vectorPtr =
      (typename std::_Vector_base<
          T, boost::alignment::aligned_allocator<T, 64> >::_Vector_impl
           *)((void *)&targetVector);
  vectorPtr->_M_start = vectorPtr->_M_finish = vectorPtr->_M_end_of_storage =
      nullptr;
}
template void releaseVectorWrapper(
    std::vector<char, boost::alignment::aligned_allocator<char, 64> >
        &targetVector);

template<typename T>
std::vector<T> computePercentiles(std::vector<T> &input, std::vector<double> &percentiles) {
  std::sort(input.begin(), input.end());
  std::vector<T> res;
  for (auto perc : percentiles) {
    int pos;
    if (perc < 0)
      pos = 0;
    else if (perc > 1.0)
      pos = (int) input.size() - 1;
    else
      pos = (int) std::round(perc * input.size());
    res.push_back(input[pos]);
  }
  return res;
}
template std::vector<double> computePercentiles<double>(std::vector<double> &input, std::vector<double> &percentiles);