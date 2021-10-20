#pragma once

#include <dirent.h>
#include <dlfcn.h>
#include <sched.h>
#include <sys/types.h>

#include <algorithm>  // std::sort
#include <boost/align/aligned_allocator.hpp>
#include <cerrno>
#include <cfenv>
#include <cmath>
#include <cstdio> /* defines FILENAME_MAX */
#include <fstream>
#include <functional>
#include <ios>
#include <iosfwd>
#include <iostream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

/*
 * \brief Utility functions for the system.
 *
 * */

// #define WINDOWS  /* uncomment this line to use it for windows.*/
#ifdef WINDOWS
#include <direct.h>
#define GetCurrentDir _getcwd
#else
#include <unistd.h>
#define GetCurrentDir getcwd
#endif

//#define DEBUG
#undef DEBUG

#undef dbg
#ifdef DEBUG
#define dbg(fmt, args...)                                                      \
  do {                                                                         \
    fprintf(stdout, "DEBUG %35s (l. %4d) > " fmt, __FILE__, __LINE__, ##args); \
    fflush(stdout);                                                            \
  } while (0)
#else
#define dbg(fmt, args...)
#endif

#define _info(fmt, args...)                                                    \
  do {                                                                         \
    fprintf(stdout, "INFO  %35s (l. %4d) > " fmt, __FILE__, __LINE__, ##args); \
    fflush(stdout);                                                            \
  } while (0)

#define print_error_then_terminate(en, msg) \
  do {                                      \
    errno = en;                             \
    perror(msg);                            \
    exit(EXIT_FAILURE);                     \
  } while (0)

namespace Utils {
void bindProcess(int core_id);

void bindProcess(std::thread &thread, int id);

long pack(long systemTimestamp, long tupleTimestamp);

int getSystemTimestamp(long value);

int getTupleTimestamp(long value);

int getPowerOfTwo(int value);

bool _is_pointer_aligned(const void *p, int alignment);

bool _is_length_aligned(int length, int alignment);

std::string getCurrentWorkingDir();

std::string getHomeDir();

// process_mem_usage(double &, double &) - takes two doubles by reference,
// attempts to read the system-dependent data for a process' virtual memory
// size and resident set size, and return the results in KB.
// On failure, returns 0.0, 0.0
void process_mem_usage(double &vm_usage, double &resident_set);

std::string getStdoutFromCommand(std::string cmd);

int getNumberOfSockets();

int getNumberOfCoresPerSocket();

void getOrderedCores(std::vector<int> &orderedCores);

int getFirstCoreFromSocket(size_t socket);

int fileExists(char const *file);

void readDirectory(const std::string &name, std::vector<std::string> &v);

void tryCreateDirectory(std::string dir);

class Timer {
 public:
  Timer() : m_beg(clock_::now()) {}
  void reset() { m_beg = clock_::now(); }

  [[nodiscard]] double elapsed_nsec() const {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
        clock_::now() - m_beg).count();
  }

  [[nodiscard]] double elapsed_msec() const {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               clock_::now() - m_beg).count();
  }

  [[nodiscard]] double elapsed_sec() const {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               clock_::now() - m_beg).count() / 1000.0;
  }

  void printElapsed(std::string name = "") const {
    std::cout << name + "Timer: " << elapsed_sec() << " sec" << std::endl;
  }

 private:
  typedef std::chrono::high_resolution_clock clock_;
  typedef std::chrono::duration<double, std::ratio<1> > second_;
  std::chrono::time_point<clock_> m_beg;
};

class DynamicLoader {
 public:
  explicit DynamicLoader() = default;

  explicit DynamicLoader(std::string const &filename) {
    m_libHandles[filename] = HandlePtr(dlopen(filename.c_str(), RTLD_LAZY));
    if (!m_libHandles[filename]) {
      throw std::logic_error("error: can't load library named \"" + filename + "\"");
    }
  }

  void addLibrary(std::string const &filename) {
    if (m_libHandles.find(filename) != m_libHandles.end())
      std::cout << "warning: library already exists" << std::endl;
    m_libHandles[filename] = HandlePtr(dlopen(filename.c_str(), RTLD_LAZY));
    if (!m_libHandles[filename]) {
      throw std::logic_error("error: can't load library named \"" + filename + "\"");
    }
  }

  template <class T>
  std::function<T> load(std::string const &filename,
                        std::string const &functionName) {
    auto handle = m_libHandles[filename].get();
    if (!handle) {
      throw std::logic_error("error: can't load library named \"" + filename + "\"");
    }
    dlerror();
    void *const result = dlsym(handle, functionName.c_str());
    if (!result) {
      char *const error = dlerror();
      if (error) {
        throw std::logic_error("error: can't find symbol named \"" +
                               functionName + "\": " + error);
      }
    }
    return reinterpret_cast<T *>(result);
  }

 private:
  struct dl_closer{ void operator()(void* dl) const { dlclose(dl); }};
  using HandlePtr = std::unique_ptr<void, dl_closer>;
  std::unordered_map<std::string, HandlePtr> m_libHandles;
};
}  // namespace Utils

template <typename T>
void wrapArrayInVector(
    T *sourceArray, size_t arraySize,
    std::vector<T, boost::alignment::aligned_allocator<T, 64> > &targetVector);

template <typename T>
void releaseVectorWrapper(
    std::vector<T, boost::alignment::aligned_allocator<T, 64> > &targetVector);

template <typename T>
std::vector<T> computePercentiles(std::vector<T> &input,
                                  std::vector<double> &percentiles);