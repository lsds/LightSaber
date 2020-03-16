#pragma once

#include <iosfwd>
#include <sched.h>
#include <cerrno>
#include <vector>
#include <algorithm>  // std::sort
#include <cmath>
#include <cfenv>
#include <thread>
#include <cstdio>  /* defines FILENAME_MAX */
#include <ios>
#include <iostream>
#include <fstream>
#include <string>

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
#	define dbg(fmt, args...) do { fprintf(stdout, "DEBUG %35s (l. %4d) > " fmt, __FILE__, __LINE__, ## args); fflush(stdout); } while (0)
#else
#	define dbg(fmt, args...)
#endif

#define info(fmt, args...) do { fprintf(stdout, "INFO  %35s (l. %4d) > " fmt, __FILE__, __LINE__, ## args); fflush(stdout); } while (0)

#define print_error_then_terminate(en, msg) \
  do { errno = en; perror(msg); exit(EXIT_FAILURE); } while (0)

namespace Utils {
void bindProcess(const int core_id);

void bindProcess(std::thread &thread, const int id);

long pack(long systemTimestamp, long tupleTimestamp);

int getSystemTimestamp(long value);

int getTupleTimestamp(long value);

int getPowerOfTwo(int value);

bool __is_pointer_aligned(const void *p, int alignment);

bool __is_length_aligned(int length, int alignment);

std::string GetCurrentWorkingDir();

std::string GetHomeDir ();

// process_mem_usage(double &, double &) - takes two doubles by reference,
// attempts to read the system-dependent data for a process' virtual memory
// size and resident set size, and return the results in KB.
// On failure, returns 0.0, 0.0
void process_mem_usage(double &vm_usage, double &resident_set);

std::string GetStdoutFromCommand(std::string cmd);

int getNumberOfSockets ();

int getNumberOfCoresPerSocket ();

void getOrderedCores (std::vector<int> &orderedCores);

}

template<typename T>
std::vector<T> computePercentiles(std::vector<T> &input, std::vector<double> &percentiles);