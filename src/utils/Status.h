#pragma once

/*
 * \brief Return codes for file and memory allocation handling.
 *
 * */

enum class Status : uint8_t {
  Ok = 0,
  Pending = 1,
  NotFound = 2,
  OutOfMemory = 3,
  IOError = 4,
  Corruption = 5,
  Aborted = 6,
};