#pragma once

#include <atomic>
#include <memory>

#include "tbb/concurrent_queue.h"

#include "tasks/Task.h"
#include "tasks/WindowBatch.h"

/*
 * This class implements a pool of @Tasks used from the @TaskDispatcher
 * to create new tasks for the system.
 *
 * */

class TaskFactory {
 private:
  std::atomic<long> m_count;
  tbb::concurrent_queue<std::shared_ptr<Task>> m_pool;
  TaskFactory() {};

 public:
  static TaskFactory &getInstance() {
    static TaskFactory instance;
    return instance;
  }

  TaskFactory(TaskFactory const &) = delete;
  void operator=(TaskFactory const &) = delete;

  std::shared_ptr<Task> newInstance(int taskId, std::shared_ptr<WindowBatch> batch) {
    std::shared_ptr<Task> task;
    bool hasRemaining = m_pool.try_pop(task);
    if (!hasRemaining) {
      m_count.fetch_add(1);
      task = std::make_shared<Task>(taskId, batch);
    }
    task->set(taskId, batch);
    return task;
  }

  void free(std::shared_ptr<Task> &task) {
    //std::cout << "[DBG] free task "+std::to_string(task->getTaskId())+" task-refs "+std::to_string(task.use_count()) << std::endl;
    m_pool.push(task);
    task.reset();
  }

  long getCount() {
    return m_count.load();
  }
};