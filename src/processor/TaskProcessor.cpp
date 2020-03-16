#include "processor/TaskProcessor.h"
#include "tasks/Task.h"
#include "tasks/TaskFactory.h"
#include "utils/Utils.h"
#include "utils/SystemConf.h"

TaskProcessor::TaskProcessor(int pid, std::shared_ptr<TaskQueue> queue) :
    m_queue(queue),
    m_pid(pid) {}

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wmissing-noreturn"
void TaskProcessor::operator()() {
  static thread_local std::shared_ptr<Task> task;
#if defined(HAVE_NUMA)
  int cpu = sched_getcpu();
  m_numaNodeId = numa_node_of_cpu(cpu);
  while (!m_queue->isReady())
      ;
#else
  m_numaNodeId = 0;
#endif

  while (true) {
    try {
      if (m_monitor) {
        /* Queue poll latency measurements */
        m_start = std::chrono::system_clock::now().time_since_epoch().count();
      }

#if defined(HAVE_NUMA)
      while (!m_queue->try_dequeue(task, m_numaNodeId)) {
#else
      while (!m_queue->try_dequeue(task)) {
#endif
        std::this_thread::sleep_for(std::chrono::nanoseconds(1));
      }

      if (m_monitor) {
        m_dt = (double) (std::chrono::system_clock::now().time_since_epoch().count() - m_start);
        m_count += 1;
        if (m_count > 1) {
          if (m_count == 2) {
            m__m = m_m = m_dt;
            m__s = m_s = 0;
          } else {
            m_m = m__m + (m_dt - m__m) / (m_count - 1);
            m_s = m__s + (m_dt - m__m) * (m_dt - m_m);
            m__m = m_m;
            m__s = m_s;
          }
        }
      }

      /*std::cout << "[DBG] processor "+std::to_string(m_pid)+" processor-numa "+std::to_string(m_numaNodeId)
      +" query "+std::to_string(task->getQueryId())+" task "+std::to_string(task->getTaskId())
      +" task-numa "+std::to_string(task->getNumaNodeId())+" task-refs "+std::to_string(task.use_count()) << std::endl;*/
      //tasksProcessed[task->getQueryId()].fetch_add(1);
      task->run(m_pid);
      TaskFactory::getInstance().free(task);

    } catch (std::exception &e) {
      std::cout << e.what() << std::endl;
      exit(1);
    }
  }
}
#pragma clang diagnostic pop

void TaskProcessor::enableMonitoring() {
  m_monitor = true;
}

long TaskProcessor::getProcessedTasks(int qid) {
  (void) qid;
  throw std::runtime_error("error: these metrics are not supported yet.");
  //return tasksProcessed[qid].load();
}

double TaskProcessor::mean() {
  if (!m_monitor)
    return 0;
  m_avg = (m_count > 0) ? m_m : 0;
  return m_avg;
}

double TaskProcessor::stdv() {
  if (!m_monitor)
    return 0;
  m_std = (m_count > 2) ? std::sqrt(m_s / (double) (m_count - 1 - 1)) : 0;
  return m_std;
}

TaskProcessor::~TaskProcessor() {
  //delete[] (tasksProcessed);
}