#include "dispatcher/ITaskDispatcher.h"

#include "checkpoint/FileBackedCheckpointCoordinator.h"
#include "utils/Query.h"

ITaskDispatcher::ITaskDispatcher(Query &query, bool triggerCheckpoints)
    : m_workerQueue(query.getTaskQueue()),
      m_parent(query),
      m_triggerCheckpoints(triggerCheckpoints) {}