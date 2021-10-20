#pragma once

#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <thread>

#include "utils/Status.h"

#define RETURN_NOT_OK(s)             \
  do {                               \
    Status _s = (s);                 \
    if (_s != Status::Ok) return _s; \
  } while (0)

class IAsyncContext;

/*
 * \brief Signature of the async callback for I/Os.
 *
 * */

typedef void (*AsyncIOCallback)(IAsyncContext* context, Status result,
                                size_t bytesTransferred);

/*
 * \brief Standard interface for contexts used by async callbacks.
 *
 * */

class IAsyncContext {
 private:
  // Whether the internal state for the async context has been copied to a
  // heap-allocated memory block.
  bool m_fromDeepCopy;

 public:
  IAsyncContext() : m_fromDeepCopy{false} {}

  virtual ~IAsyncContext() {}

  // Contexts are initially allocated (as local variables) on the stack. When an
  // operation goes async, it deep copies its context to a new heap allocation;
  // this context must also deep copy its parent context, if any. Once a context
  // has been deep copied, subsequent DeepCopy() calls just return the original,
  // heap-allocated copy.
  Status deepCopy(IAsyncContext*& contextCopy) {
    if (m_fromDeepCopy) {
      // Already on the heap: nothing to do.
      contextCopy = this;
      return Status::Ok;
    } else {
      RETURN_NOT_OK(deepCopyInternal(contextCopy));
      contextCopy->m_fromDeepCopy = true;
      return Status::Ok;
    }
  }

  // Whether the internal state for the async context has been copied to a
  // heap-allocated memory block.
  bool getFromDeepCopy() const { return m_fromDeepCopy; }

  void setFromDeepCopy() { m_fromDeepCopy = true; }

 protected:
  // Override this method to make a deep, persistent copy of your context. A
  // context should:
  //   1. Allocate memory for its copy. If the allocation fails, return
  //   Status::OutOfMemory.
  //   2. If it has a parent/caller context, call DeepCopy() on that context. If
  //   the call fails, free the memory it just allocated and return the call's error code.
  //   3. Initialize its copy and return Status::Ok..
  virtual Status deepCopyInternal(IAsyncContext*& contextCopy) = 0;

  // A common pattern: deep copy, when context has no parent/caller context.
  template <class C>
  inline static Status deepCopyInternal(C& context,
                                        IAsyncContext*& contextCopy) {
    contextCopy = nullptr;
    auto ctxt = std::unique_ptr<C>( new C(context) );
    if (!ctxt.get()) return Status::OutOfMemory;
    contextCopy = ctxt.release();
    return Status::Ok;
  }
  // Another common pattern: deep copy, when context has a parent/caller
  // context.
  template <class C>
  inline static Status deepCopyInternal(C& context,
                                        IAsyncContext* callerContext,
                                        IAsyncContext*& contextCopy) {
    contextCopy = nullptr;
    IAsyncContext* callerContextCopy;
    RETURN_NOT_OK(callerContext->deepCopy(callerContextCopy));
    //auto ctxt = std::make_unique<C>(context, callerContextCopy);
    auto ctxt = std::unique_ptr<C>(new C(context, callerContextCopy));
    std::unique_ptr<C>( new C(context) );
    if (!ctxt.get()) return Status::OutOfMemory;
    contextCopy = ctxt.release();
    return Status::Ok;
  }
};

/*
 * \brief User-defined callbacks for async FishStore operations. Async callback
 * equivalent of: Status some_function(context* arg).
 *
 * */

typedef void (*AsyncCallback)(IAsyncContext* ctxt, Status result);

/*
 * \brief Helper class, for use inside a continuation callback, that ensures the
 * context will be freed when the callback exits.
 *
 * */

template <class C>
class CallbackContext {
 public:
  bool m_async;
 protected:
  std::unique_ptr<C> m_context;

 public:
  CallbackContext(IAsyncContext* context) : m_async{false} {
    m_context = std::unique_ptr<C>(static_cast<C*>(context));
  }
  C* get() const { return m_context.get(); }
  C* operator->() const { return m_context.get(); }
  ~CallbackContext() {
    if (m_async || !m_context->getFromDeepCopy()) {
      // The callback went async again, or it never went async. The next
      // callback or the caller is responsible for freeing the context.
      m_context.release();
    }
  }
};