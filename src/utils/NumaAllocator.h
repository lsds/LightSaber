#include <numa.h>
#include <mm_malloc.h>

namespace NumaAlloc {

/**
 * \brief An STL allocator that uses memory of a specific NUMA node only
 * based on The library KASKADE 7
 * see http://www.zib.de/projects/kaskade7-finite-element-toolbox
 */

template<class T>
class NumaAllocator : public std::__allocator_base<T> {
 public:
  typedef T m_value_type;
  typedef T *m_pointer;
  typedef T const *m_const_pointer;
  typedef T &m_reference;
  typedef T const &m_const_reference;
  typedef std::size_t m_size_type;
  typedef std::ptrdiff_t m_difference_type;

  // make sure that on copying, the target copy has its data in the same
  // NUMA memory region as the source by default -- this improves data locality.
  typedef std::true_type m_propagate_on_container_copy_assignment;
  typedef std::true_type m_propagate_on_container_move_assignment;
  typedef std::true_type m_propagate_on_container_swap;

  template<class U>
  struct rebind {
    typedef NumaAllocator<U> other;
  };

  /**
   * \brief Construct an allocator for allocating on the given NUMA node.
   *
   * \param node the NUMA node on which to allocate the memory. This has to be less than NumaThreadPool::instance().nodes().
   *             For negative values, the memory is allocated in interleaved mode.
   */
  NumaAllocator(int node = -1) : m_numaNode(node) {
  }

  NumaAllocator(const NumaAllocator &other) : m_numaNode(other.node()) {}

  template<class U>
  NumaAllocator(const NumaAllocator<U> &other) : m_numaNode(other.node()) {}

  ~NumaAllocator() {}

  /**
   * \brief Reports the node on which we allocate.
   */
  int node() const {
    return m_numaNode;
  }

  m_pointer address(m_reference x) const {
    return &x;
  }

  m_const_pointer address(m_const_reference x) const {
    return &x;
  }

  m_size_type max_size() const {

#ifdef HAVE_NUMA
    long free;
    if (m_numaNode >= 0)
      return numa_node_size(m_numaNode, &free);
    else {
      std::allocator<char> a;
      return a.max_size();
    }
#else
    std::allocator<char> a;
return a.max_size() / sizeof(T);
#endif
  }

  /**
   * \brief Allocates the requested amount of memory.
   *
   * If \arg n == 0, a null pointer is returned.
   *
   * \param n number of objects of type T
   */
  m_pointer allocate(m_size_type n, std::allocator<void>::const_pointer /* hint */ = 0) {
    if (n == 0) {
      return 0;
    }

    if (m_numaNode == -1) {
      int cpu = sched_getcpu();
      m_numaNode = numa_node_of_cpu(cpu);
    }

    numa_set_preferred(m_numaNode);
    numa_set_bind_policy(m_numaNode);
    numa_set_strict(m_numaNode);
    auto ret = numa_alloc_onnode(n * sizeof(T), m_numaNode);
    if (!ret)
      throw std::bad_alloc();

    ((char *) ret)[0] = 0;
    //memset(ret, 0, n);
    return static_cast<m_pointer>(ret);
  }

  void deallocate(m_pointer p, m_size_type n) {
    if (p)
      numa_free(static_cast<void *>(p), n * sizeof(T));
  }

  template<class U, class... Args>
  void construct(U *p, Args &&... args) {
    ::new((void *) p) U(std::forward<Args>(args)...);
  }

  template<class U>
  void destroy(U *p) {
    p->~U();
  }

  /**
   * \brief comparison for equality
   *
   * Allocators compare equal, if they allocate on the same NUMA node.
   */
  template<class U>
  bool operator==(NumaAllocator<U> const &other) const {
    return node() == other.node();
  }

  template<class U>
  bool operator!=(NumaAllocator<U> const &other) const {
    return !(node() == other.node());
  }

 private:
  int m_numaNode;
};

template<class T1, class T2>
bool operator==(const NumaAllocator<T1> &,
                const NumaAllocator<T2> &) noexcept {
  return true;
}

template<class T1, class T2>
bool operator!=(const NumaAllocator<T1> &,
                const NumaAllocator<T2> &) noexcept {
  return false;
}
}

