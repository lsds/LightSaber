#pragma once

#include <cassert>
#include <climits>
#include <map>
#include <memory>
#include <sstream>
#include <string>

struct AbstractType {
  virtual ~AbstractType() = 0; // NEVER directly instantiate this type.
  virtual std::string toSExpr() const = 0;
};

inline AbstractType::~AbstractType() = default;
enum class BasicType { Integer, Long, String, Float, Double, Char, Date, LongLong };
static const BasicType BasicReferenceType = BasicType::Integer;

class AttributeType : public AbstractType {
 public:
  static const std::map<const BasicType, const std::string> m_typeNames;
  static const std::map<const std::string, const BasicType> m_namesToTypes;
  ~AttributeType() {}
  struct Bounds {
    /*
     * the partition id is calculated as minimum + (knownFactor*int(i/knownDivisor))%knownCap
     */
    long m_min = 0;
    long m_factor = 0;
    long m_divisor = 1;
    long m_cap = LONG_MAX; // its not really a cap, but a cycle size.

    Bounds withFactor(long factor) {
      auto result = *this;
      result.m_factor = factor;
      return result;
    }
    Bounds withMin(long min) {
      auto result = *this;
      result.m_min = min;
      return result;
    }
    Bounds withDivisor(long divisor) {
      auto result = *this;
      result.m_divisor = divisor;
      return result;
    }
    Bounds withCap(long cap) {
      auto result = *this;
      result.m_cap = cap;
      return result;
    }
  };

 private:
  bool m_hasKnownBounds_ = false;
  long m_looseUpperBoundForData = LONG_MAX;
  Bounds m_knownBounds{};
  BasicType m_basicType;
  std::shared_ptr<AttributeType> m_surrogateAspect = NULL;

  int m_bits = 32;

 public:
  short getBitWidth() const { return getWidth() * 8; }

  short getWidth() const {
    return std::map<BasicType, short>({
                                          {BasicType::LongLong, 16},
                                          {BasicType::Long, 8},
                                          {BasicType::Float, 4},
                                          {BasicType::Char, 1},
                                          {BasicType::Date, 4},
                                          {BasicType::String, 8},
                                          {BasicType::Double, 8},
                                          {BasicType::Integer, 4}})
        .at(getBasicType());
  }

  bool hasFixedPoint() const {
    return m_basicType == BasicType::Integer || m_basicType == BasicType::Long;
  }

  bool hasFloatingPoint() const {
    return m_basicType == BasicType::Double || m_basicType == BasicType::Float;
  }

  const BasicType &getBasicType() const { return m_basicType; }

  AttributeType(const BasicType &basicType = BasicReferenceType)
      : m_basicType(basicType),
        m_bits((basicType == BasicType::Float || basicType == BasicType::Integer) ? 32 :
               ((basicType == BasicType::LongLong) ? 128 : 64)) {}

  long getKnownFactor() const;

  const Bounds getBounds() const { return m_knownBounds; }

  long getKnownDivisor() const;

  long getKnownCap() const;

  long getKnownMinimum() const;

  long getLooseUpperBoundForData() const { return m_looseUpperBoundForData; };

  void setLooseUpperBoundForData(long _) { m_looseUpperBoundForData = _; };

  AttributeType(const BasicType &basicType, const long knownMinimum, const long knownFactor,
                const long knownDivisor, const long knownCap,
                const long bits = 32) // temporary. 63 to be compatible with existing code.
      : m_hasKnownBounds_(true),
        m_knownBounds(Bounds()
                          .withMin(knownMinimum)
                          .withFactor(knownFactor)
                          .withDivisor(knownDivisor)
                          .withCap(knownCap)),
        m_basicType(basicType),
        m_bits(bits) {}

  AttributeType(const BasicType &basicType, const long bits)
      : AttributeType(basicType, 0, 1, 1, 1 << (bits - 1), bits) {
    assert(bits > 0);
    assert(bits <= 128);
  }

  bool hasKnownBounds() const;

  void setHasKnownBounds(bool _) { m_hasKnownBounds_ = _; };

  bool hasBeenPartitionedByType() const;

  void markWithSurrogateAspect(AttributeType const &_) {
    m_surrogateAspect.reset(new AttributeType{_.getBasicType(), _.m_knownBounds.m_min,
                                              _.m_knownBounds.m_factor, _.m_knownBounds.m_divisor,
                                              _.m_knownBounds.m_cap, _.m_bits});
  }

  bool hasSurrogateAspect() const { return m_surrogateAspect != NULL; }

  AttributeType const getSurrogateAspect() const { return *m_surrogateAspect; }

  explicit operator std::string() const {
    using namespace std;
    using namespace std::string_literals;
    if (m_typeNames.count(getBasicType()))
      return m_typeNames.at(getBasicType());
    else
      return "unknown type (tell holger)"s;
  }

  std::string toSExpr() const override {
    std::stringstream o;
    //			o << "("
    //				<< "BasicType " << getBitWidth() << ")";
    return o.str();
  }

  /*static const clang::QualType toClangType(clang::ASTContext& context, BasicType type) {
      switch (type) {
          case BasicType::Integer:
              return context.IntTy;
          case BasicType::Long:
              return context.LongTy;
          case BasicType::String:
              return context.getPointerType(context.CharTy);
          case BasicType::Float:
              return context.FloatTy;
          case BasicType::Double:
              return context.DoubleTy;
          case BasicType::Char:
              return context.CharTy;
          case BasicType::Date:
              return context.IntTy;
      }
  }

  const clang::QualType toClangType(clang::ASTContext& context) const {
      return toClangType(context, basicType);
  }*/

  static short getWidth(BasicType type) {
    return std::map<BasicType, short>({{BasicType::LongLong, 16},
                                       {BasicType::Long, 8},
                                       {BasicType::Float, 4},
                                       {BasicType::Char, 1},
                                       {BasicType::Double, 8},
                                       {BasicType::Integer, 4}})
        .at(type);
  }
};