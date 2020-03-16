#include "AttributeType.h"

using namespace std;

const std::map<const BasicType, const std::string>
    AttributeType::m_typeNames({{BasicType::Integer, "int"},
                                {BasicType::Float, "float"},
                                {BasicType::Long, "long"},
                                {BasicType::LongLong, "longlong"},
                                {BasicType::Double, "double"},
                                {BasicType::Char, "char"},
                                {BasicType::String, "string"}});

const std::map<const std::string, const BasicType> AttributeType::m_namesToTypes{
    {"int", BasicType::Integer}, {"float", BasicType::Float},
    {"long", BasicType::Long}, {"double", BasicType::Double},
    {"char", BasicType::Char}, {"string", BasicType::String},
    {"longlong", BasicType::LongLong}};
