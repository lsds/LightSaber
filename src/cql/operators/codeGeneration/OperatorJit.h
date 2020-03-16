#ifndef OPERATOR_JIT_H
#define OPERATOR_JIT_H

#include "iostream"

#include "clang/CodeGen/CodeGenAction.h"
#include "llvm/ExecutionEngine/Orc/CompileUtils.h"
#include "llvm/ExecutionEngine/Orc/IRCompileLayer.h"
#include "llvm/ExecutionEngine/Orc/IRTransformLayer.h"
#include "llvm/ExecutionEngine/Orc/OrcError.h"
#include "llvm/ExecutionEngine/Orc/RTDyldObjectLinkingLayer.h"
#include "llvm/Target/TargetMachine.h"

/*
 * \brief This class is used to JIT generate code for the system.
 *
 * */

#define STRINGIFY_DETAIL(X) #X
#define STRINGIFY(X) STRINGIFY_DETAIL(X)

// Show the error message and exit.
LLVM_ATTRIBUTE_NORETURN static void fatalError(llvm::Error E) {
  llvm::handleAllErrors(std::move(E), [&](const llvm::ErrorInfoBase &EI) {
    llvm::errs() << "Fatal Error: ";
    EI.log(llvm::errs());
    llvm::errs() << "\n";
    llvm::errs().flush();
  });

  exit(1);
}

namespace llvm {
namespace orc {
class OperatorJit {
 private:
  ExecutionSession ES;
  std::shared_ptr<SymbolResolver> Resolver;
  std::unique_ptr<TargetMachine> TM;
  const DataLayout DL;
  LegacyRTDyldObjectLinkingLayer ObjectLayer;
  LegacyIRCompileLayer<decltype(ObjectLayer), SimpleCompiler> CompileLayer;
  using OptimizeFunction = std::function<std::unique_ptr<Module>(std::unique_ptr<Module>)>;
  LegacyIRTransformLayer<decltype(CompileLayer), OptimizeFunction> OptimizeLayer;
  std::vector<VModuleKey> keys;
 public:
  OperatorJit();

  const TargetMachine &getTargetMachine() const;

  VModuleKey addModule(std::unique_ptr<Module> M);

  JITSymbol findSymbol(const StringRef &Name);

  JITSymbol findSymbolIn(VModuleKey &key, const StringRef &Name);

  JITTargetAddress getSymbolAddress(const StringRef &Name);

  void removeAllModules();

  void removeModule(VModuleKey K);

  template<class Signature_t>
  llvm::Expected<std::function<Signature_t>> getFunction(const StringRef &Name) {
    using namespace llvm;

    // Find symbol name in committed modules.
    JITSymbol sym = findSymbol(Name);
    if (!sym)
      return make_error<JITSymbolNotFound>(Name);

    // Access symbol address.
    // Invokes compilation for the respective module if not compiled yet.
    Expected<JITTargetAddress> addr = sym.getAddress();
    if (!addr)
      return addr.takeError();

    auto typedFunctionPtr = reinterpret_cast<Signature_t *>(*addr);
    return std::function<Signature_t>(typedFunctionPtr);
  }

  template<class Signature_t>
  llvm::Expected<std::function<Signature_t>> getFunctionInModule(VModuleKey &key, const StringRef &Name) {
    using namespace llvm;

    // Find symbol name in committed modules.
    JITSymbol sym = findSymbolIn(key, Name);
    if (!sym)
      return make_error<JITSymbolNotFound>(Name);

    // Access symbol address.
    // Invokes compilation for the respective module if not compiled yet.
    Expected<JITTargetAddress> addr = sym.getAddress();
    if (!addr)
      return addr.takeError();

    auto typedFunctionPtr = reinterpret_cast<Signature_t *>(*addr);
    return std::function<Signature_t>(typedFunctionPtr);
  }

 private:
  std::unique_ptr<Module> optimizeModule(std::unique_ptr<Module> M);
};
} // end namespace orc
} // end namespace llvm

class CodeGenWrapper {
 private:
  llvm::orc::OperatorJit *J;

 public:
  CodeGenWrapper();

  uint64_t parseAndCodeGen(int argc, const char **argv);

  template<class Signature_t>
  llvm::Expected<std::function<Signature_t>> getFunction(const clang::StringRef &Name) {
    return J->getFunction<Signature_t>(Name);
  };

  template<class Signature_t>
  llvm::Expected<std::function<Signature_t>> getFunctionInModule(llvm::orc::VModuleKey &key,
                                                                 const clang::StringRef &Name) {
    return J->getFunctionInModule<Signature_t>(key, Name);
  };

  void removeAllModules();

  ~CodeGenWrapper();

 private:
  void populateArgs(clang::SmallVector<const char *, 16> &args, llvm::StringRef cpp);
};

#endif