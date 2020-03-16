#include "OperatorJit.h"

#include "clang/Driver/Compilation.h"
#include "clang/Driver/Driver.h"
#include "clang/Driver/Tool.h"
#include "clang/Frontend/CompilerInstance.h"
#include "clang/Frontend/CompilerInvocation.h"
#include "clang/Frontend/FrontendDiagnostic.h"
#include "clang/Frontend/TextDiagnosticPrinter.h"

#include "llvm/ADT/SmallString.h"
#include "llvm/ADT/StringRef.h"
#include <llvm/Analysis/TargetLibraryInfo.h>
#include <llvm/Analysis/TargetTransformInfo.h>
#include "llvm/ExecutionEngine/ExecutionEngine.h"
#include "llvm/ExecutionEngine/JITSymbol.h"
#include "llvm/ExecutionEngine/Orc/ExecutionUtils.h"
#include "llvm/ExecutionEngine/Orc/JITTargetMachineBuilder.h"
#include "llvm/ExecutionEngine/SectionMemoryManager.h"
#include "llvm/IR/DataLayout.h"
#include "llvm/IR/LLVMContext.h"
#include "llvm/IR/LegacyPassManager.h"
#include "llvm/IR/Mangler.h"
#include "llvm/IR/Module.h"
#include "llvm/IR/Verifier.h"
#include "llvm/Support/FileSystem.h"
#include "llvm/Support/Host.h"
#include "llvm/Support/ManagedStatic.h"
#include "llvm/Support/Path.h"
#include "llvm/Support/TargetSelect.h"
#include "llvm/Support/Timer.h"
#include "llvm/Support/raw_ostream.h"
#include "llvm/Transforms/InstCombine/InstCombine.h"
#include "llvm/Transforms/IPO.h"
#include "llvm/Transforms/IPO/PassManagerBuilder.h"
#include "llvm/Transforms/Scalar.h"
#include "llvm/Transforms/Scalar/GVN.h"
//#include "llvm/Transforms/Vectorize.h"

std::string replaceExtension(llvm::StringRef name, llvm::StringRef ext) {
  return name.substr(0, name.find_last_of('.') + 1).str() + ext.str();
}

// This function isn't referenced outside its translation unit, but it
// can't use the "static" keyword because its address is used for
// GetMainExecutable (since some platforms don't support taking the
// address of main, and some platforms can't implement GetMainExecutable
// without being given the address of a function in the main executable).
std::string GetExecutablePath(const char *Argv0, void *MainAddr) {
  return llvm::sys::fs::getMainExecutable(Argv0, MainAddr);
}

namespace llvm {
namespace orc {
OperatorJit::OperatorJit()
    : Resolver(createLegacyLookupResolver(
    ES,
    [this](const std::string &Name) -> JITSymbol {
      if (auto Sym = OptimizeLayer.findSymbol(Name, false))
        return Sym;
      else if (auto Err = Sym.takeError())
        return std::move(Err);
      if (auto SymAddr =
          RTDyldMemoryManager::getSymbolAddressInProcess(Name))
        return JITSymbol(SymAddr, JITSymbolFlags::Exported);
      return nullptr;
    },
    [](Error Err) { cantFail(std::move(Err), "lookupFlags failed"); })),
      TM(EngineBuilder().selectTarget()), DL(TM->createDataLayout()),
      ObjectLayer(ES,
                  [this](VModuleKey) {
                    return LegacyRTDyldObjectLinkingLayer::Resources{
                        std::make_shared<SectionMemoryManager>(), Resolver};
                  }),
      CompileLayer(ObjectLayer, SimpleCompiler(*TM)),
      OptimizeLayer(CompileLayer, [this](std::unique_ptr<Module> M) {
        return optimizeModule(std::move(M));
      }) {
  llvm::sys::DynamicLibrary::LoadLibraryPermanently(nullptr);
}

const TargetMachine &OperatorJit::getTargetMachine() const { return *TM; }

VModuleKey OperatorJit::addModule(std::unique_ptr<Module> M) {
  // Add the module to the JIT with a new VModuleKey.
  auto K = ES.allocateVModule();
  cantFail(OptimizeLayer.addModule(K, std::move(M)));
  keys.push_back(K);
  return K;
}

JITSymbol OperatorJit::findSymbol(const StringRef &Name) {
  std::string MangledName;
  raw_string_ostream MangledNameStream(MangledName);
  Mangler::getNameWithPrefix(MangledNameStream, Name, DL);
  return OptimizeLayer.findSymbol(MangledNameStream.str(), true);
}

JITSymbol OperatorJit::findSymbolIn(VModuleKey &key, const StringRef &Name) {
  std::string MangledName;
  raw_string_ostream MangledNameStream(MangledName);
  Mangler::getNameWithPrefix(MangledNameStream, Name, DL);
  return OptimizeLayer.findSymbolIn(key, MangledNameStream.str(), true);
}

JITTargetAddress OperatorJit::getSymbolAddress(const StringRef &Name) {
  return cantFail(findSymbol(Name).getAddress());
}

void OperatorJit::removeAllModules() {
  for (auto m : keys)
    removeModule(m);
  keys.clear();
}

void OperatorJit::removeModule(VModuleKey K) {
  cantFail(OptimizeLayer.removeModule(K));
}

std::unique_ptr<Module> OperatorJit::optimizeModule(std::unique_ptr<Module> M) {

  // Optimize the emitted LLVM IR.
  Timer topt;
  topt.startTimer();

  llvm::InitializeNativeTarget();
  llvm::InitializeNativeTargetAsmPrinter();

  auto PM_BuilderP = llvm::make_unique<llvm::PassManagerBuilder>();
  PM_BuilderP->OptLevel = 3;
  PM_BuilderP->SizeLevel = 0;
  PM_BuilderP->LoopVectorize = true;
  PM_BuilderP->SLPVectorize = true;
  PM_BuilderP->Inliner = llvm::createFunctionInliningPass(3, 0, false);
  PM_BuilderP->VerifyInput = true;
  PM_BuilderP->PrepareForLTO = true;

  TM->adjustPassManager(*PM_BuilderP);

  M->setTargetTriple(TM->getTargetTriple().str());
  M->setDataLayout(TM->createDataLayout());

  // Create a function pass manager.
  auto FPM = llvm::make_unique<legacy::FunctionPassManager>(M.get());

  // Add some optimizations.
  FPM->add(llvm::createTargetTransformInfoWrapperPass(TM->getTargetIRAnalysis()));

  FPM->add(createInstructionCombiningPass());
  FPM->add(createReassociatePass());
  FPM->add(createGVNPass());
  FPM->add(createCFGSimplificationPass());

  FPM->add(createSROAPass());
  FPM->add(createEarlyCSEPass(false));
  FPM->add(createMemCpyOptPass());
  FPM->add(createReassociatePass());
  FPM->add(createLoopFusePass());
  FPM->add(createDeadCodeEliminationPass());

  auto PMP = llvm::make_unique<llvm::legacy::PassManager>();
  PMP->add(new llvm::TargetLibraryInfoWrapperPass(TM->getTargetTriple()));
  PM_BuilderP->populateFunctionPassManager(*FPM);
  PM_BuilderP->populateModulePassManager(*PMP);
  PM_BuilderP->populateLTOPassManager(*PMP);
  //PM_BuilderP->populateThinLTOPassManager(*PMP);

  PMP->add(createVerifierPass());
  PMP->add(createGlobalOptimizerPass());

  // Run the optimizations over all functions in the module being added to
  // the JIT.
  FPM->doInitialization();
  for (auto &F : *M)
    FPM->run(F);
  FPM->doFinalization();

  // Finally run the module passes
  PMP->run(*M);

  //if (verbose) {
  topt.stopTimer();
  //std::cout << "[Optimization elapsed:] " << topt.getTotalTime().getProcessTime() << "s\n";
  //    const char* post_opt_file = "/tmp/llvmjit-post-opt.ll";
  //    llvm_module_to_file(*M, post_opt_file);
  //    std::cout << "[Post optimization module] dumped to " << post_opt_file
  //              << "\n";
  //}
  //M->dump();
  return M;
}
}
}

using namespace clang;
using namespace clang::driver;

CodeGenWrapper::CodeGenWrapper() {
}

uint64_t CodeGenWrapper::parseAndCodeGen(int argc, const char **argv) {

  // give the path of the file...
  //argv[1] = "/home/george/clion/workspace/llvm_test/cmake-build-debug/dummy.cpp";

  // This just needs to be some symbol in the binary; C++ doesn't
  // allow taking the address of ::main however.
  void *MainAddr = (void *) (intptr_t) GetExecutablePath;
  std::string Path = GetExecutablePath(argv[0], MainAddr);
  IntrusiveRefCntPtr<DiagnosticOptions> DiagOpts = new DiagnosticOptions();
  TextDiagnosticPrinter *DiagClient =
      new TextDiagnosticPrinter(llvm::errs(), &*DiagOpts);

  IntrusiveRefCntPtr<DiagnosticIDs> DiagID(new DiagnosticIDs());
  DiagnosticsEngine Diags(DiagID, &*DiagOpts, DiagClient);

  const std::string TripleStr = llvm::sys::getProcessTriple();
  llvm::Triple T(TripleStr);

  // Use ELF on Windows-32 and MingW for now.
#ifndef CLANG_INTERPRETER_COFF_FORMAT
  if (T.isOSBinFormatCOFF())
    T.setObjectFormat(llvm::Triple::ELF);
#endif

  Driver TheDriver(Path, T.str(), Diags);
  TheDriver.setTitle("clang interpreter");
  TheDriver.setCheckInputsExist(false);

  // FIXME: This is a hack to try to force the driver to do something we can
  // recognize. We need to extend the driver library to support this use model
  // (basically, exactly one input, and the operation mode is hard wired).
  SmallVector<const char *, 16> Args(argv, argv + argc);
  //Args.push_back("-fsyntax-only");
  std::string cpp = "JITFromSource.cpp";
  populateArgs(Args, cpp);

  std::unique_ptr<Compilation> C(TheDriver.BuildCompilation(Args));
  if (!C)
    return 1;

  // FIXME: This is copied from ASTUnit.cpp; simplify and eliminate.

  // We expect to get back exactly one command job, if we didn't something
  // failed. Extract that job from the compilation.
  const driver::JobList &Jobs = C->getJobs();
  if (Jobs.size() == 0 || !isa<driver::Command>(*Jobs.begin())) {
    SmallString<256> Msg;
    llvm::raw_svector_ostream OS(Msg);
    Jobs.Print(OS, "; ", true);
    Diags.Report(diag::err_fe_expected_compiler_job) << OS.str();
    return 1;
  }

  const driver::Command &Cmd = cast<driver::Command>(*Jobs.begin());
  if (llvm::StringRef(Cmd.getCreator().getName()) != "clang") {
    Diags.Report(diag::err_fe_expected_clang_command);
    return 1;
  }

  // Initialize a compiler invocation object from the clang (-cc1) arguments.
  const llvm::opt::ArgStringList &CCArgs = Cmd.getArguments();
  std::unique_ptr<CompilerInvocation> CI(new CompilerInvocation);
  CompilerInvocation::CreateFromArgs(*CI,
                                     const_cast<const char **>(CCArgs.data()),
                                     const_cast<const char **>(CCArgs.data()) +
                                         CCArgs.size(),
                                     Diags);

  // Show the invocation, with -v.
  if (CI->getHeaderSearchOpts().Verbose) {
    llvm::errs() << "clang invocation:\n";
    Jobs.Print(llvm::errs(), "\n", true);
    llvm::errs() << "\n";
  }

  // FIXME: This is copied from cc1_main.cpp; simplify and eliminate.

  // Create a compiler instance to handle the actual work.
  CompilerInstance Clang;
  Clang.setInvocation(std::move(CI));

  // Create the compilers actual diagnostics engine.
  Clang.createDiagnostics();
  if (!Clang.hasDiagnostics())
    return 1;

  // Infer the builtin include path if unspecified.
  if (Clang.getHeaderSearchOpts().UseBuiltinIncludes &&
      Clang.getHeaderSearchOpts().ResourceDir.empty())
    Clang.getHeaderSearchOpts().ResourceDir =
        CompilerInvocation::GetResourcesPath(argv[0], MainAddr);

  // Create and execute the frontend to generate an LLVM bitcode module.
  std::unique_ptr<CodeGenAction> Act(new EmitLLVMOnlyAction());
  if (!Clang.ExecuteAction(*Act))
    return 1;

  llvm::InitializeNativeTarget();
  llvm::InitializeNativeTargetAsmPrinter();
  //LLVMLinkInMCJIT();

  std::unique_ptr<llvm::Module> Module = Act->takeModule();
  llvm::orc::VModuleKey moduleKey = 0;
  //Module->dump();
  if (Module) {
    J = new llvm::orc::OperatorJit;
    moduleKey = J->addModule(std::move(Module));
  }

  return moduleKey;
}

void CodeGenWrapper::removeAllModules() {
  // remove module keys;
  J->removeAllModules();
}

CodeGenWrapper::~CodeGenWrapper() {
  // remove module keys;
  J->removeAllModules();
  // Shutdown.
  llvm::llvm_shutdown();
}

void CodeGenWrapper::populateArgs(SmallVector<const char *, 16> &args, llvm::StringRef cpp) {

  //args.push_back("-g");
  //args.push_back("-ccc-print-phases");
  //args.push_back("-v");
  //args.push_back("-march=native");
  //args.push_back("-stdlib=libc++");

  args.push_back("-emit-llvm");
  args.push_back("-emit-llvm-bc");
  args.push_back("-emit-llvm-uselists");

  //args.push_back("-main-file-name");
  //args.push_back(cpp.data());

  args.push_back("-mavx2");
  args.push_back("-std=c++14");
  args.push_back("-disable-free");
  args.push_back("-fdeprecated-macro");
  args.push_back("-fmath-errno");
  args.push_back("-fuse-init-array");

  args.push_back("-mrelocation-model");
  args.push_back("static");
  args.push_back("-mthread-model");
  args.push_back("posix");
  args.push_back("-masm-verbose");
  args.push_back("-mconstructor-aliases");
  args.push_back("-munwind-tables");

  args.push_back("-dwarf-column-info");
  args.push_back("-debugger-tuning=gdb");

#if DEBUG
  args.push_back("-debug-info-kind=limited");
  args.push_back("-dwarf-version=4");
#else
  args.push_back("-O3");
  args.push_back("-mdisable-fp-elim");
  args.push_back("-momit-leaf-frame-pointer");
  //args.push_back("-vectorize-loops");
  args.push_back("-loop-vectorize");
  //args.push_back("-vectorize-slp");
  args.push_back("-slp-vectorizer");
#endif

  args.push_back("-resource-dir");
  args.push_back(STRINGIFY(OPERATOR_JIT_LIB_CLANG_RESOURCE_DIR));
  /*
  "-internal-isystem"
  "/usr/lib/gcc/x86_64-linux-gnu/5.4.0/../../../../include/c++/5.4.0"
  "-internal-isystem"
  "/usr/lib/gcc/x86_64-linux-gnu/5.4.0/../../../../include/x86_64-linux-gnu/c++/5.4.0"
  "-internal-isystem"
  "/usr/lib/gcc/x86_64-linux-gnu/5.4.0/../../../../include/x86_64-linux-gnu/c++/5.4.0"
  "-internal-isystem"
  "/usr/lib/gcc/x86_64-linux-gnu/5.4.0/../../../../include/c++/5.4.0/backward"
  "-internal-isystem"
  "/usr/local/include"
  */
  args.push_back("-internal-isystem");
  args.push_back(STRINGIFY(OPERATOR_JIT_LIB_CLANG_RESOURCE_DIR) "/include");
  //args.push_back("-internal-isystem " STRINGIFY(OPERATOR_JIT_LIB_CLANG_RESOURCE_DIR) "/include");

  /*
  "-internal-externc-isystem"
  "/usr/include/x86_64-linux-gnu"
  "-internal-externc-isystem"
  "/include"
  "-internal-externc-isystem"
  "/usr/include"
  */
  /*
  std::string bc = replaceExtension (cpp, "bc");
  args.push_back("-o");
  args.push_back(bc.data());
  args.push_back("-x");
  args.push_back("c++");
  args.push_back(cpp.data());*/

  // args.push_back("opt -O3");
  args.push_back("-flto");
}