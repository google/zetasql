//
// Copyright 2018 Google LLC
// Copyright 2018 Asylo authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

#ifndef THIRD_PARTY_ZETASQL_ZETASQL_BASE_LOGGING_H_
#define THIRD_PARTY_ZETASQL_ZETASQL_BASE_LOGGING_H_

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <ostream>
#include <sstream>
#include <string>

#include "absl/base/attributes.h"
#include "absl/base/log_severity.h"

#define ZETASQL_INTERNAL_LOGGING_INFO \
  ::zetasql_base::logging_internal::LogMessage(__FILE__, __LINE__)
#define ZETASQL_INTERNAL_LOGGING_WARNING \
  ::zetasql_base::logging_internal::LogMessage( \
      __FILE__, __LINE__, absl::LogSeverity::kWarning)
#define ZETASQL_INTERNAL_LOGGING_ERROR \
  ::zetasql_base::logging_internal::LogMessage( \
      __FILE__, __LINE__, absl::LogSeverity::kError)
#define ZETASQL_INTERNAL_LOGGING_FATAL \
  ::zetasql_base::logging_internal::LogMessageFatal(__FILE__, __LINE__)

#define ZETASQL_INTERNAL_LOGGING_QFATAL ZETASQL_INTERNAL_LOGGING_FATAL

#ifdef NDEBUG
#define ZETASQL_INTERNAL_LOGGING_DFATAL ZETASQL_INTERNAL_LOGGING_ERROR
#else
#define ZETASQL_INTERNAL_LOGGING_DFATAL ZETASQL_INTERNAL_LOGGING_FATAL
#endif

#ifdef NDEBUG
#define ZETASQL_DEBUG_MODE false
#else
#define ZETASQL_DEBUG_MODE true
#endif

// Creates a message and logs it to file.
//
// ZETASQL_LOG(severity) returns a stream object that can be written to with the <<
// operator. Log messages are emitted with terminating newlines.
// Example:
//   ZETASQL_LOG(INFO) << "Found" << num_cookies << " cookies";
//
// severity: the severity of the log message, one of LogSeverity. The
//           FATAL severity will terminate the program after the log is emitted.
//           Must be exactly one of INFO WARNING ERROR FATAL QFATAL DFATAL
#define ZETASQL_LOG(severity) ZETASQL_INTERNAL_LOGGING_##severity.stream()

// A command to ZETASQL_LOG only if a condition is true. If the condition is false,
// nothing is logged.
// Example:
//
// ZETASQL_LOG_IF(INFO, num_cookies > 10) << "Got lots of cookies";
//
// severity: the severity of the log message, one of LogSeverity. The
//           FATAL severity will terminate the program after the log is emitted.
// condition: the condition that determines whether to log the message.
#define ZETASQL_LOG_IF(severity, condition)                              \
  !(condition) ? (void)0                                                 \
               : ::zetasql_base::logging_internal::LogMessageVoidify() & \
                     ZETASQL_INTERNAL_LOGGING_##severity.stream()

// A ZETASQL_LOG command with an associated verbosity level. The verbosity threshold
// may be configured at runtime with set_vlog_level and InitLogging.
//
// ZETASQL_VLOG statements are logged at INFO severity if they are logged at all.
// The numeric levels are on a different scale than the severity levels.
// Example:
//
//   ZETASQL_VLOG(1) << "Print when ZETASQL_VLOG level is set to be 1 or higher";
//
// level: the numeric level that determines whether to log the message.
#define ZETASQL_VLOG(level) \
  ZETASQL_LOG_IF(INFO, (level) <= ::zetasql_base::get_vlog_level())

// Terminates the program with a fatal error if the specified condition is
// false.
//
// Example:
//   ZETASQL_CHECK(!cheese.empty()) << "Out of Cheese";
//
//
// Might produce a message like:
//   "Check_failed: !cheese.empty() Out of Cheese"
#define ZETASQL_CHECK(condition) \
  ZETASQL_LOG_IF(FATAL, !(condition)) << ("Check failed: " #condition " ")

namespace zetasql_base {

// This formats a value for a failing CHECK_XX statement.  Ordinarily,
// it uses the definition for operator<<, with a few special cases below.
template <typename T>
inline void MakeCheckOpValueString(std::ostream *os, const T &v) {
  (*os) << v;
}

// Overrides for char types provide readable values for unprintable
// characters.
template <>
void MakeCheckOpValueString(std::ostream *os, const char &v);
template <>
void MakeCheckOpValueString(std::ostream *os, const signed char &v);
template <>
void MakeCheckOpValueString(std::ostream *os, const unsigned char &v);

// We need an explicit specialization for std::nullptr_t.
template <>
void MakeCheckOpValueString(std::ostream *os, const std::nullptr_t &v);

// A helper class for formatting "expr (V1 vs. V2)" in a CHECK_XX
// statement.  See MakeCheckOpString for sample usage.
class CheckOpMessageBuilder {
 public:
  // Constructs an object to format a CheckOp message. This constructor
  // initializes the message first with exprtext followed by " (".
  //
  // exprtext A string representation of the code in file at line.
  explicit CheckOpMessageBuilder(const char *exprtext);
  // Deletes "stream_".
  ~CheckOpMessageBuilder();
  // Gets the output stream for the first argument of the message.
  std::ostream *ForVar1() { return stream_; }
  // Gets the output stream for writing the argument of the message. This
  // writes " vs. " to the stream first.
  std::ostream *ForVar2();
  // Gets the built string contents. The stream is finished with an added ")".
  std::string *NewString();

 private:
  std::ostringstream *stream_;
};

template <typename T1, typename T2>
std::string *MakeCheckOpString(const T1 &v1, const T2 &v2,
                               const char *exprtext) {
  CheckOpMessageBuilder comb(exprtext);
  MakeCheckOpValueString(comb.ForVar1(), v1);
  MakeCheckOpValueString(comb.ForVar2(), v2);
  return comb.NewString();
}

// Helper functions for CHECK_OP macro.
// The (int, int) specialization works around the issue that the compiler
// will not instantiate the template version of the function on values of
// unnamed enum type - see comment below.
//
// name: an identifier that is the name of the comparison, such as
//       Check_EQ or Check_NE.
// op: the comparison operator, such as == or !=.
#define DEFINE_CHECK_OP_IMPL(name, op)                                   \
  template <typename T1, typename T2>                                    \
  inline std::string *name##Impl(const T1 &v1, const T2 &v2,             \
                                 const char *exprtext) {                 \
    if (v1 op v2) return nullptr;                                        \
    return MakeCheckOpString(v1, v2, exprtext);                          \
  }                                                                      \
  inline std::string *name##Impl(int v1, int v2, const char *exprtext) { \
    return name##Impl<int, int>(v1, v2, exprtext);                       \
  }

// We use the full name Check_EQ, Check_NE, etc.
//
// This is to prevent conflicts when the file including logging.h provides its
// own #defines for the simpler names EQ, NE, etc. This happens if, for
// example, those are used as token names in a yacc grammar.
DEFINE_CHECK_OP_IMPL(Check_EQ, ==)
DEFINE_CHECK_OP_IMPL(Check_NE, !=)
DEFINE_CHECK_OP_IMPL(Check_LE, <=)
DEFINE_CHECK_OP_IMPL(Check_LT, <)
DEFINE_CHECK_OP_IMPL(Check_GE, >=)
DEFINE_CHECK_OP_IMPL(Check_GT, >)
#undef DEFINE_CHECK_OP_IMPL

// Function is overloaded for integral types to allow static const
// integrals declared in classes and not defined to be used as arguments to
// ZETASQL_CHECK* macros. It's not encouraged though.
template <typename T>
inline const T &GetReferenceableValue(const T &t) {
  return t;
}


inline char GetReferenceableValue(char t) { return t; }
inline unsigned char GetReferenceableValue(unsigned char t) { return t; }
inline signed char GetReferenceableValue(signed char t) { return t; }
// NOLINTNEXTLINE(runtime/int)
inline short GetReferenceableValue(short t) { return t; }
// NOLINTNEXTLINE(runtime/int)
inline unsigned short GetReferenceableValue(unsigned short t) { return t; }
inline int GetReferenceableValue(int t) { return t; }
inline unsigned int GetReferenceableValue(unsigned int t) { return t; }
// NOLINTNEXTLINE(runtime/int)
inline long GetReferenceableValue(long t) { return t; }
// NOLINTNEXTLINE(runtime/int)
inline unsigned long GetReferenceableValue(unsigned long t) { return t; }
// NOLINTNEXTLINE(runtime/int)
inline long long GetReferenceableValue(long long t) { return t; }
// NOLINTNEXTLINE(runtime/int)
inline unsigned long long GetReferenceableValue(unsigned long long t) {
  return t;
}

// Compares val1 and val2 with op, and produces a ZETASQL_LOG(FATAL) if false.
//
// name An identifier that is the name of the comparison, such as
//        Check_EQ or Check_NE.
// op: comparison operator, such as == or !=.
// val1: first variable to be compared.
// val2: second variable to be compared.
#define ZETASQL_INTERNAL_CHECK_OP(name, op, val1, val2)                 \
  while (std::unique_ptr<std::string> _result =                         \
             std::unique_ptr<std::string>(::zetasql_base::name##Impl(   \
                 ::zetasql_base::GetReferenceableValue(val1),           \
                 ::zetasql_base::GetReferenceableValue(val2),           \
                 #val1 " " #op " " #val2)))                             \
  ::zetasql_base::logging_internal::LogMessageFatal(__FILE__, __LINE__, \
                                                    *_result)           \
      .stream()

// Produces a ZETASQL_LOG(FATAL) unless val1 equals val2.
#define ZETASQL_CHECK_EQ(val1, val2) \
  ZETASQL_INTERNAL_CHECK_OP(Check_EQ, ==, val1, val2)
// Produces a ZETASQL_LOG(FATAL) unless val1 does not equal to val2.
#define ZETASQL_CHECK_NE(val1, val2) \
  ZETASQL_INTERNAL_CHECK_OP(Check_NE, !=, val1, val2)
// Produces a ZETASQL_LOG(FATAL) unless val1 is less than or equal to val2.
#define ZETASQL_CHECK_LE(val1, val2) \
  ZETASQL_INTERNAL_CHECK_OP(Check_LE, <=, val1, val2)
// Produces a ZETASQL_LOG(FATAL) unless val1 is less than val2.
#define ZETASQL_CHECK_LT(val1, val2) \
  ZETASQL_INTERNAL_CHECK_OP(Check_LT, <, val1, val2)
// Produces a ZETASQL_LOG(FATAL) unless val1 is greater than or equal to val2.
#define ZETASQL_CHECK_GE(val1, val2) \
  ZETASQL_INTERNAL_CHECK_OP(Check_GE, >=, val1, val2)
// Produces a ZETASQL_LOG(FATAL) unless val1 is greater than val2.
#define ZETASQL_CHECK_GT(val1, val2) \
  ZETASQL_INTERNAL_CHECK_OP(Check_GT, >, val1, val2)

#define ZETASQL_DCHECK(c) ZETASQL_CHECK(c)
// Another alias for ZETASQL_CHECK that in the future may include more posix/errno
// related data.
#define ZETASQL_PCHECK(c) ZETASQL_CHECK(c)

// Another alias for ZETASQL_CHECK that in the future may log less verbosely.
#define ZETASQL_ZETASQL_CHECK(c) ZETASQL_CHECK(c)

#define ZETASQL_DCHECK_EQ(a, b) ZETASQL_CHECK_EQ(a, b)
#define ZETASQL_DCHECK_NE(a, b) ZETASQL_CHECK_NE(a, b)
#define ZETASQL_DCHECK_LE(a, b) ZETASQL_CHECK_LE(a, b)
#define ZETASQL_DCHECK_LT(a, b) ZETASQL_CHECK_LT(a, b)
#define ZETASQL_DCHECK_GE(a, b) ZETASQL_CHECK_GE(a, b)
#define ZETASQL_DCHECK_GT(a, b) ZETASQL_CHECK_GT(a, b)

#define ZETASQL_DLOG(c) ZETASQL_LOG(c)

#define ZETASQL_VLOG_IS_ON(level) ::zetasql_base::get_vlog_level() <= (level)

// Gets the verbosity threshold for ZETASQL_VLOG. A ZETASQL_VLOG command with a level greater
// than this will be ignored.
int get_vlog_level();

// Gets the log directory that was specified when initialized.
std::string get_log_directory();

// Initializes minimal logging library.
//
// This should be called in main().
//
// directory: log file directory.
// file_name: name of the log file (recommend this be initialized with argv[0]).
// level: verbosity threshold for ZETASQL_VLOG commands. A ZETASQL_VLOG command with
//        a level equal to or lower than it will be logged.
// Returns true if initialized successfully. Behavior is undefined false.
bool InitLogging(const char *directory, const char *file_name, int level);

namespace logging_internal {

// Class representing a log message created by a log macro.
class LogMessage {
 public:
  // Constructs a new message with INFO severity.
  //
  // file: source file that produced the log.
  // line: source code line that produced the log.
  LogMessage(const char *file, int line);

  // Constructs a new message with the specified severity.
  //
  // file: source file that produced the log.
  // line: source code line that produced the log.
  // severity: severity level of the log.
  LogMessage(const char *file, int line, absl::LogSeverity severity);

  // Constructs a log message with additional text that is provided by CHECK
  // macros.  Severity is implicitly FATAL.
  //
  // file: source file that produced the log.
  // line: source code line that produced the log.
  // result: result message of the failed check.
  LogMessage(const char *file, int line, const std::string &result);

  // The destructor flushes the message.
  ~LogMessage();

  LogMessage(const LogMessage &) = delete;
  void operator=(const LogMessage &) = delete;

  // Gets a reference to the underlying string stream.
  std::ostream &stream() { return stream_; }

 protected:
  void Flush();

 private:
  void Init(const char *file, int line, absl::LogSeverity severity);

  // Sends the message to print.
  void SendToLog(const std::string &message_text);

  // stream_ reads all the input messages into a stringstream, then it's
  // converted into a string in the destructor for printing.
  std::ostringstream stream_;
  const absl::LogSeverity severity_;
};

// This class is used just to take an ostream type and make it a void type to
// satisfy the ternary operator in ZETASQL_LOG_IF.
// operator& is used because it has precedence lower than << but higher than :?
class LogMessageVoidify {
 public:
  void operator&(const std::ostream &) {}
};

// Default LogSeverity FATAL version of LogMessage.
// Identical to LogMessage(..., FATAL), but see comments on destructor.
class LogMessageFatal : public LogMessage {
 public:
  // Constructs a new message with FATAL severity.
  //
  // file: source file that produced the log.
  // line: source code line that produced the log.
  LogMessageFatal(const char *file, int line)
    : LogMessage(file, line, absl::LogSeverity::kFatal) {}

  // Constructs a message with FATAL severity for use by ZETASQL_CHECK macros.
  //
  // file: source file that produced the log.
  // line: source code line that produced the log.
  // result: result message when check fails.
  LogMessageFatal(const char *file, int line, const std::string &result)
      : LogMessage(file, line, result) {}

  // Suppresses warnings in some cases, example:
  // if (impossible)
  //   ZETASQL_LOG(FATAL)
  // else
  //   return 0;
  // which would otherwise yield the following compiler warning.
  // "warning: control reaches end of non-void function [-Wreturn-type]"
  ABSL_ATTRIBUTE_NORETURN ~LogMessageFatal();
};

}  // namespace logging_internal

}  // namespace zetasql_base

#endif  // THIRD_PARTY_ZETASQL_ZETASQL_BASE_LOGGING_H_
