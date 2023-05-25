//
// Created by chiro on 23-5-25.
//

#include <csignal>
#include "env.h"
#include "system_clock.h"
#include "lang.h"
#include "sys_time.h"
#include "io_posix.h"
#include "string_util.h"
#include "unique_id_gen.h"

namespace aquafs {

class PosixClock : public SystemClock {
public:
  static const char *kClassName() { return "PosixClock"; }
  // const char* Name() const override { return kDefaultName(); }
  // const char* NickName() const override { return kClassName(); }

  uint64_t NowMicros() override {
    port::TimeVal tv;
    port::GetTimeOfDay(&tv, nullptr);
    return static_cast<uint64_t>(tv.tv_sec) * 1000000 + tv.tv_usec;
  }

  uint64_t NowNanos() override {
#if defined(OS_LINUX) || defined(OS_FREEBSD) || defined(OS_GNU_KFREEBSD) || \
    defined(OS_AIX)
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return static_cast<uint64_t>(ts.tv_sec) * 1000000000 + ts.tv_nsec;
#elif defined(OS_SOLARIS)
    return gethrtime();
#elif defined(__MACH__)
    clock_serv_t cclock;
    mach_timespec_t ts;
    host_get_clock_service(mach_host_self(), CALENDAR_CLOCK, &cclock);
    clock_get_time(cclock, &ts);
    mach_port_deallocate(mach_task_self(), cclock);
    return static_cast<uint64_t>(ts.tv_sec) * 1000000000 + ts.tv_nsec;
#else
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
#endif
  }

  uint64_t CPUMicros() override {
#if defined(OS_LINUX) || defined(OS_FREEBSD) || defined(OS_GNU_KFREEBSD) || \
    defined(OS_AIX) || (defined(__MACH__) && defined(__MAC_10_12))
    struct timespec ts;
    clock_gettime(CLOCK_THREAD_CPUTIME_ID, &ts);
    return (static_cast<uint64_t>(ts.tv_sec) * 1000000000 + ts.tv_nsec) / 1000;
#endif
    return 0;
  }

  uint64_t CPUNanos() override {
#if defined(OS_LINUX) || defined(OS_FREEBSD) || defined(OS_GNU_KFREEBSD) || \
    defined(OS_AIX) || (defined(__MACH__) && defined(__MAC_10_12))
    struct timespec ts;
    clock_gettime(CLOCK_THREAD_CPUTIME_ID, &ts);
    return static_cast<uint64_t>(ts.tv_sec) * 1000000000 + ts.tv_nsec;
#endif
    return 0;
  }

  void SleepForMicroseconds(int micros) override { usleep(micros); }

  Status GetCurrentTime(int64_t *unix_time) override {
    time_t ret = time(nullptr);
    if (ret == (time_t) -1) {
      return IOError("GetCurrentTime", "", errno);
    }
    *unix_time = (int64_t) ret;
    return Status::OK();
  }

  std::string TimeToString(uint64_t secondsSince1970) override {
    const auto seconds = (time_t) secondsSince1970;
    struct tm t{};
    int maxsize = 64;
    std::string dummy;
    dummy.reserve(maxsize);
    dummy.resize(maxsize);
    char *p = &dummy[0];
    port::LocalTimeR(&seconds, &t);
    snprintf(p, maxsize, "%04d/%02d/%02d-%02d:%02d:%02d ", t.tm_year + 1900,
             t.tm_mon + 1, t.tm_mday, t.tm_hour, t.tm_min, t.tm_sec);
    return dummy;
  }
};

std::string Env::GenerateUniqueId() {
  std::string result;
  bool success = port::GenerateRfcUuid(&result);
  if (!success) {
    // Fall back on our own way of generating a unique ID and adapt it to
    // RFC 4122 variant 1 version 4 (a random ID).
    // https://en.wikipedia.org/wiki/Universally_unique_identifier
    // We already tried GenerateRfcUuid so no need to try it again in
    // GenerateRawUniqueId
    constexpr bool exclude_port_uuid = true;
    uint64_t upper, lower;
    GenerateRawUniqueId(&upper, &lower, exclude_port_uuid);

    // Set 4-bit version to 4
    upper = (upper & (~uint64_t{0xf000})) | 0x4000;
    // Set unary-encoded variant to 1 (0b10)
    lower = (lower & (~(uint64_t{3} << 62))) | (uint64_t{2} << 62);

    // Use 36 character format of RFC 4122
    result.resize(36U);
    char *buf = &result[0];
    PutBaseChars<16>(&buf, 8, upper >> 32, /*!uppercase*/ false);
    *(buf++) = '-';
    PutBaseChars<16>(&buf, 4, upper >> 16, /*!uppercase*/ false);
    *(buf++) = '-';
    PutBaseChars<16>(&buf, 4, upper, /*!uppercase*/ false);
    *(buf++) = '-';
    PutBaseChars<16>(&buf, 4, lower >> 48, /*!uppercase*/ false);
    *(buf++) = '-';
    PutBaseChars<16>(&buf, 12, lower, /*!uppercase*/ false);
    assert(buf == &result[36]);

    // Verify variant 1 version 4
    assert(result[14] == '4');
    assert(result[19] == '8' || result[19] == '9' || result[19] == 'a' ||
           result[19] == 'b');
  }
  return result;
}

// SequentialFile::~SequentialFile() {}
//
// RandomAccessFile::~RandomAccessFile() {}
//
// WritableFile::~WritableFile() {}

MemoryMappedFileBuffer::~MemoryMappedFileBuffer() {}

// Logger::~Logger() {}

// Status Logger::Close() {
//   if (!closed_) {
//     closed_ = true;
//     return CloseImpl();
//   } else {
//     return Status::OK();
//   }
// }
//
// Status Logger::CloseImpl() { return Status::NotSupported(); }

FileLock::~FileLock() {}

void LogFlush(Logger *info_log) {
  if (info_log) {
    info_log->Flush();
  }
}

static void Logv(Logger *info_log, const char *format, va_list ap) {
  if (info_log && info_log->GetInfoLogLevel() <= InfoLogLevel::INFO_LEVEL) {
    info_log->Logv(InfoLogLevel::INFO_LEVEL, format, ap);
  }
}

void Log(Logger *info_log, const char *format, ...) {
  va_list ap;
  va_start(ap, format);
  Logv(info_log, format, ap);
  va_end(ap);
}

void Logger::Logv(const InfoLogLevel log_level, const char *format,
                  va_list ap) {
  static const char *kInfoLogLevelNames[5] = {"DEBUG", "INFO", "WARN", "ERROR",
                                              "FATAL"};
  if (log_level < log_level_) {
    return;
  }

  if (log_level == InfoLogLevel::INFO_LEVEL) {
    // Doesn't print log level if it is INFO level.
    // This is to avoid unexpected performance regression after we add
    // the feature of log level. All the logs before we add the feature
    // are INFO level. We don't want to add extra costs to those existing
    // logging.
    Logv(format, ap);
  } else if (log_level == InfoLogLevel::HEADER_LEVEL) {
    LogHeader(format, ap);
  } else {
    char new_format[500];
    snprintf(new_format, sizeof(new_format) - 1, "[%s] %s",
             kInfoLogLevelNames[log_level], format);
    Logv(new_format, ap);
  }

  if (log_level >= InfoLogLevel::WARN_LEVEL &&
      log_level != InfoLogLevel::HEADER_LEVEL) {
    // Log messages with severity of warning or higher should be rare and are
    // sometimes followed by an unclean crash. We want to be sure important
    // messages are not lost in an application buffer when that happens.
    Flush();
  }
}

static void Logv(const InfoLogLevel log_level, Logger *info_log,
                 const char *format, va_list ap) {
  if (info_log && info_log->GetInfoLogLevel() <= log_level) {
    if (log_level == InfoLogLevel::HEADER_LEVEL) {
      info_log->LogHeader(format, ap);
    } else {
      info_log->Logv(log_level, format, ap);
    }
  }
}

void Log(const InfoLogLevel log_level, Logger *info_log, const char *format,
         ...) {
  va_list ap;
  va_start(ap, format);
  Logv(log_level, info_log, format, ap);
  va_end(ap);
}

static void Headerv(Logger *info_log, const char *format, va_list ap) {
  if (info_log) {
    info_log->LogHeader(format, ap);
  }
}

void Header(Logger *info_log, const char *format, ...) {
  va_list ap;
  va_start(ap, format);
  Headerv(info_log, format, ap);
  va_end(ap);
}

static void Debugv(Logger *info_log, const char *format, va_list ap) {
  if (info_log && info_log->GetInfoLogLevel() <= InfoLogLevel::DEBUG_LEVEL) {
    info_log->Logv(InfoLogLevel::DEBUG_LEVEL, format, ap);
  }
}

void Debug(Logger *info_log, const char *format, ...) {
  va_list ap;
  va_start(ap, format);
  Debugv(info_log, format, ap);
  va_end(ap);
}

static void Infov(Logger *info_log, const char *format, va_list ap) {
  if (info_log && info_log->GetInfoLogLevel() <= InfoLogLevel::INFO_LEVEL) {
    info_log->Logv(InfoLogLevel::INFO_LEVEL, format, ap);
  }
}

void Info(Logger *info_log, const char *format, ...) {
  va_list ap;
  va_start(ap, format);
  Infov(info_log, format, ap);
  va_end(ap);
}

static void Warnv(Logger *info_log, const char *format, va_list ap) {
  if (info_log && info_log->GetInfoLogLevel() <= InfoLogLevel::WARN_LEVEL) {
    info_log->Logv(InfoLogLevel::WARN_LEVEL, format, ap);
  }
}

void Warn(Logger *info_log, const char *format, ...) {
  va_list ap;
  va_start(ap, format);
  Warnv(info_log, format, ap);
  va_end(ap);
}

static void Errorv(Logger *info_log, const char *format, va_list ap) {
  if (info_log && info_log->GetInfoLogLevel() <= InfoLogLevel::ERROR_LEVEL) {
    info_log->Logv(InfoLogLevel::ERROR_LEVEL, format, ap);
  }
}

void Error(Logger *info_log, const char *format, ...) {
  va_list ap;
  va_start(ap, format);
  Errorv(info_log, format, ap);
  va_end(ap);
}

static void Fatalv(Logger *info_log, const char *format, va_list ap) {
  if (info_log && info_log->GetInfoLogLevel() <= InfoLogLevel::FATAL_LEVEL) {
    info_log->Logv(InfoLogLevel::FATAL_LEVEL, format, ap);
  }
}

void Fatal(Logger *info_log, const char *format, ...) {
  va_list ap;
  va_start(ap, format);
  Fatalv(info_log, format, ap);
  va_end(ap);
}

// void LogFlush(const std::shared_ptr<Logger>& info_log) {
//   LogFlush(info_log.get());
// }

void Log(const InfoLogLevel log_level, const std::shared_ptr<Logger> &info_log,
         const char *format, ...) {
  va_list ap;
  va_start(ap, format);
  Logv(log_level, info_log.get(), format, ap);
  va_end(ap);
}

void Header(const std::shared_ptr<Logger> &info_log, const char *format, ...) {
  va_list ap;
  va_start(ap, format);
  Headerv(info_log.get(), format, ap);
  va_end(ap);
}

void Debug(const std::shared_ptr<Logger> &info_log, const char *format, ...) {
  va_list ap;
  va_start(ap, format);
  Debugv(info_log.get(), format, ap);
  va_end(ap);
}

void Info(const std::shared_ptr<Logger> &info_log, const char *format, ...) {
  va_list ap;
  va_start(ap, format);
  Infov(info_log.get(), format, ap);
  va_end(ap);
}

void Warn(const std::shared_ptr<Logger> &info_log, const char *format, ...) {
  va_list ap;
  va_start(ap, format);
  Warnv(info_log.get(), format, ap);
  va_end(ap);
}

void Error(const std::shared_ptr<Logger> &info_log, const char *format, ...) {
  va_list ap;
  va_start(ap, format);
  Errorv(info_log.get(), format, ap);
  va_end(ap);
}

void Fatal(const std::shared_ptr<Logger> &info_log, const char *format, ...) {
  va_list ap;
  va_start(ap, format);
  Fatalv(info_log.get(), format, ap);
  va_end(ap);
}

void Log(const std::shared_ptr<Logger> &info_log, const char *format, ...) {
  va_list ap;
  va_start(ap, format);
  Logv(info_log.get(), format, ap);
  va_end(ap);
}

// Status WriteStringToFile(Env* env, const Slice& data, const std::string& fname,
//                          bool should_sync) {
//   const auto& fs = env->GetFileSystem();
//   return WriteStringToFile(fs.get(), data, fname, should_sync);
// }
//
// Status ReadFileToString(Env* env, const std::string& fname, std::string* data) {
//   const auto& fs = env->GetFileSystem();
//   return ReadFileToString(fs.get(), fname, data);
// }

//
// Default Posix SystemClock
//
const std::shared_ptr<SystemClock> &SystemClock::Default() {
  STATIC_AVOID_DESTRUCTION(std::shared_ptr<SystemClock>, instance)
      (std::make_shared<PosixClock>());
  return instance;
}

Status Env::GetHostNameString(std::string *result) {
  std::array<char, kMaxHostNameLen> hostname_buf{};
  Status s = GetHostName(hostname_buf.data(), hostname_buf.size());
  if (s.ok()) {
    hostname_buf[hostname_buf.size() - 1] = '\0';
    result->assign(hostname_buf.data());
  }
  return s;
}

Status Env::GetHostName(char *name, uint64_t len) {
  int ret = gethostname(name, static_cast<size_t>(len));
  if (ret < 0) {
    if (errno == EFAULT || errno == EINVAL) {
      return Status::InvalidArgument(errnoStr(errno).c_str());
    } else
      return IOError("GetHostName", name, errno);

  }
  return Status::OK();
}

Status Env::GetCurrentTime(int64_t *unix_time) {
  time_t ret = time(nullptr);
  if (ret == (time_t) -1) {
    return IOError("GetCurrentTime", "", errno);
  }
  *unix_time = (int64_t) ret;
  return Status::OK();
}
}