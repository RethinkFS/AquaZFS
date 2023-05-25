//
// Created by chiro on 23-5-25.
//

#include <csignal>
#include "env.h"
#include "system_clock.h"
#include "lang.h"
#include "sys_time.h"
#include "io_posix.h"

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

//
// Default Posix SystemClock
//
const std::shared_ptr<SystemClock> &SystemClock::Default() {
  STATIC_AVOID_DESTRUCTION(std::shared_ptr<SystemClock>, instance)
      (std::make_shared<PosixClock>());
  return instance;
}
}