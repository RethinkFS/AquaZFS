//
// Created by chiro on 23-5-25.
//

#ifndef AQUAFS_ENV_H
#define AQUAFS_ENV_H

#if defined(__GNUC__) || defined(__clang__)
#define AQUAFS_PRINTF_FORMAT_ATTR(format_param, dots_param) \
  __attribute__((__format__(__printf__, format_param, dots_param)))
#else
#define AQUAFS_PRINTF_FORMAT_ATTR(format_param, dots_param)
#endif

namespace aquafs {
enum InfoLogLevel : unsigned char {
  DEBUG_LEVEL = 0,
  INFO_LEVEL,
  WARN_LEVEL,
  ERROR_LEVEL,
  FATAL_LEVEL,
  HEADER_LEVEL,
  NUM_INFO_LOG_LEVELS,
};

class Logger {
private:
  InfoLogLevel log_level_;
protected:
public:
  explicit Logger(InfoLogLevel logLevel) : log_level_(logLevel) {}

public:
  // Write an entry to the log file with the specified format.
  //
  // Users who override the `Logv()` overload taking `InfoLogLevel` do not need
  // to implement this, unless they explicitly invoke it in
  // `Logv(InfoLogLevel, ...)`.
  virtual void Logv(const char * /* format */, va_list /* ap */) {
    assert(false);
  }

  void SetInfoLogLevel(InfoLogLevel logLevel) {
    log_level_ = logLevel;
  }
};

extern void Debug(const std::shared_ptr<Logger> &info_log, const char *format,
                  ...) AQUAFS_PRINTF_FORMAT_ATTR(2, 3);

extern void Info(const std::shared_ptr<Logger> &info_log, const char *format,
                 ...) AQUAFS_PRINTF_FORMAT_ATTR(2, 3);

extern void Warn(const std::shared_ptr<Logger> &info_log, const char *format,
                 ...) AQUAFS_PRINTF_FORMAT_ATTR(2, 3);

extern void Error(const std::shared_ptr<Logger> &info_log, const char *format,
                  ...) AQUAFS_PRINTF_FORMAT_ATTR(2, 3);

extern void Fatal(const std::shared_ptr<Logger> &info_log, const char *format,
                  ...) AQUAFS_PRINTF_FORMAT_ATTR(2, 3);

class Env {
public:
  static Env *Default() {
    static Env static_env{};
    return &static_env;
  }

  std::string GenerateUniqueId() { return "todo"; }
};

}

#endif //AQUAFS_ENV_H
