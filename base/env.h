//
// Created by chiro on 23-5-25.
//

#ifndef AQUAFS_ENV_H
#define AQUAFS_ENV_H

#include "file_system.h"

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

const size_t kDefaultPageSize = 4 * 1024;

class Env {
public:
  static Env *Default() {
    static Env static_env{};
    return &static_env;
  }

  bool use_direct_reads = false;

  std::string GenerateUniqueId() { return "todo"; }
};

// A file abstraction for random reading and writing.
class RandomRWFile {
public:
  RandomRWFile() {}

  // No copying allowed
  RandomRWFile(const RandomRWFile &) = delete;

  RandomRWFile &operator=(const RandomRWFile &) = delete;

  virtual ~RandomRWFile() {}

  // Indicates if the class makes use of direct I/O
  // If false you must pass aligned buffer to Write()
  virtual bool use_direct_io() const { return false; }

  // Use the returned alignment value to allocate
  // aligned buffer for Direct I/O
  virtual size_t GetRequiredBufferAlignment() const { return kDefaultPageSize; }

  // Write bytes in `data` at  offset `offset`, Returns Status::OK() on success.
  // Pass aligned buffer when use_direct_io() returns true.
  virtual Status Write(uint64_t offset, const Slice &data) = 0;

  // Read up to `n` bytes starting from offset `offset` and store them in
  // result, provided `scratch` size should be at least `n`.
  //
  // After call, result->size() < n only if end of file has been
  // reached (or non-OK status). Read might fail if called again after
  // first result->size() < n.
  //
  // Returns Status::OK() on success.
  virtual Status Read(uint64_t offset, size_t n, Slice *result,
                      char *scratch) const = 0;

  virtual Status Flush() = 0;

  virtual Status Sync() = 0;

  virtual Status Fsync() { return Sync(); }

  virtual Status Close() = 0;

  // If you're adding methods here, remember to add them to
  // RandomRWFileWrapper too.
};

// MemoryMappedFileBuffer object represents a memory-mapped file's raw buffer.
// Subclasses should release the mapping upon destruction.
class MemoryMappedFileBuffer {
public:
  MemoryMappedFileBuffer(void *_base, size_t _length)
      : base_(_base), length_(_length) {}

  virtual ~MemoryMappedFileBuffer() = 0;

  // We do not want to unmap this twice. We can make this class
  // movable if desired, however, since
  MemoryMappedFileBuffer(const MemoryMappedFileBuffer &) = delete;

  MemoryMappedFileBuffer &operator=(const MemoryMappedFileBuffer &) = delete;

  void *GetBase() const { return base_; }

  size_t GetLen() const { return length_; }

protected:
  void *base_;
  const size_t length_;
};

// Directory object represents collection of files and implements
// filesystem operations that can be executed on directories.
class Directory {
public:
  virtual ~Directory() {}

  // Fsync directory. Can be called concurrently from multiple threads.
  virtual Status Fsync() = 0;

  // Close directory.
  virtual Status Close() { return Status::NotSupported("Close"); }

  virtual size_t GetUniqueId(char * /*id*/, size_t /*max_size*/) const {
    return 0;
  }

  // If you're adding methods here, remember to add them to
  // DirectoryWrapper too.
};

}

#endif //AQUAFS_ENV_H
