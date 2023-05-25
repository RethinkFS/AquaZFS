//
// Created by chiro on 23-5-25.
//

#ifndef AQUAFS_ENV_H
#define AQUAFS_ENV_H

#include <cassert>
#include <cstdarg>
#include <memory>
#include <chrono>
#include <unistd.h>

#include "status.h"

#if defined(__GNUC__) || defined(__clang__)
#define AQUAFS_PRINTF_FORMAT_ATTR(format_param, dots_param) \
  __attribute__((__format__(__printf__, format_param, dots_param)))
#else
#define AQUAFS_PRINTF_FORMAT_ATTR(format_param, dots_param)
#endif

namespace aquafs {

extern std::string errnoStr(int err);

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

  // Write an entry to the log file with the specified log level
  // and format.  Any log with level under the internal log level
  // of *this (see @SetInfoLogLevel and @GetInfoLogLevel) will not be
  // printed.
  virtual void Logv(const InfoLogLevel log_level, const char *format,
                    va_list ap);

  // Write a header to the log file with the specified format
  // It is recommended that you log all header information at the start of the
  // application. But it is not enforced.
  virtual void LogHeader(const char *format, va_list ap) {
    // Default implementation does a simple INFO level log write.
    // Please override as per the logger class requirement.
    Logv(InfoLogLevel::INFO_LEVEL, format, ap);
  }

  // Flush to the OS buffers
  virtual void Flush() {}

  void SetInfoLogLevel(InfoLogLevel logLevel) {
    log_level_ = logLevel;
  }

  InfoLogLevel GetInfoLogLevel() const {
    return log_level_;
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

  // If true, then use mmap to read data.
  // Not recommended for 32-bit OS.
  bool use_mmap_reads = false;

  // If true, then use mmap to write data
  bool use_mmap_writes = true;

  // If true, then use O_DIRECT for reading data
  bool use_direct_reads = false;

  // If true, then use O_DIRECT for writing data
  bool use_direct_writes = false;

  // If false, fallocate() calls are bypassed
  bool allow_fallocate = true;

  // If true, set the FD_CLOEXEC on open fd.
  bool set_fd_cloexec = true;

  // Allows OS to incrementally sync files to disk while they are being
  // written, in the background. Issue one request for every bytes_per_sync
  // written. 0 turns it off.
  // Default: 0
  uint64_t bytes_per_sync = 0;

  // When true, guarantees the file has at most `bytes_per_sync` bytes submitted
  // for writeback at any given time.
  //
  //  - If `sync_file_range` is supported it achieves this by waiting for any
  //    prior `sync_file_range`s to finish before proceeding. In this way,
  //    processing (compression, etc.) can proceed uninhibited in the gap
  //    between `sync_file_range`s, and we block only when I/O falls behind.
  //  - Otherwise the `WritableFile::Sync` method is used. Note this mechanism
  //    always blocks, thus preventing the interleaving of I/O and processing.
  //
  // Note: Enabling this option does not provide any additional persistence
  // guarantees, as it may use `sync_file_range`, which does not write out
  // metadata.
  //
  // Default: false
  bool strict_bytes_per_sync = false;

  // If true, we will preallocate the file with FALLOC_FL_KEEP_SIZE flag, which
  // means that file size won't change as part of preallocation.
  // If false, preallocation will also change the file size. This option will
  // improve the performance in workloads where you sync the data on every
  // write. By default, we set it to true for MANIFEST writes and false for
  // WAL writes
  bool fallocate_with_keep_size = true;

  std::string GenerateUniqueId();

  uint64_t NowMicros() {
    auto now = std::chrono::high_resolution_clock::now();
    auto micros = std::chrono::duration_cast<std::chrono::microseconds>(now.time_since_epoch());
    return micros.count();
  }

  // Returns the number of nano-seconds since some fixed point in time. Only
  // useful for computing deltas of time in one run.
  // Default implementation simply relies on NowMicros.
  // In platform-specific implementations, NowNanos() should return time points
  // that are MONOTONIC.
  uint64_t NowNanos() { return NowMicros() * 1000; }

  uint64_t GetThreadID() const {
    uint64_t thread_id = 0;
#if defined(_GNU_SOURCE) && defined(__GLIBC_PREREQ)
#if __GLIBC_PREREQ(2, 30)
    thread_id = ::gettid();
#else   // __GLIBC_PREREQ(2, 30)
    pthread_t tid = pthread_self();
    memcpy(&thread_id, &tid, std::min(sizeof(thread_id), sizeof(tid)));
#endif  // __GLIBC_PREREQ(2, 30)
#else   // defined(_GNU_SOURCE) && defined(__GLIBC_PREREQ)
    pthread_t tid = pthread_self();
    memcpy(&thread_id, &tid, std::min(sizeof(thread_id), sizeof(tid)));
#endif  // defined(_GNU_SOURCE) && defined(__GLIBC_PREREQ)
    return thread_id;
  }

  // Get the current host name as a null terminated string iff the string
  // length is < len. The hostname should otherwise be truncated to len.
  Status GetHostName(char *name, uint64_t len);

  // Get the current hostname from the given env as a std::string in result.
  // The result may be truncated if the hostname is too
  // long
  Status GetHostNameString(std::string *result);

  // Get the number of seconds since the Epoch, 1970-01-01 00:00:00 (UTC).
  // Only overwrites *unix_time on success.
  Status GetCurrentTime(int64_t *unix_time);

private:
  static const size_t kMaxHostNameLen = 256;
};

// A file abstraction for random reading and writing.
class RandomRWFile {
public:
  RandomRWFile() = default;

  // No copying allowed
  RandomRWFile(const RandomRWFile &) = delete;

  RandomRWFile &operator=(const RandomRWFile &) = delete;

  virtual ~RandomRWFile() = default;

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

class EnvLogger : public Logger {
public:
  EnvLogger() : Logger(InfoLogLevel::DEBUG_LEVEL) {}
};

}

#endif //AQUAFS_ENV_H
