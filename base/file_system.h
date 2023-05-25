//
// Created by chiro on 23-5-25.
//

#ifndef AQUAFS_FILE_SYSTEM_H
#define AQUAFS_FILE_SYSTEM_H

#include <unordered_map>
#include <map>
#include <functional>
#include "status.h"
#include "io_status.h"
#include "env.h"

namespace aquafs {

struct FileAttributes {
  // File name
  std::string name;

  // Size of file in bytes
  uint64_t size_bytes;
};

// Priority for requesting bytes in rate limiter scheduler
enum IOPriority {
  IO_LOW = 0,
  IO_MID = 1,
  IO_HIGH = 2,
  IO_USER = 3,
  IO_TOTAL = 4
};

// DEPRECATED
// Priority of an IO request. This is a hint and does not guarantee any
// particular QoS.
// IO_LOW - Typically background reads/writes such as compaction/flush
// IO_HIGH - Typically user reads/synchronous WAL writes
enum class KIOPriority : uint8_t {
  kIOLow,
  kIOHigh,
  kIOTotal,
};

// Type of the data begin read/written. It can be passed down as a flag
// for the FileSystem implementation to optionally handle different types in
// different ways
enum class IOType : uint8_t {
  kData,
  kFilter,
  kIndex,
  kMetadata,
  kWAL,
  kManifest,
  kLog,
  kUnknown,
  kInvalid,
};

// Per-request options that can be passed down to the FileSystem
// implementation. These are hints and are not necessarily guaranteed to be
// honored. More hints can be added here in the future to indicate things like
// storage media (HDD/SSD) to be used, replication level etc.
struct IOOptions {
  // Timeout for the operation in microseconds
  std::chrono::microseconds timeout;

  // DEPRECATED
  // Priority - high or low
  KIOPriority prio;

  // Priority used to charge rate limiter configured in file system level (if
  // any)
  // Limitation: right now RocksDB internal does not consider this
  // rate_limiter_priority
  IOPriority rate_limiter_priority;

  // Type of data being read/written
  IOType type;

  // EXPERIMENTAL
  // An option map that's opaque to RocksDB. It can be used to implement a
  // custom contract between a FileSystem user and the provider. This is only
  // useful in cases where a RocksDB user directly uses the FileSystem or file
  // object for their own purposes, and wants to pass extra options to APIs
  // such as NewRandomAccessFile and NewWritableFile.
  std::unordered_map<std::string, std::string> property_bag;

  // Force directory fsync, some file systems like btrfs may skip directory
  // fsync, set this to force the fsync
  bool force_dir_fsync;

  // Can be used by underlying file systems to skip recursing through sub
  // directories and list only files in GetChildren API.
  bool do_not_recurse;

  IOOptions() : IOOptions(false) {}

  explicit IOOptions(bool force_dir_fsync_)
      : timeout(std::chrono::microseconds::zero()),
        prio(KIOPriority::kIOLow),
        rate_limiter_priority(IO_TOTAL),
        type(IOType::kUnknown),
        force_dir_fsync(force_dir_fsync_),
        do_not_recurse(false) {}
};

// Types of checksums to use for checking integrity of logical blocks within
// files. All checksums currently use 32 bits of checking power (1 in 4B
// chance of failing to detect random corruption).
enum ChecksumType : char {
  kNoChecksum = 0x0,
  kCRC32c = 0x1,
  kxxHash = 0x2,
  kxxHash64 = 0x3,
  kXXH3 = 0x4,  // Supported since RocksDB 6.27
};

// Temperature of a file. Used to pass to FileSystem for a different
// placement and/or coding.
// Reserve some numbers in the middle, in case we need to insert new tier
// there.
enum class Temperature : uint8_t {
  kUnknown = 0,
  kHot = 0x04,
  kWarm = 0x08,
  kCold = 0x0C,
  kLastTemperature,
};

struct DirFsyncOptions {
  enum FsyncReason : uint8_t {
    kNewFileSynced,
    kFileRenamed,
    kDirRenamed,
    kFileDeleted,
    kDefault,
  } reason;

  std::string renamed_new_name;  // for kFileRenamed
  // add other options for other FsyncReason

  DirFsyncOptions();

  explicit DirFsyncOptions(std::string file_renamed_new_name);

  explicit DirFsyncOptions(FsyncReason fsync_reason);
};

// File scope options that control how a file is opened/created and accessed
// while its open. We may add more options here in the future such as
// redundancy level, media to use etc.
struct FileOptions {
  // Embedded IOOptions to control the parameters for any IOs that need
  // to be issued for the file open/creation
  IOOptions io_options;

  // EXPERIMENTAL
  // The feature is in development and is subject to change.
  // When creating a new file, set the temperature of the file so that
  // underlying file systems can put it with appropriate storage media and/or
  // coding.
  Temperature temperature = Temperature::kUnknown;

  // The checksum type that is used to calculate the checksum value for
  // handoff during file writes.
  ChecksumType handoff_checksum_type;

  FileOptions() : handoff_checksum_type(ChecksumType::kCRC32c) {}

  FileOptions(const FileOptions &opts) :
      io_options(opts.io_options),
      temperature(opts.temperature),
      handoff_checksum_type(opts.handoff_checksum_type) {}

  FileOptions &operator=(const FileOptions &) = default;
};

// A structure to pass back some debugging information from the FileSystem
// implementation to RocksDB in case of an IO error
struct IODebugContext {
  // file_path to be filled in by RocksDB in case of an error
  std::string file_path;

  // A map of counter names to values - set by the FileSystem implementation
  std::map<std::string, uint64_t> counters;

  // To be set by the FileSystem implementation
  std::string msg;

  // To be set by the underlying FileSystem implementation.
  std::string request_id;

  // In order to log required information in IO tracing for different
  // operations, Each bit in trace_data stores which corresponding info from
  // IODebugContext will be added in the trace. Foreg, if trace_data = 1, it
  // means bit at position 0 is set so TraceData::kRequestID (request_id) will
  // be logged in the trace record.
  //
  enum TraceData : char {
    // The value of each enum represents the bitwise position for
    // that information in trace_data which will be used by IOTracer for
    // tracing. Make sure to add them sequentially.
    kRequestID = 0,
  };
  uint64_t trace_data = 0;

  IODebugContext() {}

  void AddCounter(std::string &name, uint64_t value) {
    counters.emplace(name, value);
  }

  // Called by underlying file system to set request_id and log request_id in
  // IOTracing.
  void SetRequestId(const std::string &_request_id) {
    request_id = _request_id;
    trace_data |= (1 << TraceData::kRequestID);
  }

  std::string ToString() {
    std::ostringstream ss;
    ss << file_path << ", ";
    for (auto counter: counters) {
      ss << counter.first << " = " << counter.second << ",";
    }
    ss << msg;
    return ss.str();
  }
};

// A function pointer type for custom destruction of void pointer passed to
// ReadAsync API. RocksDB/caller is responsible for deleting the void pointer
// allocated by FS in ReadAsync API.
using IOHandleDeleter = std::function<void(void *)>;

// Identifies a locked file. Except in custom Env/Filesystem implementations,
// the lifetime of a FileLock object should be managed only by LockFile() and
// UnlockFile().
class FileLock {
public:
  FileLock() {}

  virtual ~FileLock();

private:
  // No copying allowed
  FileLock(const FileLock &) = delete;

  void operator=(const FileLock &) = delete;
};

class FileSystem {
public:
  FileSystem();

  // No copying allowed
  FileSystem(const FileSystem &) = delete;

  virtual ~FileSystem();

  static const char *Type() { return "FileSystem"; }

  static const char *kDefaultName() { return "DefaultFileSystem"; }

  // Loads the FileSystem specified by the input value into the result
  // @see Customizable for a more detailed description of the parameters and
  // return codes
  // @param config_options Controls how the FileSystem is loaded
  // @param value The name and optional properties describing the file system
  //      to load.
  // @param result On success, returns the loaded FileSystem
  // @return OK if the FileSystem was successfully loaded.
  // @return not-OK if the load failed.
  static Status CreateFromString(const std::string &value,
                                 std::shared_ptr<FileSystem> *result);

  // Return a default FileSystem suitable for the current operating
  // system.
  static std::shared_ptr<FileSystem> Default();

  virtual const char *Name() const { return "FileSystem"; }

  // Handles the event when a new DB or a new ColumnFamily starts using the
  // specified data paths.
  //
  // The data paths might be shared by different DBs or ColumnFamilies,
  // so RegisterDbPaths might be called with the same data paths.
  // For example, when CreateColumnFamily is called multiple times with the same
  // data path, RegisterDbPaths will also be called with the same data path.
  //
  // If the return status is ok, then the paths must be correspondingly
  // called in UnregisterDbPaths;
  // otherwise this method should have no side effect, and UnregisterDbPaths
  // do not need to be called for the paths.
  //
  // Different implementations may take different actions.
  // By default, it's a no-op and returns Status::OK.
  virtual Status RegisterDbPaths(const std::vector<std::string> & /*paths*/) {
    return Status::OK();
  }

  // Handles the event a DB or a ColumnFamily stops using the specified data
  // paths.
  //
  // It should be called corresponding to each successful RegisterDbPaths.
  //
  // Different implementations may take different actions.
  // By default, it's a no-op and returns Status::OK.
  virtual Status UnregisterDbPaths(const std::vector<std::string> & /*paths*/) {
    return Status::OK();
  }

  // Create a brand new sequentially-readable file with the specified name.
  // On success, stores a pointer to the new file in *result and returns OK.
  // On failure stores nullptr in *result and returns non-OK.  If the file does
  // not exist, returns a non-OK status.
  //
  // The returned file will only be accessed by one thread at a time.
  // virtual IOStatus NewSequentialFile(const std::string &fname,
  //                                    const FileOptions &file_opts,
  //                                    std::unique_ptr<FSSequentialFile> *result,
  //                                    IODebugContext *dbg) = 0;

  // Create a brand new random access read-only file with the
  // specified name.  On success, stores a pointer to the new file in
  // *result and returns OK.  On failure stores nullptr in *result and
  // returns non-OK.  If the file does not exist, returns a non-OK
  // status.
  //
  // The returned file may be concurrently accessed by multiple threads.
  // virtual IOStatus NewRandomAccessFile(
  //     const std::string &fname, const FileOptions &file_opts,
  //     std::unique_ptr<FSRandomAccessFile> *result, IODebugContext *dbg) = 0;

  // These values match Linux definition
  // https://git.kernel.org/pub/scm/linux/kernel/git/torvalds/linux.git/tree/include/uapi/linux/fcntl.h#n56
  enum WriteLifeTimeHint {
    kWLTHNotSet = 0,  // No hint information set
    kWLTHNone,        // No hints about write life time
    kWLTHShort,       // Data written has a short life time
    kWLTHMedium,      // Data written has a medium life time
    kWLTHLong,        // Data written has a long life time
    kWLTHExtreme,     // Data written has an extremely long life time
  };

  // Create an object that writes to a new file with the specified
  // name.  Deletes any existing file with the same name and creates a
  // new file.  On success, stores a pointer to the new file in
  // *result and returns OK.  On failure stores nullptr in *result and
  // returns non-OK.
  //
  // The returned file will only be accessed by one thread at a time.
  // virtual IOStatus NewWritableFile(const std::string &fname,
  //                                  const FileOptions &file_opts,
  //                                  std::unique_ptr<FSWritableFile> *result,
  //                                  IODebugContext *dbg) = 0;

  // Create an object that writes to a file with the specified name.
  // `FSWritableFile::Append()`s will append after any existing content.  If the
  // file does not already exist, creates it.
  //
  // On success, stores a pointer to the file in *result and returns OK.  On
  // failure stores nullptr in *result and returns non-OK.
  //
  // The returned file will only be accessed by one thread at a time.
  // virtual IOStatus ReopenWritableFile(
  //     const std::string & /*fname*/, const FileOptions & /*options*/,
  //     std::unique_ptr<FSWritableFile> * /*result*/, IODebugContext * /*dbg*/) {
  //   return IOStatus::NotSupported("ReopenWritableFile");
  // }

  // Reuse an existing file by renaming it and opening it as writable.
  // virtual IOStatus ReuseWritableFile(const std::string &fname,
  //                                    const std::string &old_fname,
  //                                    const FileOptions &file_opts,
  //                                    std::unique_ptr<FSWritableFile> *result,
  //                                    IODebugContext *dbg);

  // Open `fname` for random read and write, if file doesn't exist the file
  // will be created.  On success, stores a pointer to the new file in
  // *result and returns OK.  On failure returns non-OK.
  //
  // The returned file will only be accessed by one thread at a time.
  // virtual IOStatus NewRandomRWFile(const std::string & /*fname*/,
  //                                  const FileOptions & /*options*/,
  //                                  std::unique_ptr<FSRandomRWFile> * /*result*/,
  //                                  IODebugContext * /*dbg*/) {
  //   return IOStatus::NotSupported(
  //       "RandomRWFile is not implemented in this FileSystem");
  // }

  // Opens `fname` as a memory-mapped file for read and write (in-place updates
  // only, i.e., no appends). On success, stores a raw buffer covering the whole
  // file in `*result`. The file must exist prior to this call.
  // virtual IOStatus NewMemoryMappedFileBuffer(
  //     const std::string & /*fname*/,
  //     std::unique_ptr<MemoryMappedFileBuffer> * /*result*/) {
  //   return IOStatus::NotSupported(
  //       "MemoryMappedFileBuffer is not implemented in this FileSystem");
  // }

  // Create an object that represents a directory. Will fail if directory
  // doesn't exist. If the directory exists, it will open the directory
  // and create a new Directory object.
  //
  // On success, stores a pointer to the new Directory in
  // *result and returns OK. On failure stores nullptr in *result and
  // returns non-OK.
  // virtual IOStatus NewDirectory(const std::string &name,
  //                               const IOOptions &io_opts,
  //                               std::unique_ptr<FSDirectory> *result,
  //                               IODebugContext *dbg) = 0;

  // Returns OK if the named file exists.
  //         NotFound if the named file does not exist,
  //                  the calling process does not have permission to determine
  //                  whether this file exists, or if the path is invalid.
  //         IOError if an IO Error was encountered
  virtual IOStatus FileExists(const std::string &fname,
                              const IOOptions &options,
                              IODebugContext *dbg) = 0;

  // Store in *result the names of the children of the specified directory.
  // The names are relative to "dir".
  // Original contents of *results are dropped.
  // Returns OK if "dir" exists and "*result" contains its children.
  //         NotFound if "dir" does not exist, the calling process does not have
  //                  permission to access "dir", or if "dir" is invalid.
  //         IOError if an IO Error was encountered
  virtual IOStatus GetChildren(const std::string &dir, const IOOptions &options,
                               std::vector<std::string> *result,
                               IODebugContext *dbg) = 0;

  // Store in *result the attributes of the children of the specified directory.
  // In case the implementation lists the directory prior to iterating the files
  // and files are concurrently deleted, the deleted files will be omitted from
  // result.
  // The name attributes are relative to "dir".
  // Original contents of *results are dropped.
  // Returns OK if "dir" exists and "*result" contains its children.
  //         NotFound if "dir" does not exist, the calling process does not have
  //                  permission to access "dir", or if "dir" is invalid.
  //         IOError if an IO Error was encountered
  virtual IOStatus GetChildrenFileAttributes(
      const std::string &dir, const IOOptions &options,
      std::vector<FileAttributes> *result, IODebugContext *dbg) {
    assert(result != nullptr);
    std::vector<std::string> child_fnames;
    IOStatus s = GetChildren(dir, options, &child_fnames, dbg);
    if (!s.ok()) {
      return s;
    }
    result->resize(child_fnames.size());
    size_t result_size = 0;
    for (size_t i = 0; i < child_fnames.size(); ++i) {
      const std::string path = dir + "/" + child_fnames[i];
      if (!(s = GetFileSize(path, options, &(*result)[result_size].size_bytes,
                            dbg))
          .ok()) {
        if (FileExists(path, options, dbg).IsNotFound()) {
          // The file may have been deleted since we listed the directory
          continue;
        }
        return s;
      }
      (*result)[result_size].name = std::move(child_fnames[i]);
      result_size++;
    }
    result->resize(result_size);
    return IOStatus::OK();
  }

// This seems to clash with a macro on Windows, so #undef it here
#ifdef DeleteFile
#undef DeleteFile
#endif

  // Delete the named file.
  virtual IOStatus DeleteFile(const std::string &fname,
                              const IOOptions &options,
                              IODebugContext *dbg) = 0;

  // Truncate the named file to the specified size.
  virtual IOStatus Truncate(const std::string & /*fname*/, size_t /*size*/,
                            const IOOptions & /*options*/,
                            IODebugContext * /*dbg*/) {
    return IOStatus::NotSupported(
        "Truncate is not supported for this FileSystem");
  }

  // Create the specified directory. Returns error if directory exists.
  virtual IOStatus CreateDir(const std::string &dirname,
                             const IOOptions &options, IODebugContext *dbg) = 0;

  // Creates directory if missing. Return Ok if it exists, or successful in
  // Creating.
  virtual IOStatus CreateDirIfMissing(const std::string &dirname,
                                      const IOOptions &options,
                                      IODebugContext *dbg) = 0;

  // Delete the specified directory.
  virtual IOStatus DeleteDir(const std::string &dirname,
                             const IOOptions &options, IODebugContext *dbg) = 0;

  // Store the size of fname in *file_size.
  virtual IOStatus GetFileSize(const std::string &fname,
                               const IOOptions &options, uint64_t *file_size,
                               IODebugContext *dbg) = 0;

  // Store the last modification time of fname in *file_mtime.
  virtual IOStatus GetFileModificationTime(const std::string &fname,
                                           const IOOptions &options,
                                           uint64_t *file_mtime,
                                           IODebugContext *dbg) = 0;

  // Rename file src to target.
  virtual IOStatus RenameFile(const std::string &src, const std::string &target,
                              const IOOptions &options,
                              IODebugContext *dbg) = 0;

  // Hard Link file src to target.
  virtual IOStatus LinkFile(const std::string & /*src*/,
                            const std::string & /*target*/,
                            const IOOptions & /*options*/,
                            IODebugContext * /*dbg*/) {
    return IOStatus::NotSupported(
        "LinkFile is not supported for this FileSystem");
  }

  virtual IOStatus NumFileLinks(const std::string & /*fname*/,
                                const IOOptions & /*options*/,
                                uint64_t * /*count*/, IODebugContext * /*dbg*/) {
    return IOStatus::NotSupported(
        "Getting number of file links is not supported for this FileSystem");
  }

  virtual IOStatus AreFilesSame(const std::string & /*first*/,
                                const std::string & /*second*/,
                                const IOOptions & /*options*/, bool * /*res*/,
                                IODebugContext * /*dbg*/) {
    return IOStatus::NotSupported(
        "AreFilesSame is not supported for this FileSystem");
  }

  // Lock the specified file.  Used to prevent concurrent access to
  // the same db by multiple processes.  On failure, stores nullptr in
  // *lock and returns non-OK.
  //
  // On success, stores a pointer to the object that represents the
  // acquired lock in *lock and returns OK.  The caller should call
  // UnlockFile(*lock) to release the lock.  If the process exits,
  // the lock will be automatically released.
  //
  // If somebody else already holds the lock, finishes immediately
  // with a failure.  I.e., this call does not wait for existing locks
  // to go away.
  //
  // May create the named file if it does not already exist.
  virtual IOStatus LockFile(const std::string &fname, const IOOptions &options,
                            FileLock **lock, IODebugContext *dbg) = 0;

  // Release the lock acquired by a previous successful call to LockFile.
  // REQUIRES: lock was returned by a successful LockFile() call
  // REQUIRES: lock has not already been unlocked.
  virtual IOStatus UnlockFile(FileLock *lock, const IOOptions &options,
                              IODebugContext *dbg) = 0;

  // *path is set to a temporary directory that can be used for testing. It may
  // or many not have just been created. The directory may or may not differ
  // between runs of the same process, but subsequent calls will return the
  // same directory.
  virtual IOStatus GetTestDirectory(const IOOptions &options, std::string *path,
                                    IODebugContext *dbg) = 0;

  // Create and returns a default logger (an instance of EnvLogger) for storing
  // informational messages. Derived classes can override to provide custom
  // logger.
  virtual IOStatus NewLogger(const std::string &fname, const IOOptions &io_opts,
                             std::shared_ptr<Logger> *result,
                             IODebugContext *dbg);

  // Get full directory name for this db.
  virtual IOStatus GetAbsolutePath(const std::string &db_path,
                                   const IOOptions &options,
                                   std::string *output_path,
                                   IODebugContext *dbg) = 0;

  // Sanitize the FileOptions. Typically called by a FileOptions/EnvOptions
  // copy constructor
  // virtual void SanitizeFileOptions(FileOptions * /*opts*/) const {}

  // OptimizeForLogRead will create a new FileOptions object that is a copy of
  // the FileOptions in the parameters, but is optimized for reading log files.
  // virtual FileOptions OptimizeForLogRead(const FileOptions &file_options) const;

  // OptimizeForManifestRead will create a new FileOptions object that is a copy
  // of the FileOptions in the parameters, but is optimized for reading manifest
  // files.
  // virtual FileOptions OptimizeForManifestRead(
  //     const FileOptions &file_options) const;

  // OptimizeForLogWrite will create a new FileOptions object that is a copy of
  // the FileOptions in the parameters, but is optimized for writing log files.
  // Default implementation returns the copy of the same object.
  // virtual FileOptions OptimizeForLogWrite(const FileOptions &file_options,
  //                                         const DBOptions &db_options) const;

  // OptimizeForManifestWrite will create a new FileOptions object that is a
  // copy of the FileOptions in the parameters, but is optimized for writing
  // manifest files. Default implementation returns the copy of the same
  // object.
  // virtual FileOptions OptimizeForManifestWrite(
  //     const FileOptions &file_options) const;

  // OptimizeForCompactionTableWrite will create a new FileOptions object that
  // is a copy of the FileOptions in the parameters, but is optimized for
  // writing table files.
  // virtual FileOptions OptimizeForCompactionTableWrite(
  //     const FileOptions &file_options,
  //     const ImmutableDBOptions &immutable_ops) const;

  // OptimizeForCompactionTableRead will create a new FileOptions object that
  // is a copy of the FileOptions in the parameters, but is optimized for
  // reading table files.
  // virtual FileOptions OptimizeForCompactionTableRead(
  //     const FileOptions &file_options,
  //     const ImmutableDBOptions &db_options) const;

  // OptimizeForBlobFileRead will create a new FileOptions object that
  // is a copy of the FileOptions in the parameters, but is optimized for
  // reading blob files.
  // virtual FileOptions OptimizeForBlobFileRead(
  //     const FileOptions &file_options,
  //     const ImmutableDBOptions &db_options) const;

// This seems to clash with a macro on Windows, so #undef it here
#ifdef GetFreeSpace
#undef GetFreeSpace
#endif

  // Get the amount of free disk space
  virtual IOStatus GetFreeSpace(const std::string & /*path*/,
                                const IOOptions & /*options*/,
                                uint64_t * /*diskfree*/,
                                IODebugContext * /*dbg*/) {
    return IOStatus::NotSupported("GetFreeSpace");
  }

  virtual IOStatus IsDirectory(const std::string & /*path*/,
                               const IOOptions &options, bool *is_dir,
                               IODebugContext * /*dgb*/) = 0;

  // EXPERIMENTAL
  // Poll for completion of read IO requests. The Poll() method should call the
  // callback functions to indicate completion of read requests.
  // Underlying FS is required to support Poll API. Poll implementation should
  // ensure that the callback gets called at IO completion, and return only
  // after the callback has been called.
  // If Poll returns partial results for any reads, its caller reponsibility to
  // call Read or ReadAsync in order to get the remaining bytes.
  //
  // Default implementation is to return IOStatus::OK.

  virtual IOStatus Poll(std::vector<void *> & /*io_handles*/,
                        size_t /*min_completions*/) {
    return IOStatus::OK();
  }

  // EXPERIMENTAL
  // Abort the read IO requests submitted asynchronously. Underlying FS is
  // required to support AbortIO API. AbortIO implementation should ensure that
  // the all the read requests related to io_handles should be aborted and
  // it shouldn't call the callback for these io_handles.
  //
  // Default implementation is to return IOStatus::OK.
  virtual IOStatus AbortIO(std::vector<void *> & /*io_handles*/) {
    return IOStatus::OK();
  }

  // Indicates to upper layers whether the FileSystem supports/uses async IO
  // or not
  virtual bool use_async_io() { return true; }

  // If you're adding methods here, remember to add them to EnvWrapper too.

private:
  void operator=(const FileSystem &);
};

// An implementation of Env that forwards all calls to another Env.
// May be useful to clients who wish to override just part of the
// functionality of another Env.
class FileSystemWrapper : public FileSystem {
public:
  // Initialize an EnvWrapper that delegates all calls to *t
  explicit FileSystemWrapper(const std::shared_ptr<FileSystem> &t);

  ~FileSystemWrapper() override {}

  // Return the target to which this Env forwards all calls
  FileSystem *target() const { return target_.get(); }

  // The following text is boilerplate that forwards all methods to target()
  // IOStatus NewSequentialFile(const std::string& f, const FileOptions& file_opts,
  //                            std::unique_ptr<FSSequentialFile>* r,
  //                            IODebugContext* dbg) override {
  //   return target_->NewSequentialFile(f, file_opts, r, dbg);
  // }
  // IOStatus NewRandomAccessFile(const std::string& f,
  //                              const FileOptions& file_opts,
  //                              std::unique_ptr<FSRandomAccessFile>* r,
  //                              IODebugContext* dbg) override {
  //   return target_->NewRandomAccessFile(f, file_opts, r, dbg);
  // }
  // IOStatus NewWritableFile(const std::string& f, const FileOptions& file_opts,
  //                          std::unique_ptr<FSWritableFile>* r,
  //                          IODebugContext* dbg) override {
  //   return target_->NewWritableFile(f, file_opts, r, dbg);
  // }
  // IOStatus ReopenWritableFile(const std::string& fname,
  //                             const FileOptions& file_opts,
  //                             std::unique_ptr<FSWritableFile>* result,
  //                             IODebugContext* dbg) override {
  //   return target_->ReopenWritableFile(fname, file_opts, result, dbg);
  // }
  // IOStatus ReuseWritableFile(const std::string& fname,
  //                            const std::string& old_fname,
  //                            const FileOptions& file_opts,
  //                            std::unique_ptr<FSWritableFile>* r,
  //                            IODebugContext* dbg) override {
  //   return target_->ReuseWritableFile(fname, old_fname, file_opts, r, dbg);
  // }
  // IOStatus NewRandomRWFile(const std::string& fname,
  //                          const FileOptions& file_opts,
  //                          std::unique_ptr<FSRandomRWFile>* result,
  //                          IODebugContext* dbg) override {
  //   return target_->NewRandomRWFile(fname, file_opts, result, dbg);
  // }
  // IOStatus NewMemoryMappedFileBuffer(
  //     const std::string& fname,
  //     std::unique_ptr<MemoryMappedFileBuffer>* result) override {
  //   return target_->NewMemoryMappedFileBuffer(fname, result);
  // }
  // IOStatus NewDirectory(const std::string& name, const IOOptions& io_opts,
  //                       std::unique_ptr<FSDirectory>* result,
  //                       IODebugContext* dbg) override {
  //   return target_->NewDirectory(name, io_opts, result, dbg);
  // }
  IOStatus FileExists(const std::string &f, const IOOptions &io_opts,
                      IODebugContext *dbg) override {
    return target_->FileExists(f, io_opts, dbg);
  }

  IOStatus GetChildren(const std::string &dir, const IOOptions &io_opts,
                       std::vector<std::string> *r,
                       IODebugContext *dbg) override {
    return target_->GetChildren(dir, io_opts, r, dbg);
  }

  IOStatus GetChildrenFileAttributes(const std::string &dir,
                                     const IOOptions &options,
                                     std::vector<FileAttributes> *result,
                                     IODebugContext *dbg) override {
    return target_->GetChildrenFileAttributes(dir, options, result, dbg);
  }

  IOStatus DeleteFile(const std::string &f, const IOOptions &options,
                      IODebugContext *dbg) override {
    return target_->DeleteFile(f, options, dbg);
  }

  IOStatus Truncate(const std::string &fname, size_t size,
                    const IOOptions &options, IODebugContext *dbg) override {
    return target_->Truncate(fname, size, options, dbg);
  }

  IOStatus CreateDir(const std::string &d, const IOOptions &options,
                     IODebugContext *dbg) override {
    return target_->CreateDir(d, options, dbg);
  }

  IOStatus CreateDirIfMissing(const std::string &d, const IOOptions &options,
                              IODebugContext *dbg) override {
    return target_->CreateDirIfMissing(d, options, dbg);
  }

  IOStatus DeleteDir(const std::string &d, const IOOptions &options,
                     IODebugContext *dbg) override {
    return target_->DeleteDir(d, options, dbg);
  }

  IOStatus GetFileSize(const std::string &f, const IOOptions &options,
                       uint64_t *s, IODebugContext *dbg) override {
    return target_->GetFileSize(f, options, s, dbg);
  }

  IOStatus GetFileModificationTime(const std::string &fname,
                                   const IOOptions &options,
                                   uint64_t *file_mtime,
                                   IODebugContext *dbg) override {
    return target_->GetFileModificationTime(fname, options, file_mtime, dbg);
  }

  IOStatus GetAbsolutePath(const std::string &db_path, const IOOptions &options,
                           std::string *output_path,
                           IODebugContext *dbg) override {
    return target_->GetAbsolutePath(db_path, options, output_path, dbg);
  }

  IOStatus RenameFile(const std::string &s, const std::string &t,
                      const IOOptions &options, IODebugContext *dbg) override {
    return target_->RenameFile(s, t, options, dbg);
  }

  IOStatus LinkFile(const std::string &s, const std::string &t,
                    const IOOptions &options, IODebugContext *dbg) override {
    return target_->LinkFile(s, t, options, dbg);
  }

  IOStatus NumFileLinks(const std::string &fname, const IOOptions &options,
                        uint64_t *count, IODebugContext *dbg) override {
    return target_->NumFileLinks(fname, options, count, dbg);
  }

  IOStatus AreFilesSame(const std::string &first, const std::string &second,
                        const IOOptions &options, bool *res,
                        IODebugContext *dbg) override {
    return target_->AreFilesSame(first, second, options, res, dbg);
  }

  IOStatus LockFile(const std::string &f, const IOOptions &options,
                    FileLock **l, IODebugContext *dbg) override {
    return target_->LockFile(f, options, l, dbg);
  }

  IOStatus UnlockFile(FileLock *l, const IOOptions &options,
                      IODebugContext *dbg) override {
    return target_->UnlockFile(l, options, dbg);
  }

  IOStatus GetTestDirectory(const IOOptions &options, std::string *path,
                            IODebugContext *dbg) override {
    return target_->GetTestDirectory(options, path, dbg);
  }

  IOStatus NewLogger(const std::string &fname, const IOOptions &options,
                     std::shared_ptr<Logger> *result,
                     IODebugContext *dbg) override {
    return target_->NewLogger(fname, options, result, dbg);
  }

  // void SanitizeFileOptions(FileOptions* opts) const override {
  //   target_->SanitizeFileOptions(opts);
  // }
  //
  // FileOptions OptimizeForLogRead(
  //     const FileOptions& file_options) const override {
  //   return target_->OptimizeForLogRead(file_options);
  // }
  // FileOptions OptimizeForManifestRead(
  //     const FileOptions& file_options) const override {
  //   return target_->OptimizeForManifestRead(file_options);
  // }
  // FileOptions OptimizeForLogWrite(const FileOptions& file_options,
  //                                 const DBOptions& db_options) const override {
  //   return target_->OptimizeForLogWrite(file_options, db_options);
  // }
  // FileOptions OptimizeForManifestWrite(
  //     const FileOptions& file_options) const override {
  //   return target_->OptimizeForManifestWrite(file_options);
  // }
  // FileOptions OptimizeForCompactionTableWrite(
  //     const FileOptions& file_options,
  //     const ImmutableDBOptions& immutable_ops) const override {
  //   return target_->OptimizeForCompactionTableWrite(file_options,
  //                                                   immutable_ops);
  // }
  // FileOptions OptimizeForCompactionTableRead(
  //     const FileOptions& file_options,
  //     const ImmutableDBOptions& db_options) const override {
  //   return target_->OptimizeForCompactionTableRead(file_options, db_options);
  // }
  // FileOptions OptimizeForBlobFileRead(
  //     const FileOptions& file_options,
  //     const ImmutableDBOptions& db_options) const override {
  //   return target_->OptimizeForBlobFileRead(file_options, db_options);
  // }
  IOStatus GetFreeSpace(const std::string &path, const IOOptions &options,
                        uint64_t *diskfree, IODebugContext *dbg) override {
    return target_->GetFreeSpace(path, options, diskfree, dbg);
  }

  IOStatus IsDirectory(const std::string &path, const IOOptions &options,
                       bool *is_dir, IODebugContext *dbg) override {
    return target_->IsDirectory(path, options, is_dir, dbg);
  }

  const FileSystem *Inner() const { return target_.get(); }
  // Status PrepareOptions(const ConfigOptions& options) override;
  // std::string SerializeOptions(const ConfigOptions& config_options,
  //                              const std::string& header) const override;

  virtual IOStatus Poll(std::vector<void *> &io_handles,
                        size_t min_completions) override {
    return target_->Poll(io_handles, min_completions);
  }

  virtual IOStatus AbortIO(std::vector<void *> &io_handles) override {
    return target_->AbortIO(io_handles);
  }

  virtual bool use_async_io() override { return target_->use_async_io(); }

protected:
  std::shared_ptr<FileSystem> target_;
};

}

#endif //AQUAFS_FILE_SYSTEM_H
