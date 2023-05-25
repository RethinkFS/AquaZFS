// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
// Copyright (c) 2019-present, Western Digital Corporation
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

// Metrics Framework Introduction
//
// The metrics framework is used for users to identify AquaFS's performance
// bottomneck, it can collect throuput, qps and latency of each critical
// function call.
//
// For different RocksDB forks, users could custom their own metrics reporter to
// define how they would like to report these collected information.
//
// Steps to add new metrics trace point:
//   1. Add a new trace point label name in `AquaFSMetricsHistograms`.
//   2. Find target function, add these lines for tracing
//       // Latency Trace
//       AquaFSMetricsGuard guard(zoneFile_->GetZBDMetrics(),
//       AQUAFS_WAL_WRITE_LATENCY, Default());
//       // Throughput Trace
//       zoneFile_->GetZBDMetrics()->ReportThroughput(AQUAFS_WRITE_THROUGHPUT,
//       data.size());
//       // QPS Trace
//       zoneFile_->GetZBDMetrics()->ReportQPS(AQUAFS_WRITE_QPS, 1);
//    3. Implement a `AquaFSMetrics` to define how you would like to report your
//    data (Refer to file  `metrics_sample.h`)
//    4. Define your customized label name when implement AquaFSMetrics
//    5. Init your metrics and pass it into `NewAquaFS()` function (default is
//    `NoAquaFSMetrics`)

#pragma once

#include "base/env.h"

namespace aquafs {

class AquaFSMetricsGuard;
class AquaFSSnapshot;
class AquaFSSnapshotOptions;

// Types of Reporter that may be used for statistics.
enum AquaFSMetricsReporterType : uint32_t {
  AQUAFS_REPORTER_TYPE_WITHOUT_CHECK = 0,
  AQUAFS_REPORTER_TYPE_GENERAL,
  AQUAFS_REPORTER_TYPE_LATENCY,
  AQUAFS_REPORTER_TYPE_QPS,
  AQUAFS_REPORTER_TYPE_THROUGHPUT,
};

// Names of Reporter that may be used for statistics.
enum AquaFSMetricsHistograms : uint32_t {
  AQUAFS_HISTOGRAM_ENUM_MIN,

  AQUAFS_READ_LATENCY,
  AQUAFS_READ_QPS,

  AQUAFS_WRITE_LATENCY,
  AQUAFS_WAL_WRITE_LATENCY,
  AQUAFS_NON_WAL_WRITE_LATENCY,
  AQUAFS_WRITE_QPS,
  AQUAFS_WRITE_THROUGHPUT,

  AQUAFS_SYNC_LATENCY,
  AQUAFS_WAL_SYNC_LATENCY,
  AQUAFS_NON_WAL_SYNC_LATENCY,
  AQUAFS_SYNC_QPS,

  AQUAFS_IO_ALLOC_LATENCY,
  AQUAFS_WAL_IO_ALLOC_LATENCY,
  AQUAFS_NON_WAL_IO_ALLOC_LATENCY,
  AQUAFS_IO_ALLOC_QPS,

  AQUAFS_META_ALLOC_LATENCY,
  AQUAFS_META_ALLOC_QPS,

  AQUAFS_META_SYNC_LATENCY,

  AQUAFS_ROLL_LATENCY,
  AQUAFS_ROLL_QPS,
  AQUAFS_ROLL_THROUGHPUT,

  AQUAFS_ACTIVE_ZONES_COUNT,
  AQUAFS_OPEN_ZONES_COUNT,

  AQUAFS_FREE_SPACE_SIZE,
  AQUAFS_USED_SPACE_SIZE,
  AQUAFS_RECLAIMABLE_SPACE_SIZE,

  AQUAFS_RESETABLE_ZONES_COUNT,

  AQUAFS_HISTOGRAM_ENUM_MAX,

  AQUAFS_ZONE_WRITE_THROUGHPUT,
  AQUAFS_ZONE_WRITE_LATENCY,

  AQUAFS_L0_IO_ALLOC_LATENCY,
};

struct AquaFSMetrics {
 public:
  typedef uint32_t Label;
  typedef uint32_t ReporterType;
  // We give an enum to identify the reporters and an enum to identify the
  // reporter types: AquaFSMetricsHistograms and AquaFSMetricsReporterType,
  // respectively, at the end of the code.
 public:
  AquaFSMetrics() {}
  virtual ~AquaFSMetrics() {}

 public:
  // Add a reporter named label.
  // You can give a type for type-checking.
  virtual void AddReporter(Label label, ReporterType type = 0) = 0;
  // Report a value for the reporter named label.
  // You can give a type for type-checking.
  virtual void Report(Label label, size_t value,
                      ReporterType type_check = 0) = 0;
  virtual void ReportSnapshot(const AquaFSSnapshot& snapshot) = 0;

 public:
  // Syntactic sugars for type-checking.
  // Overwrite them if you think type-checking is necessary.
  virtual void ReportQPS(Label label, size_t qps) { Report(label, qps, 0); }
  virtual void ReportThroughput(Label label, size_t throughput) {
    Report(label, throughput, 0);
  }
  virtual void ReportLatency(Label label, size_t latency) {
    Report(label, latency, 0);
  }
  virtual void ReportGeneral(Label label, size_t data) {
    Report(label, data, 0);
  }

  // and more
};

struct NoAquaFSMetrics : public AquaFSMetrics {
  NoAquaFSMetrics() : AquaFSMetrics() {}
  virtual ~NoAquaFSMetrics() {}

 public:
  virtual void AddReporter(uint32_t /*label*/, uint32_t /*type*/) override {}
  virtual void Report(uint32_t /*label*/, size_t /*value*/,
                      uint32_t /*type_check*/) override {}
  virtual void ReportSnapshot(const AquaFSSnapshot& /*snapshot*/) override {}
};

// The implementation of this class will start timing when initialized,
// stop timing when it is destructured,
// and report the difference in time to the target label via
// metrics->ReportLatency(). By default, the method to collect the time will be
// to call env->NowMicros().
struct AquaFSMetricsLatencyGuard {
  std::shared_ptr<AquaFSMetrics> metrics_;
  uint32_t label_;
  Env* env_;
  uint64_t begin_time_micro_;

  AquaFSMetricsLatencyGuard(std::shared_ptr<AquaFSMetrics> metrics,
                            uint32_t label, Env* env)
      : metrics_(metrics),
        label_(label),
        env_(env),
        begin_time_micro_(GetTime()) {}

  virtual ~AquaFSMetricsLatencyGuard() {
    uint64_t end_time_micro_ = GetTime();
    assert(end_time_micro_ >= begin_time_micro_);
    metrics_->ReportLatency(label_,
                            Report(end_time_micro_ - begin_time_micro_));
  }
  // overwrite this function if you wish to capture time by other methods.
  virtual uint64_t GetTime() { return env_->NowMicros(); }
  // overwrite this function if you do not intend to report delays measured in
  // microseconds.
  virtual uint64_t Report(uint64_t time) { return time; }
};

#define AQUAFS_LABEL(label, type) AQUAFS_##label##_##type
#define AQUAFS_LABEL_DETAILED(label, sub_label, type) \
  AQUAFS_##sub_label##_##label##_##type
// eg : AQUAFS_LABEL(WRITE, WAL, THROUGHPUT) => AQUAFS_WAL_WRITE_THROUGHPUT

}  // namespace aquafs
