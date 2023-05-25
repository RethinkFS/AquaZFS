#pragma once

#include <prometheus/counter.h>
#include <prometheus/exposer.h>
#include <prometheus/registry.h>

#include <atomic>
#include <cstdint>
#include <memory>
#include <thread>


#include "metrics.h"


class ResettingGauge;

namespace aquafs {

// using namespace prometheus;

class GaugeMetric {
 public:
  prometheus::Family<prometheus::Gauge> *family;
  prometheus::Gauge *gmin;
  prometheus::Gauge *gmax;
  prometheus::Gauge *gtotal;
  prometheus::Gauge *gcount;
  std::atomic<uint64_t> value;
  std::atomic<uint64_t> count;
  std::atomic<uint64_t> max;
  std::atomic<uint64_t> min;
};

class AquaFSPrometheusMetrics : public aquafs::AquaFSMetrics {
 private:
  std::shared_ptr<prometheus::Registry> registry_;
  std::unordered_map<AquaFSMetricsHistograms, std::shared_ptr<GaugeMetric>>
      metric_map_;
  uint64_t report_interval_ms_ = 5000;
  std::thread *collect_thread_;
  std::atomic_bool stop_collect_thread_;

  const std::unordered_map<uint32_t, std::pair<std::string, uint32_t>>
      info_map_ = {
          {AQUAFS_NON_WAL_WRITE_LATENCY,
           {"aquafs_non_wal_write_latency", AQUAFS_REPORTER_TYPE_LATENCY}},
          {AQUAFS_WAL_WRITE_LATENCY,
           {"aquafs_wal_write_latency", AQUAFS_REPORTER_TYPE_LATENCY}},
          {AQUAFS_READ_LATENCY,
           {"aquafs_read_latency", AQUAFS_REPORTER_TYPE_LATENCY}},
          {AQUAFS_WAL_SYNC_LATENCY,
           {"aquafs_wal_sync_latency", AQUAFS_REPORTER_TYPE_LATENCY}},
          {AQUAFS_NON_WAL_SYNC_LATENCY,
           {"aquafs_non_wal_sync_latency", AQUAFS_REPORTER_TYPE_LATENCY}},
          {AQUAFS_ZONE_WRITE_LATENCY,
           {"aquafs_zone_write_latency", AQUAFS_REPORTER_TYPE_LATENCY}},
          {AQUAFS_ROLL_LATENCY,
           {"aquafs_roll_latency", AQUAFS_REPORTER_TYPE_LATENCY}},
          {AQUAFS_META_ALLOC_LATENCY,
           {"aquafs_meta_alloc_latency", AQUAFS_REPORTER_TYPE_LATENCY}},
          {AQUAFS_META_SYNC_LATENCY,
           {"aquafs_meta_sync_latency", AQUAFS_REPORTER_TYPE_LATENCY}},
          {AQUAFS_WRITE_QPS, {"aquafs_write_qps", AQUAFS_REPORTER_TYPE_QPS}},
          {AQUAFS_READ_QPS, {"aquafs_read_qps", AQUAFS_REPORTER_TYPE_QPS}},
          {AQUAFS_SYNC_QPS, {"aquafs_sync_qps", AQUAFS_REPORTER_TYPE_QPS}},
          {AQUAFS_META_ALLOC_QPS,
           {"aquafs_meta_alloc_qps", AQUAFS_REPORTER_TYPE_QPS}},
          {AQUAFS_IO_ALLOC_QPS,
           {"aquafs_io_alloc_qps", AQUAFS_REPORTER_TYPE_QPS}},
          {AQUAFS_ROLL_QPS, {"aquafs_roll_qps", AQUAFS_REPORTER_TYPE_QPS}},
          {AQUAFS_WRITE_THROUGHPUT,
           {"aquafs_write_throughput", AQUAFS_REPORTER_TYPE_THROUGHPUT}},
          {AQUAFS_RESETABLE_ZONES_COUNT,
           {"aquafs_resetable_zones", AQUAFS_REPORTER_TYPE_GENERAL}},
          {AQUAFS_OPEN_ZONES_COUNT,
           {"aquafs_open_zones", AQUAFS_REPORTER_TYPE_GENERAL}},
          {AQUAFS_ACTIVE_ZONES_COUNT,
           {"aquafs_active_zones", AQUAFS_REPORTER_TYPE_GENERAL}},
      };

  void run();

 public:
  AquaFSPrometheusMetrics();
  ~AquaFSPrometheusMetrics();

 private:
  virtual void AddReporter(uint32_t label, ReporterType type = 0) override;
  virtual void Report(uint32_t label_uint, size_t value,
                      uint32_t type_uint = 0) override;

 public:
  virtual void ReportQPS(uint32_t label, size_t qps) override {
    Report(label, qps, AQUAFS_REPORTER_TYPE_QPS);
  }
  virtual void ReportLatency(uint32_t label, size_t latency) override {
    Report(label, latency, AQUAFS_REPORTER_TYPE_LATENCY);
  }
  virtual void ReportThroughput(uint32_t label, size_t throughput) override {
    Report(label, throughput, AQUAFS_REPORTER_TYPE_THROUGHPUT);
  }
  virtual void ReportGeneral(uint32_t label, size_t value) override {
    Report(label, value, AQUAFS_REPORTER_TYPE_GENERAL);
  }

  virtual void ReportSnapshot(const AquaFSSnapshot &snapshot) override {}
};

}  // namespace ROCKSDB_NAMESPACE
