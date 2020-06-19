// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

namespace metric_names {
const std::string get_qps = "get_qps";
const std::string multi_get_qps = "multi_get_qps";
const std::string put_qps = "put_qps";
const std::string multi_put_qps = "multi_put_qps";
const std::string remove_qps = "remove_qps";
const std::string multi_remove_qps = "multi_remove_qps";
const std::string incr_qps = "incr_qps";
const std::string check_and_set_qps = "check_and_set_qps";
const std::string check_and_mutate_qps = "check_and_mutate_qps";
const std::string scan_qps = "scan_qps";
const std::string duplicate_qps = "duplicate_qps";
const std::string dup_shipped_ops = "dup_shipped_ops";
const std::string dup_failed_shipping_ops = "dup_failed_shipping_ops";
const std::string recent_read_cu = "recent_read_cu";
const std::string recent_write_cu = "recent_write_cu";
const std::string recent_expire_count = "recent_expire_count";
const std::string recent_filter_count = "recent_filter_count";
const std::string recent_abnormal_count = "recent_abnormal_count";
const std::string recent_write_throttling_delay_count = "recent_write_throttling_delay_count";
const std::string recent_write_throttling_reject_count = "recent_write_throttling_reject_count";
const std::string storage_mb = "storage_mb";
const std::string storage_count = "storage_count";
const std::string rdb_block_cache_hit_count = "rdb_block_cache_hit_count";
const std::string rdb_block_cache_total_count = "rdb_block_cache_total_count";
const std::string rdb_index_and_filter_blocks_mem_usage = "rdb_index_and_filter_blocks_mem_usage";
const std::string rdb_memtable_mem_usage = "rdb_memtable_mem_usage";
const std::string rdb_estimate_num_keys = "rdb_estimate_num_keys";
const std::string rdb_bf_seek_negatives = "rdb_bf_seek_negatives";
const std::string rdb_bf_seek_total = "rdb_bf_seek_total";
const std::string rdb_bf_point_positive_true = "rdb_bf_point_positive_true";
const std::string rdb_bf_point_positive_total = "rdb_bf_point_positive_total";
const std::string rdb_bf_point_negatives = "rdb_bf_point_negatives";
const std::string backup_request_qps = "backup_request_qps";
const std::string get_bytes = "get_bytes";
const std::string multi_get_bytes = "multi_get_bytes";
const std::string scan_bytes = "scan_bytes";
const std::string put_bytes = "put_bytes";
const std::string multi_put_bytes = "multi_put_bytes";
const std::string check_and_set_bytes = "check_and_set_bytes";
const std::string check_and_mutate_bytes = "check_and_mutate_bytes";
};
