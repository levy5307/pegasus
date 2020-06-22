// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

namespace perf_counter_names {
const std::string GET_QPS = "get_qps";
const std::string MULTI_GET_QPS = "multi_get_qps";
const std::string PUT_QPS = "put_qps";
const std::string MULTI_PUT_QPS = "multi_put_qps";
const std::string REMOVE_QPS = "remove_qps";
const std::string MULTI_REMOVE_QPS = "multi_remove_qps";
const std::string INCR_QPS = "incr_qps";
const std::string CHECK_AND_SET_QPS = "check_and_set_qps";
const std::string CHECK_AND_MUTATE_QPS = "check_and_mutate_qps";
const std::string SCAN_QPS = "scan_qps";
const std::string DUPLICATE_QPS = "duplicate_qps";
const std::string DUP_SHIPPED_OPS = "dup_shipped_ops";
const std::string DUP_FAILED_SHIPPING_OPS = "dup_failed_shipping_ops";
const std::string RECENT_READ_CU = "recent_read_cu";
const std::string RECENT_WRITE_CU = "recent_write_cu";
const std::string RECENT_EXPIRE_COUNT = "recent_expire_count";
const std::string RECENT_FILTER_COUNT = "recent_filter_count";
const std::string RECENT_ABNORMAL_COUNT = "recent_abnormal_count";
const std::string RECENT_WRITE_THROTTLING_DELAY_COUNT = "recent_write_throttling_delay_count";
const std::string RECENT_WRITE_THROTTLING_REJECT_COUNT = "recent_write_throttling_reject_count";
const std::string STORAGE_MB = "storage_mb";
const std::string STORAGE_COUNT = "storage_count";
const std::string RDB_BLOCK_CACHE_HIT_COUNT = "rdb_block_cache_hit_count";
const std::string RDB_BLOCK_CACHE_TOTAL_COUNT = "rdb_block_cache_total_count";
const std::string RDB_INDEX_AND_FILTER_BLOCKS_MEM_USAGE = "rdb_index_and_filter_blocks_mem_usage";
const std::string RDB_MEMTABLE_MEM_USAGE = "rdb_memtable_mem_usage";
const std::string RDB_ESTIMATE_NUM_KEYS = "rdb_estimate_num_keys";
const std::string RDB_BF_SEEK_NEGATIVES = "rdb_bf_seek_negatives";
const std::string RDB_BF_SEEK_TOTAL = "rdb_bf_seek_total";
const std::string RDB_BF_POINT_POSITIVE_TRUE = "rdb_bf_point_positive_true";
const std::string RDB_BF_POINT_POSITIVE_TOTAL = "rdb_bf_point_positive_total";
const std::string RDB_BF_POINT_NEGATIVES = "rdb_bf_point_negatives";
const std::string BACKUP_REQUEST_QPS = "backup_request_qps";
const std::string GET_BYTES = "get_bytes";
const std::string MULTI_GET_BYTES = "multi_get_bytes";
const std::string SCAN_BYTES = "scan_bytes";
const std::string PUT_BYTES = "put_bytes";
const std::string MULTI_PUT_BYTES = "multi_put_bytes";
const std::string CHECK_AND_SET_BYTES = "check_and_set_bytes";
const std::string CHECK_AND_MUTATE_BYTES = "check_and_mutate_bytes";
} // namespace perf_counter_names
