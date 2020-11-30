/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#pragma once

#include <dsn/utility/string_view.h>
#include <dsn/dist/replication/replica_base.h>
#include <base/pegasus_value_schema.h>
#include <rocksdb/write_batch.h>
#include <rocksdb/db.h>

#include <gtest/gtest_prod.h>

namespace pegasus {
namespace server {
class pegasus_server_impl;
struct db_write_context;
struct db_get_context;

/// internal error codes used for fail injection
static constexpr int FAIL_DB_WRITE_BATCH_PUT = -101;
static constexpr int FAIL_DB_WRITE_BATCH_DELETE = -102;
static constexpr int FAIL_DB_WRITE = -103;
static constexpr int FAIL_DB_GET = -104;

class rocksdb_wrapper : public dsn::replication::replica_base
{
public:
    rocksdb_wrapper(pegasus_server_impl *server,
                    rocksdb::DB *db,
                    rocksdb::ColumnFamilyHandle *meta_cf,
                    const uint32_t _pegasus_data_version,
                    rocksdb::ReadOptions &_rd_opts);
    ~rocksdb_wrapper() = default;

    int write_batch_put(int64_t decree,
                        dsn::string_view raw_key,
                        dsn::string_view value,
                        uint32_t expire_sec);
    int write_batch_put_ctx(const db_write_context &ctx,
                            dsn::string_view raw_key,
                            dsn::string_view value,
                            uint32_t expire_sec);
    int write_batch_delete(int64_t decree, dsn::string_view raw_key);
    int write(int64_t decree);
    int get(dsn::string_view raw_key, /*out*/ db_get_context *ctx);

    void clear_up_write_batch();
    void set_default_ttl(uint32_t ttl);
    dsn::error_code ingest_external_file(const std::vector<std::string> &sst_file_list,
                                         const int64_t decree);

private:
    uint32_t db_expire_ts(uint32_t expire_ts);

    friend class pegasus_write_service_impl_test;
    friend class pegasus_write_service_test;
    friend class pegasus_server_write_test;
    FRIEND_TEST(pegasus_write_service_impl_test, put_verify_timetag);
    FRIEND_TEST(pegasus_write_service_impl_test, verify_timetag_compatible_with_version_0);

    pegasus_value_generator _value_generator;
    rocksdb::WriteBatch _write_batch;
    rocksdb::DB *_db;
    rocksdb::WriteOptions _wt_opts;
    rocksdb::ReadOptions &_rd_opts;
    rocksdb::ColumnFamilyHandle *_meta_cf;
    const uint32_t _pegasus_data_version;
    volatile uint32_t _default_ttl;
};
} // namespace server
} // namespace pegasus
