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

#include "pegasus_write_service.h"

#include <gtest/gtest_prod.h>

namespace pegasus {
namespace server {
class rocksdb_wrapper;
class pegasus_server_impl;

struct db_get_context
{
    // value read from DB.
    std::string raw_value;

    // is the record found in DB.
    bool found{false};

    // the expiration time encoded in raw_value.
    uint32_t expire_ts{0};

    // is the record expired.
    bool expired{false};
};

class pegasus_write_service::impl : public dsn::replication::replica_base
{
public:
    explicit impl(pegasus_server_impl *server);
    ~impl();

    int empty_put(int64_t decree);
    int multi_put(const db_write_context &ctx,
                  const dsn::apps::multi_put_request &update,
                  dsn::apps::update_response &resp);
    int multi_remove(int64_t decree,
                     const dsn::apps::multi_remove_request &update,
                     dsn::apps::multi_remove_response &resp);
    int incr(int64_t decree, const dsn::apps::incr_request &update, dsn::apps::incr_response &resp);
    int check_and_set(int64_t decree,
                      const dsn::apps::check_and_set_request &update,
                      dsn::apps::check_and_set_response &resp);
    int check_and_mutate(int64_t decree,
                         const dsn::apps::check_and_mutate_request &update,
                         dsn::apps::check_and_mutate_response &resp);
    // \return ERR_WRONG_CHECKSUM: verify files failed
    // \return ERR_INGESTION_FAILED: rocksdb ingestion failed
    // \return ERR_OK: rocksdb ingestion succeed
    dsn::error_code ingestion_files(const int64_t decree,
                                    const std::string &bulk_load_dir,
                                    const dsn::replication::bulk_load_metadata &metadata);

    /// For batch write.
    int batch_put(const db_write_context &ctx,
                  const dsn::apps::update_request &update,
                  dsn::apps::update_response &resp);
    int batch_remove(int64_t decree, const dsn::blob &key, dsn::apps::update_response &resp);
    int batch_commit(int64_t decree);
    void batch_abort(int64_t decree, int err);

    void set_default_ttl(uint32_t ttl);

private:
    void clear_up_batch_states(int64_t decree, int err);

    static dsn::blob composite_raw_key(dsn::string_view hash_key, dsn::string_view sort_key);
    // return true if the check type is supported
    static bool is_check_type_supported(::dsn::apps::cas_check_type::type check_type);

    // return true if check passed.
    // for int compare, if check operand or value are not valid integer, then return false,
    // and set out param `invalid_argument' to false.
    bool validate_check(int64_t decree,
                        ::dsn::apps::cas_check_type::type check_type,
                        const ::dsn::blob &check_operand,
                        bool value_exist,
                        const ::dsn::blob &value,
                        bool &invalid_argument);

private:
    friend class pegasus_write_service_test;
    friend class pegasus_server_write_test;
    friend class pegasus_write_service_impl_test;
    FRIEND_TEST(pegasus_write_service_impl_test, put_verify_timetag);
    FRIEND_TEST(pegasus_write_service_impl_test, verify_timetag_compatible_with_version_0);

    const std::string _primary_address;
    const uint32_t _pegasus_data_version;
    std::unique_ptr<rocksdb_wrapper> _rocksdb_wrapper;

    ::dsn::perf_counter_wrapper &_pfc_recent_expire_count;

    // for setting update_response.error after committed.
    std::vector<dsn::apps::update_response *> _update_responses;
};

} // namespace server
} // namespace pegasus
