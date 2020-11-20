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

#include <cstdint>
#include <dsn/utility/string_view.h>
#include <dsn/dist/replication/replica_base.h>
#include <base/pegasus_value_schema.h>

namespace pegasus {
namespace server {
class pegasus_server_impl;
class db_write_context;
class db_get_context;

class rocksdb_wrapper : public dsn::replication::replica_base
{
public:
    explicit rocksdb_wrapper(pegasus_server_impl *server);

    int db_write_batch_put(int64_t decree,
                           dsn::string_view raw_key,
                           dsn::string_view value,
                           uint32_t expire_sec);

    int db_write_batch_put_ctx(const db_write_context &ctx,
                               dsn::string_view raw_key,
                               dsn::string_view value,
                               uint32_t expire_sec);

    int db_write_batch_delete(int64_t decree, dsn::string_view raw_key);

    int db_write(int64_t decree);

    int db_get(dsn::string_view raw_key, /*out*/ db_get_context *ctx);


private:
    uint32_t db_expire_ts(uint32_t expire_ts);

    pegasus_value_generator _value_generator;
};
} // namespace server
} // namespace pegasus
