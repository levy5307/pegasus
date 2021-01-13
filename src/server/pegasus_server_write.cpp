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

#include <dsn/cpp/message_utils.h>
#include <dsn/dist/replication/duplication_common.h>
#include <dsn/utility/defer.h>

#include "base/pegasus_key_schema.h"
#include "pegasus_server_write.h"
#include "pegasus_server_impl.h"
#include "logging_utils.h"
#include "pegasus_mutation_duplicator.h"

namespace pegasus {
namespace server {

pegasus_server_write::pegasus_server_write(pegasus_server_impl *server, bool verbose_log)
    : replica_base(server),
      _write_svc(new pegasus_write_service(server, verbose_log)),
      _verbose_log(verbose_log)
{
}

int pegasus_server_write::on_batched_write_requests(dsn::message_ex **requests,
                                                    int count,
                                                    int64_t decree,
                                                    uint64_t timestamp)
{
    _write_ctx = db_write_context::create(decree, timestamp);
    _decree = decree;

    // Write down empty record (RPC_REPLICATION_WRITE_EMPTY) to update
    // rocksdb's `last_flushed_decree` (see rocksdb::DB::GetLastFlushedDecree())
    // TODO(wutao1): remove it when shared log is removed.
    if (count == 0) {
        return _write_svc->empty_put(_decree);
    }

    dsn::task_code rpc_code(requests[0]->rpc_code());
    if (rpc_code == dsn::apps::RPC_RRDB_RRDB_MULTI_PUT) {
        dassert(count == 1, "count = %d", count);
        auto rpc = multi_put_rpc::auto_reply(requests[0]);
        return _write_svc->multi_put(_write_ctx, rpc.request(), rpc.response());
    }
    if (rpc_code == dsn::apps::RPC_RRDB_RRDB_MULTI_REMOVE) {
        dassert(count == 1, "count = %d", count);
        auto rpc = multi_remove_rpc::auto_reply(requests[0]);
        return _write_svc->multi_remove(_decree, rpc.request(), rpc.response());
    }
    if (rpc_code == dsn::apps::RPC_RRDB_RRDB_INCR) {
        dassert(count == 1, "count = %d", count);
        auto rpc = incr_rpc::auto_reply(requests[0]);
        return _write_svc->incr(_decree, rpc.request(), rpc.response());
    }
    if (rpc_code == dsn::apps::RPC_RRDB_RRDB_DUPLICATE) {
        dassert(count == 1, "count = %d", count);
        auto rpc = duplicate_rpc::auto_reply(requests[0]);
        return _write_svc->duplicate(_decree, rpc.request(), rpc.response());
    }
    if (rpc_code == dsn::apps::RPC_RRDB_RRDB_CHECK_AND_SET) {
        dassert(count == 1, "count = %d", count);
        auto rpc = check_and_set_rpc::auto_reply(requests[0]);
        return _write_svc->check_and_set(_decree, rpc.request(), rpc.response());
    }
    if (rpc_code == dsn::apps::RPC_RRDB_RRDB_CHECK_AND_MUTATE) {
        dassert(count == 1, "count = %d", count);
        auto rpc = check_and_mutate_rpc::auto_reply(requests[0]);
        return _write_svc->check_and_mutate(_decree, rpc.request(), rpc.response());
    }
    if (rpc_code == dsn::apps::RPC_RRDB_RRDB_BULK_LOAD) {
        dassert(count == 1, "count = %d", count);
        auto rpc = ingestion_rpc::auto_reply(requests[0]);
        return _write_svc->ingestion_files(_decree, rpc.request(), rpc.response());
    }

    return _write_svc->on_batched_writes(_write_ctx, requests, count);
}

void pegasus_server_write::set_default_ttl(uint32_t ttl) { _write_svc->set_default_ttl(ttl); }
} // namespace server
} // namespace pegasus
