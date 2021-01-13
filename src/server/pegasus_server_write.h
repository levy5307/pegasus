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

#include <dsn/dist/replication/replica_base.h>

#include "base/pegasus_rpc_types.h"
#include "pegasus_write_service.h"

namespace pegasus {
namespace server {

/// This class implements the interface of `pegasus_sever_impl::on_batched_write_requests`.
/// TODO(zlw): remove class pegasus_server_write, move all of its' member functions to
/// pegassu_write_service
class pegasus_server_write : public dsn::replication::replica_base
{
public:
    pegasus_server_write(pegasus_server_impl *server, bool verbose_log);

    /// \return error code returned by rocksdb, i.e rocksdb::Status::code.
    /// **NOTE**
    /// Error returned is regarded as the failure of replica, thus will trigger
    /// cluster membership changes. Make sure no error is returned because of
    /// invalid user argument.
    /// As long as the returned error is 0, the operation is guaranteed to be
    /// successfully applied into rocksdb, which means an empty_put will be called
    /// even if there's no write.
    int on_batched_write_requests(dsn::message_ex **requests,
                                  int count,
                                  int64_t decree,
                                  uint64_t timestamp);

    void set_default_ttl(uint32_t ttl);

private:
    friend class pegasus_server_write_test;
    friend class pegasus_write_service_test;
    friend class pegasus_write_service_impl_test;
    friend class rocksdb_wrapper_test;

    std::unique_ptr<pegasus_write_service> _write_svc;
    db_write_context _write_ctx;
    int64_t _decree;

    const bool _verbose_log;
};

} // namespace server
} // namespace pegasus
