// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

const std::vector<std::string> &get_base_metric_names()
{
    static const std::vector<std::string> base_metric_names{"get_qps"};
    return base_metric_names;
}

// todo: move row_data here, and rename to stat_metrics
