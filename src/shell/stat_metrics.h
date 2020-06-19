// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

const std::vector<std::string>& get_base_metric_names() {
    static const std::vector<std::string> base_metric_names{"get_qps"};
    return base_metric_names;
}

const std::vector<std::string>& get_composite_metric_names() {
    static const std::vector<std::string> composite_metric_names{"get_qps"};
    return composite_metric_names;
}

/// check whether base metric and composite name have same name or not.
struct metric_name_validator {
    metric_name_validator() {
        std::set<std::string> metric_name_set;

        const std::vector<std::string>& base_metric_names = get_base_metric_names();
        for (const auto &metric_name : base_metric_names) {
            if (metric_name_set.find(metric_name) != metric_name_set.end()) {
                dassert(false, "There have some same metric name in base metrics");
            }
            metric_name_set.insert(metric_name);
        }

        const std::vector<std::string>& composite_metric_names = get_composite_metric_names();
        for (const auto &metric_name : composite_metric_names) {
            if (metric_name_set.find(metric_name) != metric_name_set.end()) {
                dassert(false, "There have some same metric name in composite metrics");
            }
            metric_name_set.insert(metric_name);
        }
    }
};

#define METRIC_NAME_VALIDATOR                  \
    metric_name_validator validator

// todo: move row_data here, and rename to stat_metrics

