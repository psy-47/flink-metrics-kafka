/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.metrics.kafka;

import com.alibaba.fastjson2.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.metrics.*;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.runtime.metrics.scope.ScopeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * An abstract reporter with registry for metrics.
 */
abstract class AbstractReporter implements MetricReporter {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    protected static final String METRIC_GROUP = "metric_group";
    protected static final String METRIC = "metric";
    protected static final String METRIC_TYPE = "metric_type";
    protected static final String METRIC_SCOPE_TYPE = "metric_scope_type";
    protected static final String METRIC_SCOPE = "metric_scope";
    protected static final String METRIC_NAME = "metric_name";
    protected static final String METRIC_FULL_NAME = "metric_full_name";
    protected static final String METRIC_IDENTIFIER = "metric_identifier";

    protected final Map<Gauge<?>, JSONObject> gauges = new HashMap<>();
    protected final Map<Counter, JSONObject> counters = new HashMap<>();
    protected final Map<Histogram, JSONObject> histograms = new HashMap<>();
    protected final Map<Meter, JSONObject> meters = new HashMap<>();
    protected final List<Metric> delayRemoveList = new ArrayList<>();

    @Override
    public void notifyOfAddedMetric(Metric metric, String metricName, MetricGroup group) {
        JSONObject metricGroup = convert(metricName, group);
        synchronized (this) {
            if (metric instanceof Counter) {
                counters.put((Counter) metric, metricGroup);
            } else if (metric instanceof Gauge) {
                gauges.put((Gauge<?>) metric, metricGroup);
            } else if (metric instanceof Histogram) {
                histograms.put((Histogram) metric, metricGroup);
            } else if (metric instanceof Meter) {
                meters.put((Meter) metric, metricGroup);
            } else {
                log.warn("Cannot add unknown metric type {}. This indicates that the reporter " +
                        "does not support this metric type.", metric.getClass().getName());
            }
        }
    }

    /**
     * metrics.scope.jm		    配置JobManager相关metrics       默认格式为 <host>.jobmanager
     * metrics.scope.jm.job	    配置JobManager上Job的相关metrics 默认格式为 <host>.jobmanager.<job_name>
     * metrics.scope.tm		    配置TaskManager上相关metrics     默认格式为 <host>.taskmanager.<tm_id>
     * metrics.scope.tm.job	    配置TaskManager上Job相关metrics  默认格式为 <host>.taskmanager.<tm_id>.<job_name>
     * metrics.scope.task		配置Task相关metrics             默认格式为 <host>.taskmanager.<tm_id>.<job_name>.<task_name>.<subtask_index>
     * metrics.scope.operator	配置Operator相关metrics         默认格式为 <host>.taskmanager.<tm_id>.<job_name>.<operator_name>.<subtask_index>
     *
     * @param metricName
     * @param group
     * @return
     */
    private JSONObject convert(String metricName, MetricGroup group) {
        final JSONObject metricGroup = new JSONObject();
        final Map<String, String> variables = group.getAllVariables();
        for (Map.Entry<String, String> variable : variables.entrySet()) {
            final String name = variable.getKey();
            metricGroup.put(name.substring(1, name.length() - 1), variable.getValue());
        }

        // 设置 metricScope 类型
        int size = variables.size() + 1;
        switch (variables.size()) {
            case 1:
                metricGroup.put(METRIC_SCOPE_TYPE, "jobManager");
                break;
            case 2:
                metricGroup.put(METRIC_SCOPE_TYPE, "taskManager");
                break;
            case 3:
                metricGroup.put(METRIC_SCOPE_TYPE, "jobManagerJob");
                size = 3;
                break;
            case 4:
                metricGroup.put(METRIC_SCOPE_TYPE, "taskManagerJob");
                size = 4;
                break;
            case 9:
                metricGroup.put(METRIC_SCOPE_TYPE, "task");
                size = 6;
                break;
            case 11:
                metricGroup.put(METRIC_SCOPE_TYPE, "operator");
                size = 6;
                break;
            default:
                metricGroup.put(METRIC_SCOPE_TYPE, "none");
        }
        final String metricIdentifier = group.getMetricIdentifier(metricName);
        final int ordinalIndexOf = StringUtils.ordinalIndexOf(metricIdentifier, ScopeFormat.SCOPE_SEPARATOR, size);
        if (ordinalIndexOf != -1) {
            metricGroup.put(METRIC_SCOPE, metricIdentifier.substring(0, ordinalIndexOf));
            metricGroup.put(METRIC_FULL_NAME, metricIdentifier.substring(ordinalIndexOf + 1));
        }
        metricGroup.put(METRIC_NAME, metricName);
        metricGroup.put(METRIC_IDENTIFIER, metricIdentifier);
//        metricGroup.put(SCOPE_COMPONENTS, group.getScopeComponents());
        return metricGroup;
    }

    @Override
    public void notifyOfRemovedMetric(Metric metric, String metricName, MetricGroup group) {
        synchronized (this) {
            delayRemoveList.add(metric);
        }
    }

    protected void tryRemove() {
        final List<Metric> removed = new ArrayList<>();
        for (Metric metric : delayRemoveList) {
            if (metric instanceof Counter) {
                counters.remove(metric);
            } else if (metric instanceof Gauge) {
                gauges.remove(metric);
            } else if (metric instanceof Histogram) {
                histograms.remove(metric);
            } else if (metric instanceof Meter) {
                meters.remove(metric);
            } else {
                log.warn("Cannot remove unknown metric type {}. This indicates that the reporter " +
                        "does not support this metric type.", metric.getClass().getName());
            }
            removed.add(metric);
        }
        delayRemoveList.removeAll(removed);
    }

    /**
     * 获取指标完整命名
     *
     * @param metricName 指标名
     * @param group      指标分组
     * @return
     */
    protected String getMetricFullName(String metricName, MetricGroup group) {
        final Map<String, String> variables = group.getAllVariables();
        int size = variables.size() + 1;
        switch (variables.size()) {
            case 1:
            case 2:
                break;
            case 3:
                size = 3;
                break;
            case 4:
                size = 4;
                break;
            case 9:
            case 11:
                size = 6;
                break;
            default:
        }
        final String metricIdentifier = group.getMetricIdentifier(metricName);
        final int ordinalIndexOf = StringUtils.ordinalIndexOf(metricIdentifier, ScopeFormat.SCOPE_SEPARATOR, size);
        if (ordinalIndexOf != -1) {
            return metricIdentifier.substring(ordinalIndexOf + 1);
        }
        return null;
    }

}
