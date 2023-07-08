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
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.HistogramStatistics;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.reporter.InstantiateViaFactory;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.reporter.Scheduled;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Properties;


/**
 * {@link MetricReporter} that exports {@link Metric Metrics} via Kafka.
 */
@InstantiateViaFactory(factoryClassName = "org.apache.flink.metrics.kafka.KafkaReporterFactory")
public class KafkaReporter extends AbstractReporter implements Scheduled {

    private KafkaProducer<String, String> kafkaProducer;
    private final List<String> metricsFilter = new ArrayList<>();
    private String topic;

    @Override
    public void open(MetricConfig metricConfig) {
        final String filter = metricConfig.getString("filter", "numRecordsIn,numRecordsOut");
        if (!"none".equals(filter)) {
            this.metricsFilter.addAll(Arrays.asList(filter.split(",")));
        }
        this.topic = metricConfig.getString("topic", "FLINK_METRICS");

        final Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, metricConfig.getString("bootstrapServers", "localhost:9092"));
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, metricConfig.getString("batchSize", "16384"));
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, metricConfig.getString("lingerMs", "1000"));
        properties.setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG, metricConfig.getString("bufferMemory", "33554432"));
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        // acks = 0，不关心写入数据的可靠性
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "0");
        // request.required.acks 和 min.insync.replicas 配套 acks = all，才能提高写入数据的可靠性
//        properties.setProperty("request.required.acks","0");
//        properties.setProperty("min.insync.replicas","0");
        final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(null);
        kafkaProducer = new KafkaProducer<>(properties);
        Thread.currentThread().setContextClassLoader(classLoader);
    }

    @Override
    public void notifyOfAddedMetric(Metric metric, String metricName, MetricGroup group) {
//        if (!this.metricsFilter.isEmpty() && this.metricsFilter.contains(metricName)) {
        super.notifyOfAddedMetric(metric, metricName, group);
//        }
    }

    @Override
    public void notifyOfRemovedMetric(Metric metric, String metricName, MetricGroup group) {
//        if (!this.metricsFilter.isEmpty() && this.metricsFilter.contains(metricName)) {
        super.notifyOfRemovedMetric(metric, metricName, group);
//        }
    }

    @Override
    public void close() {
        if (kafkaProducer != null) {
            kafkaProducer.close();
        }
    }

    @Override
    public void report() {
        synchronized (this) {
            tryReport();
            tryRemove();
        }
    }

    private void tryReport() {
        final LinkedHashMap<String, JSONObject> map = new LinkedHashMap<>();
        this.counters.forEach((k, v) -> {
            final JSONObject jsonObject = new JSONObject();
            jsonObject.put(METRIC_GROUP, v);
            jsonObject.put(METRIC, this.getCounterValue(k));
            jsonObject.put(METRIC_TYPE, "Counter");
            map.put(v.getString(METRIC_IDENTIFIER), jsonObject);
        });

        this.gauges.forEach((k, v) -> {
            final JSONObject jsonObject = new JSONObject();
            jsonObject.put(METRIC_GROUP, v);
            jsonObject.put(METRIC, this.getGaugeValue(k));
            jsonObject.put(METRIC_TYPE, "Gauge");
            map.put(v.getString(METRIC_IDENTIFIER), jsonObject);
        });

        this.meters.forEach((k, v) -> {
            final JSONObject jsonObject = new JSONObject();
            jsonObject.put(METRIC_GROUP, v);
            jsonObject.put(METRIC, this.getMeterValue(k));
            jsonObject.put(METRIC_TYPE, "Meter");
            map.put(v.getString(METRIC_IDENTIFIER), jsonObject);
        });

        this.histograms.forEach((k, v) -> {
            final JSONObject jsonObject = new JSONObject();
            jsonObject.put(METRIC_GROUP, v);
            jsonObject.put(METRIC, this.getHistogramValue(k));
            jsonObject.put(METRIC_TYPE, "Histogram");
            map.put(v.getString(METRIC_IDENTIFIER), jsonObject);
        });

        map.forEach((k, v) -> {
            final ProducerRecord<String, String> producerRecord = new ProducerRecord<>(this.topic, k, v.toString());
            kafkaProducer.send(producerRecord);
        });
    }

    private JSONObject getCounterValue(Counter counter) {
        final JSONObject value = new JSONObject();
        value.put("count", counter.getCount());
        return value;
    }

    private JSONObject getGaugeValue(Gauge<?> gauge) {
        final JSONObject value = new JSONObject();
        value.put("value", gauge.getValue());
        return value;
    }

    private JSONObject getMeterValue(Meter meter) {
        final JSONObject value = new JSONObject();
        value.put("count", meter.getCount());
        value.put("rate", meter.getRate());
        return value;
    }

    private JSONObject getHistogramValue(Histogram histogram) {
        final JSONObject value = new JSONObject();
        value.put("count", histogram.getCount());
        final JSONObject statisticsJson = new JSONObject();
        final HistogramStatistics statistics = histogram.getStatistics();
        statisticsJson.put("values", statistics.getValues());       // 示例元素
        statisticsJson.put("size", statistics.size());              // 样本大小
        statisticsJson.put("mean", statistics.getMean());           // 平均值
        statisticsJson.put("max", statistics.getMax());             // 最大值
        statisticsJson.put("min", statistics.getMin());             // 最小值
        statisticsJson.put("stdDev", statistics.getStdDev());       // 标准偏差
        statisticsJson.put("p50", statistics.getQuantile(0.5));     // 分为数值
        statisticsJson.put("p75", statistics.getQuantile(0.75));    // 分为数值
        statisticsJson.put("p90", statistics.getQuantile(0.90));    // 分为数值
        statisticsJson.put("p95", statistics.getQuantile(0.95));    // 分为数值
        statisticsJson.put("p98", statistics.getQuantile(0.98));    // 分为数值
        statisticsJson.put("p99", statistics.getQuantile(0.99));    // 分为数值
        statisticsJson.put("p999", statistics.getQuantile(0.999));  // 分为数值
        value.put("statistics", statisticsJson);
        return value;
    }

}
