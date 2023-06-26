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

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.reporter.InstantiateViaFactory;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.reporter.Scheduled;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;


/**
 * {@link MetricReporter} that exports {@link Metric Metrics} via Kafka.
 */
@InstantiateViaFactory(factoryClassName = "org.apache.flink.metrics.kafka.KafkaReporterFactory")
public class KafkaReporter extends AbstractReporter implements Scheduled {


    private KafkaProducer<String, String> kafkaProducer;
    private final List<String> metricsFilter = new ArrayList<>();
    private int chunkSize;
    private String topic;

    @Override
    public void open(MetricConfig metricConfig) {
        final String bootstrapServer = metricConfig.getString("bootstrapServers", "localhost:9092");
        final String filter = metricConfig.getString("filter", "numRecordsIn,numRecordsOut");
        final Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", bootstrapServer);
        properties.setProperty("acks", "all");
        properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(null);
        kafkaProducer = new KafkaProducer<>(properties);
        Thread.currentThread().setContextClassLoader(classLoader);
        if (!"none".equals(filter)) {
            this.metricsFilter.addAll(Arrays.asList(filter.split(",")));
        }
        this.chunkSize = Integer.parseInt(metricConfig.getString("chunkSize", "20"));
        this.topic = metricConfig.getString("topic", "flink_metrics");
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
        JSONArray jsonArray = new JSONArray();
        for (Gauge gauge : gauges.keySet()) {
            JSONObject jsonObject = new JSONObject();
            jsonObject.put(METRIC, gauges.get(gauge));
            jsonObject.put(NAME, gauges.get(gauge).getString(NAME));
            jsonObject.put(VALUE, gauge);
            jsonObject.put(TYPE, "Gauge");
            jsonArray.add(jsonObject);
        }
        for (Counter counter : counters.keySet()) {
            JSONObject jsonObject = new JSONObject();
            jsonObject.put(METRIC, counters.get(counter));
            jsonObject.put(NAME, counters.get(counter).getString(NAME));
            jsonObject.put(VALUE, counter);
            jsonObject.put(TYPE, "Counter");
            jsonArray.add(jsonObject);
        }
        for (Histogram histogram : histograms.keySet()) {
            JSONObject jsonObject = new JSONObject();
            jsonObject.put(METRIC, histograms.get(histogram));
            jsonObject.put(NAME, histograms.get(histogram).getString(NAME));
            jsonObject.put(VALUE, histogram);
            jsonObject.put(TYPE, "Histogram");
            jsonArray.add(jsonObject);

        }
        for (Meter meter : meters.keySet()) {
            JSONObject jsonObject = new JSONObject();
            jsonObject.put(METRIC, meters.get(meter));
            jsonObject.put(NAME, meters.get(meter).getString(NAME));
            jsonObject.put(VALUE, meter);
            jsonObject.put(TYPE, "Meter");
            jsonArray.add(jsonObject);

        }

        final Map<String, List<Object>> values = jsonArray.stream().collect(Collectors.groupingBy(item -> {
            JSONObject object = (JSONObject) item;
            return object.getString(NAME);
        }));

        for (Map.Entry<String, List<Object>> entry : values.entrySet()) {
            final List<Object> objects = entry.getValue();
            //spilt to chuck
            final List<List<Object>> batchValues = new ArrayList<>();
            for (int i = 0; i < objects.size(); i += chunkSize) {
                int end = Math.min(objects.size(), i + chunkSize);
                batchValues.add(objects.subList(i, end));
            }
            for (List<Object> chunk : batchValues) {
                final ProducerRecord<String, String> producerRecord = new ProducerRecord<>(this.topic, entry.getKey(), JSONObject.toJSONString(chunk));
                kafkaProducer.send(producerRecord);
            }
        }
    }

}
