package com.ofbizian.kafka.connect.camel;

import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class CamelSinkTask extends SinkTask {
    private static Logger log = LoggerFactory.getLogger(CamelSinkTask.class);
    private CamelSinkConnectorConfig config;
    private Map<String, String> configProps;
    private CamelContext camelContext;
    private ProducerTemplate producerTemplate;

    @Override
    public String version() {
        return VersionUtil.getVersion();
    }

    @Override
    public void start(Map<String, String> map) {
        configProps = map;
        config = new CamelSinkConnectorConfig(map);
        camelContext = new DefaultCamelContext();
        producerTemplate = camelContext.createProducerTemplate();
    }

    @Override
    public void put(Collection<SinkRecord> collection) {
        if (collection.isEmpty()) {
            return;
        }

        String URI = config.getString(CamelSinkConnectorConfig.CAMEL_URI_CONF);

        final int recordsCount = collection.size();
        log.info("Received {} records", recordsCount);
        Iterator it = collection.iterator();
        while (it.hasNext()) {
            final SinkRecord record = (SinkRecord) it.next();
            log.info("Record kafka coordinates:({}-{}-{}). Writing it to Infinispan...", record.topic(), record.key(),
                    record.value());

            Object key = record.key();
            Object value = record.value();
            String topic = record.topic();
            long offset = record.kafkaOffset();
            Integer partition = record.kafkaPartition();
            String s = record.toString();
            log.info("record: " + s);
            Map<String, Object> headers = new HashMap<>();
            headers.put("KEY", key);
            headers.put("TOPIC", topic);
            headers.put("OFFSET", offset);
            headers.put("PARTITION", partition);

            producerTemplate.sendBodyAndHeaders(URI, value, headers);
        }
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> map) {
    }

    @Override
    public void stop() {
        try {
            producerTemplate.stop();
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            camelContext.stop();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
