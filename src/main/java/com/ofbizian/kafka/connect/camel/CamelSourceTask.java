package com.ofbizian.kafka.connect.camel;

import org.apache.camel.CamelContext;
import org.apache.camel.ConsumerTemplate;
import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class CamelSourceTask extends SourceTask {
    static final Logger log = LoggerFactory.getLogger(CamelSourceTask.class);
    private CamelSinkConnectorConfig config;
    private Map<String, String> configProps;
    private CamelContext camelContext;
    private ConsumerTemplate consumerTemplate;

    @Override
    public String version() {
        return VersionUtil.getVersion();
    }

    @Override
    public void start(Map<String, String> map) {
        configProps = map;
        config = new CamelSinkConnectorConfig(map);
        camelContext = new DefaultCamelContext();
        consumerTemplate = camelContext.createConsumerTemplate();
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        String URI = config.getString(CamelSinkConnectorConfig.CAMEL_URI_CONF);

        try {
            ArrayList<SourceRecord> records = new ArrayList<>();
            while (records.isEmpty()) {
                Exchange exchange = consumerTemplate.receive(URI, 1000); //wait up to a second
                if (exchange != null) {
                    Map sourcePartition = Collections.singletonMap("filename", configProps.get("sourcePartition"));
                    Map sourceOffset = Collections.singletonMap("position", configProps.get("sourceOffset"));
                    records.add(new SourceRecord(sourcePartition, sourceOffset, configProps.get("topic"), Schema.STRING_SCHEMA, exchange.getIn().getBody()));
                }
            }
            return records;
        } catch (Exception e) {
        }
        return null;
    }

    @Override
    public void stop() {
        try {
            consumerTemplate.stop();
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