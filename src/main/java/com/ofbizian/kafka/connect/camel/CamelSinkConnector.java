package com.ofbizian.kafka.connect.camel;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CamelSinkConnector extends SinkConnector {
    private static Logger log = LoggerFactory.getLogger(CamelSinkConnector.class);
    private CamelSinkConnectorConfig config;
    private Map<String, String> configProps;

    @Override
    public String version() {
        return VersionUtil.getVersion();
    }

    @Override
    public void start(Map<String, String> map) {
        configProps = map;
        config = new CamelSinkConnectorConfig(map);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return CamelSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        log.info("Setting task configurations for {} workers.", maxTasks);
        final List<Map<String, String>> configs = new ArrayList<>(maxTasks);
        for (int i = 0; i < maxTasks; ++i) {
            configs.add(configProps);
        }
        return configs;
    }

    @Override
    public void stop() {
    }

    @Override
    public ConfigDef config() {
        return CamelSinkConnectorConfig.conf();
    }
}
