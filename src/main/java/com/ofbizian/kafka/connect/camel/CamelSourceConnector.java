package com.ofbizian.kafka.connect.camel;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CamelSourceConnector extends SourceConnector {
    private static Logger log = LoggerFactory.getLogger(CamelSourceConnector.class);
    private CamelSourceConnectorConfig config;
    private Map<String, String> configProps;

    @Override
    public String version() {
        return VersionUtil.getVersion();
    }

    @Override
    public void start(Map<String, String> map) {
        configProps = map;
        config = new CamelSourceConnectorConfig(map);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return CamelSourceTask.class;
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
        return CamelSourceConnectorConfig.conf();
    }
}
