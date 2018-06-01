package com.ofbizian.kafka.connect.camel;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Importance;

import java.util.Map;

public class CamelSinkConnectorConfig extends AbstractConfig {

  public static final String CAMEL_URI_CONF = "camel.uri";
  private static final String CAMEL_URI_DOC = "The is Camel URI";
  public static final String CAMEL_URI_DEFAULT = "file:outbox";

  public CamelSinkConnectorConfig(ConfigDef config, Map<String, String> parsedConfig) {
    super(config, parsedConfig);
  }

  public CamelSinkConnectorConfig(Map<String, String> parsedConfig) {
    this(conf(), parsedConfig);
  }

  public static ConfigDef conf() {
    return new ConfigDef()
        .define(CAMEL_URI_CONF, Type.STRING, CAMEL_URI_DEFAULT, Importance.HIGH,
                CAMEL_URI_DOC);
  }
}
