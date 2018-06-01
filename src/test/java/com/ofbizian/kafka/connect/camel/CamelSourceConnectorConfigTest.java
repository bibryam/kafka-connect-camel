package com.ofbizian.kafka.connect.camel;

import org.junit.Test;

public class CamelSourceConnectorConfigTest {
  @Test
  public void doc() {
    System.out.println(CamelSourceConnectorConfig.conf().toRst());
  }
}