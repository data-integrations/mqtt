/*
 * Copyright © 2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.mqtt;

import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.common.Constants;
import io.cdap.plugin.common.IdUtils;
import io.cdap.plugin.common.ReferencePluginConfig;

import java.io.Serializable;

/**
 * Configuration for the MQTT source.
 */
public class MQTTStreamingSourceConfig extends ReferencePluginConfig implements Serializable {
  private static final long serialVersionUID = -8810631081535690190L;

  public static final String BROKER_URL = "brokerUrl";
  public static final String TOPIC = "topic";

  @Name(BROKER_URL)
  @Macro
  @Description("The URL of the MQTT broker to connect to.")
  private final String brokerUrl;

  @Name(TOPIC)
  @Macro
  @Description("The MQTT topic to listen to.")
  private final String topic;

  public MQTTStreamingSourceConfig(String referenceName, String brokerUrl, String topic) {
    super(referenceName);
    this.brokerUrl = brokerUrl;
    this.topic = topic;
  }

  public String getBrokerUrl() {
    return brokerUrl;
  }

  public String getTopic() {
    return topic;
  }

  public void validate(FailureCollector failureCollector) {
    try {
      IdUtils.validateId(referenceName);
    } catch (IllegalArgumentException e) {
      failureCollector.addFailure(e.getMessage(),
                                  "Reference Name may contain only letters, numbers, and _, -, ., or $.")
        .withConfigProperty(Constants.Reference.REFERENCE_NAME);
    }
    if (!containsMacro(BROKER_URL) && Strings.isNullOrEmpty(brokerUrl)) {
      failureCollector.addFailure("Broker URL must be specified", null)
        .withConfigProperty(BROKER_URL);
    }
    if (!containsMacro(TOPIC) && Strings.isNullOrEmpty(topic)) {
      failureCollector.addFailure("MQTT Topic must be specified", null)
        .withConfigProperty(TOPIC);
    }
  }
}
