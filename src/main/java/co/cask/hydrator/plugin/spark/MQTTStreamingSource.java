/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.hydrator.plugin.spark;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.streaming.StreamingContext;
import co.cask.cdap.etl.api.streaming.StreamingSource;
import co.cask.hydrator.common.ReferencePluginConfig;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.mqtt.MQTTUtils;

/**
 * Source that subscribes to a MQTT broker and listens to it.
 */
@Plugin(type = StreamingSource.PLUGIN_TYPE)
@Name("MQTT")
@Description("Listens to an MQTT broker and subscribes to a given topic.")
public class MQTTStreamingSource extends ReferenceStreamingSource<StructuredRecord> {
  public static final String FIELD_NAME = "event";
  public static final Schema SCHEMA =
    Schema.recordOf("schema", Schema.Field.of(FIELD_NAME, Schema.of(Schema.Type.STRING)));

  private final Config config;

  public MQTTStreamingSource(Config config) {
    super(config);
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    super.configurePipeline(pipelineConfigurer);
    pipelineConfigurer.getStageConfigurer().setOutputSchema(SCHEMA);
  }

  @Override
  public JavaDStream<StructuredRecord> getStream(StreamingContext context) throws Exception {
    JavaDStream<String> mqttStringStream =
      MQTTUtils.createStream(context.getSparkStreamingContext(), config.getBrokerUrl(), config.getTopic());

    return mqttStringStream.map(new Function<String, StructuredRecord>() {
      @Override
      public StructuredRecord call(String input) throws Exception {
        return StructuredRecord.builder(SCHEMA)
          .set(FIELD_NAME, input)
          .build();
      }
    });
  }

  /**
   * Configuration for the source.
   */
  public static class Config extends ReferencePluginConfig {
    @Macro
    @Description("The URL of the MQTT broker to connect to.")
    private String brokerUrl;

    @Macro
    @Description("The MQTT topic to listen to.")
    private String topic;

    public Config() {
      super(null);
      this.brokerUrl = "";
      this.topic = "";
    }

    private String getBrokerUrl() {
      return brokerUrl;
    }

    private String getTopic() {
      return topic;
    }
  }
}
