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

package io.cdap.plugin.mqtt;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.streaming.StreamingContext;
import io.cdap.cdap.etl.api.streaming.StreamingSource;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.mqtt.MQTTUtils;

/**
 * Source that subscribes to a MQTT broker and listens to it.
 */
@Plugin(type = StreamingSource.PLUGIN_TYPE)
@Name("MQTT")
@Description("Listens to an MQTT broker and subscribes to a given topic.")
public class MQTTStreamingSource extends StreamingSource<StructuredRecord> {
  public static final String FIELD_NAME = "event";
  public static final Schema SCHEMA =
    Schema.recordOf("schema", Schema.Field.of(FIELD_NAME, Schema.of(Schema.Type.STRING)));

  private final MQTTStreamingSourceConfig config;

  public MQTTStreamingSource(MQTTStreamingSourceConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    super.configurePipeline(pipelineConfigurer);
    FailureCollector failureCollector = pipelineConfigurer.getStageConfigurer().getFailureCollector();
    config.validate(failureCollector);
    failureCollector.getOrThrowException();
    pipelineConfigurer.getStageConfigurer().setOutputSchema(SCHEMA);
  }

  @Override
  public JavaDStream<StructuredRecord> getStream(StreamingContext context) {
    JavaDStream<String> mqttStringStream =
      MQTTUtils.createStream(context.getSparkStreamingContext(), config.getBrokerUrl(), config.getTopic());

    return mqttStringStream.map(new Function<String, StructuredRecord>() {
      public StructuredRecord call(String input) {
        return StructuredRecord.builder(SCHEMA)
          .set(FIELD_NAME, input)
          .build();
      }
    });
  }
}
