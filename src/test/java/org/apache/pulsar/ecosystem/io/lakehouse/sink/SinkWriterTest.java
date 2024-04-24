/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.pulsar.ecosystem.io.lakehouse.sink;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Value;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.ecosystem.io.lakehouse.SinkConnectorConfig;
import org.apache.pulsar.ecosystem.io.lakehouse.common.ProtobufSchemaHelper;
import org.apache.pulsar.ecosystem.io.lakehouse.common.SchemaConverter;
import org.apache.pulsar.ecosystem.io.lakehouse.sink.delta.DeltaSinkConnectorConfig;
import org.apache.pulsar.functions.api.Record;
import org.bouncycastle.util.encoders.Base64;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Delta writer thread test.
 *
 */
@Slf4j
public class SinkWriterTest {

  @Test
  public void testAvroGenericDataConverter() {
    SinkConnectorConfig sinkConnectorConfig = new DeltaSinkConnectorConfig();
    SinkWriter sinkWriter = new SinkWriter(sinkConnectorConfig, new LinkedBlockingQueue<>());

    Map<String, SchemaType> schemaMap = new HashMap<>();
    schemaMap.put("name", SchemaType.STRING);
    schemaMap.put("age", SchemaType.INT32);
    schemaMap.put("phone", SchemaType.STRING);
    schemaMap.put("address", SchemaType.STRING);
    schemaMap.put("score", SchemaType.DOUBLE);

    Map<String, Object> recordMap = new HashMap<>();
    recordMap.put("name", "hang");
    recordMap.put("age", 18);
    recordMap.put("phone", "110");
    recordMap.put("address", "GuangZhou, China");
    recordMap.put("score", 59.9);

    Record<GenericObject> record =
        SinkConnectorUtils.generateRecord(schemaMap, recordMap,
            SchemaType.AVRO, "MyRecord");

    try {
      GenericRecord genericRecord =
          sinkWriter.convertToAvroGenericData(new PulsarSinkRecord(record), null, null).get();
      assertEquals(genericRecord.get("name"), "hang");
      assertEquals(genericRecord.get("age"), 18);
      assertEquals(genericRecord.get("phone"), "110");
      assertEquals(genericRecord.get("address"), "GuangZhou, China");
      assertEquals(genericRecord.get("score"), 59.9);
      assertEquals(genericRecord.getSchema().toString(),
          record.getSchema().getSchemaInfo().getSchemaDefinition());
    } catch (IOException e) {
      fail();
    }
  }

  @Test
  public void testJsonGenericDataConverter() {
    SinkConnectorConfig sinkConnectorConfig = new DeltaSinkConnectorConfig();
    SinkWriter sinkWriter = new SinkWriter(sinkConnectorConfig, new LinkedBlockingQueue<>());

    Map<String, SchemaType> schemaMap = new HashMap<>();
    schemaMap.put("name", SchemaType.STRING);
    schemaMap.put("age", SchemaType.INT32);
    schemaMap.put("phone", SchemaType.STRING);
    schemaMap.put("address", SchemaType.STRING);
    schemaMap.put("score", SchemaType.DOUBLE);

    Map<String, Object> recordMap = new HashMap<>();
    recordMap.put("name", "hang");
    recordMap.put("age", 18);
    recordMap.put("phone", "110");
    recordMap.put("address", "GuangZhou, China");
    recordMap.put("score", 59.9);

    Record<GenericObject> record =
        SinkConnectorUtils.generateRecord(schemaMap, recordMap,
            SchemaType.JSON, "MyRecord");

    try {
      String schemaStr = record.getSchema().getSchemaInfo().getSchemaDefinition();
      Schema schema = new org.apache.avro.Schema.Parser().parse(schemaStr);
      Schema schemaWithoutNull = SchemaConverter.convertPulsarAvroSchemaToNonNullSchema(schema);
      GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
      datumReader.setExpected(schemaWithoutNull);
      datumReader.setSchema(schemaWithoutNull);
      log.info("[hangc] schemaStr: {}", schemaStr);
      GenericRecord genericRecord =
          sinkWriter.convertToAvroGenericData(new PulsarSinkRecord(record), schemaWithoutNull,
              datumReader).get();
      assertEquals(String.valueOf(genericRecord.get("name")), "hang");
      assertEquals(genericRecord.get("age"), 18);
      assertEquals(String.valueOf(genericRecord.get("phone")), "110");
      assertEquals(String.valueOf(genericRecord.get("address")), "GuangZhou, China");
      assertEquals(genericRecord.get("score"), 59.9);
      assertEquals(genericRecord.getSchema().toString(), schemaWithoutNull.toString());
    } catch (IOException e) {
      fail();
    }
  }

  @Test
  public void testPrimitiveSchemaTypeConverter() {
    // TODO
  }

  @Test
  public void testProtoToAvroSchemaConverter() {
    SinkConnectorConfig sinkConnectorConfig = new DeltaSinkConnectorConfig();
    SinkWriter sinkWriter = new SinkWriter(sinkConnectorConfig, new LinkedBlockingQueue<>());
    SchemaInfo schemaInfo = org.apache.pulsar.client.api.Schema.PROTOBUF_NATIVE(
            Value.class)
        .getSchemaInfo();
    Schema avroSchema = sinkWriter.convertProtobufNativeToAvro(schemaInfo);
    Assert.assertNotNull(avroSchema);
    Assert.assertEquals(avroSchema.getName(), Value.getDescriptor().getName());
  }

  @Test
  public void testProtobufSchemaHelper() throws Exception {
    SchemaInfo schemaInfo = org.apache.pulsar.client.api.Schema.PROTOBUF_NATIVE(Value.class)
        .getSchemaInfo();
    JsonObject jsonSchema = JsonParser.parseString(
        new String(schemaInfo.getSchema(), StandardCharsets.UTF_8)).getAsJsonObject();
    byte[] fileDescrBytes = Base64.decode(jsonSchema.get("fileDescriptorSet")
        .getAsString());
    String messageTypeName = jsonSchema.get("rootMessageTypeName").getAsString();
    String rootFileDescriptorName = jsonSchema.get("rootFileDescriptorName").getAsString();
    FileDescriptorSet fileDescriptorSet = FileDescriptorSet.parseFrom(fileDescrBytes);
    List<Descriptor> descriptorList = ProtobufSchemaHelper.parseFileDescriptorSet(fileDescriptorSet,
        rootFileDescriptorName);
    assertFalse(descriptorList.isEmpty());
    assertTrue(descriptorList.stream()
        .anyMatch(descriptor -> descriptor.getFullName().equals(messageTypeName)));
  }
}
