package com.example.messaging;

import com.example.messaging.config.KafkaConfig;
import com.example.messaging.model.*;
import com.example.messaging.serde.JsonSerde;
import com.example.messaging.stream.UnifiedFeatureProcessFunction;
import com.example.messaging.stream.Watermarks;

import org.apache.flink.api.common.serialization.*;
import org.apache.flink.connector.kafka.source.*;
import org.apache.flink.connector.kafka.sink.*;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.*;

import java.nio.charset.StandardCharsets;

public class App {

  public static void main(String[] args) throws Exception {
    final String bootstrap = env("KAFKA_BOOTSTRAP", "localhost:9092");
    final String groupId   = env("KAFKA_GROUP_ID", "messaging-feature-store");
    final String activityTopic = env("ACTIVITY_TOPIC", "user_activity_events");
    final String msgTopic      = env("MESSAGE_TOPIC", "messaging_events");
    final String outTopic      = env("OUT_TOPIC", "member_messaging_features");

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.enableCheckpointing(30_000); // important for Kafka EOS-ish semantics

    KafkaSource<ActivityEvent> activitySource = KafkaSource.<ActivityEvent>builder()
        .setBootstrapServers(bootstrap)
        .setTopics(activityTopic)
        .setGroupId(groupId + "-activity")
        .setValueOnlyDeserializer(new ActivityDeserializer())
        .build();

    KafkaSource<MessagingEvent> msgSource = KafkaSource.<MessagingEvent>builder()
        .setBootstrapServers(bootstrap)
        .setTopics(msgTopic)
        .setGroupId(groupId + "-message")
        .setValueOnlyDeserializer(new MessageDeserializer())
        .build();

    DataStream<ActivityEvent> activityStream = env
        .fromSource(activitySource, org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks(), "activity-kafka")
        .name("activity-source");

    DataStream<MessagingEvent> messageStream = env
        .fromSource(msgSource, org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks(), "message-kafka")
        .name("message-source");

    DataStream<UnifiedEvent> unified = activityStream
        .map(UnifiedEvent::of)
        .union(messageStream.map(UnifiedEvent::of))
        .assignTimestampsAndWatermarks(Watermarks.unifiedWatermarks());

    DataStream<MemberMessagingFeatures> features = unified
        .keyBy(u -> u.memberId)
        .process(new UnifiedFeatureProcessFunction())
        .name("unified-feature-process");

    KafkaSink<MemberMessagingFeatures> sink = KafkaSink.<MemberMessagingFeatures>builder()
        .setBootstrapServers(bootstrap)
        .setRecordSerializer(
            KafkaRecordSerializationSchema.builder()
                .setTopic(outTopic)
                .setValueSerializationSchema(new FeaturesSerializer())
                .build()
        )
        .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build();

    features.sinkTo(sink).name("features-kafka-sink");

    env.execute("Messaging Feature Store (Flink + Kafka)");
  }

  static String env(String k, String def) {
    String v = System.getenv(k);
    return (v == null || v.isBlank()) ? def : v;
  }

  // --- Deserializers ---
  static class ActivityDeserializer implements DeserializationSchema<ActivityEvent> {
    @Override public ActivityEvent deserialize(byte[] message) {
      return JsonSerde.fromJson(message, ActivityEvent.class);
    }
    @Override public boolean isEndOfStream(ActivityEvent nextElement) { return false; }
    @Override public TypeInformation<ActivityEvent> getProducedType() {
      return TypeInformation.of(ActivityEvent.class);
    }
  }

  static class MessageDeserializer implements DeserializationSchema<MessagingEvent> {
    @Override public MessagingEvent deserialize(byte[] message) {
      return JsonSerde.fromJson(message, MessagingEvent.class);
    }
    @Override public boolean isEndOfStream(MessagingEvent nextElement) { return false; }
    @Override public TypeInformation<MessagingEvent> getProducedType() {
      return TypeInformation.of(MessagingEvent.class);
    }
  }

  static class FeaturesSerializer implements SerializationSchema<MemberMessagingFeatures> {
    @Override public byte[] serialize(MemberMessagingFeatures element) {
      return JsonSerde.toJsonBytes(element);
    }
  }
}
