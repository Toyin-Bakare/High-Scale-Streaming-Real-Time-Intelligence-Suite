package com.example.messaging.serde;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonSerde {
  public static final ObjectMapper MAPPER = new ObjectMapper()
      .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

  public static <T> T fromJson(byte[] bytes, Class<T> clazz) {
    try {
      return MAPPER.readValue(bytes, clazz);
    } catch (Exception e) {
      throw new RuntimeException("JSON parse failed: " + clazz.getSimpleName(), e);
    }
  }

  public static byte[] toJsonBytes(Object obj) {
    try {
      return MAPPER.writeValueAsBytes(obj);
    } catch (Exception e) {
      throw new RuntimeException("JSON serialization failed", e);
    }
  }
}
