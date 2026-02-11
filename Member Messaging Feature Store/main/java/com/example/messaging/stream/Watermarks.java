package com.example.messaging.stream;

import com.example.messaging.model.UnifiedEvent;
import org.apache.flink.api.common.eventtime.*;

import java.time.Duration;

public class Watermarks {
  public static WatermarkStrategy<UnifiedEvent> unifiedWatermarks() {
    return WatermarkStrategy
        .<UnifiedEvent>forBoundedOutOfOrderness(Duration.ofSeconds(30))
        .withTimestampAssigner((e, ts) -> e.eventTsMs);
  }
}
