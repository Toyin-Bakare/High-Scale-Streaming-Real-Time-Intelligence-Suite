package com.example.messaging.model;

import java.util.HashMap;
import java.util.Map;

public class MemberMessagingFeatures {
  public String memberId;
  public long snapshotTsMs;

  // Recent activity (rolling)
  public long lastActivityTsMs;
  public long plays_1h;
  public long browses_1h;
  public long searches_1h;
  public long plays_24h;
  public long browses_24h;
  public long searches_24h;

  // Messaging history
  public Map<String, Long> lastSentTsByChannel = new HashMap<>(); // push/email/interstitial -> ts
  public long messagesSent_24h;
  public long opens_7d;
  public long clicks_7d;

  // Basic suppression flags (example)
  public boolean eligiblePush;
  public boolean eligibleEmail;

  // Reason strings help debugging / portfolio storytelling
  public Map<String, String> suppressionReasons = new HashMap<>();

  public MemberMessagingFeatures() {}
}
