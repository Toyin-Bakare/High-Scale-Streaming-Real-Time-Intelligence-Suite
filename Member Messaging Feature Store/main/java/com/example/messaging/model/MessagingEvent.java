package com.example.messaging.model;

public class MessagingEvent {
  public String memberId;
  public String channel;       // email, push, interstitial
  public String messageType;   // live_reminder, winback, new_release...
  public String campaignId;    // optional
  public String messageId;     // idempotency key
  public String eventType;     // sent, delivered, open, click, dismiss, impression
  public long eventTsMs;

  public MessagingEvent() {}
}
