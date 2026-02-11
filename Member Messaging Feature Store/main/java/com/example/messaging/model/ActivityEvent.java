package com.example.messaging.model;

public class ActivityEvent {
  public String memberId;
  public String eventType;     // play, browse, search, session_start, add_to_list...
  public long eventTsMs;       // event-time timestamp
  public String device;        // optional
  public String titleId;       // optional

  public ActivityEvent() {}
}
