package com.example.messaging.model;

public class UnifiedEvent {
  public enum Kind { ACTIVITY, MESSAGE }

  public Kind kind;
  public ActivityEvent activity;
  public MessagingEvent message;
  public String memberId;
  public long eventTsMs;

  public static UnifiedEvent of(ActivityEvent a) {
    UnifiedEvent u = new UnifiedEvent();
    u.kind = Kind.ACTIVITY;
    u.activity = a;
    u.memberId = a.memberId;
    u.eventTsMs = a.eventTsMs;
    return u;
  }

  public static UnifiedEvent of(MessagingEvent m) {
    UnifiedEvent u = new UnifiedEvent();
    u.kind = Kind.MESSAGE;
    u.message = m;
    u.memberId = m.memberId;
    u.eventTsMs = m.eventTsMs;
    return u;
  }

  public UnifiedEvent() {}
}
