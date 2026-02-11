package com.example.messaging.stream;

import com.example.messaging.model.MemberMessagingFeatures;
import com.example.messaging.model.UnifiedEvent;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.*;

public class UnifiedFeatureProcessFunction extends KeyedProcessFunction<String, UnifiedEvent, MemberMessagingFeatures> {

  // ---- Bucketing choices ----
  // Activity: minute buckets for 24h rolling
  private static final long ACTIVITY_BUCKET_MS = Duration.ofMinutes(1).toMillis();
  private static final long ACTIVITY_RETENTION_MS = Duration.ofHours(24).toMillis();

  // Messaging: hour buckets for 7d rolling; also used for 24h sent counts
  private static final long MSG_BUCKET_MS = Duration.ofHours(1).toMillis();
  private static final long MSG_RETENTION_MS = Duration.ofDays(7).toMillis();

  // Cooldowns example
  private static final long PUSH_COOLDOWN_MS = Duration.ofHours(6).toMillis();
  private static final long EMAIL_COOLDOWN_MS = Duration.ofDays(1).toMillis();

  // ---- State ----
  // activityBuckets: bucketStartMs -> map(eventType -> count)
  private transient MapState<Long, Map<String, Long>> activityBuckets;

  // msgBuckets: bucketStartMs -> counters {sent, open, click}
  private transient MapState<Long, MsgCounters> msgBuckets;

  // last sent per channel
  private transient MapState<String, Long> lastSentTsByChannel;

  private transient ValueState<Long> lastActivityTs;

  // timer guard to avoid scheduling too often
  private transient ValueState<Long> nextCleanupTimerTs;

  @Override
  public void open(Configuration parameters) {
    StateTtlConfig activityTtl = StateTtlConfig
        .newBuilder(Time.hours(26)) // slightly above 24h
        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
        .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
        .cleanupFullSnapshot()
        .build();

    MapStateDescriptor<Long, Map<String, Long>> activityDesc =
        new MapStateDescriptor<>("activityBuckets", Long.class, (Class<Map<String, Long>>)(Class<?>)Map.class);
    activityDesc.enableTimeToLive(activityTtl);
    activityBuckets = getRuntimeContext().getMapState(activityDesc);

    StateTtlConfig msgTtl = StateTtlConfig
        .newBuilder(Time.days(8))
        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
        .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
        .cleanupFullSnapshot()
        .build();

    MapStateDescriptor<Long, MsgCounters> msgDesc =
        new MapStateDescriptor<>("msgBuckets", Long.class, MsgCounters.class);
    msgDesc.enableTimeToLive(msgTtl);
    msgBuckets = getRuntimeContext().getMapState(msgDesc);

    StateTtlConfig lastSentTtl = StateTtlConfig
        .newBuilder(Time.days(90))
        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
        .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
        .cleanupFullSnapshot()
        .build();

    MapStateDescriptor<String, Long> lastSentDesc =
        new MapStateDescriptor<>("lastSentTsByChannel", String.class, Long.class);
    lastSentDesc.enableTimeToLive(lastSentTtl);
    lastSentTsByChannel = getRuntimeContext().getMapState(lastSentDesc);

    ValueStateDescriptor<Long> lastActDesc = new ValueStateDescriptor<>("lastActivityTs", Long.class);
    lastActivityTs = getRuntimeContext().getState(lastActDesc);

    ValueStateDescriptor<Long> nextTimerDesc = new ValueStateDescriptor<>("nextCleanupTimerTs", Long.class);
    nextCleanupTimerTs = getRuntimeContext().getState(nextTimerDesc);
  }

  @Override
  public void processElement(UnifiedEvent value, Context ctx, Collector<MemberMessagingFeatures> out) throws Exception {
    long now = value.eventTsMs;

    if (value.kind == UnifiedEvent.Kind.ACTIVITY && value.activity != null) {
      handleActivity(value, now);
    } else if (value.kind == UnifiedEvent.Kind.MESSAGE && value.message != null) {
      handleMessage(value, now);
    }

    // schedule periodic cleanup timer (e.g., every 5 minutes)
    scheduleCleanupIfNeeded(ctx, now);

    // emit fresh snapshot
    out.collect(buildSnapshot(now));
  }

  @Override
  public void onTimer(long timestamp, OnTimerContext ctx, Collector<MemberMessagingFeatures> out) throws Exception {
    // cleanup old buckets and emit snapshot occasionally
    cleanup(timestamp);
    out.collect(buildSnapshot(timestamp));
  }

  private void handleActivity(UnifiedEvent e, long ts) throws Exception {
    long bucket = floorTo(ts, ACTIVITY_BUCKET_MS);

    Map<String, Long> counts = activityBuckets.get(bucket);
    if (counts == null) counts = new HashMap<>();
    counts.put(e.activity.eventType, counts.getOrDefault(e.activity.eventType, 0L) + 1L);
    activityBuckets.put(bucket, counts);

    Long last = lastActivityTs.value();
    if (last == null || ts > last) lastActivityTs.update(ts);
  }

  private void handleMessage(UnifiedEvent e, long ts) throws Exception {
    long bucket = floorTo(ts, MSG_BUCKET_MS);

    MsgCounters c = msgBuckets.get(bucket);
    if (c == null) c = new MsgCounters();

    String ev = e.message.eventType == null ? "" : e.message.eventType.toLowerCase(Locale.ROOT);
    if (ev.equals("sent")) {
      c.sent += 1L;
      if (e.message.channel != null) {
        lastSentTsByChannel.put(e.message.channel, ts);
      }
    } else if (ev.equals("open")) {
      c.open += 1L;
    } else if (ev.equals("click")) {
      c.click += 1L;
    }
    msgBuckets.put(bucket, c);
  }

  private void scheduleCleanupIfNeeded(Context ctx, long ts) throws Exception {
    Long next = nextCleanupTimerTs.value();
    long everyMs = Duration.ofMinutes(5).toMillis();
    long nextAligned = floorTo(ts + everyMs, everyMs);

    if (next == null || nextAligned > next) {
      ctx.timerService().registerEventTimeTimer(nextAligned);
      nextCleanupTimerTs.update(nextAligned);
    }
  }

  private void cleanup(long now) throws Exception {
    // Remove old activity buckets
    long minActivity = now - ACTIVITY_RETENTION_MS;
    List<Long> toRemove = new ArrayList<>();
    for (Long k : activityBuckets.keys()) {
      if (k < floorTo(minActivity, ACTIVITY_BUCKET_MS)) toRemove.add(k);
    }
    for (Long k : toRemove) activityBuckets.remove(k);

    // Remove old messaging buckets
    long minMsg = now - MSG_RETENTION_MS;
    toRemove.clear();
    for (Long k : msgBuckets.keys()) {
      if (k < floorTo(minMsg, MSG_BUCKET_MS)) toRemove.add(k);
    }
    for (Long k : toRemove) msgBuckets.remove(k);
  }

  private MemberMessagingFeatures buildSnapshot(long now) throws Exception {
    MemberMessagingFeatures f = new MemberMessagingFeatures();
    f.memberId = getCurrentKey();
    f.snapshotTsMs = now;

    // recent activity rollups (1h + 24h)
    long lastAct = Optional.ofNullable(lastActivityTs.value()).orElse(0L);
    f.lastActivityTsMs = lastAct;

    // activity counts
    long cutoff1h = now - Duration.ofHours(1).toMillis();
    long cutoff24h = now - Duration.ofHours(24).toMillis();

    for (Map.Entry<Long, Map<String, Long>> entry : activityBuckets.entries()) {
      long bucketTs = entry.getKey();
      Map<String, Long> m = entry.getValue();
      if (bucketTs >= floorTo(cutoff24h, ACTIVITY_BUCKET_MS)) {
        f.plays_24h += m.getOrDefault("play", 0L);
        f.browses_24h += m.getOrDefault("browse", 0L);
        f.searches_24h += m.getOrDefault("search", 0L);
      }
      if (bucketTs >= floorTo(cutoff1h, ACTIVITY_BUCKET_MS)) {
        f.plays_1h += m.getOrDefault("play", 0L);
        f.browses_1h += m.getOrDefault("browse", 0L);
        f.searches_1h += m.getOrDefault("search", 0L);
      }
    }

    // messaging rollups: sent_24h, opens/clicks_7d
    long cutoffMsg24h = now - Duration.ofHours(24).toMillis();
    long cutoffMsg7d = now - Duration.ofDays(7).toMillis();

    for (Map.Entry<Long, MsgCounters> entry : msgBuckets.entries()) {
      long bucketTs = entry.getKey();
      MsgCounters c = entry.getValue();
      if (bucketTs >= floorTo(cutoffMsg24h, MSG_BUCKET_MS)) {
        f.messagesSent_24h += c.sent;
      }
      if (bucketTs >= floorTo(cutoffMsg7d, MSG_BUCKET_MS)) {
        f.opens_7d += c.open;
        f.clicks_7d += c.click;
      }
    }

    // last sent per channel
    for (Map.Entry<String, Long> e : lastSentTsByChannel.entries()) {
      f.lastSentTsByChannel.put(e.getKey(), e.getValue());
    }

    // simple eligibility rules (portfolio example)
    applyEligibility(now, f);

    return f;
  }

  private void applyEligibility(long now, MemberMessagingFeatures f) {
    long lastPush = f.lastSentTsByChannel.getOrDefault("push", 0L);
    long lastEmail = f.lastSentTsByChannel.getOrDefault("email", 0L);

    f.eligiblePush = (now - lastPush) >= PUSH_COOLDOWN_MS;
    f.eligibleEmail = (now - lastEmail) >= EMAIL_COOLDOWN_MS;

    if (!f.eligiblePush) f.suppressionReasons.put("push", "cooldown_active");
    if (!f.eligibleEmail) f.suppressionReasons.put("email", "cooldown_active");

    // Example: if user inactive for long time, you might suppress certain types
    // (left intentionally minimal)
  }

  private static long floorTo(long ts, long sizeMs) {
    return (ts / sizeMs) * sizeMs;
  }

  // simple POJO for msg counters
  public static class MsgCounters {
    public long sent;
    public long open;
    public long click;

    public MsgCounters() {}
  }
}
