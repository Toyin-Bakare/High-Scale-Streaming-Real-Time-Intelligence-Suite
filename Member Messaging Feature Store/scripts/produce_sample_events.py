import json, time, random
from kafka import KafkaProducer

BOOTSTRAP = "localhost:9092"
producer = KafkaProducer(bootstrap_servers=BOOTSTRAP, value_serializer=lambda v: json.dumps(v).encode("utf-8"))

members = ["m1", "m2", "m3"]
activity_types = ["play", "browse", "search"]
channels = ["push", "email", "interstitial"]

def now_ms():
    return int(time.time() * 1000)

while True:
    mid = random.choice(members)
    ts = now_ms()

    # activity
    a = {
        "memberId": mid,
        "eventType": random.choice(activity_types),
        "eventTsMs": ts,
        "device": random.choice(["ios", "android", "tv"]),
        "titleId": str(random.randint(1, 50)),
    }
    producer.send("user_activity_events", a)

    # messaging (sometimes)
    if random.random() < 0.5:
        m = {
            "memberId": mid,
            "channel": random.choice(channels),
            "messageType": random.choice(["live_reminder", "new_release", "winback"]),
            "campaignId": "c" + str(random.randint(1, 5)),
            "messageId": "msg-" + str(random.randint(1000, 9999)),
            "eventType": random.choice(["sent", "delivered", "open", "click"]),
            "eventTsMs": ts,
        }
        producer.send("messaging_events", m)

    producer.flush()
    time.sleep(0.25)
