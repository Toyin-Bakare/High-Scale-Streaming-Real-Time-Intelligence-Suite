# Messaging Feature Store (Flink + Kafka on AWS/MSK)

A real-time streaming feature store that merges:
- recent user activity (plays/browses/searches)
- messaging history (sent/open/click)

to produce a unified `member_messaging_features` snapshot stream for:
- eligibility/suppression
- dashboards
- A/B experimentation
- downstream algorithmic features

## Architecture
Kafka topics:
- `user_activity_events`
- `messaging_events`
Output:
- `member_messaging_features`

Flink job:
- event-time processing + watermarks
- keyed state per member
- minute buckets for activity (1h + 24h rollups)
- hour buckets for messaging (24h sent + 7d opens/clicks)
- cooldown-based eligibility signals

## Run locally (Docker)
1) Start Kafka + Flink:

docker compose up -d

2) Create Kafka topics:
docker exec -it $(docker ps -qf name=kafka) bash -lc

3) Build job jar:
mvn -q -DskipTests package

4) Submit to Flink
docker cp target/messaging-feature-store-flink-1.0.0.jar $(docker ps -qf name=jobmanager):/job.jar
docker exec -it $(docker ps -qf name=jobmanager) flink run -c com.example.messaging.App /job.jar

5)Produce sample events:
python scripts/produce_sample_events.py

6) Consume output:
docker exec -it $(docker ps -qf name=kafka) bash -lc

Deploy to AWS

Use Amazon MSK for Kafka.

Run the job on Amazon Managed Service for Apache Flink (recommended) or Flink on EKS.

Configure authentication if using MSK IAM (SASL/IAM).

Notes for MSK IAM auth

If using MSK IAM, add SASL settings in KafkaConfig and provide the MSK IAM login module + callback handler.
(Left as an extension because exact configs differ by org.)