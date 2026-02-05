# Delayed Queue

[![Maven Central](https://img.shields.io/maven-central/v/org.funfix/delayedqueue-jvm.svg)](https://search.maven.org/artifact/org.funfix/delayedqueue-jvm)
[![javadoc](https://javadoc.io/badge2/org.funfix/delayedqueue-jvm/javadoc.svg)](https://javadoc.io/doc/org.funfix/delayedqueue-jvm)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

A delayed, FIFO queue for the JVM with Java-first APIs and explicit acknowledgement semantics.

- Schedule messages for future delivery with `Instant` timestamps
- Poll with an acknowledgement callback; unacked messages get redelivered after a timeout
- Batch offers for bulk scheduling
- Cron-like scheduling helpers for periodic or daily ticks

## Install

Pick the latest version from Maven Central.

Maven:
```xml
<dependency>
  <groupId>org.funfix</groupId>
  <artifactId>delayedqueue-jvm</artifactId>
  <version>x.y.z</version>
</dependency>
```

Gradle:
```kotlin
dependencies {
    implementation("org.funfix:delayedqueue-jvm:x.y.z")
}
```

sbt:
```scala
libraryDependencies += "org.funfix" % "delayedqueue-jvm" % "x.y.z"
```

## Quickstart (Java)

```java
import java.time.Instant;
import org.funfix.delayedqueue.jvm.AckEnvelope;
import org.funfix.delayedqueue.jvm.DelayedQueue;
import org.funfix.delayedqueue.jvm.DelayedQueueInMemory;

DelayedQueue<String> queue = DelayedQueueInMemory.create();

queue.offerOrUpdate("order-123", "ready", Instant.now().plusSeconds(5));

AckEnvelope<String> envelope = queue.poll();
try {
    System.out.println(envelope.payload());
    envelope.acknowledge();
} catch (Exception e) {
    // skip acknowledge: message will be redelivered after acquireTimeout
}
```

## Batch offers (Java)

```java
import java.time.Instant;
import java.util.List;
import org.funfix.delayedqueue.jvm.BatchedMessage;
import org.funfix.delayedqueue.jvm.BatchedReply;
import org.funfix.delayedqueue.jvm.ScheduledMessage;

List<BatchedMessage<String, String>> batch = List.of(
    new BatchedMessage<>(
        "input-1",
        new ScheduledMessage<>("key-1", "payload-1", Instant.now().plusSeconds(1), true)
    ),
    new BatchedMessage<>(
        "input-2",
        new ScheduledMessage<>("key-2", "payload-2", Instant.now().plusSeconds(2), false)
    )
);

List<BatchedReply<String, String>> replies = queue.offerBatch(batch);
```

## Cron-style scheduling (Java)

```java
import java.time.Duration;
import org.funfix.delayedqueue.jvm.DelayedQueue;
import org.funfix.delayedqueue.jvm.DelayedQueueInMemory;

DelayedQueue<String> queue = DelayedQueueInMemory.create();

try (AutoCloseable handle = queue.getCron().installPeriodicTick(
    "heartbeat",
    Duration.ofMinutes(5),
    at -> "beat@" + at
)) {
    // keep the scheduler alive while your app runs
}
```

## Configuration

Use `DelayedQueueTimeConfig` to control polling cadence and redelivery behavior.

```java
import java.time.Duration;
import org.funfix.delayedqueue.jvm.DelayedQueueInMemory;
import org.funfix.delayedqueue.jvm.DelayedQueueTimeConfig;

DelayedQueueTimeConfig timeConfig =
    DelayedQueueTimeConfig.create(Duration.ofSeconds(20), Duration.ofMillis(50));

DelayedQueueInMemory<String> queue =
    DelayedQueueInMemory.create(timeConfig, "orders", java.time.Clock.systemUTC());
```

## Documentation

- Javadoc: https://javadoc.io/doc/org.funfix/delayedqueue-jvm
- Source: `jvm/src/main/kotlin/org/funfix/delayedqueue/jvm`

## Development

```bash
./gradlew build
./gradlew ktfmtCheck
```

## Requirements

- JDK 21+

## License

Apache-2.0. See `LICENSE`.
