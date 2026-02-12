# Delayed Queue: Scala Developer Guide

- [Introduction](#introduction)
- [Getting Started](#getting-started)
  - [Dependencies](#dependencies)
  - [SQLite Setup](#sqlite-setup)
- [Basic Usage](#basic-usage)
  - [Offering Messages](#offering-messages)
  - [Polling Messages](#polling-messages)
  - [Custom Message Types](#custom-message-types)
  - [Cron-like Scheduling](#cron-like-scheduling)
- [Scenarios](#scenarios)
  - [1: Scheduling Outside Business Hours](#1-scheduling-outside-business-hours)
  - [2: Daily Cron Job with Multi-Node Coordination](#2-daily-cron-job-with-multi-node-coordination)
- [Best Practices](#best-practices)
  - [1. Understand Acknowledgement Semantics](#1-understand-acknowledgement-semantics)
  - [2. Configure Appropriate Timeouts](#2-configure-appropriate-timeouts)
  - [3. Separate Queues by Concern](#3-separate-queues-by-concern)
  - [4. Test with Mocked Time](#4-test-with-mocked-time)
- [Additional Resources](#additional-resources)

## Introduction

Delayed Queue is a high-performance FIFO queue backed by your favorite RDBMS, with a functional Scala API built on Cats Effect. It enables you to:

- **Schedule messages** for future delivery at specific times
- **Poll with acknowledgement** - unacknowledged messages are automatically redelivered
- **Batch operations** for efficient bulk scheduling
- **Cron-like scheduling** for periodic tasks
- **Multi-node coordination** - multiple instances can share the same queue safely
- **Resource-safe** - automatic cleanup via Cats Effect `Resource`

Supported databases: H2, HSQLDB, MariaDB, Microsoft SQL Server, PostgreSQL, SQLite

## Getting Started

### Dependencies

```scala
libraryDependencies += "org.funfix" %% "delayedqueue-scala" % "x.y.z"
```

### SQLite Setup

SQLite is perfect for getting started - it requires no external database server.

```scala
import cats.effect.{IO, IOApp}
import org.funfix.delayedqueue.scala.*
import java.time.Instant
import scala.concurrent.duration.*

object QuickStart extends IOApp.Simple {
  // Import the given PayloadCodec for String
  import PayloadCodec.given

  def run: IO[Unit] = {
    // 1. Configure the database connection
    val dbConfig = JdbcConnectionConfig(
      url = "jdbc:sqlite:/tmp/myapp.db",  // Database file path
      driver = JdbcDriver.Sqlite,          // Database driver
      username = None,                     // Username (not needed for SQLite)
      password = None                      // Password (not needed for SQLite)
    )

    // 2. Configure the queue
    val queueConfig = DelayedQueueJDBCConfig.create(
      db = dbConfig,
      tableName = "delayed_queue",         // Table name
      queueName = "my-queue"               // Queue name (for partitioning)
    )

    // 3. Run migrations once per database, then use the queue
    for {
      _ <- DelayedQueueJDBC.runMigrations(queueConfig)
      _ <- DelayedQueueJDBC[String](queueConfig).use { queue =>
        IO.println("DelayedQueue is ready!")
      }
    } yield ()
  }
}
```

The queue is created as a `Resource[IO, DelayedQueue[A]]`, which ensures proper cleanup of database connections when the resource scope ends.

## Basic Usage

### Offering Messages

Schedule a message for future processing:

```scala
import cats.effect.IO
import org.funfix.delayedqueue.scala.*
import java.time.{Instant, Duration}

// Import the given PayloadCodec for String
import PayloadCodec.given

def scheduleMessage(queue: DelayedQueue[String]): IO[Unit] = {
  for {
    // Schedule a message for 1 hour from now
    now <- IO.realTimeInstant
    deliveryTime = now.plus(Duration.ofHours(1))
    
    outcome <- queue.offerOrUpdate(
      key = "transaction-12345",           // Unique key
      payload = "Process shipment for order",  // Payload
      scheduleAt = deliveryTime            // When to deliver
    )
    
    _ <- IO.println(s"Message scheduled: $outcome")
  } yield ()
}
```

### Polling Messages

Retrieve and process messages in a loop. `poll` blocks until a message is available (using semantic blocking via `IO.sleep`), with polling cadence controlled by `DelayedQueueTimeConfig`:

```scala
import cats.effect.IO
import cats.syntax.all.*

def processMessages(queue: DelayedQueue[String]): IO[Unit] = {
  val processOne = for {
    envelope <- queue.poll
    _ <- IO.println(s"Processing: ${envelope.payload}")
    _ <- processMessage(envelope.payload)
    _ <- envelope.acknowledge
  } yield ()

  // Handle errors without acknowledging (message will be redelivered)
  processOne.handleErrorWith { error =>
    IO.println(s"Failed to process message: ${error.getMessage}")
  }.flatMap(_ => 
    // Continue processing
    processMessages(queue)
  ) 
}

def processMessage(message: String): IO[Unit] =
  IO.println(s"Doing work for: $message")

// Use in an IOApp
DelayedQueueJDBC[String](queueConfig).use { queue =>
  processMessages(queue)
}
```

Use `tryPoll` only when you need a non-blocking check, and you plan to handle idling elsewhere. Prefer `poll` with `DelayedQueueTimeConfig` to control polling intervals.

### Custom Message Types

Define a custom `PayloadCodec` for complex types:

```scala
import io.circe.Codec
import io.circe.parser.decode
import io.circe.syntax.*
import org.funfix.delayedqueue.scala.PayloadCodec

case class Task(taskId: String, description: String)
  derives Codec.AsObject

object Task {
  // Define PayloadCodec for serialization/deserialization in the database
  given PayloadCodec[Task] = new PayloadCodec[Task] {
    override def typeName: String = "org.example.Task"
    
    override def serialize(payload: Task): Array[Byte] =
      payload.asJson.noSpaces.getBytes("UTF-8")
    
    override def deserialize(serialized: Array[Byte]): Either[IllegalArgumentException, Task] =
      decode[Task](new String(serialized, "UTF-8")).left.map { e => 
        new IllegalArgumentException(s"Deserialization failed", e)
      }
  }
}

// Use it
DelayedQueueJDBC[Task](queueConfig).use { queue =>
  for {
    task <- IO.pure(Task("TASK-123", "Process monthly report"))
    now <- IO.realTimeInstant
    _ <- queue.offerOrUpdate(
      s"task-${task.taskId}",
      task,
      now.plusSeconds(300)
    )
  } yield ()
}
```

### Cron-like Scheduling

Schedule recurring tasks using the `CronService`:

#### Periodic Tick

Run a task every N hours/minutes:

```scala
import scala.concurrent.duration.*

DelayedQueueJDBC[String](queueConfig).use { queue =>
  for {
    cron <- queue.cron
    // Schedule a message every hour
    _ <- cron.installPeriodicTick(
      keyPrefix = "health-check",
      period = Duration.ofHours(1),
      generator = instant => s"Health check at $instant"
    ).use { _ =>
      IO.never // Keep running indefinitely
    }
  } yield ()
}
```

#### Daily Schedule

Run tasks at specific times each day:

```scala
import java.time.{LocalTime, ZoneId}
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

DelayedQueueJDBC[String](queueConfig).use { queue =>
  for {
    cron <- queue.cron
    
    // Run at 2:00 AM and 2:00 PM daily (Eastern time)
    schedule = CronDailySchedule.create(
      zoneId = ZoneId.of("America/New_York"),
      hoursOfDay = List(LocalTime.of(2, 0), LocalTime.of(14, 0)),
      scheduleInAdvance = Duration.ofDays(7),  // Schedule 7 days ahead
      scheduleInterval = Duration.ofHours(1)   // Check every hour
    )
    
    _ <- cron.installDailySchedule(
      keyPrefix = "daily-backup",
      schedule = schedule,
      generator = instant => CronMessage("Run backup", instant)
    ).use { _ =>
      IO.never // Keep running indefinitely
    }
  } yield ()
}
```

## Scenarios

### 1: Scheduling Outside Business Hours

**Use Case**: You need to send email notifications, but want to avoid sending them during nighttime hours (20:00–08:00). Messages queued during off-hours should be scheduled for 08:00 the next morning.

```scala
import cats.effect.{IO, IOApp}
import org.funfix.delayedqueue.scala.*
import java.time.{Instant, LocalTime, ZoneId, ZonedDateTime}

object EmailScheduler extends IOApp.Simple {
  import PayloadCodec.given

  private val QUIET_HOURS_END = LocalTime.of(8, 0)
  private val QUIET_HOURS_START = LocalTime.of(20, 0)

  def run: IO[Unit] = {
    val dbConfig = JdbcConnectionConfig(
      url = "jdbc:sqlite:/tmp/emails.db",
      driver = JdbcDriver.Sqlite
    )
    
    val config = DelayedQueueJDBCConfig.create(
      db = dbConfig,
      tableName = "email_queue",
      queueName = "emails"
    )

    for {
      _ <- DelayedQueueJDBC.runMigrations(config)
      _ <- DelayedQueueJDBC[String](config).use { queue =>
        for {
          _ <- produceEmail(queue, "Order #12345 has shipped")
          _ <- consumeEmails(queue)
        } yield ()
      }
    } yield ()
  }

  def produceEmail(queue: DelayedQueue[String], emailMessage: String): IO[Unit] =
    for {
      now <- IO.realTimeInstant
      sendAt = calculateSendTime(now)
      _ <- queue.offerOrUpdate("email-order-12345", emailMessage, sendAt)
      _ <- IO.println(s"Email scheduled for: $sendAt")
    } yield ()

  def consumeEmails(queue: DelayedQueue[String]): IO[Unit] = {
    val processOne = for {
      envelope <- queue.poll
      _ <- sendEmail(envelope.payload)
      _ <- envelope.acknowledge
      _ <- IO.println("Email sent successfully")
    } yield ()

    processOne.handleErrorWith { error =>
      IO.println(s"Failed to send: ${error.getMessage}")
    }.flatMap(_ => consumeEmails(queue))
  }

  /**
   * Calculate when to send based on quiet hours.
   * - During daytime (08:00-20:00): send immediately
   * - During nighttime: schedule for 08:00 next morning
   */
  def calculateSendTime(now: Instant): Instant = {
    val zdt = now.atZone(ZoneId.systemDefault())
    val time = zdt.toLocalTime

    if (time.isBefore(QUIET_HOURS_END)) {
      // Before 08:00 - send at 08:00 today
      zdt.`with`(QUIET_HOURS_END).toInstant
    } else if (time.isBefore(QUIET_HOURS_START)) {
      // 08:00-20:00 - send now
      now
    } else {
      // After 20:00 - send at 08:00 tomorrow
      zdt.plusDays(1).`with`(QUIET_HOURS_END).toInstant
    }
  }

  def sendEmail(message: String): IO[Unit] =
    IO.println(s"Sending email: $message")
    // Actually send the email...
}
```

**Key Points:**
- `calculateSendTime` implements the business hours logic
- `offerOrUpdate` schedules the message for the calculated time
- `poll` blocks until a message is ready (using semantic blocking)
- `acknowledge` marks successful processing (or skip it to retry)
- Error handling via `handleErrorWith` ensures the consumer continues

### 2: Daily Cron Job with Multi-Node Coordination

**Use Case**: Run a daily data cleanup job at 02:00 AM. Multiple application instances are running for high availability, but the job should only run once per day - the first node to poll wins.

```scala
import cats.effect.{IO, IOApp}
import org.funfix.delayedqueue.scala.*
import java.time.{Duration, LocalTime, ZoneId}
import scala.jdk.CollectionConverters.*

object DailyCleanupJob extends IOApp.Simple {
  import PayloadCodec.given

  def run: IO[Unit] = {
    val dbConfig = JdbcConnectionConfig(
      url = "jdbc:postgresql://db.example.com:5432/myapp",
      driver = JdbcDriver.PostgreSQL,
      username = Some("appuser"),
      password = Some("password")
    )
    
    val config = DelayedQueueJDBCConfig.create(
      db = dbConfig,
      tableName = "scheduled_jobs",
      queueName = "cleanup"
    )

    for {
      _ <- DelayedQueueJDBC.runMigrations(config)
      _ <- DelayedQueueJDBC[String](config).use { queue =>
        for {
          cron <- queue.cron
          
          // Install daily schedule: run at 02:00 AM Eastern time
          schedule = CronDailySchedule.create(
            zoneId = ZoneId.of("America/New_York"),
            hoursOfDay = List(LocalTime.of(2, 0)),     // 02:00 AM
            scheduleInAdvance = Duration.ofDays(7),    // Schedule 7 days ahead
            scheduleInterval = Duration.ofHours(1)     // Update schedule hourly
          )

          _ <- cron.installDailySchedule(
            keyPrefix = "daily-cleanup",
            schedule = schedule,
            generator = instant => CronMessage(s"Cleanup job for $instant", instant)
          ).use { _ =>
            // Worker loop: continuously poll for jobs
            // This runs on ALL nodes, but only one will get each job
            workerLoop(queue)
          }
        } yield ()
      }
    } yield ()
  }

  def workerLoop(queue: DelayedQueue[String]): IO[Unit] = {
    val processOne = for {
      envelope <- queue.poll
      nodeId <- getNodeId
      _ <- IO.println(s"[Node-$nodeId] Got job: ${envelope.payload}")
      _ <- runCleanup
      _ <- envelope.acknowledge  // Success - job done
      _ <- IO.println("Cleanup completed")
    } yield ()

    processOne.handleErrorWith { error =>
      IO.println(s"Cleanup failed: ${error.getMessage}")
      // Don't acknowledge - another node can retry
    }.flatMap(_ => workerLoop(queue))
  }

  def runCleanup: IO[Unit] =
    IO.println("Deleting old records...")
    // Delete expired sessions, old logs, etc.

  def getNodeId: IO[String] =
    IO(java.net.InetAddress.getLocalHost.getHostName)
      .handleError(_ => "unknown")
}
```

**Key Points:**
- `CronDailySchedule` automatically creates future tasks at 02:00 AM
- Multiple nodes can run this code - **database locking ensures only one gets each task**
- `poll` blocks until a task is available (semantic blocking via `IO.sleep`)
- If the winning node fails without calling `acknowledge`, the task becomes available again

**How Multi-Node Works:**
1. `CronService` creates scheduled tasks in the database (one per day at 02:00 AM)
2. All nodes continuously call `poll`
3. Database-level locking ensures only one node acquires each task
4. The winning node processes and calls `acknowledge`
5. Other nodes block waiting for the next task
6. If the winner crashes, the task is automatically retried after the timeout

## Best Practices

### 1. Understand Acknowledgement Semantics

Whether to acknowledge a message depends on the error type. For transient errors (network issues, temporary unavailability), you typically want to skip acknowledgement, so the message is retried. For permanent errors (invalid data, business rule violations), you should acknowledge to prevent infinite retries:

```scala
def processEnvelope(envelope: AckEnvelope[String]): IO[Unit] =
  processMessage(envelope.payload)
    .flatMap(_ => envelope.acknowledge)
    .handleErrorWith {
      case _: ResourceUnavailableException =>
        // Transient error - don't acknowledge, will be redelivered
        IO.println("Service unavailable, will retry later")
      
      case e: ValidationException =>
        // Permanent error - acknowledge to prevent retrying invalid data
        envelope.acknowledge.flatMap(_ =>
          IO.println(s"Invalid message, acknowledged and discarding: ${e.getMessage}")
        )
      
      case e =>
        // Unknown error - log and decide based on your use case
        IO.println(s"Processing failed: ${e.getMessage}")
    }
```

### 2. Configure Appropriate Timeouts

Adjust timeouts based on your processing time:

```scala
// Default for RDBMS access
val defaultConfig = DelayedQueueTimeConfig.DEFAULT_JDBC

// Custom: 5 minute timeout for long-running tasks
val customConfig = DelayedQueueTimeConfig(
  acquireTimeout = 5.minutes, // How long a polled message is locked
  pollPeriod = 1.second      // How often to check for new messages
)

val config = DelayedQueueJDBCConfig(
  db = dbConfig,
  tableName = "my_queue",
  time = customConfig,  // Use custom timeouts
  queueName = "my-queue-name",
  ackEnvSource = "DelayedQueueJDBC:my-queue-name",
  retryPolicy = Some(RetryConfig.DEFAULT)
)
```

### 3. Separate Queues by Concern

Use different queue names for different types of work:

```scala
// Different queues for different concerns
val emailQueue: Resource[IO, DelayedQueue[Email]] = 
  DelayedQueueJDBC[Email](
    DelayedQueueJDBCConfig.create(dbConfig, "delayed_queue", "emails")
  )

val reportsQueue: Resource[IO, DelayedQueue[Report]] = 
  DelayedQueueJDBC[Report](
    DelayedQueueJDBCConfig.create(dbConfig, "delayed_queue", "reports")
  )

// They can share the same table, but are isolated by queue name + message type
```

### 4. Test with Mocked Time

Use a custom `Clock[IO]` for testing time-dependent behavior:

```scala
import cats.effect.{Clock, IO}
import cats.effect.testkit.TestControl
import java.time.Instant
import scala.concurrent.duration.*

test("schedule and poll with mocked time") {
  TestControl.executeEmbed {
    DelayedQueueInMemory[String]().use { queue =>
      for {
        now <- IO.realTimeInstant
        
        // Schedule for 60 seconds in the future
        _ <- queue.offerOrUpdate("test", "message", now.plusSeconds(60))
        
        // Message not available yet
        result1 <- queue.tryPoll
        _ <- IO(assertEquals(result1, None))
        
        // Advance time by 61 seconds
        _ <- IO.sleep(61.seconds)
        
        // Now message is available
        result2 <- queue.tryPoll
        _ <- IO(assert(result2.isDefined))
      } yield ()
    }
  }
}
```

---

## Additional Resources

- [Scaladoc](https://javadoc.io/doc/org.funfix/delayedqueue-scala_3)
- [Internals Documentation](./internals.md)
- [GitHub Repository](https://github.com/funfix/database)
- [Cats Effect Documentation](https://typelevel.org/cats-effect/)
