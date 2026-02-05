package org.funfix.delayedqueue.jvm

import java.time.Duration
import java.time.Instant
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class DelayedQueueInMemoryTest {

    // ========== Basic Operations ==========

    @Test
    fun `offerOrUpdate creates new message`() {
        val clock = Clock.test(Instant.parse("2024-01-01T00:00:00Z"))
        val queue = DelayedQueueInMemory.create<String>(clock = clock)
        val scheduleAt = clock.now().plusSeconds(10)

        val result = queue.offerOrUpdate("key1", "payload1", scheduleAt)

        assertEquals(OfferOutcome.Created::class.java, result::class.java)
        assertTrue(result is OfferOutcome.Created)
    }

    @Test
    fun `offerOrUpdate updates existing message`() {
        val clock = Clock.test(Instant.parse("2024-01-01T00:00:00Z"))
        val queue = DelayedQueueInMemory.create<String>(clock = clock)
        val scheduleAt = clock.now().plusSeconds(10)

        queue.offerOrUpdate("key1", "payload1", scheduleAt)
        val result = queue.offerOrUpdate("key1", "payload2", scheduleAt.plusSeconds(5))

        assertEquals(OfferOutcome.Updated::class.java, result::class.java)
    }

    @Test
    fun `offerIfNotExists creates new message`() {
        val clock = Clock.test(Instant.parse("2024-01-01T00:00:00Z"))
        val queue = DelayedQueueInMemory.create<String>(clock = clock)
        val scheduleAt = clock.now().plusSeconds(10)

        val result = queue.offerIfNotExists("key1", "payload1", scheduleAt)

        assertEquals(OfferOutcome.Created::class.java, result::class.java)
    }

    @Test
    fun `offerIfNotExists ignores existing message`() {
        val clock = Clock.test(Instant.parse("2024-01-01T00:00:00Z"))
        val queue = DelayedQueueInMemory.create<String>(clock = clock)
        val scheduleAt = clock.now().plusSeconds(10)

        queue.offerOrUpdate("key1", "payload1", scheduleAt)
        val result = queue.offerIfNotExists("key1", "payload2", scheduleAt.plusSeconds(5))

        assertEquals(OfferOutcome.Ignored::class.java, result::class.java)
    }

    // ========== Polling Operations ==========

    @Test
    fun `tryPoll returns null when queue is empty`() {
        val clock = Clock.test(Instant.parse("2024-01-01T00:00:00Z"))
        val queue = DelayedQueueInMemory.create<String>(clock = clock)

        val result = queue.tryPoll()

        assertNull(result)
    }

    @Test
    fun `tryPoll returns null when message not ready yet`() {
        val clock = Clock.test(Instant.parse("2024-01-01T00:00:00Z"))
        val queue = DelayedQueueInMemory.create<String>(clock = clock)
        val future = clock.now().plusSeconds(10)

        queue.offerOrUpdate("key1", "payload1", future)
        val result = queue.tryPoll()

        assertNull(result)
    }

    @Test
    fun `tryPoll returns message when ready`() {
        val clock = Clock.test(Instant.parse("2024-01-01T00:00:00Z"))
        val queue = DelayedQueueInMemory.create<String>(clock = clock)
        val scheduleAt = clock.now()

        queue.offerOrUpdate("key1", "payload1", scheduleAt)
        val envelope = queue.tryPoll()

        assertNotNull(envelope)
        assertEquals("payload1", envelope!!.payload)
        assertEquals(DeliveryType.FIRST_DELIVERY, envelope.deliveryType)
    }

    @Test
    fun `tryPoll respects FIFO ordering for messages with same schedule time`() {
        val clock = Clock.test(Instant.parse("2024-01-01T00:00:00Z"))
        val queue = DelayedQueueInMemory.create<String>(clock = clock)
        val scheduleAt = clock.now()

        queue.offerOrUpdate("key1", "payload1", scheduleAt)
        queue.offerOrUpdate("key2", "payload2", scheduleAt)
        queue.offerOrUpdate("key3", "payload3", scheduleAt)

        val msg1 = queue.tryPoll()
        val msg2 = queue.tryPoll()
        val msg3 = queue.tryPoll()

        assertNotNull(msg1)
        assertNotNull(msg2)
        assertNotNull(msg3)
        assertEquals("payload1", msg1!!.payload)
        assertEquals("payload2", msg2!!.payload)
        assertEquals("payload3", msg3!!.payload)
    }

    @Test
    fun `tryPoll marks message for redelivery after timeout`() {
        val clock = Clock.test(Instant.parse("2024-01-01T00:00:00Z"))
        val timeConfig = DelayedQueueTimeConfig(acquireTimeout = Duration.ofSeconds(5))
        val queue = DelayedQueueInMemory.create<String>(timeConfig = timeConfig, clock = clock)
        val scheduleAt = clock.now()

        queue.offerOrUpdate("key1", "payload1", scheduleAt)
        val envelope1 = queue.tryPoll()
        assertNotNull(envelope1)
        assertEquals(DeliveryType.FIRST_DELIVERY, envelope1!!.deliveryType)

        // Don't acknowledge, advance time past timeout
        clock.advance(Duration.ofSeconds(6))

        val envelope2 = queue.tryPoll()
        assertNotNull(envelope2)
        assertEquals("payload1", envelope2!!.payload)
        assertEquals(DeliveryType.REDELIVERY, envelope2.deliveryType)
    }

    @Test
    fun `tryPollMany returns empty list when queue is empty`() {
        val clock = Clock.test(Instant.parse("2024-01-01T00:00:00Z"))
        val queue = DelayedQueueInMemory.create<String>(clock = clock)

        val envelope = queue.tryPollMany(10)

        assertNotNull(envelope)
        assertTrue(envelope.payload.isEmpty())
    }

    @Test
    fun `tryPollMany returns available messages up to limit`() {
        val clock = Clock.test(Instant.parse("2024-01-01T00:00:00Z"))
        val queue = DelayedQueueInMemory.create<String>(clock = clock)
        val scheduleAt = clock.now()

        queue.offerOrUpdate("key1", "payload1", scheduleAt)
        queue.offerOrUpdate("key2", "payload2", scheduleAt)
        queue.offerOrUpdate("key3", "payload3", scheduleAt)
        queue.offerOrUpdate("key4", "payload4", scheduleAt)

        val envelope = queue.tryPollMany(2)

        assertNotNull(envelope)
        assertEquals(2, envelope.payload.size)
    }

    @Test
    fun `tryPollMany does not return future messages`() {
        val clock = Clock.test(Instant.parse("2024-01-01T00:00:00Z"))
        val queue = DelayedQueueInMemory.create<String>(clock = clock)
        val now = clock.now()

        queue.offerOrUpdate("key1", "payload1", now)
        queue.offerOrUpdate("key2", "payload2", now.plusSeconds(10))
        queue.offerOrUpdate("key3", "payload3", now.plusSeconds(20))

        val envelope = queue.tryPollMany(10)

        assertNotNull(envelope)
        assertEquals(1, envelope.payload.size)
        assertEquals("payload1", envelope.payload[0])
    }

    // ========== Acknowledgment ==========

    @Test
    fun `acknowledge removes message from queue`() {
        val clock = Clock.test(Instant.parse("2024-01-01T00:00:00Z"))
        val queue = DelayedQueueInMemory.create<String>(clock = clock)
        val scheduleAt = clock.now()

        queue.offerOrUpdate("key1", "payload1", scheduleAt)
        val envelope = queue.tryPoll()
        assertNotNull(envelope)

        envelope!!.acknowledge()

        // Message should not be available for redelivery
        clock.advance(Duration.ofMinutes(10))
        assertNull(queue.tryPoll())
    }

    @Test
    fun `acknowledge is idempotent`() {
        val clock = Clock.test(Instant.parse("2024-01-01T00:00:00Z"))
        val queue = DelayedQueueInMemory.create<String>(clock = clock)
        val scheduleAt = clock.now()

        queue.offerOrUpdate("key1", "payload1", scheduleAt)
        val envelope = queue.tryPoll()
        assertNotNull(envelope)

        envelope!!.acknowledge()
        envelope.acknowledge() // Second call should be safe

        clock.advance(Duration.ofMinutes(10))
        assertNull(queue.tryPoll())
    }

    @Test
    fun `acknowledge does not remove if message was updated`() {
        val clock = Clock.test(Instant.parse("2024-01-01T00:00:00Z"))
        val queue = DelayedQueueInMemory.create<String>(clock = clock)
        val scheduleAt = clock.now()

        queue.offerOrUpdate("key1", "payload1", scheduleAt)
        val envelope = queue.tryPoll()
        assertNotNull(envelope)

        // Update the message before acknowledging
        queue.offerOrUpdate("key1", "payload2", scheduleAt)
        envelope!!.acknowledge()

        // The updated message should still be available
        val envelope2 = queue.tryPoll()
        assertNotNull(envelope2)
        assertEquals("payload2", envelope2!!.payload)
    }

    @Test
    fun `batch acknowledge removes all messages`() {
        val clock = Clock.test(Instant.parse("2024-01-01T00:00:00Z"))
        val queue = DelayedQueueInMemory.create<String>(clock = clock)
        val scheduleAt = clock.now()

        queue.offerOrUpdate("key1", "payload1", scheduleAt)
        queue.offerOrUpdate("key2", "payload2", scheduleAt)
        queue.offerOrUpdate("key3", "payload3", scheduleAt)

        val envelope = queue.tryPollMany(10)
        assertNotNull(envelope)
        assertEquals(3, envelope.payload.size)

        envelope.acknowledge()

        clock.advance(Duration.ofMinutes(10))
        assertNull(queue.tryPoll())
    }

    // ========== Read Operation ==========

    @Test
    fun `read returns message without removing it`() {
        val clock = Clock.test(Instant.parse("2024-01-01T00:00:00Z"))
        val queue = DelayedQueueInMemory.create<String>(clock = clock)
        val scheduleAt = clock.now()

        queue.offerOrUpdate("key1", "payload1", scheduleAt)
        val msg = queue.read("key1")

        assertNotNull(msg)
        assertEquals("payload1", msg!!.payload)

        // Message should still be available
        val envelope = queue.tryPoll()
        assertNotNull(envelope)
        assertEquals("payload1", envelope!!.payload)
    }

    @Test
    fun `read returns null for non-existent key`() {
        val clock = Clock.test(Instant.parse("2024-01-01T00:00:00Z"))
        val queue = DelayedQueueInMemory.create<String>(clock = clock)

        val msg = queue.read("nonexistent")

        assertNull(msg)
    }

    // ========== Concurrency ==========

    @Test
    fun `concurrent operations are thread-safe`() {
        val clock = Clock.test(Instant.parse("2024-01-01T00:00:00Z"))
        val queue = DelayedQueueInMemory.create<String>(clock = clock)
        val executor = Executors.newFixedThreadPool(4)
        val latch = CountDownLatch(1000)
        val scheduleAt = clock.now()

        // Offer 1000 messages concurrently
        repeat(1000) { i ->
            executor.submit {
                try {
                    queue.offerOrUpdate("key$i", "payload$i", scheduleAt)
                } finally {
                    latch.countDown()
                }
            }
        }

        assertTrue(latch.await(10, TimeUnit.SECONDS))

        // Poll all messages
        val polled = mutableListOf<String>()
        while (true) {
            val envelope = queue.tryPoll() ?: break
            polled.add(envelope.payload)
            envelope.acknowledge()
        }

        assertEquals(1000, polled.size)
        assertEquals(1000, polled.toSet().size) // All unique

        executor.shutdown()
        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS))
    }

    @Test
    fun `poll blocks until message is available`() {
        val clock = Clock.test(Instant.parse("2024-01-01T00:00:00Z"))
        val queue = DelayedQueueInMemory.create<String>(clock = clock)
        val scheduleAt = clock.now()
        val polled = AtomicInteger(0)
        val latch = CountDownLatch(1)

        // Start a thread that will poll (blocking)
        val thread = Thread {
            try {
                val envelope = queue.poll()
                if (envelope.payload == "payload1") {
                    polled.incrementAndGet()
                }
                envelope.acknowledge()
            } catch (e: InterruptedException) {
                // Expected when we interrupt
            } finally {
                latch.countDown()
            }
        }
        thread.start()

        // Give thread time to start waiting
        Thread.sleep(100)

        // Offer a message
        queue.offerOrUpdate("key1", "payload1", scheduleAt)

        // Wait for poll to complete
        assertTrue(latch.await(2, TimeUnit.SECONDS))
        assertEquals(1, polled.get())

        thread.join(1000)
    }

    // ========== Future Message Timing Tests ==========

    @Test
    fun `time advances correctly with TestClock`() {
        val clock = Clock.test(Instant.parse("2024-01-01T00:00:00Z"))
        val queue = DelayedQueueInMemory.create<String>(clock = clock)
        val future = clock.now().plusSeconds(10)

        queue.offerOrUpdate("key1", "payload1", future)
        assertNull(queue.tryPoll())

        clock.advance(Duration.ofSeconds(9))
        assertNull(queue.tryPoll())

        clock.advance(Duration.ofSeconds(2))
        val envelope = queue.tryPoll()
        assertNotNull(envelope)
        assertEquals("payload1", envelope!!.payload)
    }

    @Test
    fun `future message becomes available at exact scheduled time`() {
        val clock = Clock.test(Instant.parse("2024-01-01T00:00:00Z"))
        val queue = DelayedQueueInMemory.create<String>(clock = clock)
        val scheduleAt = clock.now().plusSeconds(100)

        queue.offerOrUpdate("key1", "payload1", scheduleAt)

        // 1 second before
        clock.setTime(scheduleAt.minusSeconds(1))
        assertNull(queue.tryPoll())

        // Exact time
        clock.setTime(scheduleAt)
        val envelope = queue.tryPoll()
        assertNotNull(envelope)
        assertEquals("payload1", envelope!!.payload)
    }

    @Test
    fun `multiple future messages become available in order`() {
        val clock = Clock.test(Instant.parse("2024-01-01T00:00:00Z"))
        val queue = DelayedQueueInMemory.create<String>(clock = clock)
        val base = clock.now()

        queue.offerOrUpdate("key1", "payload1", base.plusSeconds(10))
        queue.offerOrUpdate("key2", "payload2", base.plusSeconds(20))
        queue.offerOrUpdate("key3", "payload3", base.plusSeconds(30))

        clock.setTime(base.plusSeconds(15))
        assertEquals("payload1", queue.tryPoll()?.payload)
        assertNull(queue.tryPoll())

        clock.setTime(base.plusSeconds(25))
        assertEquals("payload2", queue.tryPoll()?.payload)
        assertNull(queue.tryPoll())

        clock.setTime(base.plusSeconds(35))
        assertEquals("payload3", queue.tryPoll()?.payload)
        assertNull(queue.tryPoll())
    }

    @Test
    fun `future messages all become available when time advances past all schedules`() {
        val clock = Clock.test(Instant.parse("2024-01-01T00:00:00Z"))
        val queue = DelayedQueueInMemory.create<String>(clock = clock)
        val base = clock.now()

        queue.offerOrUpdate("key1", "payload1", base.plusSeconds(10))
        queue.offerOrUpdate("key2", "payload2", base.plusSeconds(20))
        queue.offerOrUpdate("key3", "payload3", base.plusSeconds(30))

        clock.setTime(base.plusSeconds(100))

        val messages = mutableListOf<String>()
        while (true) {
            val envelope = queue.tryPoll() ?: break
            messages.add(envelope.payload)
            envelope.acknowledge()
        }

        assertEquals(3, messages.size)
        assertTrue(messages.contains("payload1"))
        assertTrue(messages.contains("payload2"))
        assertTrue(messages.contains("payload3"))
    }

    @Test
    fun `tryPollMany respects schedule times for future messages`() {
        val clock = Clock.test(Instant.parse("2024-01-01T00:00:00Z"))
        val queue = DelayedQueueInMemory.create<String>(clock = clock)
        val base = clock.now()

        queue.offerOrUpdate("key1", "payload1", base)
        queue.offerOrUpdate("key2", "payload2", base.plusSeconds(10))
        queue.offerOrUpdate("key3", "payload3", base.plusSeconds(20))

        val envelope1 = queue.tryPollMany(10)
        assertEquals(1, envelope1.payload.size)
        assertEquals("payload1", envelope1.payload[0])
        envelope1.acknowledge()

        clock.setTime(base.plusSeconds(15))
        val envelope2 = queue.tryPollMany(10)
        assertEquals(1, envelope2.payload.size)
        assertEquals("payload2", envelope2.payload[0])
        envelope2.acknowledge()

        clock.setTime(base.plusSeconds(25))
        val envelope3 = queue.tryPollMany(10)
        assertEquals(1, envelope3.payload.size)
        assertEquals("payload3", envelope3.payload[0])
    }

    @Test
    fun `redelivery happens at correct time after acquire timeout`() {
        val clock = Clock.test(Instant.parse("2024-01-01T00:00:00Z"))
        val timeConfig = DelayedQueueTimeConfig(acquireTimeout = Duration.ofSeconds(30))
        val queue = DelayedQueueInMemory.create<String>(timeConfig = timeConfig, clock = clock)
        val scheduleAt = clock.now()

        queue.offerOrUpdate("key1", "payload1", scheduleAt)
        val envelope1 = queue.tryPoll()
        assertNotNull(envelope1)
        assertEquals(DeliveryType.FIRST_DELIVERY, envelope1!!.deliveryType)

        // Don't acknowledge, check before timeout
        clock.advance(Duration.ofSeconds(29))
        assertNull(queue.tryPoll())

        // After timeout
        clock.advance(Duration.ofSeconds(2))
        val envelope2 = queue.tryPoll()
        assertNotNull(envelope2)
        assertEquals(DeliveryType.REDELIVERY, envelope2!!.deliveryType)
    }

    @Test
    fun `updating future message changes when it becomes available`() {
        val clock = Clock.test(Instant.parse("2024-01-01T00:00:00Z"))
        val queue = DelayedQueueInMemory.create<String>(clock = clock)
        val base = clock.now()

        queue.offerOrUpdate("key1", "payload1", base.plusSeconds(10))

        // Update to earlier time
        queue.offerOrUpdate("key1", "payload2", base.plusSeconds(5))

        clock.setTime(base.plusSeconds(6))
        val envelope = queue.tryPoll()
        assertNotNull(envelope)
        assertEquals("payload2", envelope!!.payload)
    }
}
