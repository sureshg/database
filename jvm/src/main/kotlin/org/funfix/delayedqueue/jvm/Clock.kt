package org.funfix.delayedqueue.jvm

import java.time.Instant

/**
 * Clock abstraction for time-dependent operations.
 *
 * This allows testing time-dependent logic without Thread.sleep() by injecting a controllable clock
 * implementation.
 */
public interface Clock {
    /** Returns the current instant. */
    public fun now(): Instant

    public companion object {
        /** System clock using Instant.now(). */
        @JvmStatic public fun system(): Clock = SystemClock

        /** Test clock with manual time control. */
        @JvmStatic
        public fun test(initialTime: Instant = Instant.EPOCH): TestClock = TestClock(initialTime)
    }
}

/** System clock implementation using the actual system time. */
internal object SystemClock : Clock {
    override fun now(): Instant = Instant.now()
}

/**
 * Test clock implementation with manual time control.
 *
 * This clock allows tests to control time progression without Thread.sleep().
 */
public class TestClock(private var currentTime: Instant) : Clock {
    override fun now(): Instant = currentTime

    /** Advances the clock by the specified duration. */
    public fun advance(duration: java.time.Duration) {
        currentTime = currentTime.plus(duration)
    }

    /** Sets the clock to a specific instant. */
    public fun setTime(instant: Instant) {
        currentTime = instant
    }
}
