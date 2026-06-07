package io.tapdata.construct.constructImpl;

import org.apache.logging.log4j.LogManager;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Fast, pure-logic tests for {@link WriteBehindBackpressure} (no Hazelcast, virtual clock/sleeper).
 * Verifies the watermark/hysteresis gate, cumulative timeout, resume, interrupt handling,
 * the LOW&lt;HIGH clamp and the optional heap guard.
 */
class WriteBehindBackpressureTest {

	/** Mutable backlog source. */
	static final class FakeBacklog implements WriteBehindBackpressure.BacklogSource {
		volatile long total;

		FakeBacklog(long total) {
			this.total = total;
		}

		@Override
		public long total() {
			return total;
		}

		@Override
		public long ofMap(String name) {
			return total;
		}
	}

	private static WriteBehindBackpressure bp(long high, long low, long timeoutMs,
											  WriteBehindBackpressure.BacklogSource backlog,
											  WriteBehindBackpressure.Clock clock,
											  WriteBehindBackpressure.Sleeper sleeper,
											  WriteBehindBackpressure.HeapGauge heap, int heapPct) {
		return new WriteBehindBackpressure(true, high, low, 10L, timeoutMs,
				Long.MAX_VALUE, Long.MAX_VALUE, heapPct, backlog, clock, sleeper, heap,
				LogManager.getLogger("WriteBehindBackpressureTest"));
	}

	@Test
	void returnsImmediately_whenBelowHigh() throws Exception {
		AtomicInteger sleeps = new AtomicInteger();
		WriteBehindBackpressure p = bp(100, 50, 0,
				new FakeBacklog(0), () -> 0L, ms -> sleeps.incrementAndGet(), () -> 0, 0);
		p.apply("m");
		assertEquals(0, sleeps.get(), "must not block below the high watermark");
	}

	@Test
	void blocksThenResumes_whenBacklogDrainsBelowLow() throws Exception {
		FakeBacklog b = new FakeBacklog(200);
		AtomicLong now = new AtomicLong(0);
		AtomicInteger sleeps = new AtomicInteger();
		WriteBehindBackpressure.Sleeper sleeper = ms -> {
			now.addAndGet(ms);
			if (sleeps.incrementAndGet() == 3) {
				b.total = 40; // drained below the low watermark
			}
		};
		WriteBehindBackpressure p = bp(100, 50, 0, b, now::get, sleeper, () -> 0, 0);
		p.apply("m");
		assertTrue(sleeps.get() >= 3, "should have blocked until backlog dropped below low");
		assertEquals(40, b.total);
	}

	@Test
	void cumulativeTimeout_failsTaskWhenNeverRecovers() {
		FakeBacklog b = new FakeBacklog(200); // stays high forever
		AtomicLong now = new AtomicLong(0);
		WriteBehindBackpressure.Sleeper sleeper = now::addAndGet; // advance virtual clock, never drains
		WriteBehindBackpressure p = bp(100, 50, 1000, b, now::get, sleeper, () -> 0, 0);
		RuntimeException ex = assertThrows(RuntimeException.class, () -> p.apply("m"));
		assertTrue(ex.getMessage().contains("Failing the task"), ex.getMessage());
	}

	@Test
	void interrupt_restoresFlagAndPropagates() {
		FakeBacklog b = new FakeBacklog(200);
		WriteBehindBackpressure.Sleeper sleeper = ms -> {
			throw new InterruptedException("task stop");
		};
		WriteBehindBackpressure p = bp(100, 50, 0, b, () -> 0L, sleeper, () -> 0, 0);
		assertThrows(InterruptedException.class, () -> p.apply("m"));
		// Interrupt flag must be restored (and this also clears it for later tests).
		assertTrue(Thread.interrupted(), "interrupt flag should be restored");
	}

	@Test
	void resolveLow_clampsWhenNotBelowHigh() {
		assertEquals(50, WriteBehindBackpressure.resolveLow(100, 150, null), "LOW>HIGH clamps to HIGH/2");
		assertEquals(50, WriteBehindBackpressure.resolveLow(100, 100, null), "LOW==HIGH clamps to HIGH/2");
		assertEquals(50, WriteBehindBackpressure.resolveLow(100, 50, null), "valid LOW unchanged");
	}

	@Test
	void heapGuard_triggersOnHighHeapAndResumesWhenItDrops() throws Exception {
		FakeBacklog b = new FakeBacklog(0); // backlog far below watermark
		AtomicInteger heap = new AtomicInteger(95);
		AtomicLong now = new AtomicLong(0);
		AtomicInteger sleeps = new AtomicInteger();
		WriteBehindBackpressure.Sleeper sleeper = ms -> {
			now.addAndGet(ms);
			if (sleeps.incrementAndGet() == 2) {
				heap.set(80); // below the heap low band (90 - 5)
			}
		};
		WriteBehindBackpressure p = bp(100, 50, 0, b, now::get, sleeper, heap::get, 90);
		p.apply("m");
		assertTrue(sleeps.get() >= 2, "heap guard should block until heap usage drops");
	}

	@Test
	void heapGuard_disabledByDefault() throws Exception {
		AtomicInteger sleeps = new AtomicInteger();
		WriteBehindBackpressure p = bp(100, 50, 0,
				new FakeBacklog(0), () -> 0L, ms -> sleeps.incrementAndGet(), () -> 99, 0);
		p.apply("m");
		assertEquals(0, sleeps.get(), "with heap guard off (0), high heap must not trigger backpressure");
	}

	@Test
	void disabled_returnsImmediately() throws Exception {
		AtomicInteger sleeps = new AtomicInteger();
		WriteBehindBackpressure p = new WriteBehindBackpressure(false, 100, 50, 10, 0,
				Long.MAX_VALUE, Long.MAX_VALUE, 0, new FakeBacklog(10_000),
				() -> 0L, ms -> sleeps.incrementAndGet(), () -> 0,
				LogManager.getLogger("WriteBehindBackpressureTest"));
		p.apply("m");
		assertEquals(0, sleeps.get(), "disabled policy must never block");
	}
}
