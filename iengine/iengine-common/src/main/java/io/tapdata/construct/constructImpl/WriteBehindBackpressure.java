package io.tapdata.construct.constructImpl;

import io.tapdata.pdk.core.utils.CommonUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Write-side "watermark backpressure" policy for write-behind cache IMaps.
 *
 * <p>When the GLOBAL write-behind backlog (total pending entries across all cache IMaps) exceeds the high
 * watermark, the calling (non-cooperative) write thread is blocked until the backlog falls back below the low
 * watermark, throttling producers to the external-storage drain rate so memory stays bounded instead of OOMing.</p>
 *
 * <p>All collaborators (backlog source, clock, sleeper, heap gauge) are injected so the policy can be unit
 * tested without starting Hazelcast and without real waits. Production wiring is built by {@link #fromEnv}.</p>
 *
 * @author samuel
 */
class WriteBehindBackpressure {

	/** Current write-behind backlog. */
	interface BacklogSource {
		long total();

		long ofMap(String name);
	}

	@FunctionalInterface
	interface Clock {
		long nowMs();
	}

	@FunctionalInterface
	interface Sleeper {
		void sleepMs(long ms) throws InterruptedException;
	}

	@FunctionalInterface
	interface HeapGauge {
		int usedPercent();
	}

	private static final int HEAP_HYSTERESIS_PCT = 5;

	private final boolean enabled;
	private final long high;
	private final long low;
	private final long waitStepMs;
	private final long timeoutMs;
	private final long logAfterMs;
	private final long logIntervalMs;
	private final int heapPercent;
	private final BacklogSource backlog;
	private final Clock clock;
	private final Sleeper sleeper;
	private final HeapGauge heap;
	private final Logger logger;

	/** Shared timestamp (ms) of when backpressure was first entered; -1 = not blocked. Reset only on real recovery. */
	private volatile long blockedSinceMs = -1L;

	WriteBehindBackpressure(boolean enabled, long high, long low, long waitStepMs, long timeoutMs,
							long logAfterMs, long logIntervalMs, int heapPercent,
							BacklogSource backlog, Clock clock, Sleeper sleeper, HeapGauge heap, Logger logger) {
		this.enabled = enabled;
		this.high = high;
		this.low = low;
		this.waitStepMs = waitStepMs;
		this.timeoutMs = timeoutMs;
		this.logAfterMs = logAfterMs;
		this.logIntervalMs = logIntervalMs;
		this.heapPercent = heapPercent;
		this.backlog = backlog;
		this.clock = clock;
		this.sleeper = sleeper;
		this.heap = heap;
		this.logger = logger;
	}

	/** Build the production policy from env / -D config with the real clock, sleeper and heap gauge. */
	static WriteBehindBackpressure fromEnv(BacklogSource backlog) {
		Logger log = LogManager.getLogger(WriteBehindBackpressure.class);
		boolean enabled = CommonUtils.getPropertyBool("TAPDATA_IMAP_BACKPRESSURE_ENABLED", true);
		long high = CommonUtils.getPropertyLong("TAPDATA_IMAP_BACKPRESSURE_HIGH", 300_000L);
		long low = resolveLow(high, CommonUtils.getPropertyLong("TAPDATA_IMAP_BACKPRESSURE_LOW", 150_000L), log);
		long waitStep = Math.max(1L, CommonUtils.getPropertyLong("TAPDATA_IMAP_BACKPRESSURE_WAIT_MS", 20L));
		long timeout = CommonUtils.getPropertyLong("TAPDATA_IMAP_BACKPRESSURE_TIMEOUT_MS", 0L);
		long logAfter = CommonUtils.getPropertyLong("TAPDATA_IMAP_BACKPRESSURE_LOG_AFTER_MS", 30_000L);
		long logInterval = CommonUtils.getPropertyLong("TAPDATA_IMAP_BACKPRESSURE_LOG_INTERVAL_MS", 60_000L);
		int heapPct = (int) CommonUtils.getPropertyLong("TAPDATA_IMAP_BACKPRESSURE_HEAP_PERCENT", 0L);
		return new WriteBehindBackpressure(enabled, high, low, waitStep, timeout, logAfter, logInterval, heapPct,
				backlog, System::currentTimeMillis, Thread::sleep, WriteBehindBackpressure::realHeapPercent, log);
	}

	/** Clamp LOW below HIGH so the watermark logic stays effective even if misconfigured. */
	static long resolveLow(long high, long lowRaw, Logger logger) {
		if (lowRaw >= high) {
			long fixed = Math.max(1L, high / 2L);
			if (logger != null) {
				logger.warn("TAPDATA_IMAP_BACKPRESSURE_LOW ({}) must be < HIGH ({}); clamping LOW to {} to keep backpressure effective.",
						lowRaw, high, fixed);
			}
			return fixed;
		}
		return lowRaw;
	}

	static int realHeapPercent() {
		Runtime r = Runtime.getRuntime();
		long max = r.maxMemory();
		if (max <= 0L) {
			return 0;
		}
		long used = r.totalMemory() - r.freeMemory();
		return (int) (used * 100L / max);
	}

	boolean isEnabled() {
		return enabled;
	}

	boolean overHigh() {
		if (backlog.total() >= high) {
			return true;
		}
		return heapPercent > 0 && heap.usedPercent() >= heapPercent;
	}

	boolean belowLow() {
		if (backlog.total() > low) {
			return false;
		}
		return !(heapPercent > 0 && heap.usedPercent() > heapPercent - HEAP_HYSTERESIS_PCT);
	}

	/**
	 * Block the current thread while the global backlog is above the watermark band.
	 *
	 * @param mapName name of the cache being written (for diagnostics only)
	 * @throws InterruptedException if the thread is interrupted while parked (interrupt flag is restored)
	 * @throws RuntimeException     if the cumulative pause exceeds the configured timeout
	 */
	void apply(String mapName) throws Exception {
		if (!enabled) {
			return;
		}
		if (!overHigh()) {
			if (belowLow()) {
				blockedSinceMs = -1L;
			}
			return;
		}
		long enteredAt = clock.nowMs();
		if (blockedSinceMs < 0L) {
			blockedSinceMs = enteredAt;
		}
		long lastLog = 0L;
		boolean logged = false;
		while (!belowLow()) {
			long now = clock.nowMs();
			long waited = now - blockedSinceMs;
			// Cumulative timeout: only fires if we never genuinely recovered (dropped below low watermark) for this long.
			if (timeoutMs > 0L && waited > timeoutMs) {
				blockedSinceMs = -1L;
				throw new RuntimeException("Global write-behind cache backlog stayed above the low watermark for "
						+ waited + "ms (total pending=" + backlog.total() + ", low=" + low
						+ (heapPercent > 0 ? ", heapUsed=" + heap.usedPercent() + "%" : "")
						+ "); the external storage appears unavailable. Failing the task to avoid engine OOM "
						+ "(controlled by TAPDATA_IMAP_BACKPRESSURE_TIMEOUT_MS).");
			}
			// Only log after a sustained block (brief, normal throttling stays silent), then repeat at an interval.
			if (waited >= logAfterMs && now - lastLog >= logIntervalMs) {
				String heapClause = heapPercent > 0
						? (", heapUsed=" + heap.usedPercent() + "% (guard " + heapPercent + "%)") : "";
				String timeoutClause = timeoutMs > 0L
						? ("The task will fail if this lasts ~" + (timeoutMs / 1000) + "s of cumulative pause without recovery.")
						: ("No timeout is set (TAPDATA_IMAP_BACKPRESSURE_TIMEOUT_MS=0); it will keep waiting until the external storage recovers.");
				logger.info("Data sync PAUSED by write-behind cache backpressure (expected behavior, protects the engine from OOM): "
								+ "total pending write-behind entries across caches={} (high={}, low={}){}; this cache IMap[{}] pending={}; "
								+ "waiting for the external storage (e.g. MongoDB) to flush. Paused for ~{}s. {} "
								+ "Tunables: -DTAPDATA_IMAP_BACKPRESSURE_HIGH / -DTAPDATA_IMAP_BACKPRESSURE_LOW; "
								+ "timeout -DTAPDATA_IMAP_BACKPRESSURE_TIMEOUT_MS=<ms> (0 = wait indefinitely); "
								+ "optional heap guard -DTAPDATA_IMAP_BACKPRESSURE_HEAP_PERCENT.",
						backlog.total(), high, low, heapClause, mapName, backlog.ofMap(mapName), waited / 1000, timeoutClause);
				lastLog = now;
				logged = true;
			}
			try {
				sleeper.sleepMs(waitStepMs);
			} catch (InterruptedException e) {
				// Task is being stopped: restore the interrupt flag and let it propagate as an interrupt, not a failure.
				Thread.currentThread().interrupt();
				throw e;
			}
		}
		blockedSinceMs = -1L;
		if (logged) {
			logger.info("Data sync RESUMED from write-behind cache backpressure: total pending dropped below the low watermark {}; "
							+ "continuing sync. This pause lasted ~{}s.",
					low, (clock.nowMs() - enteredAt) / 1000);
		}
	}
}
