package io.tapdata.construct.constructImpl;

import com.hazelcast.map.IMap;
import io.tapdata.pdk.core.utils.CommonUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Background sampler of the write-behind backlog for the registered cache IMaps, used by
 * {@link ConstructIMap} for write-side "watermark backpressure" so that a slow or failing external storage
 * (e.g. MongoDB) cannot let the write-behind queue grow unbounded into an engine OOM.
 *
 * Design notes:
 * - Backlog is read from Hazelcast's public stats: {@code IMap.getLocalMapStats().getDirtyEntryCount()},
 *   which equals the sum of the (primary) write-behind queue sizes for that map — no internal reflection,
 *   no partition scan, no backup double-count.
 * - A single daemon thread samples the registered maps periodically into volatile fields; the write path
 *   does one O(1) read ({@link #totalBacklog()}), so there is no hot-path cost.
 * - Robust by design: a transient per-map failure keeps the last known value (never reports a spurious 0 that
 *   would release all blocked writers at once); a map that fails repeatedly (likely destroyed) is dropped;
 *   the sampler is never permanently disabled.
 *
 * @author samuel
 */
final class WriteBehindBacklogSampler {

	private static final Logger logger = LogManager.getLogger(WriteBehindBacklogSampler.class);

	private static final WriteBehindBacklogSampler INSTANCE = new WriteBehindBacklogSampler();

	static WriteBehindBacklogSampler getInstance() {
		return INSTANCE;
	}

	/** Sampling interval (ms), configurable at process level via env / -D. */
	private static final long SAMPLE_INTERVAL_MS =
			Math.max(20L, CommonUtils.getPropertyLong("TAPDATA_IMAP_BACKLOG_SAMPLE_INTERVAL_MS", 200L));
	/** Drop a map after this many consecutive sampling failures (it has almost certainly been destroyed). */
	private static final int MAX_MAP_FAILURES = 10;

	private final Map<String, IMap<?, ?>> registered = new ConcurrentHashMap<>();
	private final Map<String, Integer> failures = new ConcurrentHashMap<>();
	private volatile Map<String, Long> backlogByMap = Collections.emptyMap();
	private volatile long totalBacklog = 0L;
	private boolean started = false;

	private WriteBehindBacklogSampler() {
	}

	/** Register a write-behind cache IMap to be sampled, and lazily start the sampling thread (idempotent). */
	void register(String name, IMap<?, ?> imap) {
		if (name == null || imap == null) {
			return;
		}
		registered.put(name, imap);
		ensureStarted();
	}

	/** Stop sampling a map (call on destroy) so we don't poll a dead IMap. */
	void unregister(String name) {
		if (name == null) {
			return;
		}
		registered.remove(name);
		failures.remove(name);
	}

	/** Total pending write-behind entries across all registered caches (last sample); 0 before the first sample. */
	long totalBacklog() {
		return totalBacklog;
	}

	/** Pending write-behind entries for one cache (last sample); 0 if unknown. */
	long backlog(String name) {
		if (name == null) {
			return 0L;
		}
		Long v = backlogByMap.get(name);
		return v == null ? 0L : v;
	}

	private synchronized void ensureStarted() {
		if (started) {
			return;
		}
		Thread t = new Thread(this::loop, "imap-writebehind-backlog-sampler");
		t.setDaemon(true);
		t.start();
		started = true;
		logger.info("Write-behind backlog sampler started, interval={}ms", SAMPLE_INTERVAL_MS);
	}

	private void loop() {
		while (true) {
			Map<String, Long> prev = backlogByMap;
			try {
				Map<String, Long> current = new HashMap<>(registered.size() * 2);
				long total = 0L;
				for (Map.Entry<String, IMap<?, ?>> e : registered.entrySet()) {
					String name = e.getKey();
					try {
						long c = e.getValue().getLocalMapStats().getDirtyEntryCount();
						current.put(name, c);
						total += c;
						failures.remove(name);
					} catch (Throwable t) {
						// Transient (migration/destroy in progress): keep the last known value so we never
						// publish a spurious 0 that releases all blocked writers at once.
						int f = failures.merge(name, 1, Integer::sum);
						if (f >= MAX_MAP_FAILURES) {
							registered.remove(name);
							failures.remove(name);
						} else {
							Long last = prev.get(name);
							if (last != null) {
								current.put(name, last);
								total += last;
							}
						}
					}
				}
				backlogByMap = current;
				totalBacklog = total;
			} catch (Throwable t) {
				// Never let the sampler die or permanently disable backpressure; keep the previous snapshot.
				logger.debug("Write-behind backlog sampling cycle failed, keeping previous snapshot: {}", t.toString());
			}
			try {
				Thread.sleep(SAMPLE_INTERVAL_MS);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				return;
			}
		}
	}
}
