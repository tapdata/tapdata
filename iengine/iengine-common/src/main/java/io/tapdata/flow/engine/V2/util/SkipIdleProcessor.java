package io.tapdata.flow.engine.V2.util;

import io.tapdata.entity.memory.MemoryFetcher;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.core.utils.CommonUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Supplier;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2023/5/31 15:41 Create
 */
public class SkipIdleProcessor<T> implements AutoCloseable, MemoryFetcher {
	private final static Logger logger = LogManager.getLogger(SkipIdleProcessor.class);

	protected final static int SLEEP_INTERVAL = CommonUtils.getPropertyInt("SLEEP_INTERVAL", 100);
	protected final static int LOOP_INTERVAL = 50;
	protected final static int MAX_COUNTS = CommonUtils.getPropertyInt("MAX_COUNTS", 40);

	private final Queue<Item> queue;
	private final LinkedList<Item> idleList = new LinkedList<>();
	private final Supplier<Boolean> isRunning;
	private Thread th;
	private final int waitCounts;

	public SkipIdleProcessor(Supplier<Boolean> isRunning, Collection<T> collection, int waitCounts) {
		this.isRunning = isRunning;
		this.queue = new LinkedBlockingQueue<>();
		for (T v : collection) {
			queue.add(new Item(v));
		}
		if (waitCounts <= 0) {
			this.waitCounts = MAX_COUNTS;
		} else {
			this.waitCounts = waitCounts;
		}

		th = new Thread(() -> {
			Item tmp;
			while (isRunning.get() && !Thread.interrupted()) {
				synchronized (idleList) {
					tmp = idleList.pollFirst();
				}
				if (null == tmp) {
					try {
						Thread.sleep(LOOP_INTERVAL);
						continue;
					} catch (InterruptedException e) {
						break;
					}
				}

				long sleepTimes = tmp.nextTimes - System.currentTimeMillis();
				if (sleepTimes > SLEEP_INTERVAL) {
					// sleep and add to idle
					try {
						Thread.sleep(LOOP_INTERVAL);
					} catch (InterruptedException e) {
						break;
					} finally {
						synchronized (idleList) {
							idleList.add(tmp);
						}
					}
				} else if (sleepTimes > 0) {
					try {
						Thread.sleep(sleepTimes);
					} catch (InterruptedException e) {
						break;
					} finally {
						queue.add(tmp);
					}
				} else {
					queue.add(tmp);
				}
			}

			logger.info("Exit skip idle monitor");
		});
		th.start();
	}

	public <P, V> V process(P param, IFn<T, P, V> processor) throws Exception {
		Item item;
		while (null == (item = queue.poll())) {
			if (isRunning.get()) {
				Thread.sleep(LOOP_INTERVAL);
			} else {
				return null;
			}
		}

		V val = null;
		try {
			val = processor.process(item.value, param);
			return val;
		} finally {
			if (null == val) {
				item.toIdleList();
			} else {
				item.reset();
				queue.add(item);
			}
		}
	}

	@Override
	public synchronized void close() throws Exception {
		if (null != th) {
			queue.clear();
			idleList.clear();
			th.interrupt();
			th = null;
		}
	}

	@Override
	public DataMap memory(String keyRegex, String memoryLevel) {
		DataMap dataMap = new DataMap();
		try {
			dataMap.kv("idle", idleList);
		} catch (Exception e) {
			dataMap.kv("idle error", e.getMessage() + "; Stack: " + ExceptionUtils.getStackTrace(e));
		}
		try {
			dataMap.kv("process queue", queue);
		} catch (Exception e) {
			dataMap.kv("process queue error", e.getMessage() + "; Stack: " + ExceptionUtils.getStackTrace(e));
		}
		return dataMap;
	}

	public interface IFn<T, P, V> {
		V process(T tag, P param) throws Exception;
	}

	private class Item {
		private long nextTimes;
		private long counts = 0;
		private final T value;

		public Item(T value) {
			this.value = value;
		}

		public void toIdleList() {
			// max sleep interval 2 seconds
			if (counts < waitCounts) counts++;

			nextTimes = System.currentTimeMillis() + (SLEEP_INTERVAL * counts);
			synchronized (idleList) {
				idleList.add(this);
			}
		}

		public void reset() {
			counts = 0;
		}
	}
}
