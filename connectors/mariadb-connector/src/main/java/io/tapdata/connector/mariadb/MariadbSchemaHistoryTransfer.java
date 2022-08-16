package io.tapdata.connector.mariadb;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;

/**
 * @author samuel
 * @Description
 * @create 2022-05-25 16:10
 **/
public class MariadbSchemaHistoryTransfer {
	public static Map<String, Set<String>> historyMap = new ConcurrentHashMap<>();
	private static AtomicBoolean saved = new AtomicBoolean(false);
	private static ReentrantLock lock = new ReentrantLock();

	public static void executeWithLock(Predicate<?> stop, Runner runner) {
		try {
			tryLock(stop);
			runner.execute();
		} finally {
			unLock();
		}
	}

	private static void tryLock(Predicate<?> stop) {
		while (true) {
			if (null != stop && stop.test(null)) {
				break;
			}
			try {
				if (MariadbSchemaHistoryTransfer.lock.tryLock(3L, TimeUnit.SECONDS)) {
					break;
				}
			} catch (InterruptedException e) {
				break;
			}
		}
	}

	private static void unLock() {
		MariadbSchemaHistoryTransfer.lock.unlock();
	}

	public static boolean isSave() {
		return MariadbSchemaHistoryTransfer.saved.get();
	}

	public static void unSave() {
		MariadbSchemaHistoryTransfer.saved.set(false);
	}

	public static void save() {
		MariadbSchemaHistoryTransfer.saved.set(true);
	}

	public interface Runner {
		void execute();
	}
}
