package io.tapdata.connector.mysql;

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
public class MysqlSchemaHistoryTransfer {
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
				if (MysqlSchemaHistoryTransfer.lock.tryLock(3L, TimeUnit.SECONDS)) {
					break;
				}
			} catch (InterruptedException e) {
				break;
			}
		}
	}

	private static void unLock() {
		MysqlSchemaHistoryTransfer.lock.unlock();
	}

	public static boolean isSave() {
		return MysqlSchemaHistoryTransfer.saved.get();
	}

	public static void unSave() {
		MysqlSchemaHistoryTransfer.saved.set(false);
	}

	public static void save() {
		MysqlSchemaHistoryTransfer.saved.set(true);
	}

	public interface Runner {
		void execute();
	}
}
