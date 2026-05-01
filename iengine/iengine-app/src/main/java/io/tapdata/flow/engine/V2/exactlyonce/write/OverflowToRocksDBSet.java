package io.tapdata.flow.engine.V2.exactlyonce.write;

import com.tapdata.constant.OsUtil;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.Closeable;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.AbstractSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.UUID;

/**
 * A {@link Set}<{@link String}> implementation that keeps the first {@code memoryThreshold} entries
 * in an in-memory {@link HashSet}. Once the in-memory size exceeds the threshold, subsequent entries
 * are spilled to a local RocksDB instance to avoid OOM.
 *
 * <p>Iteration is not supported when the disk store has been opened.</p>
 */
public class OverflowToRocksDBSet extends AbstractSet<String> implements Closeable {
	public static final long DEFAULT_MEMORY_THRESHOLD = 1_000_000L;
	private static final String DEFAULT_DIR_NAME = "exactly-once-cache";
	private static final byte[] DUMMY_VALUE = new byte[0];
	private static final Logger logger = LogManager.getLogger(OverflowToRocksDBSet.class);

	private final long memoryThreshold;
	private final String dbName;
	private final Set<String> memory = new HashSet<>();
	private RocksDB db;
	private File dbDir;
	private long diskSize;

	public OverflowToRocksDBSet() {
		this(DEFAULT_MEMORY_THRESHOLD, UUID.randomUUID().toString());
	}

	public OverflowToRocksDBSet(long memoryThreshold, String dbName) {
		if (memoryThreshold <= 0) throw new IllegalArgumentException("memoryThreshold must be positive");
		this.memoryThreshold = memoryThreshold;
		this.dbName = StringUtils.isBlank(dbName) ? UUID.randomUUID().toString() : dbName;
	}

	@Override
	public synchronized boolean add(String s) {
		if (memory.size() < memoryThreshold) {
			return memory.add(s);
		}
		if (memory.contains(s)) return false;
		ensureDbOpen();
		byte[] key = s.getBytes(StandardCharsets.UTF_8);
		try {
			if (db.get(key) != null) return false;
			db.put(key, DUMMY_VALUE);
			diskSize++;
			return true;
		} catch (RocksDBException e) {
			throw new RuntimeException("Put exactly once id to rocksdb failed: " + s, e);
		}
	}

	@Override
	public synchronized boolean contains(Object o) {
		if (!(o instanceof String s)) return false;
		if (memory.contains(s)) return true;
		if (db == null) return false;
		try {
			return db.get(s.getBytes(StandardCharsets.UTF_8)) != null;
		} catch (RocksDBException e) {
			throw new RuntimeException("Get exactly once id from rocksdb failed: " + s, e);
		}
	}

	@Override
	public synchronized int size() {
		long total = (long) memory.size() + diskSize;
		return total > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) total;
	}

	@Override
	public synchronized boolean isEmpty() {
		return memory.isEmpty() && diskSize == 0;
	}

	@Override
	public synchronized void clear() {
		memory.clear();
		closeAndDeleteDb();
	}

	@Override
	public synchronized Iterator<String> iterator() {
		if (db != null) {
			throw new UnsupportedOperationException("Iteration is not supported after rocksdb spill is active");
		}
		return memory.iterator();
	}

	@Override
	public synchronized void close() {
		memory.clear();
		closeAndDeleteDb();
	}

	private void ensureDbOpen() {
		if (db != null) return;
		try {
			RocksDB.loadLibrary();
			String tapdataWorkDir = System.getenv("TAPDATA_WORK_DIR");
			if (StringUtils.isBlank(tapdataWorkDir)) {
				tapdataWorkDir = System.getProperty("user.dir");
			}
			String path = tapdataWorkDir + File.separator + DEFAULT_DIR_NAME + File.separator + dbName;
			if (OsUtil.isWindows()) {
				path = path.replace("/", "\\");
			}
			dbDir = new File(path);
			if (dbDir.exists()) {
				FileUtils.deleteDirectory(dbDir);
			}
			FileUtils.forceMkdir(dbDir.getParentFile());
			Options options = new Options().setCreateIfMissing(true);
			db = RocksDB.open(options, dbDir.getAbsolutePath());
			logger.info("Exactly once cache memory threshold {} reached, spilling to rocksdb at {}", memoryThreshold, dbDir.getAbsolutePath());
		} catch (Exception e) {
			throw new RuntimeException("Open rocksdb for exactly once cache failed", e);
		}
	}

	private void closeAndDeleteDb() {
		if (db != null) {
			try { db.close(); } catch (Exception ignore) {}
			db = null;
		}
		if (dbDir != null) {
			try { FileUtils.deleteDirectory(dbDir); } catch (Exception e) {
				logger.warn("Delete rocksdb dir {} failed: {}", dbDir.getAbsolutePath(), e.getMessage());
			}
			dbDir = null;
		}
		diskSize = 0;
	}
}
