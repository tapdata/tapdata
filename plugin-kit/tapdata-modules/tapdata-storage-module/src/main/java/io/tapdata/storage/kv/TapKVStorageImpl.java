package io.tapdata.storage.kv;

import io.tapdata.entity.annotations.Implementation;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.utils.FormatUtils;
import io.tapdata.modules.api.storage.TapKVStorage;
import io.tapdata.modules.api.storage.TapSequenceStorage;
import io.tapdata.modules.api.storage.TapStorageFactory;
import io.tapdata.pdk.core.utils.CommonUtils;
import io.tapdata.pdk.core.utils.state.StateMachine;
import io.tapdata.storage.TapStorageImpl;
import io.tapdata.storage.errors.StorageErrors;
import io.tapdata.storage.sequence.TapSequenceStorageImpl;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.File;
import java.io.IOException;

/**
 * @author aplomb
 */
@Implementation(TapKVStorage.class)
public class TapKVStorageImpl extends TapStorageImpl implements TapKVStorage {
	private static final String TAG = TapKVStorageImpl.class.getSimpleName();

	static {
		RocksDB.loadLibrary();
	}
	private volatile StateMachine<String, TapKVStorageImpl> stateMachine;
	private RocksDB db;
	private File dbDir;

	@Override
	public void put(Object key, Object value) {
		if(!stateMachine.getCurrentState().equals(STATE_INITIALIZED))
			throw new CoreException(StorageErrors.ITERATE_ON_WRONG_STATE, "Iterate on wrong state {}, expect state {}", stateMachine.getCurrentState(), STATE_INITIALIZED);
		try {
			db.put(objectSerializable.fromObject(key), objectSerializable.fromObject(value));
		} catch (RocksDBException e) {
			throw new CoreException(StorageErrors.KV_STORAGE_PUT_FAILED, e, "Put key {} value {} failed, {}" ,key, value, e.getMessage());
		}
	}

	@Override
	public Object get(Object key) {
		if(!stateMachine.getCurrentState().equals(STATE_INITIALIZED))
			throw new CoreException(StorageErrors.ITERATE_ON_WRONG_STATE, "Iterate on wrong state {}, expect state {}", stateMachine.getCurrentState(), STATE_INITIALIZED);
		try {
			return objectSerializable.toObject(db.get(objectSerializable.fromObject(key)));
		} catch (RocksDBException e) {
			throw new CoreException(StorageErrors.KV_STORAGE_GET_FAILED, e, "Get key {} failed, {}", key, e.getMessage());
		}
	}

	@Override
	public void remove(Object key) {

	}

	@Override
	public synchronized void init(String id, TapStorageFactory.StorageOptions storageOptions) {
		if(stateMachine == null) {
			synchronized (this) {
				if(stateMachine == null) {
					stateMachine = new StateMachine<>(this.getClass().getSimpleName() + "_" + id + "_" + storageOptions, STATE_NONE, this);
					stateMachine
							.configState(STATE_NONE, stateMachine.execute().nextStates(STATE_INITIALIZING, STATE_DESTROYED))
							.configState(STATE_INITIALIZING, stateMachine.execute(this::handleInitializing).nextStates(STATE_INITIALIZED, STATE_DESTROYED))
							.configState(STATE_INITIALIZED, stateMachine.execute().nextStates(STATE_INITIALIZING, STATE_DESTROYED))
							.configState(STATE_DESTROYED, stateMachine.execute().nextStates());
				}
			}
		}
		if(stateMachine.getCurrentState().equals(STATE_NONE)) {
			initState(id, storageOptions);
		} else {
			throw new CoreException(StorageErrors.INITIALIZE_ON_WRONG_STATE, "KV storage id {} initialize on wrong state {}, should be \"none\" state", id, stateMachine.getCurrentState());
		}
		stateMachine.gotoState(STATE_INITIALIZING, FormatUtils.format("KV storage id {} start initializing", id));
	}

	private void handleInitializing(TapKVStorageImpl tapKVStorage, StateMachine<String, TapKVStorageImpl> stateMachine) {
		final Options options = new Options();
		options.setCreateIfMissing(true);
		String basePath = FilenameUtils.concat(storageOptions.getRootPath(), "kv_rocksdb/");
		dbDir = new File(FilenameUtils.concat(basePath, id));
		try {
			FileUtils.forceMkdir(new File(basePath));
			db = RocksDB.open(options, dbDir.getAbsolutePath());
		} catch(RocksDBException ex) {
			TapLogger.error(TAG, "Error initializing RocksDB, check configurations and permissions, exception: {}, message: {}, stackTrace: {}",
					ex.getCause(), ex.getMessage(), ex.getStackTrace());
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		stateMachine.gotoState(STATE_INITIALIZED, FormatUtils.format("Initialized for id {} for rootPath {}", id, storageOptions.getRootPath()));
	}

	@Override
	public synchronized void clear() {
		if(!stateMachine.getCurrentState().equals(STATE_INITIALIZED))
			throw new CoreException(StorageErrors.ITERATE_ON_WRONG_STATE, "Iterate on wrong state {}, expect state {}", stateMachine.getCurrentState(), STATE_INITIALIZED);
		db.close();
		release();
		stateMachine.gotoState(STATE_INITIALIZING, FormatUtils.format("Re-initializing after reset, id {}, options {}", id, storageOptions));
	}

	@Override
	public synchronized void destroy() {
//		if(!stateMachine.getCurrentState().equals(STATE_INITIALIZED))
//			throw new CoreException(StorageErrors.ITERATE_ON_WRONG_STATE, "Iterate on wrong state {}, expect state {}", stateMachine.getCurrentState(), STATE_INITIALIZED);
		if (!stateMachine.getCurrentState().equals(STATE_DESTROYED)) {
			stateMachine.gotoState(STATE_DESTROYED, FormatUtils.format("Force destroy, id {}, options {}", id, storageOptions));
			db.close();
			release();
		}
	}

	private void release() {
		if(dbDir != null)
			CommonUtils.ignoreAnyError(() -> FileUtils.forceDelete(dbDir), TAG);
	}
}
