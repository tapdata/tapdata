package io.tapdata.storage.kv;

import io.tapdata.entity.annotations.Implementation;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.utils.FormatUtils;
import io.tapdata.entity.utils.ObjectSerializable;
import io.tapdata.modules.api.storage.TapKVStorage;
import io.tapdata.modules.api.storage.TapStorageFactory;
import io.tapdata.pdk.core.utils.CommonUtils;
import io.tapdata.pdk.core.utils.state.StateMachine;
import io.tapdata.storage.TapStorageImpl;
import io.tapdata.storage.errors.StorageErrors;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.rocksdb.*;

import java.io.File;
import java.io.IOException;
import java.util.function.BiFunction;
import java.util.function.Function;

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
	private Runnable initHandler;
	private ObjectSerializable.ToObjectOptions toObjectOptions;

	@Override
	public void put(Object key, Object value) {
		if(stateMachine == null)
			initHandler.run();

		if(!stateMachine.getCurrentState().equals(STATE_INITIALIZED))
			throw new CoreException(StorageErrors.ITERATE_ON_WRONG_STATE, "Iterate on wrong state {}, expect state {}", stateMachine.getCurrentState(), STATE_INITIALIZED);
		try {
			if(key instanceof byte[]) {
				db.put((byte[]) key, objectSerializable.fromObject(value));
			} else {
				db.put(objectSerializable.fromObject(key), objectSerializable.fromObject(value));
			}
		} catch (RocksDBException e) {
			throw new CoreException(StorageErrors.KV_STORAGE_PUT_FAILED, e, "Put key {} value {} failed, {}" ,key, value, e.getMessage());
		}
	}

	@Override
	public Object get(Object key) {
		if(stateMachine == null)
			initHandler.run();

		if(!stateMachine.getCurrentState().equals(STATE_INITIALIZED))
			throw new CoreException(StorageErrors.ITERATE_ON_WRONG_STATE, "Iterate on wrong state {}, expect state {}", stateMachine.getCurrentState(), STATE_INITIALIZED);
		try {
			if(key instanceof byte[]) {
				return objectSerializable.toObject(db.get((byte[]) key), toObjectOptions);
			} else {
				return objectSerializable.toObject(db.get(objectSerializable.fromObject(key)), toObjectOptions);
			}
		} catch (RocksDBException e) {
			throw new CoreException(StorageErrors.KV_STORAGE_GET_FAILED, e, "Get key {} failed, {}", key, e.getMessage());
		}
	}

	@Override
	public void remove(Object key) {
		if(stateMachine == null)
			initHandler.run();


		try {
			if(key instanceof byte[]) {
				db.delete((byte[]) key);
			} else {
				db.delete(objectSerializable.fromObject(key));
			}
		} catch (RocksDBException e) {
			throw new CoreException(StorageErrors.KV_STORAGE_DELETE_FAILED, e, "Delete key {} failed, {}", key, e.getMessage());
		}
	}
	@Override
	public Object removeAndGet(Object key) {
		if(stateMachine == null)
			initHandler.run();

		try {
			byte[] keyBytes = null;
			if(key instanceof byte[]) {
				keyBytes = (byte[]) key;
			} else {
				keyBytes = objectSerializable.fromObject(key);
			}
			byte[] dataBytes = db.get(keyBytes);
			Object data = null;
			if(dataBytes != null) {
				data = objectSerializable.toObject(dataBytes, toObjectOptions);
				db.delete(keyBytes);
			}
			return data;
		} catch (RocksDBException e) {
			throw new CoreException(StorageErrors.KV_STORAGE_DELETE_FAILED, e, "Delete key {} failed, {}", key, e.getMessage());
		}
	}
	@Override
	public void foreachValues(Function<Object, Boolean> iterateFunc) {
		foreachValues(iterateFunc, true);
	}
	@Override
	public void foreachValues(Function<Object, Boolean> iterateFunc, boolean asc) {
		if(stateMachine == null)
			initHandler.run();

		try(RocksIterator iterator = db.newIterator()) {
			if(asc) {
				for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
					Boolean result = iterateFunc.apply(objectSerializable.toObject(iterator.value(), toObjectOptions));
					if(result != null && !result)
						break;
				}
			} else {
				for (iterator.seekToLast(); iterator.isValid(); iterator.prev()) {
					Boolean result = iterateFunc.apply(objectSerializable.toObject(iterator.value(), toObjectOptions));
					if(result != null && !result)
						break;
				}
			}
		}
	}
	@Override
	public void foreachValues(BiFunction<Object, Object, Boolean> iterateFunc, GetObject getObject) {
		foreachValues(iterateFunc, getObject, true);
	}
	@Override
	public void foreachValues(BiFunction<Object, Object, Boolean> iterateFunc, GetObject getObject, boolean asc) {
		if(stateMachine == null)
			initHandler.run();

		try(RocksIterator iterator = db.newIterator()) {
			if(asc) {
				for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
					Object value1 = getObject.get(iterator.key());
					Boolean result = iterateFunc.apply(objectSerializable.toObject(iterator.value(), toObjectOptions), value1);
					if(result != null && !result)
						break;
				}
			} else {
				for (iterator.seekToLast(); iterator.isValid(); iterator.prev()) {
					Object value1 = getObject.get(iterator.key());
					Boolean result = iterateFunc.apply(objectSerializable.toObject(iterator.value(), toObjectOptions), value1);
					if(result != null && !result)
						break;
				}
			}
		}
	}

	@Override
	public void foreach(BiFunction<Object, Object, Boolean> iterateFunc) {
		if(stateMachine == null)
			initHandler.run();

		foreach(iterateFunc, true);
	}

	@Override
	public void foreach(BiFunction<Object, Object, Boolean> iterateFunc, boolean asc) {
		if(stateMachine == null)
			initHandler.run();

		try(RocksIterator iterator = db.newIterator()) {
			if(asc) {
				for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
					Boolean result = iterateFunc.apply(objectSerializable.toObject(iterator.key()), objectSerializable.toObject(iterator.value(), toObjectOptions));
					if(result != null && !result)
						break;
				}
			} else {
				for (iterator.seekToLast(); iterator.isValid(); iterator.prev()) {
					Boolean result = iterateFunc.apply(objectSerializable.toObject(iterator.key()), objectSerializable.toObject(iterator.value(), toObjectOptions));
					if(result != null && !result)
						break;
				}
			}
		}
	}
	@Override
	public synchronized void init(String id, TapStorageFactory.StorageOptions storageOptions) {
		initHandler = () -> {
			if(stateMachine == null) {
				synchronized (this) {
					if(stateMachine == null) {
						stateMachine = new StateMachine<>(this.getClass().getSimpleName() + "_" + id + "_" + storageOptions, STATE_NONE, this);
						stateMachine
								.configState(STATE_NONE, stateMachine.execute().nextStates(STATE_INITIALIZING, STATE_DESTROYED))
								.configState(STATE_INITIALIZING, stateMachine.execute(this::handleInitializing).nextStates(STATE_INITIALIZED, STATE_DESTROYED))
								.configState(STATE_INITIALIZED, stateMachine.execute().nextStates(STATE_INITIALIZING, STATE_DESTROYED))
								.configState(STATE_DESTROYED, stateMachine.execute(this::handleDestroyed).nextStates())
								.errorOccurred((throwable, fromState, toState, tapSequenceStorage, stateMachine) -> {
									if(throwable instanceof CoreException) {
										throw (CoreException) throwable;
									} else {
										throw new CoreException(StorageErrors.UNKNOWN_ERROR_IN_STATE_MACHINE, throwable, "Error occurred in state machine {}, {}", stateMachine, throwable.getMessage());
									}
								});
						toObjectOptions = new ObjectSerializable.ToObjectOptions().classLoader(classLoader);
						if(stateMachine.getCurrentState().equals(STATE_NONE)) {
							initState(id, storageOptions);
						} else {
							throw new CoreException(StorageErrors.INITIALIZE_ON_WRONG_STATE, "KV storage id {} initialize on wrong state {}, should be \"none\" state", id, stateMachine.getCurrentState());
						}
						stateMachine.gotoState(STATE_INITIALIZING, FormatUtils.format("KV storage id {} start initializing", id));
					}
				}
			}
		};
	}

	private void handleDestroyed(TapKVStorageImpl tapKVStorage, StateMachine<String, TapKVStorageImpl> stringTapKVStorageStateMachine) {
		try (Options options = new Options()){
			CommonUtils.ignoreAnyError(() -> db.syncWal(), TAG);
			CommonUtils.ignoreAnyError(() -> db.close(), TAG);
			options.setCompressionType(CompressionType.ZSTD_COMPRESSION);
			options.setCreateIfMissing(true);
			CommonUtils.ignoreAnyError(() -> RocksDB.destroyDB(dbDir.getAbsolutePath(), options), TAG);
		}
	}

	private void handleInitializing(TapKVStorageImpl tapKVStorage, StateMachine<String, TapKVStorageImpl> stateMachine) {
		String thePath = storageOptions.getRootPath();
		if(path != null)
			thePath = FilenameUtils.concat(thePath, path);
		thePath = FilenameUtils.concat(thePath, "kv_rocksdb/");

		dbDir = new File(FilenameUtils.concat(thePath, id));
		try (final Options options = new Options()) {
			options.setCompressionType(CompressionType.ZSTD_COMPRESSION);
			options.setCreateIfMissing(true);
			FileUtils.forceMkdir(dbDir);
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
		if(stateMachine == null)
			initHandler.run();

		if(!stateMachine.getCurrentState().equals(STATE_INITIALIZED))
			throw new CoreException(StorageErrors.ITERATE_ON_WRONG_STATE, "Iterate on wrong state {}, expect state {}", stateMachine.getCurrentState(), STATE_INITIALIZED);
		CommonUtils.ignoreAnyError(() -> db.syncWal(), TAG);
		handleDestroyed(this, stateMachine);
		release();
		stateMachine.gotoState(STATE_INITIALIZING, FormatUtils.format("Re-initializing after reset, id {}, options {}", id, storageOptions));
	}

	@Override
	public synchronized void destroy() {
//		if(!stateMachine.getCurrentState().equals(STATE_INITIALIZED))
//			throw new CoreException(StorageErrors.ITERATE_ON_WRONG_STATE, "Iterate on wrong state {}, expect state {}", stateMachine.getCurrentState(), STATE_INITIALIZED);
		if (stateMachine != null && !stateMachine.getCurrentState().equals(STATE_DESTROYED)) {
			stateMachine.gotoState(STATE_DESTROYED, FormatUtils.format("Force destroy, id {}, options {}", id, storageOptions));
		}
		initHandler = null;
		release();
	}

	private void release() {
		if(dbDir != null) {
			if(dbDir.exists()) {
				CommonUtils.ignoreAnyError(() -> FileUtils.forceDelete(dbDir), TAG);
			}
		}
	}
}
