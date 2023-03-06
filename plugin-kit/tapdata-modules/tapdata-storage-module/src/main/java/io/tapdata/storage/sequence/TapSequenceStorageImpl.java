package io.tapdata.storage.sequence;

import io.tapdata.entity.annotations.Bean;
import io.tapdata.entity.annotations.Implementation;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.utils.FormatUtils;
import io.tapdata.entity.utils.ObjectSerializable;
import io.tapdata.modules.api.storage.TapSequenceStorage;
import io.tapdata.modules.api.storage.TapStorageFactory;
import io.tapdata.pdk.core.utils.CommonUtils;
import io.tapdata.pdk.core.utils.state.StateErrorOccurredExecutor;
import io.tapdata.pdk.core.utils.state.StateMachine;
import io.tapdata.storage.TapStorageImpl;
import io.tapdata.storage.errors.StorageErrors;
import io.tapdata.storage.factory.TapStorageFactoryImpl;
import org.apache.commons.compress.compressors.CompressorOutputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.io.*;
import java.util.Iterator;

/**
 * @author aplomb
 */
@Implementation(TapSequenceStorage.class)
public class TapSequenceStorageImpl extends TapStorageImpl implements TapSequenceStorage {
	private static final String TAG = TapSequenceStorageImpl.class.getSimpleName();

	public static final String STATE_WRITE_DONE_START_ITERATE = "write done and start iterate";
	protected volatile StateMachine<String, TapSequenceStorageImpl> stateMachine;
	private OutputStream fileOS;
	private CompressorOutputStream compressedOS;
	private DataOutputStream dataOS;
	private File dbFile;
	private Runnable initHandler;
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
								.configState(STATE_INITIALIZED, stateMachine.execute().nextStates(STATE_INITIALIZING, STATE_WRITE_DONE_START_ITERATE, STATE_DESTROYED))
								.configState(STATE_WRITE_DONE_START_ITERATE, stateMachine.execute(this::handleWriteDone).nextStates(STATE_INITIALIZING, STATE_DESTROYED))
								.configState(STATE_DESTROYED, stateMachine.execute().nextStates())
								.errorOccurred((throwable, fromState, toState, tapSequenceStorage, stateMachine) -> {
									if(throwable instanceof CoreException) {
										throw (CoreException) throwable;
									} else {
										throw new CoreException(StorageErrors.UNKNOWN_ERROR_IN_STATE_MACHINE, throwable, "Error occurred in state machine {}, {}", stateMachine, throwable.getMessage());
									}
								})
						;
					}
					if(stateMachine.getCurrentState().equals(STATE_NONE)) {
						initState(id, storageOptions);
					} else {
						throw new CoreException(StorageErrors.INITIALIZE_ON_WRONG_STATE, "Sequence storage id {} initialize on wrong state {}, should be \"none\" state", id, stateMachine.getCurrentState());
					}

					stateMachine.gotoState(STATE_INITIALIZING, FormatUtils.format("Sequence storage id {} start initializing", id));
				}
			}
		};
	}

	private void handleWriteDone(TapSequenceStorageImpl sequenceStorage, StateMachine<String, TapSequenceStorageImpl> stateMachine) {
		closeResources();
	}

	//	private SingleThreadBlockingQueue<Object> writeQueue;
	@Override
	public void add(Object data) {
		if(stateMachine == null)
			initHandler.run();
//		writeQueue.offer(data);
		if(!stateMachine.getCurrentState().equals(STATE_INITIALIZED))
			throw new CoreException(StorageErrors.ADD_OBJECT_ON_WRONG_STATE, "Write object {} on wrong state {}, expect state {}", data, stateMachine.getCurrentState(), STATE_INITIALIZED);
		byte[] dataBytes = objectSerializable.fromObject(data);
		synchronized (this) {
			try {
				dataOS.writeInt(dataBytes.length);
				dataOS.write(dataBytes);
//				dataOS.flush();
			} catch (Throwable throwable) {
				throw new CoreException(StorageErrors.ADD_OBJECT_FAILED, "Add object {} failed, {}", data, ExceptionUtils.getStackTrace(throwable));
			}
		}
	}

	@Override
	public Iterator<Object> iterator() {
		if(stateMachine == null)
			initHandler.run();

		//TODO can read full data only after close stream. So only support add data then iterate, can not add after iterate.
		//TODO should use MemoryMapFile with zstd compression to reimplement this. Then no such limit any more.
		if(stateMachine.getCurrentState().equals(STATE_INITIALIZED)) {
			stateMachine.gotoState(STATE_WRITE_DONE_START_ITERATE, "Write done, start iterating, can not back to write anymore. ");
		}
		if(!stateMachine.getCurrentState().equals(STATE_WRITE_DONE_START_ITERATE))
			throw new CoreException(StorageErrors.ITERATE_ON_WRONG_STATE, "Iterate on wrong state {}, expect state {}", stateMachine.getCurrentState(), STATE_WRITE_DONE_START_ITERATE);

		return new SequenceIterator(id, storageOptions, dbFile, objectSerializable, classLoader);
	}


//	private void handleInitialized(TapSequenceStorageImpl sequenceStorage, StateMachine<String, TapSequenceStorageImpl> stateMachine) {
//
//	}

	private void handleInitializing(TapSequenceStorageImpl sequenceStorage, StateMachine<String, TapSequenceStorageImpl> stateMachine) {
		try {
			String thePath = storageOptions.getRootPath();
			if(path != null)
				thePath = FilenameUtils.concat(thePath, path);
			thePath = FilenameUtils.concat(thePath, "sequence_tapdata/");
			dbFile = new File(FilenameUtils.concat(thePath, id));
			fileOS = FileUtils.openOutputStream(dbFile);
			compressedOS = TapStorageFactoryImpl.factory.createCompressorOutputStream(CompressorStreamFactory.ZSTANDARD, fileOS);
			dataOS = new DataOutputStream(compressedOS);
		} catch (Throwable e) {
			release();
			throw new CoreException(StorageErrors.OPEN_OUTPUT_STREAM_FAILED, "Open OutputStream for id {} rootPath {} failed, {}", id, storageOptions.getRootPath(), ExceptionUtils.getStackTrace(e));
		}
		stateMachine.gotoState(STATE_INITIALIZED, FormatUtils.format("Initialized for id {} for rootPath {}", id, storageOptions.getRootPath()));
	}

	@Override
	public synchronized void clear() {
		if(stateMachine == null)
			initHandler.run();

		release();
		stateMachine.gotoState(STATE_INITIALIZING, FormatUtils.format("Re-initializing after clear, id {}, options {}", id, storageOptions));
	}

	@Override
	public synchronized void destroy() {
		if (stateMachine != null && !stateMachine.getCurrentState().equals(STATE_DESTROYED)) {
			stateMachine.gotoState(STATE_DESTROYED, FormatUtils.format("Force destroy, id {}, options {}", id, storageOptions));
		}
		initHandler = null;
		release();
	}

	private void release() {
		closeResources();
		if(dbFile != null)
			CommonUtils.ignoreAnyError(() -> FileUtils.forceDelete(dbFile), TAG);
	}

	private void closeResources() {
		if(dataOS != null)
			IOUtils.closeQuietly(dataOS);
		if (compressedOS != null)
			IOUtils.closeQuietly(compressedOS);
		if (fileOS != null)
			IOUtils.closeQuietly(fileOS);
		dataOS = null;
		compressedOS = null;
		fileOS = null;
	}
}
