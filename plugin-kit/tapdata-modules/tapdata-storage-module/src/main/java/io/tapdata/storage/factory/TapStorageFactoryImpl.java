package io.tapdata.storage.factory;

import io.tapdata.entity.annotations.Implementation;
import io.tapdata.entity.utils.ClassFactory;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.modules.api.storage.TapKVStorage;
import io.tapdata.modules.api.storage.TapSequenceStorage;
import io.tapdata.modules.api.storage.TapStorageFactory;
import io.tapdata.pdk.core.utils.CommonUtils;
import org.apache.commons.compress.compressors.CompressorStreamFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author aplomb
 */
@Implementation(TapStorageFactory.class)
public class TapStorageFactoryImpl implements TapStorageFactory {
	public static final CompressorStreamFactory factory = new CompressorStreamFactory();
	private static final String TAG = TapStorageFactoryImpl.class.getSimpleName();
	private StorageOptions storageOptions;
	private final Map<String, TapSequenceStorage> sequenceStorageMap = new ConcurrentHashMap<>();
	private final Map<String, TapKVStorage> kvStorageMap = new ConcurrentHashMap<>();

	@Override
	public TapStorageFactory init(StorageOptions storageOptions) {
		this.storageOptions = storageOptions;
		return this;
	}

	@Override
	public TapSequenceStorage deleteSequenceStorage(String id) {
		TapSequenceStorage sequenceStorage = getSequenceStorage(id);

		CommonUtils.ignoreAnyError(sequenceStorage::destroy, TAG);
		sequenceStorageMap.remove(id, sequenceStorage);
		return sequenceStorage;
	}

	@Override
	public TapSequenceStorage getSequenceStorage(String id) {
		TapSequenceStorage sequenceStorage = sequenceStorageMap.get(id);
		if(sequenceStorage == null) {
			synchronized (this) {
				sequenceStorage = sequenceStorageMap.get(id);
				if(sequenceStorage == null) {
					sequenceStorage = sequenceStorageMap.computeIfAbsent(id, theId -> {
						TapSequenceStorage tapSequenceStorage = ClassFactory.create(TapSequenceStorage.class);
						InstanceFactory.injectBean(tapSequenceStorage, true);
						tapSequenceStorage.init(theId, storageOptions);
						return tapSequenceStorage;
					});
				}
			}
		}
		return sequenceStorage;
	}

	@Override
	public TapKVStorage deleteKVStorage(String id) {
		TapKVStorage kvStorage = getKVStorage(id);

		CommonUtils.ignoreAnyError(kvStorage::destroy, TAG);
		kvStorageMap.remove(id, kvStorage);
		return kvStorage;
	}

	@Override
	public TapKVStorage getKVStorage(String id) {
		TapKVStorage kvStorage = kvStorageMap.get(id);
		if(kvStorage == null) {
			synchronized (this) {
				kvStorage = kvStorageMap.get(id);
				if(kvStorage == null) {
					kvStorage = kvStorageMap.computeIfAbsent(id, theId -> {
						TapKVStorage tapKVStorage = ClassFactory.create(TapKVStorage.class);
						InstanceFactory.injectBean(tapKVStorage, true);
						tapKVStorage.init(theId, storageOptions);
						return tapKVStorage;
					});
				}
			}
		}
		return kvStorage;
	}
}
