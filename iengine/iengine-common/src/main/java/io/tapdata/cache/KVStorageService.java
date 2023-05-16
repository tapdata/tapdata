package io.tapdata.cache;

import io.tapdata.entity.utils.ClassFactory;
import io.tapdata.modules.api.storage.TapKVStorage;
import io.tapdata.modules.api.storage.TapStorageFactory;
import joptsimple.internal.Strings;

import java.io.File;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2023/5/16 21:15 Create
 */
public class KVStorageService {
	private static final String DIST_CACHE_PATH = "tap_table_storage";
	private static volatile TapStorageFactory storageFactory;

	private static TapStorageFactory factoryInstance() {
		if (null == storageFactory) {
			synchronized (KVStorageService.class) {
				if (null == storageFactory) {
					storageFactory = ClassFactory.create(TapStorageFactory.class);
					String storeDir = Strings.join(new String[]{".", DIST_CACHE_PATH}, File.separator);
					storageFactory.init(TapStorageFactory.StorageOptions.create().rootPath(storeDir));
				}
			}
		}
		return storageFactory;
	}

	public static void initKVStorage(String mapKey) {
		factoryInstance().getKVStorage(mapKey).clear();
	}

	public static TapKVStorage getKVStorage(String mapKey) {
		return factoryInstance().getKVStorage(mapKey);
	}

	public static void destroyKVStorage(String mapKey) {
		factoryInstance().deleteKVStorage(mapKey);
	}
}
