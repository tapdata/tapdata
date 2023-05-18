package io.tapdata.schema;

import io.tapdata.cache.KVStorageService;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.modules.api.storage.TapKVStorage;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2023/5/15 20:37 Create
 */
public class TapTableMapTapStorage<K extends String, V extends TapTable> extends TapTableMap<K, V> {
	public static final String TAP_TABLE_PREFIX = "TAP_TABLE_";

	private final String mapKey;

	public TapTableMapTapStorage(String prefix, String nodeId, Long time, Map<K, String> tableNameAndQualifiedNameMap) {
		super(nodeId, time, tableNameAndQualifiedNameMap);
		if (StringUtils.isNotEmpty(prefix)) {
			this.mapKey = prefix + "_" + TAP_TABLE_PREFIX + nodeId;
		} else {
			this.mapKey = TAP_TABLE_PREFIX + nodeId;
		}
		KVStorageService.initKVStorage(mapKey);
	}

	private TapKVStorage getStorage() {
		return KVStorageService.getKVStorage(mapKey);
	}

	@Override
	protected V getTapTable(K key) {
		V tapTable = (V) getStorage().get(key);
		if (null == tapTable) {
			try {
				tapTable = handleWithLock(() -> {
					V tmp = (V) getStorage().get(key);
					if (null == tmp) {
						tmp = findSchema(key);
						getStorage().put(key, tmp);
					}
					return tmp;
				});
			} catch (Exception e) {
				throw new RuntimeException("Find schema failed, message: " + e.getMessage(), e);
			}
		}
		return tapTable;
	}

	@Override
	protected void putTapTable(K key, V value) {
		getStorage().put(key, value);
	}

	@Override
	protected V removeTapTable(K key) {
		getStorage().remove(key);
		return null;
	}

	@Override
	protected void clearTapTable() {
		getStorage().clear();
	}

	@Override
	protected void resetTapTable() {
		KVStorageService.destroyKVStorage(mapKey);
	}
}
