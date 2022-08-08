package io.tapdata.connector.mysql;

import io.tapdata.connector.mysql.entity.MysqlStreamOffset;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.JsonParser;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.storage.MemoryOffsetBackingStore;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * @author samuel
 * @Description
 * @create 2022-05-25 20:58
 **/
public class PdkPersistenceOffsetBackingStore extends MemoryOffsetBackingStore {
	private static final String TAG = PdkPersistenceOffsetBackingStore.class.getSimpleName();
	private String offsetStr;
	private JsonConverter keyConverter = new JsonConverter();
	private JsonConverter valueConverter = new JsonConverter();

	@Override
	public void configure(WorkerConfig config) {
		super.configure(config);
		this.offsetStr = (String) config.originals().getOrDefault("pdk.offset.string", "");
		Map<String, String> keyConfigMap = new HashMap<>(0);
		keyConverter.configure(keyConfigMap, false);
		Map<String, String> valueConfigMap = new HashMap<>(1);
		valueConfigMap.put("schemas.enable", "false");
		valueConverter.configure(valueConfigMap, false);
	}

	@Override
	public void start() {
		super.start();
		load();
	}

	private void load() {
		TapLogger.info(TAG, "Load offset with string: " + offsetStr);
		if (StringUtils.isBlank(offsetStr)) return;
		MysqlStreamOffset mysqlStreamOffset = InstanceFactory.instance(JsonParser.class).fromJson(offsetStr, MysqlStreamOffset.class);
		String name = mysqlStreamOffset.getName();
		Map<String, String> offset = mysqlStreamOffset.getOffset();
		if (MapUtils.isEmpty(offset)) return;
		for (Map.Entry<String, String> entry : offset.entrySet()) {
			String key = entry.getKey();
			String value = entry.getValue();
			ByteBuffer keyByteBuffer = null;
			ByteBuffer valueByteBuffer = null;
			if (StringUtils.isNotBlank(key)) {
				Map<?, ?> keyMap = InstanceFactory.instance(JsonParser.class).fromJson(entry.getKey(), Map.class);
				byte[] keyBytes = keyConverter.fromConnectData(name, null, Arrays.asList(name, keyMap));
				keyByteBuffer = ByteBuffer.wrap(keyBytes);
			}
			if (StringUtils.isNotBlank(value)) {
				Map<?, ?> valueMap = InstanceFactory.instance(JsonParser.class).fromJson(entry.getValue(), Map.class);
				byte[] valueBytes = valueConverter.fromConnectData(name, null, valueMap);
				valueByteBuffer = ByteBuffer.wrap(valueBytes);
			}
			data.put(keyByteBuffer, valueByteBuffer);
		}
	}
}
