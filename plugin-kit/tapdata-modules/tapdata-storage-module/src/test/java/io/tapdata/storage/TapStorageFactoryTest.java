package io.tapdata.storage;

import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.modules.api.storage.TapKVStorage;
import io.tapdata.modules.api.storage.TapSequenceStorage;
import io.tapdata.modules.api.storage.TapStorageFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;

import static io.tapdata.entity.simplify.TapSimplify.list;

/**
 * @author aplomb
 */
public class TapStorageFactoryTest {
	TapStorageFactory storageFactory;
//	@BeforeEach
//	public void setup() {
//		storageFactory = InstanceFactory.instance(TapStorageFactory.class);
//		storageFactory.init(TapStorageFactory.StorageOptions.create().rootPath("./tap_storage_test"));
//	}
	@Test
	public void testKVStorage() {
		storageFactory = InstanceFactory.instance(TapStorageFactory.class);
		storageFactory.init(TapStorageFactory.StorageOptions.create().rootPath("./tap_storage_test"));
		TapKVStorage kvStorage = storageFactory.getKVStorage("test");
		Assertions.assertNotNull(kvStorage);

		DataMap data1 = DataMap.create().kv("Key", "1234567");
		DataMap value1 = DataMap.create().kv("Value", "aaaaaaaa");
		kvStorage.put(data1, value1);

		DataMap theValue1 = (DataMap) kvStorage.get(data1);
		Assertions.assertNotNull(theValue1);
		Assertions.assertEquals("aaaaaaaa", theValue1.get("Value"));

		kvStorage.remove(data1);
		theValue1 = (DataMap) kvStorage.get(data1);
		Assertions.assertNull(theValue1);

		data1 = DataMap.create().kv("Key", "1234567");
		value1 = DataMap.create().kv("Value", "aaaaaaaa");
		DataMap data2 = DataMap.create().kv("Key", "1234568");
		DataMap value2 = DataMap.create().kv("Value", "aaaaaaaa1");
		kvStorage.put(data1, value1);
		kvStorage.put(data2, value2);

		Map<DataMap, DataMap> map = new LinkedHashMap<>();
		kvStorage.foreach((key, value) -> {
			map.put((DataMap) key, (DataMap) value);
			return null;
		});

		Map<DataMap, DataMap> descMap = new LinkedHashMap<>();
		kvStorage.foreach((key, value) -> {
			descMap.put((DataMap) key, (DataMap) value);
			return null;
		}, false);
		Assertions.assertEquals(2, map.size());
		Assertions.assertEquals("aaaaaaaa1", descMap.values().stream().findFirst().get().get("Value"));
		Assertions.assertEquals("aaaaaaaa", map.values().stream().findFirst().get().get("Value"));

		kvStorage.clear();
		Assertions.assertNull(kvStorage.get(data1));

		kvStorage.put(data1, value1);

		theValue1 = (DataMap) kvStorage.get(data1);
		Assertions.assertNotNull(theValue1);
		Assertions.assertEquals("aaaaaaaa", theValue1.get("Value"));

		kvStorage.destroy();
	}

	@Test
	public void testSequenceStorage() {
		storageFactory = InstanceFactory.instance(TapStorageFactory.class);
		storageFactory.init(TapStorageFactory.StorageOptions.create().rootPath("./tap_storage_test"));
		TapSequenceStorage sequenceStorage = storageFactory.getSequenceStorage("test");
		Assertions.assertNotNull(sequenceStorage);

		DataMap data1 = DataMap.create().kv("Key", "1234567");
		DataMap value1 = DataMap.create().kv("Value", "aaaaaaaa");
		sequenceStorage.add(data1);
		sequenceStorage.add(value1);

		List<Object> list = list();
		Iterator<Object> iterator = sequenceStorage.iterator();
		while(iterator.hasNext()) {
			list.add(iterator.next());
		}
		Assertions.assertEquals(2, list.size());
		Assertions.assertEquals("1234567", ((DataMap)list.get(0)).get("Key"));
		Assertions.assertEquals("aaaaaaaa", ((DataMap)list.get(1)).get("Value"));

		list = list();
		iterator = sequenceStorage.iterator();
		while(iterator.hasNext()) {
			list.add(iterator.next());
		}
		Assertions.assertEquals(2, list.size());
		Assertions.assertEquals("1234567", ((DataMap)list.get(0)).get("Key"));
		Assertions.assertEquals("aaaaaaaa", ((DataMap)list.get(1)).get("Value"));

		sequenceStorage.clear();

		list = list();
		iterator = sequenceStorage.iterator();
		while(iterator.hasNext()) {
			list.add(iterator.next());
		}
		Assertions.assertEquals(0, list.size());

		sequenceStorage.destroy();
	}
}
