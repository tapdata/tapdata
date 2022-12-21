package io.tapdata.storage;

import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.modules.api.storage.TapKVStorage;
import io.tapdata.modules.api.storage.TapSequenceStorage;
import io.tapdata.modules.api.storage.TapStorageFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Iterator;
import java.util.List;

import static io.tapdata.entity.simplify.TapSimplify.list;

/**
 * @author aplomb
 */
public class TapStorageFactoryTest {
	TapStorageFactory storageFactory;
	@BeforeEach
	public void setup() {
		storageFactory = InstanceFactory.instance(TapStorageFactory.class);
		storageFactory.init(TapStorageFactory.StorageOptions.create().rootPath("./tap_storage_test"));
	}
	@Test
	public void testKVStorage() {
		TapKVStorage kvStorage = storageFactory.getKVStorage("test");
		Assertions.assertNotNull(kvStorage);

		DataMap data1 = DataMap.create().kv("Key", "1234567");
		DataMap value1 = DataMap.create().kv("Value", "aaaaaaaa");
		kvStorage.put(data1, value1);

		DataMap theValue1 = (DataMap) kvStorage.get(data1);
		Assertions.assertNotNull(theValue1);
		Assertions.assertEquals("aaaaaaaa", theValue1.get("Value"));

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
