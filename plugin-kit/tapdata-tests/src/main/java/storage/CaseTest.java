package storage;

import io.tapdata.entity.schema.value.DateTime;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.modules.api.storage.TapKVStorage;
import io.tapdata.modules.api.storage.TapSequenceStorage;
import io.tapdata.modules.api.storage.TapStorageFactory;

import java.io.UnsupportedEncodingException;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

import static io.tapdata.entity.simplify.TapSimplify.entry;
import static io.tapdata.entity.simplify.TapSimplify.map;

/**
 * @author aplomb
 */
public class CaseTest {
	public static void main(String[] args) throws UnsupportedEncodingException {
		TapStorageFactory storageFactory = InstanceFactory.instance(TapStorageFactory.class);
		storageFactory.init(TapStorageFactory.StorageOptions.create().rootPath("./justtest"));

//		for(int i = 0; i < 16; i++) {
//			int finalI = i;
//			new Thread(() -> {
//				try {
//					testSequence(storageFactory, "" + finalI);
//				} catch (UnsupportedEncodingException e) {
//					throw new RuntimeException(e);
//				}
//			}).start();
//		}

		run(storageFactory, 1L, 0);
		run(storageFactory, 1L, 1);
		run(storageFactory, 2L, 0);
		run(storageFactory, 2L, 1);
		run(storageFactory, 4L, 0);
		run(storageFactory, 4L, 1);
		run(storageFactory, 8L, 0);
		run(storageFactory, 8L, 1);
		run(storageFactory, 16L, 0);
		run(storageFactory, 16L, 1);
	}

	private static void run(TapStorageFactory storageFactory, Long thread, int mode) {
		AtomicInteger counter = new AtomicInteger();
		System.out.println("Testing " + thread + " threads...");
		StringBuilder builder = new StringBuilder();
		Thread[] threads = new Thread[thread.intValue()];

		long startTime = System.currentTimeMillis();
		long count = 1000000;

		for(int i = 0; i < thread; i++) {
			int finalI = i;
			Thread t = new Thread(() -> {
				try {
					test(storageFactory, "" + finalI, count, mode);
				} catch (UnsupportedEncodingException e) {
					throw new RuntimeException(e);
				}
			});
			t.start();
			threads[i] = t;
		}
		for (int i = 0; i < thread; i++) {
			try {
				threads[i].join();
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}

		Long endTime = System.currentTimeMillis();
		if (mode == 0) {
			long speed = count*thread*1000*2/(endTime-startTime);
			System.out.println("avg write/read speed is:" + speed);
		}
		if (mode == 1) {
			long speed = count*thread*1000/(endTime-startTime);
			System.out.println("avg write speed is:" + speed);
		}
		System.out.println("=====================" + thread + " threads============================");

	}

	private static void test(TapStorageFactory storageFactory, String name, long count, int mode) throws UnsupportedEncodingException {
		TapKVStorage kvStorage = storageFactory.getKVStorage("justTest_" + name);

		Map<String, Object> map2 = map(entry("abc", 123), entry("aaa", "AKJFKLDSJFLD"), entry("jadsfl", "alskdfj"));
		map2.put("asdfa", map(entry("abc", 123), entry("aaa", "AKJFKLDSJFLD"), entry("jadsfl", "alskdfj")));
		map2.put("bytes", "helloworld".getBytes("utf8"));
		map2.put("dateTime", new DateTime(new Date()));

		for(int i = 0 ; i < count; i++) {
			Map<String, Object> key = map(entry("id", "id_" + i));
			map2.putAll(key);
			kvStorage.put(key, map2);
		}

		if (mode == 0) {
			for (int i = 0; i < count; i++) {
				Map<String, Object> key = map(entry("id", "id_" + i));
				Map<String, Object> value = (Map<String, Object>) kvStorage.get(key);
			}
		}

//		time = System.currentTimeMillis();
//		for(int i = 0 ; i < count; i++) {
//			Map<String, Object> key = map(entry("id", "id_" + i));
//			Map<String, Object> value = (Map<String, Object>) kvStorage.removeAndGet(key);
//		}
//		builder.append(Thread.currentThread() + " removeAndGet takes " + (System.currentTimeMillis() - time) + "\n");
	}

	private static void testSequence(TapStorageFactory storageFactory, String name) throws UnsupportedEncodingException {
		TapSequenceStorage sequenceStorage = storageFactory.getSequenceStorage("justTest_sequence_" + name);

		Map<String, Object> map2 = map(entry("abc", 123), entry("aaa", "AKJFKLDSJFLD"), entry("jadsfl", "alskdfj"));
		map2.put("asdfa", map(entry("abc", 123), entry("aaa", "AKJFKLDSJFLD"), entry("jadsfl", "alskdfj")));
		map2.put("bytes", "helloworld".getBytes("utf8"));
		map2.put("dateTime", new DateTime(new Date()));

		int count = 1000000;
		long time = System.currentTimeMillis();
		for(int i = 0 ; i < count; i++) {
			sequenceStorage.add(map2);
		}
		System.out.println(Thread.currentThread() + " add takes " + (System.currentTimeMillis() - time));

		time = System.currentTimeMillis();
		Iterator<Object> iterator = sequenceStorage.iterator();
		while(iterator.hasNext()) {
			Map<String, Object> value = (Map<String, Object>) iterator.next();
		}
		System.out.println(Thread.currentThread() + " iterator takes " + (System.currentTimeMillis() - time));
	}
}
