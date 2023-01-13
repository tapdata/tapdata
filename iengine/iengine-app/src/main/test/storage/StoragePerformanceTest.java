package storage;

import io.tapdata.common.JSONUtil;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.value.DateTime;
import io.tapdata.entity.utils.*;
import net.openhft.chronicle.map.ChronicleMap;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorInputStream;
import org.apache.commons.compress.compressors.CompressorOutputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.commons.io.FileUtils;
import org.rocksdb.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.LongAdder;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import static io.tapdata.entity.simplify.TapSimplify.*;

/**
 * @author aplomb
 */
public class StoragePerformanceTest {
	public static RocksDB db;
	public static RocksDB db1;
	public static Map<String, Object> the100FieldMap;
	public static void main(String[] args) throws IOException, RocksDBException, CompressorException, ClassNotFoundException {
		FileUtils.forceMkdir(new File("./rocks-db"));
		ChronicleMap chronicleMap = ChronicleMap
				.of(String.class, Object.class)
				.name("xid")
				.averageKey("xid")
				.averageValue(new TapTable())
				.entries(500000L)
				.maxBloatFactor(200)
				.createPersistedTo(new File("./rocks-db/ch"));

		RocksDB.loadLibrary();
		final Options options = new Options();
		options.setCreateIfMissing(true);
		options.setCompressionType(CompressionType.ZSTD_COMPRESSION);
		File dbDir = new File("./rocks-db", "rdb");
		File dbDir1 = new File("./rocks-db", "rdb1");
		try {
			FileUtils.forceMkdir(new File("./rocks-db/"));
			db = RocksDB.open(options, dbDir.getAbsolutePath());
			db1 = RocksDB.open(options, dbDir1.getAbsolutePath());
		} catch(RocksDBException ex) {
			System.out.println(FormatUtils.format("Error initializing RocksDB, check configurations and permissions, exception: {}, message: {}, stackTrace: {}",
					ex.getCause(), ex.getMessage(), ex.getStackTrace()));
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		System.out.println("RocksDB initialized and ready to use");
		WriteOptions writeOptions = new WriteOptions();
		ReadOptions readOptions = new ReadOptions();

		Map<String, Object> map2 = map(entry("abc", 123), entry("aaa", "AKJFKLDSJFLD"), entry("jadsfl", "alskdfj"));
		map2.put("asdfa", map(entry("abc", 123), entry("aaa", "AKJFKLDSJFLD"), entry("jadsfl", "alskdfj")));
		map2.put("bytes", "helloworld".getBytes("utf8"));
		map2.put("dateTime", new DateTime(new Date()));

		the100FieldMap = map();
		for(int i = 0; i < 100; i++) {
			the100FieldMap.put("f" + i, UUID.randomUUID().toString());
		}

		TapTable table = new TapTable("asdlkfjlsdakfj").add(index("asd").indexField(indexField("aaaa"))).add(field("aaaa", "varchar").tapType(tapString().bytes(100L)));
		Object map = map2;

		int total = 1_000_000;
//		int total = 1;
		JsonParser jsonParser = InstanceFactory.instance(JsonParser.class);
		ObjectSerializable objectSerializable = InstanceFactory.instance(ObjectSerializable.class);

		System.out.println("fastjson json " + jsonParser.toJson(getMap()));
		System.out.println("jackson json " + JSONUtil.obj2Json(getMap()));

		ObjectSerializable.FromObjectOptions fromObjectOptions = new ObjectSerializable.FromObjectOptions().useActualMapAndList(false).toJavaPlatform(false);
		byte[] sampleData = objectSerializable.fromObject(getMap());
		CompressorStreamFactory factory = new CompressorStreamFactory();
		long time;


		time = System.currentTimeMillis();
		for(int i = 0; i < total; i++) {
			chronicleMap.put("a" + i, getMap());
//			chronicleMap.get("aaa");
		}
		System.out.println("chronicleMap put takes " + (System.currentTimeMillis() - time));

		time = System.currentTimeMillis();
		for(int i = 0; i < total; i++) {
			chronicleMap.get("a" + i);
//			chronicleMap.get("aaa");
		}
		System.out.println("chronicleMap get takes " + (System.currentTimeMillis() - time));

//		time = System.currentTimeMillis();
//		for(int i = 0; i < total; i++) {
//			byte[] data = objectSerializable.fromObject(map, fromObjectOptions);
//			objectSerializable.toObject(data);
//		}
//		System.out.println("objectSerializable fromObject/toObject takes " + (System.currentTimeMillis() - time));

		time = System.currentTimeMillis();
		for(int i = 0; i < total; i++) {
			byte[] data = objectSerializable.fromObject(getMap());
			objectSerializable.toObject(data);
		}
		System.out.println("objectSerializable fromObject/toObject (default) takes " + (System.currentTimeMillis() - time));
//		System.out.println("objectSerializable toObject takes " + (System.currentTimeMillis() - time));

		time = System.currentTimeMillis();
		for(int i = 0; i < total; i++) {
			byte[] data = jsonParser.toJson(getMap()).getBytes("utf8");
			jsonParser.fromJson(new String(data, "utf8"));
		}
		System.out.println("fastjson toJson/fromJson takes " + (System.currentTimeMillis() - time));

//		ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
//		ObjectOutputStream oos = new ObjectOutputStream(byteArrayOutputStream);
//		time = System.currentTimeMillis();
//		for(int i = 0; i < total; i++) {
//			oos.writeObject(getMap());
//			byte[] data = byteArrayOutputStream.toByteArray();
//			ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(data);
//			ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream);
//			objectInputStream.readObject();
//		}
//		oos.close();
//		System.out.println("writeObject/readObject takes " + (System.currentTimeMillis() - time));


//		time = System.currentTimeMillis();
//		for(int i = 0; i < total; i++) {
//			byte[] data = JSONUtil.obj2Json(map).getBytes("utf8");
////			jsonParser.fromJson(new String(data, "utf8"));
//		}
//		System.out.println("Jackson toJson takes " + (System.currentTimeMillis() - time));

//		if(true)
//			return;

		LongAdder size = new LongAdder();
		OutputStream os = FileUtils.openOutputStream(new File("./rocks-db/fileZstd"));
		try (CompressorOutputStream outputStream = factory.createCompressorOutputStream(CompressorStreamFactory.ZSTANDARD, os);
			 DataOutputStream oos1 = new DataOutputStream(outputStream)) {
			time = System.currentTimeMillis();
			for(int i = 0; i < total; i++) {
				byte[] data = objectSerializable.fromObject(getMap());
				oos1.writeInt(data.length);
				size.add(data.length);
				oos1.write(data);
			}
			System.out.println("objectSerializable fromObject with write zstd takes " + (System.currentTimeMillis() - time) + " size " + size.longValue());
		}

		LongAdder size1 = new LongAdder();
		InputStream is1 = FileUtils.openInputStream(new File("./rocks-db/fileZstd"));
		LongAdder counter4 = new LongAdder();
		try (CompressorInputStream outputStream = factory.createCompressorInputStream(CompressorStreamFactory.ZSTANDARD, is1);
			 DataInputStream oos1 = new DataInputStream(outputStream)) {
			time = System.currentTimeMillis();
			for(int i = 0; i < total; i++) {
				int length = oos1.readInt();
				size1.add(length);
				byte[] data = new byte[length];
				oos1.readFully(data);
				objectSerializable.toObject(data);
				counter4.increment();;
			}
			System.out.println("objectSerializable toObject with read zstd takes " + (System.currentTimeMillis() - time)  + " counter " + counter4.longValue() + " size1 " + size1.longValue());
		}

		FileOutputStream fos2 = FileUtils.openOutputStream(new File("./rocks-db/fileObj"), false);
		GZIPOutputStream outputStream2 = new GZIPOutputStream(fos2);
		DataOutputStream oos1 = new DataOutputStream(outputStream2);
		time = System.currentTimeMillis();
		for(int i = 0; i < total; i++) {
			byte[] data = objectSerializable.fromObject(getMap());
			oos1.writeInt(data.length);
			oos1.write(data);
		}
		oos1.close();
		System.out.println("objectSerializable fromObject with write takes " + (System.currentTimeMillis() - time));

		FileInputStream fis3 = FileUtils.openInputStream(new File("./rocks-db/fileObj"));
		GZIPInputStream inputStream3 = new GZIPInputStream(fis3);
		DataInputStream dis3 = new DataInputStream(inputStream3);
		LongAdder counter3 = new LongAdder();
		time = System.currentTimeMillis();
		for(int i = 0; i < total; i++) {
			int length = dis3.readInt();
			byte[] data = new byte[length];
			dis3.readFully(data);
			objectSerializable.toObject(data);
			counter3.increment();;
		}
		dis3.close();
		System.out.println("objectSerializable toObject with read takes " + (System.currentTimeMillis() - time)  + " counter " + counter3.longValue());

//		FileOutputStream fos1 = FileUtils.openOutputStream(new File("./rocks-db/fileOOS"), false);
//		GZIPOutputStream outputStream1 = new GZIPOutputStream(fos1);
//		ObjectOutputStream oos = new ObjectOutputStream(outputStream1);
//		time = System.currentTimeMillis();
//		for(int i = 0; i < total; i++) {
//			oos.writeObject(getMap());
//		}
//		oos.close();
//		System.out.println("writeObject takes " + (System.currentTimeMillis() - time));

//		FileInputStream fis1 = FileUtils.openInputStream(new File("./rocks-db/fileOOS"));
//		GZIPInputStream inputStream1 = new GZIPInputStream(fis1);
//		ObjectInputStream dis1 = new ObjectInputStream(inputStream1);
//		LongAdder counter1 = new LongAdder();
//		time = System.currentTimeMillis();
//		for(int i = 0; i < total; i++) {
//			dis1.readObject();
//			counter1.increment();;
//		}
//		dis1.close();
//		System.out.println("readObject takes " + (System.currentTimeMillis() - time)  + " counter " + counter1.longValue());
		time = System.currentTimeMillis();
		for(int i = 0; i < total; i++) {
			db1.put(("abc" + i).getBytes(), objectSerializable.fromObject(getMap()));
//			jsonParser.fromJson(new String(db.get("abc".getBytes()), "utf8"), Map.class);
		}
		System.out.println("rocksdb write(objectSerialize) takes " + (System.currentTimeMillis() - time));

		time = System.currentTimeMillis();
		for(int i = 0; i < total; i++) {
//			db.put("abc".getBytes(), jsonParser.toJson(map).getBytes("utf8"));
//			jsonParser.fromJson(new String(db.get("abc".getBytes()), "utf8"), Map.class);
			objectSerializable.toObject(db1.get(("abc" + i).getBytes()));
		}
		System.out.println("rocksdb read(objectSerialize) takes " + (System.currentTimeMillis() - time));

		time = System.currentTimeMillis();
		for(int i = 0; i < total; i++) {
			db.put(("abc" + i).getBytes(), jsonParser.toJson(getMap()).getBytes("utf8"));
//			jsonParser.fromJson(new String(db.get("abc".getBytes()), "utf8"), Map.class);
		}
		System.out.println("rocksdb write takes " + (System.currentTimeMillis() - time));

		time = System.currentTimeMillis();
		for(int i = 0; i < total; i++) {
//			db.put("abc".getBytes(), jsonParser.toJson(map).getBytes("utf8"));
//			jsonParser.fromJson(new String(db.get("abc".getBytes()), "utf8"), Map.class);
			jsonParser.fromJson(new String(db.get(("abc" + i).getBytes()), "utf8"));
		}
		System.out.println("rocksdb read takes " + (System.currentTimeMillis() - time));

//		CompressorStreamFactory factory = new CompressorStreamFactory();
//		try (CompressorOutputStream outputStream = factory.createCompressorOutputStream(CompressorStreamFactory.ZSTANDARD, null)) {
//
//		} catch (CompressorException e) {
//			throw new RuntimeException(e);
//		}

		FileOutputStream fos = FileUtils.openOutputStream(new File("./rocks-db/jsonZstd"), false);
//		CompressorStreamFactory factory = new CompressorStreamFactory();
//		CompressorOutputStream outputStream = factory.createCompressorOutputStream(CompressorStreamFactory.ZSTANDARD, fos);
		try (CompressorOutputStream outputStream = factory.createCompressorOutputStream(CompressorStreamFactory.ZSTANDARD, fos);
			 DataOutputStream dos = new DataOutputStream(outputStream)) {
			time = System.currentTimeMillis();
			for(int i = 0; i < total; i++) {
				dos.writeUTF(jsonParser.toJson(getMap()));
			}
		}
		System.out.println("writeUtf json with zstd takes " + (System.currentTimeMillis() - time));

		FileInputStream fis = FileUtils.openInputStream(new File("./rocks-db/jsonZstd"));
		LongAdder counter = new LongAdder();
		try (CompressorInputStream outputStream4 = factory.createCompressorInputStream(CompressorStreamFactory.ZSTANDARD, fis);
			 DataInputStream dis = new DataInputStream(outputStream4)) {
			time = System.currentTimeMillis();
			for(int i = 0; i < total; i++) {
				jsonParser.fromJson(dis.readUTF());
				counter.increment();;
			}
		}
		System.out.println("readUTF json with zstd takes " + (System.currentTimeMillis() - time)  + " counter " + counter.longValue());
	}

	private static Map<String, Object> getMap1() throws UnsupportedEncodingException {
		Map<String, Object> map = map(entry("abc", 123), entry("aaa", "AKJFKLDSJFLD"), entry("jadsfl", "alskdfj"));
		map.put("asdfa", map(entry("abc", 123), entry("aaa", "AKJFKLDSJFLD"), entry("jadsfl", "alskdfj")));
		map.put("asdfa1", map(entry("abc", 123), entry("aaa", "AKJFKLDSJFLD"), entry("jadsfl", "alskdfj")));
		map.put("asdfa2", map(entry("abc", 123), entry("aaa", "AKJFKLDSJFLD"), entry("jadsfl", "alskdfj")));
		map.put("asdfa3", map(entry("abc", 123), entry("aaa", "AKJFKLDSJFLD"), entry("jadsfl", "alskdfj")));
		map.put("bytes", "helloworld".getBytes("utf8"));
		map.put("dateTime", new DateTime(new Date()));
		return map;
	}
	public static Map<String, Object> getMap() {
		return the100FieldMap;
	}
	public static Map<String, Object> getMap3() {
		Map<String, Object> map1 = map();
		for(int i = 0; i < 1; i++) {
			map1.put("f" + i, UUID.randomUUID().toString());
		}
		return map1;
	}
	public static Object getMap2() {
		return "kasdjflkjasdklfjsaklfjsalkdfjdsalkfjalkdsfjlkasdfjaslkdfjalskdfjslakdfjlaskdf " + UUID.randomUUID();
	}
}
