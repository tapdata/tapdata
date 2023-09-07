package io.tapdata.pdk.core.api.impl.serialize;

import io.tapdata.entity.annotations.Bean;
import io.tapdata.entity.annotations.Implementation;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.error.TapAPIErrorCodes;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.serializer.JavaCustomSerializer;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.JsonParser;
import io.tapdata.entity.utils.ObjectSerializable;
import io.tapdata.entity.utils.TapUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.*;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.zip.GZIPInputStream;

import static io.tapdata.entity.simplify.TapSimplify.*;
import static io.tapdata.entity.utils.JsonParser.ToJsonFeature.WriteMapNullValue;

@Implementation(value = ObjectSerializable.class, buildNumber = 1)
public class ObjectSerializableImplV2 implements ObjectSerializable {
	private static final byte HAS_VALUE = 1;
	private static final byte NO_VALUE = 0;
	public static final byte TYPE_SERIALIZABLE = 1;
	public static final byte TYPE_JSON = 2;
	public static final byte TYPE_MONGODB_DOCUMENT = 3;
	public static final byte TYPE_JAVA_CUSTOM_SERIALIZER = 4;
	public static final byte TYPE_MONGODB_OBJECT_ID = 5;

	public static final byte TYPE_MAP = 100;
	public static final byte TYPE_LIST = 101;
	private static final byte END = -88;
	public static final byte TYPE_LONG_STRING = 19;
	public static final byte TYPE_STRING = 20;
	public static final byte TYPE_INTEGER = 21;
	private static final int TYPE_BYTES = 22;
	private static final int TYPE_DOUBLE = 23;
	private static final int TYPE_FLOAT = 24;
	private static final int TYPE_LONG = 25;
	private static final int TYPE_BIG_DECIMAL = 26;
	private static final int TYPE_BIG_INTEGER = 27;
	private static final int TYPE_SHORT = 28;
	private static final int TYPE_BYTE = 29;
	private static final int TYPE_DATE = 30;
	private static final int TYPE_TIMESTAMP = 31;
	private static final int TYPE_INSTANT = 32;
	private static final int TYPE_TIME = 33;

	private static final byte VERSION = -128; //greater the newer
	private static final String TAG = ObjectSerializableImplV2.class.getSimpleName();
	private Class<?> documentClass;
	private Method documentParseMethod;
	private Constructor objectIdConstructor;
	private Method documentToJsonMethod;
	@Bean
	private JsonParser jsonParser;

	private static  final FromObjectOptions defaultFromObjectOptions = new FromObjectOptions();
	private final Object ENDED = new byte[0];
	private volatile ObjectSerializableImpl firstVersion;

	@Override
	public byte[] fromObject(Object obj) {
		return fromObject(obj, defaultFromObjectOptions);
	}

	@Override
	public byte[] fromObject(Object obj, FromObjectOptions options) {
		if(obj == null)
			return null;
		if(options == null)
			options = defaultFromObjectOptions;
		try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
			 DataOutputStream dos = new DataOutputStream(bos)
		) {
			dos.writeByte(VERSION);
			fromObjectPrivate(obj, dos, options);
			return bos.toByteArray();
		} catch (Throwable t) {
			throw new RuntimeException(t);
		}
	}

	public void fromObjectPrivate(Object obj, DataOutputStream dos, FromObjectOptions fromObjectOptions) throws IOException {
		if (obj == null) {
			dos.writeByte(NO_VALUE);
			return;
		}
		dos.writeByte(HAS_VALUE);
		String name = obj.getClass().getName();
		switch (name) {
			case "org.bson.Document":
				if(documentToJsonMethod == null) {
					try {
						documentToJsonMethod = obj.getClass().getMethod("toJson");
					} catch (Throwable throwable) {
						throw new RuntimeException(throwable);
					}
				}
				String json;
				try {
					json = (String) documentToJsonMethod.invoke(obj);
				} catch (Throwable e) {
//						throw new RuntimeException(e);
					if(!documentToJsonMethod.getDeclaringClass().equals(obj.getClass())) {
						try {
							documentToJsonMethod = obj.getClass().getMethod("toJson");
						} catch (Throwable throwable) {
							throwable.printStackTrace();
						}
						try {
							json = (String) documentToJsonMethod.invoke(obj);
						} catch (Throwable ex) {
							throw new RuntimeException(ex);
						}
					} else {
						throw new RuntimeException(e);
					}
				}
				dos.writeByte(TYPE_MONGODB_DOCUMENT);
				byte[] data = json.getBytes(StandardCharsets.UTF_8);
				dos.writeInt(data.length);
				dos.write(data);
				return;
			case "org.bson.types.ObjectId":
				dos.writeByte(TYPE_MONGODB_OBJECT_ID);
				dos.writeUTF(obj.toString());
				return;
		}
		//instanceof compare to Map#containsKey, for 10_000_000 is 3 milliseconds to 23 milliseconds, instanceof is a lot faster than Map#containsKey.
		if(obj instanceof Map) {
			Map<?, ?> map = new HashMap<>();
			map.putAll((Map) obj);

			dos.writeByte(TYPE_MAP);
			if(fromObjectOptions.isUseActualMapAndList())
				dos.writeUTF(obj.getClass().getName());
			else
				dos.writeUTF("");
			for(Map.Entry<?, ?> entry : map.entrySet()) {
				Object key = entry.getKey();
				Object value = entry.getValue();
				if(fromObjectOptions.isWriteNullValue() || (key != null && value != null)) {
//					writeObjectAllCases(key, dos, fromObjectOptions);
					fromObjectPrivate(key, dos, fromObjectOptions);
//					writeObjectAllCases(value, dos, fromObjectOptions);
					fromObjectPrivate(value, dos, fromObjectOptions);
				}
			}
			dos.writeByte(END);
			return;
		} else if(obj instanceof List) {
			List<?> list = new ArrayList<>((List<?>) obj);
			dos.writeByte(TYPE_LIST);
			if(fromObjectOptions.isUseActualMapAndList())
				dos.writeUTF(obj.getClass().getName());
			else
				dos.writeUTF("");
			for(Object objValue : list) {
//				writeObjectAllCases(objValue, dos, fromObjectOptions);
				fromObjectPrivate(objValue, dos, fromObjectOptions);
			}
			dos.writeByte(END);
			return;
		} else if(obj instanceof String) {
			String str = (String) obj;
			if(str.length() > 12000) { //16384 * 4(assume one char is 4 bytes as max) = 64k, UTF can only accept 64k string.
				dos.writeByte(TYPE_LONG_STRING);
				byte[] strData = str.getBytes(StandardCharsets.UTF_8);
				dos.writeInt(strData.length);
				dos.write(strData);
			} else {
				dos.writeByte(TYPE_STRING);
				dos.writeUTF((String) obj);
			}
			return;
		} else if(obj instanceof Double) {
			dos.writeByte(TYPE_DOUBLE);
			dos.writeDouble((Double) obj);
			return;
		} else if(obj instanceof Integer) {
			dos.writeByte(TYPE_INTEGER);
			dos.writeInt((Integer) obj);
			return;
		} else if(obj instanceof Float) {
			dos.writeByte(TYPE_FLOAT);
			dos.writeFloat((Float) obj);
			return;
		} else if(obj instanceof Long) {
			dos.writeByte(TYPE_LONG);
			dos.writeLong((Long) obj);
			return;
		} else if(obj instanceof BigDecimal) {
			dos.writeByte(TYPE_BIG_DECIMAL);
			dos.writeUTF(obj.toString());
			return;
		} else if(obj instanceof BigInteger) {
			dos.writeByte(TYPE_BIG_INTEGER);
			dos.writeUTF(obj.toString());
			return;
		} else if(obj instanceof Short) {
			dos.writeByte(TYPE_SHORT);
			dos.writeShort((Short) obj);
			return;
		} else if(obj instanceof Byte) {
			dos.writeByte(TYPE_BYTE);
			dos.writeByte((Byte) obj);
			return;
		} else if(obj instanceof Time) {
			dos.writeByte(TYPE_TIME);
			dos.writeUTF(obj.toString());
			return;
		} else if(obj instanceof Timestamp) {
			dos.writeByte(TYPE_TIMESTAMP);
			dos.writeLong(((Timestamp)obj).getTime());
			return;
		} else if(obj instanceof Date) {
			dos.writeByte(TYPE_DATE);
			dos.writeLong(((Date) obj).getTime());
			return;
		} else if(obj instanceof Instant) {
			dos.writeByte(TYPE_INSTANT);
			dos.writeLong(((Instant) obj).getEpochSecond());
			dos.writeInt(((Instant) obj).getNano());
			return;
		} else if(obj instanceof byte[]) {
			byte[] data1 = (byte[]) obj;
			dos.writeByte(TYPE_BYTES);
			dos.writeInt(data1.length);
			dos.write(data1);
			return;
		} else if(obj instanceof JavaCustomSerializer) {
			JavaCustomSerializer javaCustomSerializer = (JavaCustomSerializer) obj;
			dos.writeByte(TYPE_JAVA_CUSTOM_SERIALIZER);
			dos.writeUTF(obj.getClass().getName());
			javaCustomSerializer.to(dos);
			return;
		}

		if (obj instanceof Serializable && fromObjectOptions.isToJavaPlatform()) {
			dos.writeByte(TYPE_SERIALIZABLE);
			try(ObjectOutputStream oos = new ObjectOutputStream(dos)) {
				oos.writeObject(obj);
			}
			return;
		}
		//Fallback to json serialization
		String str;
		if(fromObjectOptions.isWriteNullValue()) {
			str = jsonParser.toJson(obj, WriteMapNullValue);
		} else {
			str = jsonParser.toJson(obj);
		}
		byte[] data = str.getBytes(StandardCharsets.UTF_8);
		dos.writeByte(TYPE_JSON);
		dos.writeUTF(obj.getClass().getName());
		dos.writeInt(data.length);
		dos.write(data);
	}

	@Override
	public Object toObject(byte[] data) {
		return toObject(data, null);
	}

	@Override
	public Object toObject(byte[] data, ToObjectOptions options) {
		if(data == null)
			return null;
		try (ByteArrayInputStream bos = new ByteArrayInputStream(data);
			 DataInputStream dis = new DataInputStream(bos)
		) {
			byte version = dis.readByte();
			if(version != VERSION) {
				//Fallback to firstVersion of ObjectSerialization
				if(firstVersion == null) {
					synchronized (this) {
						if(firstVersion == null) {
							firstVersion = new ObjectSerializableImpl();
							InstanceFactory.injectBean(firstVersion);
						}
					}
				}
				return firstVersion.toObject(data, options);
			}
			//gzip performance is bad, 1000000 times, takes 2878 without gzip, with gzip 14000.
			return toObjectPrivate(dis, options);
		} catch (IOException e) {
			e.printStackTrace();
			//Compatible for old gzip data.
			if(e instanceof StreamCorruptedException) {
				try (ByteArrayInputStream bos = new ByteArrayInputStream(data);
					 GZIPInputStream gos = new GZIPInputStream(bos);
					 DataInputStream dis = new DataInputStream(gos)
				) {
					return toObjectPrivate(dis, options);
				} catch (IOException e1) {
					e1.printStackTrace();
				}
			}

		}
		return null;
	}

	private Object toObjectPrivate(DataInputStream dis, ToObjectOptions options) throws IOException {
		byte hasValue = dis.readByte();
		if(hasValue == END)
			return ENDED;
		if(hasValue == NO_VALUE)
			return null;
		byte type = dis.readByte();
		switch (type) {
			case TYPE_MAP:
				String classStr = dis.readUTF();
				Map<Object, Object> map;
				if(StringUtils.isBlank(classStr)) {
					map = new LinkedHashMap<>();
				} else {
					//noinspection unchecked
					Class<? extends Map<Object, Object>> mapClass = (Class<? extends Map<Object, Object>>) findClass(options, classStr);
					try {
						map = mapClass.newInstance();
					} catch (Throwable e) {
						TapLogger.warn(TAG, "Resurrect Map class {} failed, use LinkedHashMap by default, error {}", classStr, e.getMessage());
						map = new LinkedHashMap<>();
					}
				}
				while(true) {
					Object key = toObjectPrivate(dis, options);
					if(ENDED.equals(key))
						break;
					Object value = toObjectPrivate(dis, options);
					if(key != null) {
						map.put(key, value);
					}
				}
				return map;
			case TYPE_LIST:
				String listClassStr = dis.readUTF();
				List<Object> list;
				if(StringUtils.isBlank(listClassStr)) {
					list = new ArrayList<>();
				} else {
					//noinspection unchecked
					Class<? extends List<Object>> listClass = (Class<? extends List<Object>>) findClass(options, listClassStr);
					try {
						list = listClass.newInstance();
					} catch (Throwable e) {
						TapLogger.warn(TAG, "Resurrect List class {} failed, use ArrayList by default, error {}", listClassStr, e.getMessage());
						list = new ArrayList<>();
					}
				}
				while(true) {
					Object value = toObjectPrivate(dis, options);
					if(ENDED.equals(value))
						break;
					if(value != null)
						list.add(value);
				}

				return list;
			case TYPE_STRING:
				return dis.readUTF();
			case TYPE_LONG_STRING:
				int strLength = dis.readInt();
				byte[] strBytes = new byte[strLength];
				dis.readFully(strBytes);
				return new String(strBytes, StandardCharsets.UTF_8);
			case TYPE_INTEGER:
				return dis.readInt();
			case TYPE_BYTES:
				int length = dis.readInt();
				byte[] data = new byte[length];
				dis.readFully(data);
				return data;
			case TYPE_JSON:
				String className = dis.readUTF();
				int jsonLength = dis.readInt();
				byte[] jsonBytes = new byte[jsonLength];
				dis.readFully(jsonBytes);
				String content = new String(jsonBytes, StandardCharsets.UTF_8);
				Class<?> clazz = findClass(options, className);
				return jsonParser.fromJson(content, clazz);
			case TYPE_SERIALIZABLE:
				try(ObjectInputStream oos = new ObjectInputStreamEx(dis, options)) {
					return oos.readObject();
				} catch (ClassNotFoundException e) {
//						e.printStackTrace();
					throw new CoreException(TapAPIErrorCodes.CLASS_NOT_FOUND_READ_OBJECT, "readObject failed, {}", InstanceFactory.instance(TapUtils.class).getStackTrace(e));
				}
			case TYPE_MONGODB_OBJECT_ID:
				String idStr = dis.readUTF();
				if(objectIdConstructor == null || (options != null && options.getClassLoader() != null && !objectIdConstructor.getDeclaringClass().getClassLoader().equals(options.getClassLoader()))) {
					try {
						Class<?> objectIdClass = findClass(options, "org.bson.types.ObjectId");
						objectIdConstructor = objectIdClass.getConstructor(String.class);
					} catch (Throwable throwable) {
//							throwable.printStackTrace();
						throw new CoreException(TapAPIErrorCodes.FIND_OBJECT_ID_FAILED, "findClass for org.bson.types.ObjectId failed, {}", InstanceFactory.instance(TapUtils.class).getStackTrace(throwable));
					}
				}
				if(objectIdConstructor != null) {
					try {
						return objectIdConstructor.newInstance(idStr);
					} catch (Throwable e) {
//							e.printStackTrace();
						throw new CoreException(TapAPIErrorCodes.NEW_OBJECT_ID, "ObjectId newInstance with id {} failed, {}", idStr, InstanceFactory.instance(TapUtils.class).getStackTrace(e));
					}
				}
				break;
			case TYPE_MONGODB_DOCUMENT:
				int docLength = dis.readInt();
				byte[] docBytes = new byte[docLength];
				dis.readFully(docBytes);
				String docContent = new String(docBytes, StandardCharsets.UTF_8);
				if(documentParseMethod == null || (options != null && options.getClassLoader() != null && !documentParseMethod.getDeclaringClass().getClassLoader().equals(options.getClassLoader()))) {
					try {
						documentClass = findClass(options, "org.bson.Document");
						documentParseMethod = documentClass.getMethod("parse", String.class);
					} catch (Throwable throwable) {
//							throwable.printStackTrace();
						throw new CoreException(TapAPIErrorCodes.GET_PARSE_METHOD_FAILED, "org.bson.Document get parse method failed, {}", InstanceFactory.instance(TapUtils.class).getStackTrace(throwable));
					}
				}
				if(documentParseMethod != null) {
					try {
//						Object newObj = documentClass.newInstance();
						return documentParseMethod.invoke(null, docContent);
					} catch (Throwable e) {
//							e.printStackTrace();
						throw new CoreException(TapAPIErrorCodes.PARSE_DOCUMENT_FAILED, "org.bson.Document get parse method failed, {}", InstanceFactory.instance(TapUtils.class).getStackTrace(e));
					}
				}
				break;
			case TYPE_JAVA_CUSTOM_SERIALIZER:
				String className1 = dis.readUTF();
				Class<?> clazz1 = findClass(options, className1);
				if(clazz1 != null) {
					try {
						JavaCustomSerializer javaCustomSerializer = (JavaCustomSerializer) clazz1.getConstructor().newInstance();
						javaCustomSerializer.from(dis);
						return javaCustomSerializer;
					} catch (Throwable e) {
						throw new CoreException(TapAPIErrorCodes.ERROR_JAVA_CUSTOM_DESERIALIZE_FAILED, e, "JavaCustomSerializer deserialize failed, {}", e.getMessage());
					}
				}
			case TYPE_BIG_DECIMAL:
				String decimalStr = dis.readUTF();
				return new BigDecimal(decimalStr);
			case TYPE_BIG_INTEGER:
				String bigStr = dis.readUTF();
				return new BigInteger(bigStr);
			case TYPE_BYTE:
				return dis.readByte();
			case TYPE_DATE:
				return new Date(dis.readLong());
			case TYPE_DOUBLE:
				return dis.readDouble();
			case TYPE_FLOAT:
				return dis.readFloat();
			case TYPE_INSTANT:
				return Instant.ofEpochSecond(dis.readLong(), dis.readInt());
			case TYPE_LONG:
				return dis.readLong();
			case TYPE_SHORT:
				return dis.readShort();
			case TYPE_TIME:
				return Time.valueOf(dis.readUTF());
			case TYPE_TIMESTAMP:
				return new Timestamp(dis.readLong());
		}
		return null;
	}

	private Class<?> findClass(ToObjectOptions options, String className) {
		Class<?> targetClass = null;
		if (options != null && options.getClassLoader() != null) {
			try {
				targetClass = options.getClassLoader().loadClass(className);
			} catch (ClassNotFoundException e) {
//				e.printStackTrace();
			}
		}
		if (targetClass == null) {
			try {
				targetClass = Class.forName(className);
			} catch (ClassNotFoundException e) {
//				e.printStackTrace();
				throw new CoreException(TapAPIErrorCodes.CLASS_NOT_FOUND, "Class {} not found, {}", className, tapUtils().getStackTrace(e));
			}
		}
		return targetClass;
	}

	private static class ObjectInputStreamEx extends ObjectInputStream {
		private final ToObjectOptions options;

		public ObjectInputStreamEx(InputStream in, ToObjectOptions options) throws IOException {
			super(in);
			this.options = options;
		}

		@Override
		protected Class<?> resolveClass(ObjectStreamClass desc)
				throws IOException, ClassNotFoundException {
			Class<?> theClass = null;
			String name = desc.getName();
			if (options != null && options.getClassLoader() != null) {
				try {
					theClass = options.getClassLoader().loadClass(name);
				} catch (ClassNotFoundException ignored) {
					int pos;
					if((pos = name.indexOf("$")) > 0) {
						name = name.substring(0, pos);
						try {
							theClass = options.getClassLoader().loadClass(name);
						} catch (Throwable ignored1) {}
					}
				} catch (Throwable throwable) {
					throwable.printStackTrace();
				}
			}
			if (theClass != null)
				return theClass;

			return super.resolveClass(desc);
		}
	}

	public static void main(String[] args) throws IOException {
		TapTable tapTable = new TapTable("aa");
		ObjectSerializableImpl objectSerializable = new ObjectSerializableImpl();
		byte[] data = objectSerializable.fromObject(tapTable);
		Object theObj = objectSerializable.toObject(data);

		Map<String, Object> map = map(
				entry("abc", "aaaa"),
				entry("aaa", list(map(entry("aaa", list("234", "234"))), map(entry("aaa", list("234", "234"))))),
				entry("map", map(entry("aaa", 123)))
				);

		long time = System.currentTimeMillis();
		for(int i = 0; i < 100000; i++)
			objectSerializable.toObject(objectSerializable.fromObject(map));
		System.out.println("takes " + (System.currentTimeMillis() - time) + " bytes " + objectSerializable.fromObject(map).length);

		Map<String, Object> aa = (Map<String, Object>) fromJson(toJson(map));

		time = System.currentTimeMillis();
		for(int i = 0; i < 1000000; i++)
			fromJson(toJson(map));
		System.out.println("takes " + (System.currentTimeMillis() - time) + " bytes " + toJson(map).getBytes(StandardCharsets.UTF_8).length);

		time = System.currentTimeMillis();
		byte[] theData = null;
		for(int i = 0; i < 100000; i++) {
			try (ByteArrayOutputStream bos = new ByteArrayOutputStream();ObjectOutputStream oos = new ObjectOutputStream(bos)) {
				oos.writeObject(map);
				oos.close();
				theData = bos.toByteArray();
			} catch (IOException e) {
//				e.printStackTrace();
			}
			Object obj;
			try(ObjectInputStream oos = new ObjectInputStream(new ByteArrayInputStream(theData))) {
				obj = oos.readObject();
			} catch (ClassNotFoundException e) {
//						e.printStackTrace();
			}
		}
		System.out.println("takes " + (System.currentTimeMillis() - time) + " bytes " + theData.length);
	}
}
