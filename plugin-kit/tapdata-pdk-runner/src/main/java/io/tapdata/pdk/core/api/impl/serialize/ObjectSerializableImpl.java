package io.tapdata.pdk.core.api.impl.serialize;

import io.tapdata.entity.annotations.Bean;
import io.tapdata.entity.annotations.Implementation;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.serializer.JavaCustomSerializer;
import io.tapdata.entity.utils.JsonParser;
import io.tapdata.entity.utils.ObjectSerializable;

import java.io.*;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import static io.tapdata.entity.simplify.TapSimplify.*;

/**
 * Performance is worse than v2.
 */
@Deprecated
@Implementation(value = ObjectSerializable.class, buildNumber = 0)
public class ObjectSerializableImpl implements ObjectSerializable {
	public static final byte TYPE_SERIALIZABLE = 1;
	public static final byte TYPE_JSON = 2;
	public static final byte TYPE_MONGODB_DOCUMENT = 3;
	public static final byte TYPE_JAVA_CUSTOM_SERIALIZER = 4;
	public static final byte TYPE_MONGODB_OBJECT_ID = 5;
	public static final byte TYPE_MAP = 100;
	public static final byte TYPE_LIST = 101;
	private static final int END = -88888;
	private Class<?> documentClass;
	private Method documentParseMethod;
	private Constructor objectIdConstructor;
	private Method documentToJsonMethod;
	@Bean
	private JsonParser jsonParser;
	public byte[] fromObjectContainer(Object obj, FromObjectOptions fromObjectOptions) {
		if(obj == null)
			return null;
		byte[] data = null;
		if(obj.getClass().getName().equals("org.bson.Document")) {
			return null;
		} else if(obj instanceof Map) {
			Map<?, ?> map = (Map<?, ?>) obj;
			try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
				 DataOutputStream dos = new DataOutputStream(bos);
			) {
				dos.writeByte(TYPE_MAP);
				dos.writeUTF(obj.getClass().getName());
				dos.writeInt(map.size());
				for(Map.Entry<?, ?> entry : map.entrySet()) {
					Object key = entry.getKey();
					Object value = entry.getValue();
					writeObjectAllCases(key, dos, fromObjectOptions);
					writeObjectAllCases(value, dos, fromObjectOptions);
				}
				dos.close();
				data = bos.toByteArray();
			} catch (Throwable ignored) {}
		} else if(obj instanceof List) {
			List<?> list = (List<?>) obj;
			try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
				 DataOutputStream dos = new DataOutputStream(bos);
			) {
				dos.writeByte(TYPE_LIST);
				dos.writeUTF(obj.getClass().getName());
				dos.writeInt(list.size());
				for(Object objValue : list) {
					writeObjectAllCases(objValue, dos, fromObjectOptions);
				}
				dos.close();
				data = bos.toByteArray();
			} catch (Throwable ignored) {}
		}
		return data;
	}

	private void writeObjectAllCases(Object obj, DataOutputStream oos, FromObjectOptions fromObjectOptions) throws IOException {
		byte[] containerBytes = fromObjectContainer(obj, fromObjectOptions);
		if(containerBytes == null) {
			writeObject(obj, oos);
		} else {
			oos.writeInt(containerBytes.length);
			oos.write(containerBytes);
		}
	}

	private void writeObject(Object obj, DataOutputStream oos) throws IOException {
		if(obj != null) {
			byte[] objBytes = fromObjectPrivate(obj, defaultFromObjectOptions);
			if(objBytes != null) {
				oos.writeInt(objBytes.length);
				oos.write(objBytes);
			} else {
				oos.writeInt(0);
			}
		} else {
			oos.writeInt(0);
		}
	}

	private Object readObject(DataInputStream dis, ToObjectOptions options) throws IOException {
		int length = dis.readInt();
		if(length > 0) {
			byte[] data = new byte[length];
			dis.readFully(data);
			return toObject(data, options);
		}
		return null;
	}
	private static  final FromObjectOptions defaultFromObjectOptions = new FromObjectOptions();
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
		byte[] data = fromObjectContainer(obj, options);
		if(data == null) {
			data = fromObjectPrivate(obj, options);
		}
		return data;
	}

	public byte[] fromObjectPrivate(Object obj, FromObjectOptions fromObjectOptions) {
		if (obj == null)
			return null;
		byte[] data = null;
		if(obj instanceof JavaCustomSerializer) {
			JavaCustomSerializer javaCustomSerializer = (JavaCustomSerializer) obj;
			try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
				 DataOutputStream oos = new DataOutputStream(baos)) {
				oos.writeByte(TYPE_JAVA_CUSTOM_SERIALIZER);
				oos.writeUTF(obj.getClass().getName());
				javaCustomSerializer.to(baos);
				oos.close();
				data = baos.toByteArray();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
		if(data == null) {
			String name = obj.getClass().getName();
			switch (name) {
				case "org.bson.Document":
					if(documentToJsonMethod == null) {
						try {
							documentToJsonMethod = obj.getClass().getMethod("toJson");
						} catch (Throwable throwable) {
							throwable.printStackTrace();
						}
					}
					if(documentToJsonMethod != null) {
						try {
							String json = (String) documentToJsonMethod.invoke(obj);
							try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
//						 GZIPOutputStream gos = new GZIPOutputStream(bos);
								 DataOutputStream oos = new DataOutputStream(bos);
							) {
								oos.writeByte(TYPE_MONGODB_DOCUMENT);
								oos.writeUTF(json);
								oos.close();
								data = bos.toByteArray();
							} catch (IOException e) {
//						e.printStackTrace();
							}
						} catch (Throwable e) {
//					e.printStackTrace();
						}
					}
					break;
				case "org.bson.types.ObjectId":
					try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
//						 GZIPOutputStream gos = new GZIPOutputStream(bos);
						 DataOutputStream oos = new DataOutputStream(bos);
					) {
						oos.writeByte(TYPE_MONGODB_OBJECT_ID);
						oos.writeUTF(obj.toString());
						oos.close();
						data = bos.toByteArray();
					} catch (IOException e) {
//						e.printStackTrace();
					}
					break;
			}
		}
		if (data == null && obj instanceof Serializable && fromObjectOptions.isToJavaPlatform()) {
			try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
				 DataOutputStream dis = new DataOutputStream(bos)) {
				dis.writeByte(TYPE_SERIALIZABLE);
				try(ObjectOutputStream oos = new ObjectOutputStream(bos)) {
					oos.writeObject(obj);
					oos.close();
				}
				data = bos.toByteArray();
			} catch (IOException e) {
//				e.printStackTrace();
			}
		}
		if (data == null) {
			String str = jsonParser.toJson(obj, JsonParser.ToJsonFeature.WriteMapNullValue);
			try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
//				 GZIPOutputStream gos = new GZIPOutputStream(bos);
				 DataOutputStream oos = new DataOutputStream(bos);
			) {
				oos.writeByte(TYPE_JSON);
				oos.writeUTF(obj.getClass().getName());
				oos.writeUTF(str);
				oos.close();
				data = bos.toByteArray();
			} catch (IOException e) {
//				e.printStackTrace();
			}
		}
		return data;
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
//			 GZIPInputStream gos = new GZIPInputStream(bos);
			 DataInputStream dis = new DataInputStream(bos);
		) {
			//gzip performance is bad, 1000000 times, takes 2878 without gzip, with gzip 14000.
			return deserializeObject(dis, options);
		} catch (IOException e) {
			e.printStackTrace();
			//Compatible for old gzip data.
			if(e instanceof StreamCorruptedException) {
				try (ByteArrayInputStream bos = new ByteArrayInputStream(data);
					 GZIPInputStream gos = new GZIPInputStream(bos);
					 DataInputStream dis = new DataInputStream(gos);
				) {
					return deserializeObject(dis, options);
				} catch (IOException e1) {
					e1.printStackTrace();
				}
			}

		}
		return null;
	}

	private Object deserializeObject(DataInputStream dis, ToObjectOptions options) throws IOException {
		byte type = dis.readByte();
		switch (type) {
			case TYPE_MAP:
				String classStr = dis.readUTF();
				int size = dis.readInt();
				Class<? extends Map> mapClass = (Class<? extends Map>) findClass(options, classStr);
				Map<Object, Object> map = null;
				try {
					map = mapClass.newInstance();
				} catch (Throwable e) {
					return null;
				}
				if(size > 0) {
					for(int i = 0; i < size; i++) {
						Object key = readObject(dis, options);
						Object value = readObject(dis, options);
						if(key != null) {
							map.put(key, value);
						}
					}
				}
				return map;
			case TYPE_LIST:
				String listClassStr = dis.readUTF();
				int listSize = dis.readInt();
				Class<? extends List> listClass = (Class<? extends List>) findClass(options, listClassStr);
				List<Object> list = null;
				try {
					list = listClass.newInstance();
				} catch (Throwable e) {
					return null;
				}
				if(listSize > 0) {
					for(int i = 0; i < listSize; i++) {
						Object value = readObject(dis, options);
						if(value != null)
							list.add(value);
					}
				}
				return list;
			case TYPE_JSON:
				String className = dis.readUTF();
				String content = dis.readUTF();
				Class<?> clazz = findClass(options, className);
				return jsonParser.fromJson(content, clazz);
			case TYPE_SERIALIZABLE:
				try(ObjectInputStream oos = new ObjectInputStreamEx(dis, options)) {
					return oos.readObject();
				} catch (ClassNotFoundException e) {
//						e.printStackTrace();
				}
				break;
			case TYPE_MONGODB_OBJECT_ID:
				String idStr = dis.readUTF();
				if(objectIdConstructor == null) {
					try {
						Class<?> objectIdClass = findClass(options, "org.bson.types.ObjectId");
						objectIdConstructor = objectIdClass.getConstructor(String.class);
					} catch (Throwable throwable) {
//							throwable.printStackTrace();
					}
				}
				if(objectIdConstructor != null) {
					try {
						return objectIdConstructor.newInstance(idStr);
					} catch (Throwable e) {
//							e.printStackTrace();
					}
				}
				break;
			case TYPE_MONGODB_DOCUMENT:
				String json = dis.readUTF();
				if(documentParseMethod == null) {
					try {
						documentClass = findClass(options, "org.bson.Document");
						documentParseMethod = documentClass.getMethod("parse", String.class);
					} catch (Throwable throwable) {
//							throwable.printStackTrace();
					}
				}
				if(documentParseMethod != null) {
					try {
						Object newObj = documentClass.newInstance();
						return documentParseMethod.invoke(newObj, json);
					} catch (Throwable e) {
//							e.printStackTrace();
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
//						throw new RuntimeException(e);
					}
				}
				break;
		}
		return null;
	}

	private Class<?> findClass(ToObjectOptions options, String className) {
		Class<?> targetClass = null;
		if (options != null && options.getClassLoader() != null) {
			try {
				targetClass = options.getClassLoader().loadClass(className);
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
		}
		if (targetClass == null) {
			try {
				targetClass = Class.forName(className);
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
		}
		return targetClass;
	}

	private static class ObjectInputStreamEx extends ObjectInputStream {
		private ToObjectOptions options;

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
						} catch (Throwable throwable) {}
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
		System.out.println("takes " + (System.currentTimeMillis() - time));

		Map<String, Object> aa = (Map<String, Object>) fromJson(toJson(map));

		time = System.currentTimeMillis();
		for(int i = 0; i < 1000000; i++)
			fromJson(toJson(map));
		System.out.println("takes " + (System.currentTimeMillis() - time));

		time = System.currentTimeMillis();
		for(int i = 0; i < 100000; i++) {
			byte[] theData = null;
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
		System.out.println("takes " + (System.currentTimeMillis() - time));
	}
}
