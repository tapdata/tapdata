package io.tapdata.pdk.core.api.impl;

import io.tapdata.entity.annotations.Implementation;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.JsonParser;
import io.tapdata.entity.utils.ObjectSerializable;

import java.io.*;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

@Implementation(value = ObjectSerializable.class, buildNumber = 0)
public class ObjectSerializableImpl implements ObjectSerializable {
	public static final byte TYPE_SERIALIZABLE = 1;
	public static final byte TYPE_JSON = 2;
	public static final byte TYPE_MONGODB_DOCUMENT = 3;
	public static final byte TYPE_MAP = 100;
	public static final byte TYPE_LIST = 101;
	private static final byte VERSION = 1;
	private static final int END = -88888;
	private Class<?> documentClass;
	private Method documentParseMethod;
	private Method documentToJsonMethod;
	public byte[] fromObjectContainer(Object obj) {
		byte[] data = null;
		if(obj.getClass().getName().equals("org.bson.Document")) {
			return null;
		} else if(obj instanceof Map) {
			Map<?, ?> map = (Map<?, ?>) obj;
			try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
				 ObjectOutputStream oos = new ObjectOutputStream(bos);
			) {
				oos.writeByte(VERSION);
				oos.writeByte(TYPE_MAP);
				oos.writeUTF(obj.getClass().getName());
				oos.writeInt(map.size());
				for(Map.Entry<?, ?> entry : map.entrySet()) {
					Object key = entry.getKey();
					Object value = entry.getValue();
					writeObjectAllCases(key, oos);
					writeObjectAllCases(value, oos);
				}
				oos.close();
				data = bos.toByteArray();
			} catch (Throwable ignored) {}
		} else if(obj instanceof List) {
			List<?> list = (List<?>) obj;
			try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
				 ObjectOutputStream oos = new ObjectOutputStream(bos);
			) {
				oos.writeByte(VERSION);
				oos.writeByte(TYPE_LIST);
				oos.writeUTF(obj.getClass().getName());
				oos.writeInt(list.size());
				for(Object objValue : list) {
					writeObjectAllCases(objValue, oos);
				}
				oos.close();
				data = bos.toByteArray();
			} catch (Throwable ignored) {}
		}
		return data;
	}

	private void writeObjectAllCases(Object obj, ObjectOutputStream oos) throws IOException {
		byte[] containerBytes = fromObjectContainer(obj);
		if(containerBytes == null) {
			writeObject(obj, oos);
		} else {
			oos.writeInt(containerBytes.length);
			oos.write(containerBytes);
		}
	}

	private void writeObject(Object obj, ObjectOutputStream oos) throws IOException {
		if(obj != null) {
			byte[] objBytes = fromObjectPrivate(obj);
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

	private Object readObject(ObjectInputStream ois, ToObjectOptions options) throws IOException {
		int length = ois.readInt();
		if(length > 0) {
			byte[] data = new byte[length];
			ois.readFully(data);
			return toObject(data, options);
		}
		return null;
	}

	@Override
	public byte[] fromObject(Object obj) {
		byte[] data = fromObjectContainer(obj);
		if(data == null) {
			data = fromObjectPrivate(obj);
		}
		return data;
	}
	public byte[] fromObjectPrivate(Object obj) {
		if (obj == null)
			return null;
		byte[] data = null;
		if(obj.getClass().getName().equals("org.bson.Document")) {
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
						 ObjectOutputStream oos = new ObjectOutputStream(bos);
					) {
						oos.writeByte(VERSION);
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
		}
		if (data == null && obj instanceof Serializable) {
			try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
//				 GZIPOutputStream gos = new GZIPOutputStream(bos);
				 ObjectOutputStream oos = new ObjectOutputStream(bos)) {
				oos.writeByte(VERSION);
				oos.writeByte(TYPE_SERIALIZABLE);
				oos.writeObject(obj);
				oos.close();
				data = bos.toByteArray();
			} catch (IOException e) {
//				e.printStackTrace();
			}
		}
		if (data == null) {
			String str = InstanceFactory.instance(JsonParser.class).toJson(obj);
			try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
//				 GZIPOutputStream gos = new GZIPOutputStream(bos);
				 ObjectOutputStream oos = new ObjectOutputStream(bos);
			) {
				oos.writeByte(VERSION);
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
		try (ByteArrayInputStream bos = new ByteArrayInputStream(data);
//			 GZIPInputStream gos = new GZIPInputStream(bos);
			 ObjectInputStream oos = new ObjectInputStreamEx(bos, options);
		) {
			//gzip performance is bad, 1000000 times, takes 2878 without gzip, with gzip 14000.
			return deserializeObject(oos, options);
		} catch (IOException e) {
			e.printStackTrace();
			//Compatible for old gzip data.
			if(e instanceof StreamCorruptedException) {
				try (ByteArrayInputStream bos = new ByteArrayInputStream(data);
					 GZIPInputStream gos = new GZIPInputStream(bos);
					 ObjectInputStream oos = new ObjectInputStreamEx(gos, options);
				) {
					return deserializeObject(oos, options);
				} catch (IOException e1) {
					e1.printStackTrace();
				}
			}

		}
		return null;
	}

	private Object deserializeObject(ObjectInputStream ois, ToObjectOptions options) throws IOException {
		byte version = ois.readByte();
		if (version == 1) {
			byte type = ois.readByte();
			switch (type) {
				case TYPE_MAP:
					String classStr = ois.readUTF();
					int size = ois.readInt();
					Class<? extends Map> mapClass = (Class<? extends Map>) findClass(options, classStr);
					Map<Object, Object> map = null;
					try {
						map = mapClass.newInstance();
					} catch (Throwable e) {
						return null;
					}
					if(size > 0) {
						for(int i = 0; i < size; i++) {
							Object key = readObject(ois, options);
							Object value = readObject(ois, options);
							if(key != null && value != null) {
								map.put(key, value);
							}
						}
					}
					return map;
				case TYPE_LIST:
					String listClassStr = ois.readUTF();
					int listSize = ois.readInt();
					Class<? extends List> listClass = (Class<? extends List>) findClass(options, listClassStr);
					List<Object> list = null;
					try {
						list = listClass.newInstance();
					} catch (Throwable e) {
						return null;
					}
					if(listSize > 0) {
						for(int i = 0; i < listSize; i++) {
							Object value = readObject(ois, options);
							if(value != null)
								list.add(value);
						}
					}
					return list;
				case TYPE_JSON:
					String className = ois.readUTF();
					String content = ois.readUTF();
					Class<?> clazz = findClass(options, className);
					return InstanceFactory.instance(JsonParser.class).fromJson(content, clazz);
				case TYPE_SERIALIZABLE:
					try {
						return ois.readObject();
					} catch (ClassNotFoundException e) {
//						e.printStackTrace();
					}
					break;
				case TYPE_MONGODB_DOCUMENT:
					String json = ois.readUTF();
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
			}
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
				} catch (Throwable throwable) {
					throwable.printStackTrace();
				}
			}
			if (theClass != null)
				return theClass;

			return super.resolveClass(desc);
		}
	}

	public static void main(String[] args) {
		TapTable tapTable = new TapTable("aa");
		ObjectSerializableImpl objectSerializable = new ObjectSerializableImpl();
		byte[] data = objectSerializable.fromObject(tapTable);
		Object theObj = objectSerializable.toObject(data);


	}
}
