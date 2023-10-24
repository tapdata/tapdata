package io.tapdata.entity.utils.io;


import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.serializer.JavaCustomSerializer;
import io.tapdata.entity.tracker.MessageTracker;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.IteratorEx;
import io.tapdata.entity.utils.ObjectSerializable;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.Map;

import static io.tapdata.entity.simplify.TapSimplify.fromJson;

public class DataInputStreamEx extends InputStream {
	private static final byte HASVALUE = 1;
	private static final byte NOVALUE = 0;
	private static final String TAG = DataInputStreamEx.class.getSimpleName();
	private DataInputStream dis;
	private InputStream inputStream;

	public DataInputStreamEx(InputStream arg0) {
		dis = new DataInputStream(arg0);
		inputStream = arg0;
	}

	public DataInputStream original() {
		return dis;
	}

	private boolean hasValue() throws IOException {
		byte hasValue = dis.readByte();
        return hasValue == HASVALUE;
    }

	@Override
	public int read() throws IOException {
		return dis.read();
	}

	public void close() throws IOException {
		dis.close();
	}

	public Boolean readBoolean() throws IOException {
		if(hasValue()) {
			return dis.readBoolean();
		}
		return null;
	}

	public Byte readByte() throws IOException {
		if(hasValue()) {
			return dis.readByte();
		}
		return null;
	}

	public Short readShort() throws IOException {
		if(hasValue()) {
			return dis.readShort();
		}
		return null;
	}

	public Character readChar() throws IOException {
		if(hasValue()) {
			return dis.readChar();
		}
		return null;
	}

	public Object readJson() throws IOException {
		return readJson(null);
	}
	public <T> T readJson(Class<T> clazz) throws IOException {
		return readJson(clazz, null);
	}

	public <T> T readJson(Class<T> clazz, Charset charset) throws IOException {
		return readJson(clazz, charset, null);
	}
	public <T> T readJson(Class<T> clazz, Charset charset, MessageTracker tracker) throws IOException {
		if(hasValue()) {
			String json = readLongString(charset, tracker);
			if(clazz != null)
				return fromJson(json, clazz);
			else
				return (T) fromJson(json);
		}
		return null;
	}

	public Integer readInt() throws IOException {
		if(hasValue()) {
			return dis.readInt();
		}
		return null;
	}

	public Date readDate() throws IOException {
		if(hasValue()) {
			long time = dis.readLong();
			return new Date(time);
		}
		return null;
	}

	public Long readLong() throws IOException {
		if(hasValue()) {
			return dis.readLong();
		}
		return null;
	}

	public Float readFloat() throws IOException {
		if(hasValue()) {
			return dis.readFloat();
		}
		return null;
	}

	public Double readDouble() throws IOException {
		if(hasValue()) {
			return dis.readDouble();
		}
		return null;
	}

	public String readUTF() throws IOException {
		if(hasValue()) {
			return dis.readUTF().intern();
		}
		return null;
	}
	public void readFully(byte[] buf) throws IOException {
		if(hasValue()) {
			dis.readFully(buf);
		}
	}

	public byte[] readBytes() throws IOException {
		int length = dis.readInt();
		if(length > 0) {
			byte[] data = new byte[length];
			dis.readFully(data);
			return data;
		}
		return null;
	}

	public Date readDate(String format) throws IOException {
		if(hasValue()) {
			String str = dis.readUTF();
			DateFormat formatDate = new SimpleDateFormat(format);
			try {
				return formatDate.parse(str);
			} catch (ParseException e) {
				e.printStackTrace();
				return null;
			}
		}
		return null;
	}

	public String readLongString() throws IOException {
		return readLongString(null);
	}
	public String readLongString(MessageTracker tracker) throws IOException {
		return readLongString(null, tracker);
	}

	public String readLongString(Charset charset, MessageTracker tracker) throws IOException {
		int size = 0;
		try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
			size = dis.readInt();
			if(tracker != null) {
				tracker.responseBytes(Math.max(size, 0));
			}
			if(size <= 0)
				return null;
			int readed, total = 0;
			byte[] data = new byte[size];
			while (total < size) {
				readed = dis.read(data, 0, size - total);
				if (readed > 0) {
					baos.write(data, 0, readed);
					total += readed;
				} else if(readed < 0) {
					throw new IOException("readLongString found EOF, total " + total + ", read " + readed);
				}
			}
			if(charset == null) {
				return baos.toString();
			}
			return baos.toString(charset.name());
		}
	}

	public String[] readUTFArray() throws IOException {
		int length = dis.readInt();
		if (length != 0) {
			String[] strs = new String[length];
			for (int i = 0; i < length; i++) {
				strs[i] = dis.readUTF().intern();
			}
			return strs;
		}
		return null;
	}
	public Integer[] readIntegerArray() throws IOException {
		int length = dis.readInt();
		if (length != 0) {
			Integer[] integers = new Integer[length];
			for (int i = 0; i < length; i++) {
				integers[i] = dis.readInt();
			}
			return integers;
		}
		return null;
	}

	public void readCollectionString(Collection<String> collectionStrings) throws IOException {
		int length = dis.readInt();
		for (int i = 0;i < length;i++) {
			String str = dis.readUTF().intern();
			collectionStrings.add(str);
		}
	}

	public <T extends BinarySerializable> T[] readBinaryObjectArray(Class<T> clazz) throws IOException {
		int length = dis.readInt();
		if(length != 0) {
			T[] ts = (T[]) Array.newInstance(clazz, length);
			for(int i = 0; i < length;i++) {
				byte state = dis.readByte();
				switch (state) {
					case HASVALUE:
						try {
							ts[i] = clazz.getConstructor().newInstance();
							ts[i].resurrect(dis);
						} catch (InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
							e.printStackTrace();
							TapLogger.error("readBinaryObjectArray", "new class " + clazz + " failed, " + e.getMessage());
						} catch (Throwable t) {
							t.printStackTrace();
							TapLogger.error("readBinaryObjectArray", "resurrect for class " + clazz + " failed, " + t.getMessage());
						}
						break;
					case NOVALUE:
						break;
				}
			}
			return ts;
		}
		return null;
	}

	public <T extends BinarySerializable> void readBinaryObjects(IteratorEx<T> iterator, Class<T> clazz) throws IOException {
		int length = dis.readInt();
		if(length != 0 && iterator != null) {
			for(int i = 0; i < length;i++) {
				byte state = dis.readByte();
				switch (state) {
					case HASVALUE:
						try {
							T t = clazz.getConstructor().newInstance();
							t.resurrect(dis);
							if(!iterator.iterate(t))
								break;
						} catch (InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
							e.printStackTrace();
							TapLogger.error("readBinaryObjects", "new class " + clazz + " failed, " + e.getMessage());
						} catch (Throwable t) {
							t.printStackTrace();
							TapLogger.error("readBinaryObjects", "resurrect for class " + clazz + " failed, " + t.getMessage());
						}
						break;
					case NOVALUE:
						break;
				}
			}
		}
	}
	public <T extends JavaCustomSerializer> void readCollectionCustomObject(Collection<T> collectionAcuObjects, Class<T> clazz) throws IOException {
		int length = dis.readInt();
		if(length != 0) {
			for(int i = 0; i < length;i++) {
				byte state = dis.readByte();
				switch (state) {
					case HASVALUE:
						try {
							T t = clazz.getConstructor().newInstance();
							t.from(dis);
							collectionAcuObjects.add(t);
						} catch (InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
							e.printStackTrace();
							TapLogger.error("readCollectionBinaryObject", "new class " + clazz + " failed, " + e.getMessage());
						} catch (Throwable t) {
							t.printStackTrace();
							TapLogger.error("readCollectionBinaryObject", "resurrect for class " + clazz + " failed, " + t.getMessage());
						}
						break;
					case NOVALUE:
						break;
				}
			}
		}
	}

	public <T extends BinarySerializable> void readMapBinaryObject(Map<String, T> acuObjectMap, Class<T> clazz) throws IOException {
		int length = dis.readInt();
		if(length != 0) {
			for(int i = 0; i < length;i++) {
				byte state = dis.readByte();
				switch (state) {
					case HASVALUE:
						String key = dis.readUTF();
						try {
							T t = clazz.getConstructor().newInstance();
							t.resurrect(dis);
							acuObjectMap.put(key, t);
						} catch (InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
							e.printStackTrace();
							TapLogger.error("readMapBinaryObject", "new class " + clazz + " failed, " + e.getMessage());
						} catch (Throwable t) {
							t.printStackTrace();
							TapLogger.error("readMapBinaryObject", "resurrect for class " + clazz + " failed, " + t.getMessage());
						}
						break;
					case NOVALUE:
						break;
				}
			}
		}
	}
	public <T extends JavaCustomSerializer> T readJavaCustomSerializer(Class<T> clazz) throws IOException {
		if(hasValue()) {
			try {
				T object = clazz.getConstructor().newInstance();
				object.from(dis);
				return object;
			} catch (InstantiationException | InvocationTargetException | NoSuchMethodException | IllegalAccessException e) {
				e.printStackTrace();
				TapLogger.error("readJavaCustomSerializer", "new class " + clazz + " failed, " + e.getMessage());
			} catch (Throwable t) {
				t.printStackTrace();
				TapLogger.error("readJavaCustomSerializer", "resurrect for class " + clazz + " failed, " + t.getMessage());
			}
		}
		return null;
	}
	public <T extends BinarySerializable> T readBinaryObject(Class<T> clazz) throws IOException {
		if(hasValue()) {
			try {
				T object = clazz.getConstructor().newInstance();
				object.resurrect(dis);
				return object;
			} catch (InstantiationException | InvocationTargetException | NoSuchMethodException | IllegalAccessException e) {
				e.printStackTrace();
				TapLogger.error("readBinaryObject", "new class " + clazz + " failed, " + e.getMessage());
			} catch (Throwable t) {
				t.printStackTrace();
				TapLogger.error("readBinaryObject", "resurrect for class " + clazz + " failed, " + t.getMessage());
			}
		}
		return null;
	}

    public InputStream getInputStream() {
        return inputStream;
    }

    public DataInputStream getDataInputStream() {
    	return dis;
    }

	public Object readObject() throws IOException {
		byte[] data = readBytes();
		return InstanceFactory.instance(ObjectSerializable.class).toObject(data);
	}
}
