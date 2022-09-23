package io.tapdata.modules.api.net.service.node.connection.entity;

import io.tapdata.entity.serializer.JavaCustomSerializer;
import io.tapdata.entity.utils.io.DataInputStreamEx;
import io.tapdata.entity.utils.io.DataOutputStreamEx;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Type;
import java.util.function.BiConsumer;

public class NodeMessage implements JavaCustomSerializer {
	private String id;
	public NodeMessage id(String id) {
		this.id = id;
		return this;
	}
	private Long time;
	public NodeMessage time(Long time) {
		this.time = time;
		return this;
	}
	private String fromNodeId;
	public NodeMessage fromNodeId(String fromNodeId) {
		this.fromNodeId = fromNodeId;
		return this;
	}
	private String toNodeId;
	public NodeMessage toNodeId(String toNodeId) {
		this.toNodeId = toNodeId;
		return this;
	}
	private String type;
	public NodeMessage type(String type) {
		this.type = type;
		return this;
	}
	protected Byte encode;
	public NodeMessage encode(Byte encode) {
		this.encode = encode;
		return this;
	}
	private byte[] data;
	public NodeMessage data(byte[] data) {
		this.data = data;
		return this;
	}

	private volatile BiConsumer<Object, Throwable> biConsumer;
	public NodeMessage biConsumer(BiConsumer<Object, Throwable> biConsumer) {
		this.biConsumer = biConsumer;
		return this;
	}

	private Type responseClass;
	public NodeMessage responseClass(Type responseClass) {
		this.responseClass = responseClass;
		return this;
	}

	@Override
	public void from(InputStream inputStream) throws IOException {
		DataInputStreamEx dis = dataInputStream(inputStream);
		id = dis.readUTF();
		fromNodeId = dis.readUTF();
		toNodeId = dis.readUTF();
		time = dis.readLong();
		type = dis.readUTF();
		encode = dis.readByte();
		data = dis.readBytes();
	}

	@Override
	public void to(OutputStream outputStream) throws IOException {
		DataOutputStreamEx dos = dataOutputStream(outputStream);
		dos.writeUTF(id);
		dos.writeUTF(fromNodeId);
		dos.writeUTF(toNodeId);
		dos.writeLong(time);
		dos.writeUTF(type);
		dos.writeByte(encode);
		dos.writeBytes(data);
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public Byte getEncode() {
		return encode;
	}

	public void setEncode(Byte encode) {
		this.encode = encode;
	}

	public byte[] getData() {
		return data;
	}

	public void setData(byte[] data) {
		this.data = data;
	}

	public String getFromNodeId() {
		return fromNodeId;
	}

	public void setFromNodeId(String fromNodeId) {
		this.fromNodeId = fromNodeId;
	}

	public String getToNodeId() {
		return toNodeId;
	}

	public void setToNodeId(String toNodeId) {
		this.toNodeId = toNodeId;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public Long getTime() {
		return time;
	}

	public void setTime(Long time) {
		this.time = time;
	}

	public void accept(Object response, Throwable throwable) {
		if(biConsumer != null) {
			synchronized (this) {
				if(biConsumer != null) {
					try {
						biConsumer.accept(response, throwable);
					} finally {
						biConsumer = null;
					}
				}
			}
		}
	}
	public BiConsumer<Object, Throwable> getBiConsumer() {
		return biConsumer;
	}

	public void setBiConsumer(BiConsumer<Object, Throwable> biConsumer) {
		this.biConsumer = biConsumer;
	}

	public Type getResponseClass() {
		return responseClass;
	}

	public void setResponseClass(Type responseClass) {
		this.responseClass = responseClass;
	}
}
