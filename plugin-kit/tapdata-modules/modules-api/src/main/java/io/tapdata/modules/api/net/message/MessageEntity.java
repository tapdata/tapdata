package io.tapdata.modules.api.net.message;

import io.tapdata.entity.annotations.Implementation;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.ObjectSerializable;
import io.tapdata.entity.utils.io.DataInputStreamEx;
import io.tapdata.entity.utils.io.DataOutputStreamEx;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

@Implementation(value = TapEntity.class, type = "MessageEntity")
public class MessageEntity implements TapEntity {
	private Object content;
	public MessageEntity content(Object content) {
		this.content = content;
		return this;
	}
	private Long time;
	public MessageEntity time(Long time) {
		this.time = time;
		return this;
	}
	private String subscribeId;
	public MessageEntity subscribeId(String subscribeId) {
		this.subscribeId = subscribeId;
		return this;
	}
	private String service;
	public MessageEntity service(String service) {
		this.service = service;
		return this;
	}

	@Override
	public void from(InputStream inputStream) throws IOException {
		DataInputStreamEx dis = dataInputStream(inputStream);
		byte[] data = dis.readBytes();
		if(data != null) {
			ObjectSerializable objectSerializable = InstanceFactory.instance(ObjectSerializable.class);
			content = objectSerializable.toObject(data);
			time = dis.readLong();
			subscribeId = dis.readUTF();
		}
	}

	@Override
	public void to(OutputStream outputStream) throws IOException {
		DataOutputStreamEx dos = dataOutputStream(outputStream);
		dos.writeBytes(InstanceFactory.instance(ObjectSerializable.class).fromObject(content));
		dos.writeLong(time);
		dos.writeUTF(subscribeId);
	}

	public Object getContent() {
		return content;
	}

	public void setContent(Object content) {
		this.content = content;
	}

	public Long getTime() {
		return time;
	}

	public void setTime(Long time) {
		this.time = time;
	}

	public String getSubscribeId() {
		return subscribeId;
	}

	public void setSubscribeId(String subscribeId) {
		this.subscribeId = subscribeId;
	}

	public String getService() {
		return service;
	}

	public void setService(String service) {
		this.service = service;
	}
}
