package io.tapdata.modules.api.net.message;

import io.tapdata.entity.annotations.Implementation;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.ObjectSerializable;
import io.tapdata.entity.utils.io.DataInputStreamEx;
import io.tapdata.entity.utils.io.DataOutputStreamEx;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Date;
import java.util.Map;

import static io.tapdata.entity.simplify.TapSimplify.toJson;

@Implementation(value = TapEntity.class, type = "MessageEntity")
public class MessageEntity extends TapEntityEx {
	private Map<String, Object> content;
	public MessageEntity content(Map<String, Object> content) {
		this.content = content;
		return this;
	}
	private Date time;
	public MessageEntity time(Date time) {
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
		content = (Map<String, Object>) dis.readJson();
		time = dis.readDate();
		subscribeId = dis.readUTF();
	}

	@Override
	public void to(OutputStream outputStream) throws IOException {
		DataOutputStreamEx dos = dataOutputStream(outputStream);
		dos.writeJson(content);
		dos.writeDate(time);
		dos.writeUTF(subscribeId);
	}

	public Map<String, Object> getContent() {
		return content;
	}

	public void setContent(Map<String, Object> content) {
		this.content = content;
	}

	public Date getTime() {
		return time;
	}

	public void setTime(Date time) {
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

	public String toString() {
		return contentType() + " time " + time + " content " + (content != null ? toJson(content) : "null");
	}
}
