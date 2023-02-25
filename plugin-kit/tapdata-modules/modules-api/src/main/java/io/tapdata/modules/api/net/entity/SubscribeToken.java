package io.tapdata.modules.api.net.entity;

import io.tapdata.entity.serializer.JavaCustomSerializer;

import java.io.*;

public class SubscribeToken implements JavaCustomSerializer {
	private String subscribeId;
	private String service;
	private Long expireAt;


	@Override
	public void from(InputStream inputStream) throws IOException {
		DataInputStream dis = new DataInputStream(inputStream);
		subscribeId = dis.readUTF();
		service = dis.readUTF();
		expireAt = dis.readLong();
	}

	@Override
	public void to(OutputStream outputStream) throws IOException {
		DataOutputStream dos = new DataOutputStream(outputStream);
		dos.writeUTF(subscribeId);
		dos.writeUTF(service);
		dos.writeLong(expireAt);
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

	public Long getExpireAt() {
		return expireAt;
	}

	public void setExpireAt(Long expireAt) {
		this.expireAt = expireAt;
	}
}
