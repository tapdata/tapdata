package io.tapdata.modules.api.net.entity;

import io.tapdata.entity.serializer.JavaCustomSerializer;

import java.io.*;

public class SubscribeToken implements JavaCustomSerializer {
	protected String randomId;
	protected String supplierKey;
	protected String subscribeId;
	protected String service;
	protected Long expireAt;


	@Override
	public void from(InputStream inputStream) throws IOException {
		DataInputStream dis = new DataInputStream(inputStream);
		subscribeId = dis.readUTF();
		service = dis.readUTF();
		expireAt = dis.readLong();
		supplierKey = dis.readUTF();
	}

	@Override
	public void to(OutputStream outputStream) throws IOException {
		DataOutputStream dos = new DataOutputStream(outputStream);
		dos.writeUTF(subscribeId);
		dos.writeUTF(service);
		dos.writeLong(expireAt);
		dos.writeUTF(supplierKey);
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

	public String getSupplierKey() {
		return supplierKey;
	}

	public void setSupplierKey(String supplierKey) {
		this.supplierKey = supplierKey;
	}

	public String getRandomId() {
		return randomId;
	}

	public void setRandomId(String randomId) {
		this.randomId = randomId;
	}
}
