package io.tapdata.modules.api.proxy.data;

import io.tapdata.entity.annotations.Implementation;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.ObjectSerializable;
import io.tapdata.entity.utils.io.DataInputStreamEx;
import io.tapdata.entity.utils.io.DataOutputStreamEx;
import io.tapdata.modules.api.net.message.TapEntity;
import io.tapdata.modules.api.net.message.TapEntityEx;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

@Implementation(value = TapEntity.class, type = "FetchNewData")
public class FetchNewData extends TapEntityEx {
	private Long taskStartTime;
	public FetchNewData taskStartTime(Long taskStartTime) {
		this.taskStartTime = taskStartTime;
		return this;
	}
	private String offset;
	public FetchNewData offset(String offset) {
		this.offset = offset;
		return this;
	}
	private Integer limit;
	public FetchNewData limit(Integer limit) {
		this.limit = limit;
		return this;
	}
	private String service;
	public FetchNewData service(String service) {
		this.service = service;
		return this;
	}

	private String subscribeId;
	public FetchNewData subscribeId(String subscribeId) {
		this.subscribeId = subscribeId;
		return this;
	}
	@Override
	public void from(InputStream inputStream) throws IOException {
		DataInputStreamEx dis = dataInputStream(inputStream);
		offset = dis.readUTF();
		limit = dis.readInt();
		service = dis.readUTF();
		subscribeId = dis.readUTF();
		taskStartTime = dis.readLong();
	}

	@Override
	public void to(OutputStream outputStream) throws IOException {
		DataOutputStreamEx dos = dataOutputStream(outputStream);
		dos.writeUTF(offset);
		dos.writeInt(limit);
		dos.writeUTF(service);
		dos.writeUTF(subscribeId);
		dos.writeLong(taskStartTime);
	}

	public String getOffset() {
		return offset;
	}

	public void setOffset(String offset) {
		this.offset = offset;
	}

	public Integer getLimit() {
		return limit;
	}

	public void setLimit(Integer limit) {
		this.limit = limit;
	}

	public String getService() {
		return service;
	}

	public void setService(String service) {
		this.service = service;
	}

	public String getSubscribeId() {
		return subscribeId;
	}

	public void setSubscribeId(String subscribeId) {
		this.subscribeId = subscribeId;
	}

	public Long getTaskStartTime() {
		return taskStartTime;
	}

	public void setTaskStartTime(Long taskStartTime) {
		this.taskStartTime = taskStartTime;
	}

	public String toString() {
		return contentType() + " taskStartTime " + taskStartTime + " offset " + offset + " limit " + limit + " service " + service + " subscribeId " + subscribeId;
	}
}
