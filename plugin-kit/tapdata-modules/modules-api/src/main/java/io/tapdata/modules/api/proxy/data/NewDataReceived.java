package io.tapdata.modules.api.proxy.data;

import io.tapdata.entity.annotations.Implementation;
import io.tapdata.entity.utils.io.DataInputStreamEx;
import io.tapdata.entity.utils.io.DataOutputStreamEx;
import io.tapdata.modules.api.net.message.TapEntity;
import io.tapdata.modules.api.net.message.TapEntityEx;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

@Implementation(value = TapEntity.class, type = "NewDataReceived")
public class NewDataReceived extends TapEntityEx {
	private List<String> subscribeIds;
	public NewDataReceived subscribeIds(List<String> subscribeIds) {
		this.subscribeIds = subscribeIds;
		return this;
	}
	@Override
	public void from(InputStream inputStream) throws IOException {
		DataInputStreamEx dis = dataInputStream(inputStream);
		subscribeIds = new ArrayList<>();
		dis.readCollectionString(subscribeIds);
	}

	@Override
	public void to(OutputStream outputStream) throws IOException {
		DataOutputStreamEx dos = dataOutputStream(outputStream);
		dos.writeCollectionString(subscribeIds);
	}

	public List<String> getSubscribeIds() {
		return subscribeIds;
	}

	public void setSubscribeIds(List<String> subscribeIds) {
		this.subscribeIds = subscribeIds;
	}

	@Override
	public String toString() {
		return contentType() + " subscribeIds " + subscribeIds;
	}
}
