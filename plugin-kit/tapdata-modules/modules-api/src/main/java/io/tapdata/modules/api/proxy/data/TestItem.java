package io.tapdata.modules.api.proxy.data;

import io.tapdata.entity.annotations.Implementation;
import io.tapdata.entity.utils.io.DataInputStreamEx;
import io.tapdata.entity.utils.io.DataOutputStreamEx;
import io.tapdata.modules.api.net.message.TapEntity;
import io.tapdata.modules.api.net.message.TapEntityEx;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashSet;
import java.util.Set;

@Implementation(value = TapEntity.class, type = "TestItem")
public class TestItem extends TapEntityEx {
	private String action;
	public TestItem action(String action) {
		this.action = action;
		return this;
	}
	@Override
	public void from(InputStream inputStream) throws IOException {
		DataInputStreamEx dis = dataInputStream(inputStream);
		action = dis.readUTF();
	}

	@Override
	public void to(OutputStream outputStream) throws IOException {
		DataOutputStreamEx dos = dataOutputStream(outputStream);
		dos.writeUTF(action);
	}

	public String getAction() {
		return action;
	}

	public void setAction(String action) {
		this.action = action;
	}

	@Override
	public String toString() {
		return contentType() + " action " + action;
	}
}
