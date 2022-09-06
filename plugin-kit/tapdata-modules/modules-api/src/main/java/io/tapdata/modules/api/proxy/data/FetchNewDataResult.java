package io.tapdata.modules.api.proxy.data;

import io.tapdata.entity.annotations.Implementation;
import io.tapdata.entity.utils.io.DataInputStreamEx;
import io.tapdata.entity.utils.io.DataOutputStreamEx;
import io.tapdata.modules.api.net.message.MessageEntity;
import io.tapdata.modules.api.net.message.TapEntity;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Implementation(value = TapEntity.class, type = "FetchNewDataResult")
public class FetchNewDataResult implements TapEntity {
	private List<MessageEntity> messages;
	public FetchNewDataResult messages(List<MessageEntity> messages) {
		this.messages = messages;
		return this;
	}
	private Object offset;
	public FetchNewDataResult offset(Object offset) {
		this.offset = offset;
		return this;
	}

	@Override
	public void from(InputStream inputStream) throws IOException {
		DataInputStreamEx dis = dataInputStream(inputStream);
		messages = new ArrayList<>();
		dis.readCollectionCustomObject(messages, MessageEntity.class);
		offset = dis.readObject();
	}

	@Override
	public void to(OutputStream outputStream) throws IOException {
		DataOutputStreamEx dos = dataOutputStream(outputStream);
		dos.writeCollectionBinaryObject(messages);
		dos.writeObject(offset);
	}

	public List<MessageEntity> getMessages() {
		return messages;
	}

	public void setMessages(List<MessageEntity> messages) {
		this.messages = messages;
	}

	public Object getOffset() {
		return offset;
	}

	public void setOffset(Object offset) {
		this.offset = offset;
	}
}
