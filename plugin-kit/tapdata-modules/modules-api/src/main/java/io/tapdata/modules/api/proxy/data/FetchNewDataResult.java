package io.tapdata.modules.api.proxy.data;

import io.tapdata.entity.annotations.Implementation;
import io.tapdata.entity.utils.io.DataInputStreamEx;
import io.tapdata.entity.utils.io.DataOutputStreamEx;
import io.tapdata.modules.api.net.message.MessageEntity;
import io.tapdata.modules.api.net.message.TapEntity;
import io.tapdata.modules.api.net.message.TapEntityEx;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Implementation(value = TapEntity.class, type = "FetchNewDataResult")
public class FetchNewDataResult extends TapEntityEx {
	private List<MessageEntity> messages;
	public FetchNewDataResult messages(List<MessageEntity> messages) {
		this.messages = messages;
		return this;
	}
	private String offset;
	public FetchNewDataResult offset(String offset) {
		this.offset = offset;
		return this;
	}

	@Override
	public void from(InputStream inputStream) throws IOException {
		DataInputStreamEx dis = dataInputStream(inputStream);
		messages = new ArrayList<>();
		dis.readCollectionCustomObject(messages, MessageEntity.class);
		offset = dis.readUTF();
	}

	@Override
	public void to(OutputStream outputStream) throws IOException {
		DataOutputStreamEx dos = dataOutputStream(outputStream);
		dos.writeCollectionBinaryObject(messages);
		dos.writeUTF(offset);
	}

	public List<MessageEntity> getMessages() {
		return messages;
	}

	public void setMessages(List<MessageEntity> messages) {
		this.messages = messages;
	}

	public String getOffset() {
		return offset;
	}

	public void setOffset(String offset) {
		this.offset = offset;
	}
	@Override
	public String toString() {
		return contentType() + " offset " + offset + " messages length " + (messages != null ? messages.size() : 0) + " messages " + messages;
	}
}
