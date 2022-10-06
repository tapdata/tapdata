package io.tapdata.modules.api.net.message;

import io.tapdata.entity.annotations.Implementation;
import io.tapdata.entity.utils.io.DataInputStreamEx;
import io.tapdata.entity.utils.io.DataOutputStreamEx;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Date;
import java.util.Map;

import static io.tapdata.entity.simplify.TapSimplify.toJson;

@Implementation(value = TapEntity.class, type = "CommandResultEntity")
public class CommandResultEntity implements TapEntity {
	private String message;
	public CommandResultEntity message(String message) {
		this.message = message;
		return this;
	}
	private Integer code;
	public CommandResultEntity code(Integer code) {
		this.code = code;
		return this;
	}
	private String commandId;
	public CommandResultEntity commandId(String commandId) {
		this.commandId = commandId;
		return this;
	}
	private Map<String, Object> content;
	public CommandResultEntity content(Map<String, Object> content) {
		this.content = content;
		return this;
	}

	@Override
	public void from(InputStream inputStream) throws IOException {
		DataInputStreamEx dis = dataInputStream(inputStream);
		content = (Map<String, Object>) dis.readJson();
		commandId = dis.readUTF();
		code = dis.readInt();
		message = dis.readUTF();
	}

	@Override
	public void to(OutputStream outputStream) throws IOException {
		DataOutputStreamEx dos = dataOutputStream(outputStream);
		dos.writeJson(content);
		dos.writeUTF(commandId);
		dos.writeInt(code);
		dos.writeUTF(message);
	}

	public Map<String, Object> getContent() {
		return content;
	}

	public void setContent(Map<String, Object> content) {
		this.content = content;
	}

	public String getCommandId() {
		return commandId;
	}

	public void setCommandId(String commandId) {
		this.commandId = commandId;
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	public Integer getCode() {
		return code;
	}

	public void setCode(Integer code) {
		this.code = code;
	}

	public String toString() {
		return contentType() + " content " + (content != null ? toJson(content) : "null");
	}
}
