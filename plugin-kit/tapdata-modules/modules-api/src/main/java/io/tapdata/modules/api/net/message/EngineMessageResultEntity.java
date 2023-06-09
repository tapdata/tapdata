package io.tapdata.modules.api.net.message;

import io.tapdata.entity.annotations.Implementation;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.ObjectSerializable;
import io.tapdata.entity.utils.io.BinarySerializable;
import io.tapdata.entity.utils.io.DataInputStreamEx;
import io.tapdata.entity.utils.io.DataOutputStreamEx;
import io.tapdata.modules.api.service.ArgumentsSerializer;
import io.tapdata.pdk.apis.entity.message.ServiceCaller;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.Objects;

import static io.tapdata.entity.simplify.TapSimplify.toJson;

@Implementation(value = TapEntity.class, type = "EngineMessageResultEntity")
public class EngineMessageResultEntity extends TapEntityEx {
	private static final String TAG = EngineMessageResultEntity.class.getSimpleName();
	private String message;
	public EngineMessageResultEntity message(String message) {
		this.message = message;
		return this;
	}
	private Integer code;
	public EngineMessageResultEntity code(Integer code) {
		this.code = code;
		return this;
	}
	private String id;
	public EngineMessageResultEntity id(String commandId) {
		this.id = commandId;
		return this;
	}
	private Object content;
	public EngineMessageResultEntity content(Object content) {
		this.content = content;
		return this;
	}

	private String contentClass;
	public EngineMessageResultEntity contentClass(String contentClass) {
		this.contentClass = contentClass;
		return this;
	}

	@Override
	public void from(InputStream inputStream) throws IOException {
		DataInputStreamEx dis = dataInputStream(inputStream);
		id = dis.readUTF();
		code = dis.readInt();
		message = dis.readUTF();
//		byte[] data = dis.readBytes();
//		if(data != null) {
//			ObjectSerializable objectSerializable = InstanceFactory.instance(ObjectSerializable.class);
//			content = objectSerializable.toObject(data);
//		}

//		if(contentClass != null && !contentClass.equals(ServiceCaller.RETURN_CLASS_MAP)) {
//			try {
//				content = dis.readJson(Class.forName(contentClass));
//			} catch (ClassNotFoundException e) {
//				TapLogger.debug(TAG, "contentClass {} not found while deserialize, will fallback to json object", contentClass);
//			}
//		}
//		if(content == null)
//			content = dis.readJson();

		contentClass = dis.readUTF();
		ArgumentsSerializer argumentsSerializer = InstanceFactory.instance(ArgumentsSerializer.class);
		content = Objects.requireNonNull(argumentsSerializer).returnObjectFrom(dis, contentClass, this);
	}

	@Override
	public void to(OutputStream outputStream) throws IOException {
		DataOutputStreamEx dos = dataOutputStream(outputStream);
		dos.writeUTF(id);
		dos.writeInt(code);
		dos.writeUTF(message);
//		if(content != null) {
//			ObjectSerializable objectSerializable = InstanceFactory.instance(ObjectSerializable.class);
//			byte[] data = objectSerializable.fromObject(content);
//			dos.writeBytes(data);
//		}
//		dos.writeJson(content);

		ArgumentsSerializer argumentsSerializer = InstanceFactory.instance(ArgumentsSerializer.class);
		Objects.requireNonNull(argumentsSerializer).returnObjectTo(dos, content, contentClass);
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
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

	public void setContent(Object content) {
		this.content = content;
	}

	public String getContentClass() {
		return contentClass;
	}

	public void setContentClass(String contentClass) {
		this.contentClass = contentClass;
	}

	public Object getContent() {
		return content;
	}

	public String toString() {
		return contentType() + " content " + (content != null ? toJson(content) : "null");
	}
}
