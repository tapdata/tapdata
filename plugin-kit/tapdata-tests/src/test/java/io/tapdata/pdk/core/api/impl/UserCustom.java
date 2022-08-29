package io.tapdata.pdk.core.api.impl;

import io.tapdata.entity.serializer.JavaCustomSerializer;
import io.tapdata.entity.utils.io.DataInputStreamEx;
import io.tapdata.entity.utils.io.DataOutputStreamEx;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class UserCustom implements JavaCustomSerializer {
	protected String name;
	protected Long createTime;
	protected Long updateTime;
	protected String description;
	protected Integer gender;
	protected String email;
	protected UserCustom user;

	@Override
	public void from(InputStream inputStream) throws IOException {
		DataInputStreamEx dis = dataInputStream(inputStream);
		name = dis.readUTF();
		createTime = dis.readLong();
		updateTime = dis.readLong();
		description = dis.readUTF();
		gender = dis.readInt();
		email = dis.readUTF();
		user = dis.readJavaCustomSerializer(UserCustom.class);
	}

	@Override
	public void to(OutputStream outputStream) throws IOException {
		DataOutputStreamEx dos = dataOutputStream(outputStream);
		dos.writeUTF(name);
		dos.writeLong(createTime);
		dos.writeLong(updateTime);
		dos.writeUTF(description);
		dos.writeInt(gender);
		dos.writeUTF(email);
		dos.writeJavaCustomSerializer(user);
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Long getCreateTime() {
		return createTime;
	}

	public void setCreateTime(Long createTime) {
		this.createTime = createTime;
	}

	public Long getUpdateTime() {
		return updateTime;
	}

	public void setUpdateTime(Long updateTime) {
		this.updateTime = updateTime;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public Integer getGender() {
		return gender;
	}

	public void setGender(Integer gender) {
		this.gender = gender;
	}

	public String getEmail() {
		return email;
	}

	public void setEmail(String email) {
		this.email = email;
	}

	public UserCustom getUser() {
		return user;
	}

	public void setUser(UserCustom user) {
		this.user = user;
	}
}
