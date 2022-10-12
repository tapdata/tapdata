package io.tapdata.modules.api.proxy.data;

import io.tapdata.entity.annotations.Implementation;
import io.tapdata.entity.utils.io.DataInputStreamEx;
import io.tapdata.entity.utils.io.DataOutputStreamEx;
import io.tapdata.modules.api.net.message.TapEntity;
import io.tapdata.modules.api.net.message.TapEntityEx;
import io.tapdata.pdk.apis.entity.message.CommandInfo;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

@Implementation(value = TapEntity.class, type = "CommandReceived")
public class CommandReceived extends TapEntityEx {
	private CommandInfo commandInfo;
	public CommandReceived commandInfo(CommandInfo commandInfo) {
		this.commandInfo = commandInfo;
		return this;
	}
	@Override
	public void from(InputStream inputStream) throws IOException {
		DataInputStreamEx dis = dataInputStream(inputStream);
		commandInfo = dis.readJson(CommandInfo.class);
	}

	@Override
	public void to(OutputStream outputStream) throws IOException {
		DataOutputStreamEx dos = dataOutputStream(outputStream);
		dos.writeJson(commandInfo);
	}

	public CommandInfo getCommandInfo() {
		return commandInfo;
	}

	public void setCommandInfo(CommandInfo commandInfo) {
		this.commandInfo = commandInfo;
	}

	@Override
	public String toString() {
		return contentType() + " commandInfo " + commandInfo;
	}
}
