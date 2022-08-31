package io.tapdata.modules.api.net.message;

import io.tapdata.entity.annotations.Implementation;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

@Implementation(value = TapMessage.class, type = "insertRecord")
public class InsertRecordMessage implements TapMessage {

	@Override
	public void from(InputStream inputStream) throws IOException {

	}

	@Override
	public void to(OutputStream outputStream) throws IOException {

	}

	@Override
	public String getContentType() {
		return "insertRecord";
	}
}
