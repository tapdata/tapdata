package io.tapdata.modules.api.net.message;

import io.tapdata.entity.annotations.Implementation;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

@Implementation(value = TapEntity.class, type = "insertRecord")
public class InsertRecordEntity implements TapEntity {

	@Override
	public void from(InputStream inputStream) throws IOException {

	}

	@Override
	public void to(OutputStream outputStream) throws IOException {

	}

	@Override
	public String contentType() {
		return "insertRecord";
	}
}
