package io.tapdata.entity.serializer;

import io.tapdata.entity.utils.io.DataInputStreamEx;
import io.tapdata.entity.utils.io.DataOutputStreamEx;

import java.io.*;

public interface JavaCustomSerializer {
	void from(InputStream inputStream) throws IOException;
	void to(OutputStream outputStream) throws IOException;

	default DataInputStreamEx dataInputStream(InputStream inputStream) {
		if(inputStream instanceof DataInputStreamEx) {
			return (DataInputStreamEx) inputStream;
		} else {
			return new DataInputStreamEx(inputStream);
		}
	}

	default DataOutputStreamEx dataOutputStream(OutputStream outputStream) {
		if(outputStream instanceof DataOutputStreamEx) {
			return (DataOutputStreamEx) outputStream;
		} else {
			return new DataOutputStreamEx(outputStream);
		}
	}
}
