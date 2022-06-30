package io.tapdata.entity.utils.io;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public interface BinarySerializable {

	void persistent(OutputStream os) throws IOException;

	void resurrect(InputStream is) throws IOException;

}
