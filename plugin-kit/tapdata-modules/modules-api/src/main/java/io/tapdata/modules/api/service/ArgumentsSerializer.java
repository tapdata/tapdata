package io.tapdata.modules.api.service;

import io.tapdata.entity.utils.io.DataInputStreamEx;
import io.tapdata.entity.utils.io.DataOutputStreamEx;
import io.tapdata.entity.tracker.MessageTracker;
import io.tapdata.pdk.apis.entity.message.ServiceCaller;

import java.io.IOException;

public interface ArgumentsSerializer {
	byte ARGUMENT_TYPE_BYTES = 1;
	byte ARGUMENT_TYPE_JSON = 2;
	byte ARGUMENT_TYPE_JAVA_BINARY = 3;

	byte ARGUMENT_TYPE_JAVA_CUSTOM = 4;
	byte ARGUMENT_TYPE_NONE = 10;
	Object[] argumentsFrom(DataInputStreamEx dis, ServiceCaller serviceCaller) throws IOException;

	void argumentsTo(DataOutputStreamEx dos, ServiceCaller serviceCaller) throws IOException;

	void returnObjectTo(DataOutputStreamEx dos, Object content, String contentClass) throws IOException;

	Object returnObjectFrom(DataInputStreamEx dis, String contentClass) throws IOException;
	Object returnObjectFrom(DataInputStreamEx dis, String contentClass, MessageTracker messageTracker) throws IOException;
}
