package io.tapdata.modules.api.service;

import io.tapdata.entity.utils.io.DataInputStreamEx;
import io.tapdata.entity.utils.io.DataOutputStreamEx;
import io.tapdata.pdk.apis.entity.message.ServiceCaller;

import java.io.IOException;

public interface ArgumentsSerializer {
	Object[] from(DataInputStreamEx dis, ServiceCaller serviceCaller) throws IOException;

	void to(DataOutputStreamEx dos, ServiceCaller serviceCaller) throws IOException;
}
