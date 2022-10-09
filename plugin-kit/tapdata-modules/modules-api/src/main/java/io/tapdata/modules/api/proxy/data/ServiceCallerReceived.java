package io.tapdata.modules.api.proxy.data;

import io.tapdata.entity.annotations.Implementation;
import io.tapdata.entity.utils.io.DataInputStreamEx;
import io.tapdata.entity.utils.io.DataOutputStreamEx;
import io.tapdata.modules.api.net.message.TapEntity;
import io.tapdata.pdk.apis.entity.message.ServiceCaller;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

@Implementation(value = TapEntity.class, type = "ServiceCaller")
public class ServiceCallerReceived implements TapEntity {
	private ServiceCaller serviceCaller;
	public ServiceCallerReceived serviceCaller(ServiceCaller serviceCaller) {
		this.serviceCaller = serviceCaller;
		return this;
	}
	@Override
	public void from(InputStream inputStream) throws IOException {
		DataInputStreamEx dis = dataInputStream(inputStream);
		serviceCaller = dis.readJson(ServiceCaller.class);
	}

	@Override
	public void to(OutputStream outputStream) throws IOException {
		DataOutputStreamEx dos = dataOutputStream(outputStream);
		dos.writeJson(serviceCaller);
	}

	@Override
	public String toString() {
		return contentType() + " serviceCaller " + serviceCaller;
	}
}
