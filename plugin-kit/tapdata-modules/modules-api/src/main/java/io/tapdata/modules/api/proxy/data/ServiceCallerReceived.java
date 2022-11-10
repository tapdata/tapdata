package io.tapdata.modules.api.proxy.data;

import io.tapdata.entity.annotations.Implementation;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.io.DataInputStreamEx;
import io.tapdata.entity.utils.io.DataOutputStreamEx;
import io.tapdata.modules.api.net.message.TapEntity;
import io.tapdata.modules.api.net.message.TapEntityEx;
import io.tapdata.modules.api.service.ArgumentsSerializer;
import io.tapdata.pdk.apis.entity.message.ServiceCaller;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Objects;

import static io.tapdata.entity.simplify.TapSimplify.toJson;

@Implementation(value = TapEntity.class, type = "ServiceCallerReceived")
public class ServiceCallerReceived extends TapEntityEx {
	private ServiceCaller serviceCaller;
	public ServiceCallerReceived serviceCaller(ServiceCaller serviceCaller) {
		this.serviceCaller = serviceCaller;
		return this;
	}
	@Override
	public void from(InputStream inputStream) throws IOException {
		DataInputStreamEx dis = dataInputStream(inputStream);
		String id = dis.readUTF();
		String className = dis.readUTF();
		String method = dis.readUTF();
		String returnClass = dis.readUTF();

		serviceCaller = ServiceCaller.create(id).className(className).method(method).returnClass(returnClass);

		ArgumentsSerializer argumentsSerializer = InstanceFactory.instance(ArgumentsSerializer.class);
		Object[] args = Objects.requireNonNull(argumentsSerializer).argumentsFrom(dis, serviceCaller);
		serviceCaller.args(args);
	}

	@Override
	public void to(OutputStream outputStream) throws IOException {
		DataOutputStreamEx dos = dataOutputStream(outputStream);
		dos.writeUTF(serviceCaller.getId());
		dos.writeUTF(serviceCaller.getClassName());
		dos.writeUTF(serviceCaller.getMethod());
		dos.writeUTF(serviceCaller.getReturnClass());

		ArgumentsSerializer argumentsSerializer = InstanceFactory.instance(ArgumentsSerializer.class);
		Objects.requireNonNull(argumentsSerializer).argumentsTo(dos, serviceCaller);
	}

	public ServiceCaller getServiceCaller() {
		return serviceCaller;
	}

	public void setServiceCaller(ServiceCaller serviceCaller) {
		this.serviceCaller = serviceCaller;
	}

	@Override
	public String toString() {
		return contentType() + " serviceCaller " + serviceCaller;
	}
}
