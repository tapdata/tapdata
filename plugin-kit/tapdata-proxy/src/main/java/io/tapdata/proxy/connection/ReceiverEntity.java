package io.tapdata.proxy.connection;

import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.JsonParser;
import io.tapdata.modules.api.net.data.Data;
import io.tapdata.modules.api.net.error.NetErrors;
import io.tapdata.modules.api.net.service.node.connection.Receiver;
import io.tapdata.pdk.core.utils.TapConstants;
import net.jodah.typetools.TypeResolver;

import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.function.BiConsumer;


public class ReceiverEntity<Response, Request> {
	Receiver<Response, Request> receiver;
	Class<Request> requestClass;
	Class<Response> responseClass;

	public ReceiverEntity(Receiver<Response, Request> receiver) {
		this.receiver = receiver;
		if(this.receiver == null)
			throw new CoreException(NetErrors.ILLEGAL_PARAMETERS, "receiver can not be null when construct ReceiverEntity");
		Type[] types = TypeResolver.resolveRawArguments(Receiver.class, receiver.getClass());
		if(types == null || types.length != 2) {
			throw new CoreException(NetErrors.RECEIVER_GENERIC_TYPES_ILLEGAL, "Receiver {} generic types should be 2, but {}, types {}", receiver, types != null ? types.length : 0, types);
		}
		//noinspection unchecked
		responseClass = (Class<Response>) types[0];
		//noinspection unchecked
		requestClass = (Class<Request>) types[1];
	}

	public void receive(String nodeId, Byte encode, byte[] data, BiConsumer<Object, Throwable> biConsumer) {
		if(encode == null)
			encode = Data.ENCODE_JSON;
		Request request = null;
		//noinspection SwitchStatementWithTooFewBranches
		switch (encode) {
			case Data.ENCODE_JSON:
				String jsonStr = new String(data, StandardCharsets.UTF_8);
				request = Objects.requireNonNull(InstanceFactory.instance(JsonParser.class)).fromJson(jsonStr, requestClass, TapConstants.abstractClassDetectors);
				break;
			default:
				biConsumer.accept(null, new CoreException(NetErrors.UNSUPPORTED_ENCODE, "Unsupported encode {} for ReceiverEntity", encode));
				return;
		}
		//noinspection unchecked
		receiver.received(nodeId, request, biConsumer);
	}

	public Receiver<Response, Request> getReceiver() {
		return receiver;
	}

	public Class<Request> getRequestClass() {
		return requestClass;
	}

	public Class<Response> getResponseClass() {
		return responseClass;
	}
}
