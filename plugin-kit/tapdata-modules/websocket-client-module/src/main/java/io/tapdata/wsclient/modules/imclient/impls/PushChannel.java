package io.tapdata.wsclient.modules.imclient.impls;

import io.tapdata.entity.memory.MemoryFetcher;
import io.tapdata.modules.api.net.data.*;
import io.tapdata.wsclient.modules.imclient.IMStatusListener;

import java.io.IOException;

public abstract class PushChannel implements MemoryFetcher {
	protected IMClientImpl imClient;
	protected IMStatusListener statusListener;
	protected ReceiveDataListener receiveDataListener;
	protected ReceiveMessageListener receiveMessageListener;
	
	public static final int ERRORCODE_TIMEOUT = 101010;

	public interface ReceiveDataListener {
		public void onOutgoingData(OutgoingData outgoingData);
	}
	public interface ReceiveMessageListener {
		public void onOutgoingMessage(OutgoingMessage outgoingMessage);
	}

	public PushChannel() {
	}
	
	public abstract void stop();
	
	public abstract void start(String lastBaseUrl);
	public abstract void selfCheck();

	public abstract void ping() throws IOException;
	public abstract void send(Data data) throws IOException;
	public IMStatusListener getStatusListener() {
		return statusListener;
	}

	public void setStatusListener(IMStatusListener statusListener) {
		this.statusListener = statusListener;
	}

	public ReceiveDataListener getReceiveDataListener() {
		return receiveDataListener;
	}

	public void setReceiveDataListener(ReceiveDataListener receiveDataListener) {
		this.receiveDataListener = receiveDataListener;
	}

	public ReceiveMessageListener getReceiveMessageListener() {
		return receiveMessageListener;
	}

	public void setReceiveMessageListener(ReceiveMessageListener receiveMessageListener) {
		this.receiveMessageListener = receiveMessageListener;
	}

	public IMClientImpl getImClient() {
		return imClient;
	}

	public void setImClient(IMClientImpl imClient) {
		this.imClient = imClient;
	}
}