package com.dobybros.tccore.modules.imclient.impls;

import com.dobybros.tccore.modules.imclient.IMClient;
import com.dobybros.tccore.modules.imclient.IMStatusListener;
import com.dobybros.tccore.modules.imclient.impls.data.IncomingData;
import com.dobybros.tccore.modules.imclient.impls.data.IncomingMessage;
import com.dobybros.tccore.modules.imclient.impls.data.OutgoingData;
import com.dobybros.tccore.modules.imclient.impls.data.OutgoingMessage;

import java.io.IOException;

public abstract class PushChannel {
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
	
	public abstract void start();
	public abstract void selfCheck();

	public abstract void ping() throws IOException;
	public abstract void sendData(IncomingData data) throws IOException;
	public abstract void sendMessage(IncomingMessage message) throws IOException;

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