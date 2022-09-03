package io.tapdata.proxy;

import io.tapdata.modules.api.net.data.IncomingData;
import io.tapdata.modules.api.net.data.IncomingMessage;
import io.tapdata.modules.api.net.data.ResultData;
import io.tapdata.wsserver.channels.annotation.GatewaySession;
import io.tapdata.wsserver.channels.gateway.GatewaySessionHandler;


@GatewaySession(idType = "engine")
public class MyGatewaySessionHandler extends GatewaySessionHandler {
	public static void main(String[] args) {

	}
	@Override
	public void onSessionCreated() {

	}

	@Override
	public void onChannelConnected() {

	}

	@Override
	public void onChannelDisconnected() {

	}

	@Override
	public void onSessionDestroyed() {

	}

	@Override
	public ResultData onDataReceived(IncomingData data) {
		return null;
	}

	@Override
	public ResultData onMessageReceived(IncomingMessage message) {
		return null;
	}

}
