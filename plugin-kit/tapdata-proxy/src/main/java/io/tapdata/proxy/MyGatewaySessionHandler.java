package io.tapdata.proxy;

import io.tapdata.modules.api.net.data.ResultData;
import io.tapdata.wsserver.channels.annotation.GatewaySession;
import io.tapdata.wsserver.channels.gateway.GatewaySessionHandler;

import java.util.Map;

@GatewaySession(idType = "engine")
public class MyGatewaySessionHandler extends GatewaySessionHandler {
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
	public ResultData onDataReceived(String contentType, Map<String, Object> jsonObject, String id) {
		return null;
	}

	@Override
	public ResultData onMessageReceived(String toUserId, String toGroupId, String contentType, Map<String, Object> jsonObject, String id) {
		return null;
	}
}
