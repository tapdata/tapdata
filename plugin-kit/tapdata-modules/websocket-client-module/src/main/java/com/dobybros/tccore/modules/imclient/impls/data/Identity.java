package com.dobybros.tccore.modules.imclient.impls.data;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;


public class Identity extends Data {
	private String id;
	private String sessionId;
	private Integer terminal;
	private String userId;
	private String deviceToken;
	private String service;
	private String key;
	private String appId;
	private String code;
	private Integer sdkVersion;
	
	public Identity(){
		super(HailPack.TYPE_IN_IDENTITY);
	}
	public static void main() {
		Random rand = new Random(12);
		for(int i = 0; i < 10; i++) {
			System.out.println(rand.nextInt());
		}
	}

	/**
	 * @param userId the userId to set
	 */
	public void setUserId(String userId) {
		this.userId = userId;
	}

	/**
	 * @return the userId
	 */
	public String getUserId() {
		return userId;
	}

	public String getDeviceToken() {
		return deviceToken;
	}


	public void setDeviceToken(String deviceToken) {
		this.deviceToken = deviceToken;
	}

	@Override
	public void resurrect() throws IOException {
		byte[] bytes = getData();
		Byte encode = getEncode();
		if(bytes != null) {
			if(encode != null) {
				switch(encode) {
				case ENCODE_PB:

					break;
					default:
						throw new IOException("Encoder type doesn't be found for resurrect");
				}
			}
		}
	}

	@Override
	public void persistent() throws IOException {
		Byte encode = getEncode();
		if(encode == null)
			encode = ENCODE_PB;//throw new CoreException(CoreErrorCodes.ERROR_RPC_ENCODER_NULL, "Encoder is null for persistent");
		switch(encode) {
		case ENCODE_PB:

			break;
			default:
				throw new IOException("Encoder type doesn't be found for persistent");
		}
	}
	public String getSessionId() {
		return sessionId;
	}
	public void setSessionId(String sessionId) {
		this.sessionId = sessionId;
	}
	public Integer getTerminal() {
		return terminal;
	}
	public void setTerminal(Integer terminal) {
		this.terminal = terminal;
	}
	public String getKey() {
		return key;
	}
	public void setKey(String key) {
		this.key = key;
	}
	public String getAppId() {
		return appId;
	}
	public void setAppId(String appId) {
		this.appId = appId;
	}
	public String getCode() {
		return code;
	}
	public void setCode(String code) {
		this.code = code;
	}
	public Integer getSdkVersion() {
		return sdkVersion;
	}
	public void setSdkVersion(Integer sdkVersion) {
		this.sdkVersion = sdkVersion;
	}
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getService() {
		return service;
	}
	public void setService(String service) {
		this.service = service;
	}

	@Override
	public String toString() {
		return "Identity{" +
				"id='" + id + '\'' +
				", sessionId='" + sessionId + '\'' +
				", terminal=" + terminal +
				", userId='" + userId + '\'' +
				", deviceToken='" + deviceToken + '\'' +
				", service='" + service + '\'' +
				", key='" + key + '\'' +
				", appId='" + appId + '\'' +
				", code='" + code + '\'' +
				", sdkVersion=" + sdkVersion +
				", type=" + type +
				", encode=" + encode +
				", data=" + Arrays.toString(data) +
				", encodeVersion=" + encodeVersion +
				'}';
	}
}
