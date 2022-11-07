package com.tapdata.tm.sdk.auth;

/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2020/9/9 6:35 上午
 * @description
 */
public abstract class Signer {

	private final static Signer HMACSHA1_SIGNER = new HmacSHA1Signer();

	public static Signer getSigner(Credentials credentials) {
		if (credentials instanceof BasicCredentials){
			return HMACSHA1_SIGNER;
		}
		return null;
	}

	public abstract String signString(String stringToSign, Credentials credentials);

	public abstract String signString(String stringToSign, String accessKeySecret);

	public abstract String getSignerName();

	public abstract String getSignerVersion();

	public abstract String getSignerType();
}
