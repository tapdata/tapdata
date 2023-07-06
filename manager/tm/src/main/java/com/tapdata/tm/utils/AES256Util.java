package com.tapdata.tm.utils;


import org.bouncycastle.jce.provider.BouncyCastleProvider;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.Security;

public class AES256Util {

	public static boolean initialized = false;

	public static final String ALGORITHM_EBC = "AES/ECB/PKCS7Padding";
	public static final String ALGORITHM_CBC = "AES/CBC/PKCS7Padding";
	public static final String ALGORITHM_ECB_PKCS5 = "AES/ECB/PKCS5Padding";

	public static final int KEY_LENGTH = 32;
	public static final int IV_LENGTH = 16;

	private final static String KEY = "dGcrwE7FobpZMjNKvkolwc9qxWagTQEPK8VPZxXmwC6DDDB5uGfQc3NQOYKFJSw0";

	private final static String CHARSET = "UTF-8";

	public static String Aes256Encode(String str) {
		initialize();
		byte[] result;
		String encode;
		try {
			byte[] input = str.getBytes(CHARSET);
			Cipher cipher = Cipher.getInstance(ALGORITHM_ECB_PKCS5);
			SecretKeySpec keySpec = new SecretKeySpec(getKey(), "AES"); //生成加密解密需要的Key
			cipher.init(Cipher.ENCRYPT_MODE, keySpec);

			result = new byte[cipher.getOutputSize(input.length)];
			int ctLength = cipher.update(input, 0, input.length, result, 0);
			ctLength += cipher.doFinal(result, ctLength);
			encode = parseByte2HexStr(result);
		} catch (Exception e) {
			encode = str;
		}
		return encode;
	}

	public static String Aes256Decode(String content) {
		initialize();
		String result;
		try {
			Cipher cipher = Cipher.getInstance(ALGORITHM_ECB_PKCS5);
			SecretKeySpec keySpec = new SecretKeySpec(getKey(), "AES"); //生成加密解密需要的Key
			cipher.init(Cipher.DECRYPT_MODE, keySpec);
			byte[] decoded = cipher.doFinal(toByte(content));
			result = new String(decoded, CHARSET);
		} catch (Exception e) {
			result = content;
		}
		return result;
	}

	public static void initialize() {
		if (initialized) return;
		Security.addProvider(new BouncyCastleProvider());
		initialized = true;
	}

	public static byte[] getKey() {
		String key = KEY;
		byte[] ret = {};

		try {
			MessageDigest messageDigest = MessageDigest.getInstance("MD5");

			ret = messageDigest.digest(key.getBytes(CHARSET));

		} catch (NoSuchAlgorithmException e) {
			// TODO
		} catch (UnsupportedEncodingException e) {
			// TODOd
		}
		return ret;
	}

	private static String parseByte2HexStr(byte buf[]) {
		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < buf.length; i++) {
			String hex = Integer.toHexString(buf[i] & 0xFF);
			if (hex.length() == 1) {
				hex = '0' + hex;
			}
			sb.append(hex);
		}
		return sb.toString();
	}

	private static byte[] toByte(String hexString) {
		int len = hexString.length() / 2;
		byte[] result = new byte[len];
		for (int i = 0; i < len; i++) {
			result[i] = Integer.valueOf(hexString.substring(2 * i, 2 * i + 2), 16).byteValue();
		}
		return result;
	}

}
