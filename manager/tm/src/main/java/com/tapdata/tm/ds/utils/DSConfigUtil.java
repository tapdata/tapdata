package com.tapdata.tm.ds.utils;

import com.tapdata.tm.utils.AES256Util;
import com.tapdata.tm.utils.GZIPUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.net.util.Base64;

/**
 * @author samuel
 * @Description
 * @create 2023-09-25 10:55
 **/
public class DSConfigUtil {

	public static String encrypt(String config) {
		if (StringUtils.isBlank(config)) {
			return config;
		}
		String encryptData = AES256Util.Aes256Encode(config);
		byte[] compressEncryptData = GZIPUtil.gzip(encryptData.getBytes());
		return Base64.encodeBase64String(compressEncryptData);
	}

	public static String decrypt(String encryptConfig) {
		if (StringUtils.isBlank(encryptConfig)) {
			return encryptConfig;
		}
		encryptConfig = encryptConfig.replaceAll("\\\\r", "").replaceAll("\\\\n", "");
		byte[] decodeBase64;
		try {
			decodeBase64 = Base64.decodeBase64(encryptConfig);
		} catch (Exception e) {
			throw new RuntimeException("Decode base64 failed, decode string: " + encryptConfig, e);
		}
		byte[] uncompressEncryptData = new byte[0];
		try {
			uncompressEncryptData = GZIPUtil.unGzip(decodeBase64);
		} catch (Exception e) {
			throw new RuntimeException("UnGzip decode base64 string failed: " + new String(uncompressEncryptData), e);
		}
		assert uncompressEncryptData != null;
		return AES256Util.Aes256Decode(new String(uncompressEncryptData));
	}
}
