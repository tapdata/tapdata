package com.tapdata.entity;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Verify {

	private String user;

	private int count;

	private String signature;

	public static Verify parse(String key) throws NoSuchAlgorithmException {
		Verify verify = null;

		if (StringUtils.isNotBlank(key)) {
			String[] keys = key.split(",");

			if (keys.length == 3) {
				verify = new Verify();
				Pattern pa = Pattern.compile("([a-zA-Z]+)=([a-zA-Z0-9@.]+)");

				Matcher matcher = pa.matcher(key);
				while (matcher.find()) {
					String property = matcher.group(1);
					String value = matcher.group(2);
					if ("user".equals(property)) {
						verify.setUser(value);
						continue;
					}
					if ("count".equals(property)) {
						verify.setCount(NumberUtils.isDigits(value) ? Integer.valueOf(value) : 1);
						continue;
					}
					if ("signature".equals(property)) {
						verify.setSignature(value);
						continue;
					}
				}

				String substring = key.substring(0, key.lastIndexOf(","));
				String base64 = Base64.getEncoder().encodeToString(substring.getBytes());
				MessageDigest md = MessageDigest.getInstance("MD5");
				md.update(base64.getBytes());
				String expectSign = convertByteArrayToHexString(md.digest());
				String actualSign = verify.getSignature();
				if (!expectSign.equals(actualSign)) {
					verify = null;
				}
			}

		}

		return verify;
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

	public String getSignature() {
		return signature;
	}

	public void setSignature(String signature) {
		this.signature = signature;
	}

	private static String convertByteArrayToHexString(byte[] arrayBytes) {
		StringBuffer stringBuffer = new StringBuffer();
		for (int i = 0; i < arrayBytes.length; i++) {
			stringBuffer.append(Integer.toString((arrayBytes[i] & 0xff) + 0x100, 16).substring(1));
		}
		return stringBuffer.toString();
	}

	public static void main(String[] args) throws NoSuchAlgorithmException {
		String key = "user=admin@admin.com,count=2,signature=1b116cc7e5a3dcdf2a41ca18629bf421";
		Verify parse = Verify.parse(key);
		System.out.println(parse);
	}
}
