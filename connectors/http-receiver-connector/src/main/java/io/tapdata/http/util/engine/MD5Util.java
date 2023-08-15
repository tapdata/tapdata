package io.tapdata.http.util.engine;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class MD5Util {

	/**
	 * Encodes a string 2 MD5
	 *
	 * @param str String to encode
	 * @return Encoded String
	 * @throws NoSuchAlgorithmException
	 */
	public static String crypt(String str, boolean upper) {
		if (str == null || str.length() == 0) {
			return "";
		}
		StringBuffer hexString = new StringBuffer();
		MessageDigest md;
		try {
			md = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			return "";
		}
		md.update(str.getBytes());
		byte[] hash = md.digest();
		for (int i = 0; i < hash.length; i++) {
			if ((0xff & hash[i]) < 0x10) {
				hexString.append("0" + Integer.toHexString((0xFF & hash[i])));
			} else {
				hexString.append(Integer.toHexString(0xFF & hash[i]));
			}
		}

		if (upper) {
			return hexString.toString().toUpperCase();
		} else {
			return hexString.toString();
		}
	}
}
