package io.tapdata.http.util.engine;

import org.apache.commons.lang3.RandomUtils;

import java.io.UnsupportedEncodingException;
import java.util.Base64;
import java.util.UUID;

public class UUIDGenerator {

	public static String uuid() {
		return UUID.randomUUID().toString();
	}

	public static String uuid14(boolean upper) {
		String uuid = UUID.randomUUID().toString();
		try {
			String base64String = Base64.getEncoder().encodeToString(uuid.getBytes("UTF-8"));
			int nextInt = RandomUtils.nextInt(0, 30);
			uuid = base64String.substring(nextInt, nextInt + 14);
			if (upper) {
				uuid = uuid.toUpperCase();
			}
		} catch (UnsupportedEncodingException e) {
			uuid = uuid.substring(0, 14);
		}

		return uuid;
	}
}
