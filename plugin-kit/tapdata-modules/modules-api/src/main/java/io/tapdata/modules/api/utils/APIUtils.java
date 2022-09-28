package io.tapdata.modules.api.utils;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.zip.CRC32;

public class APIUtils {
	public static String idForList(List<String> strings) {
		StringBuilder builder = new StringBuilder();
		if(strings == null) {
			builder.append("null");
		} else {
			builder.append("list: ");
			List<String> list = new ArrayList<>(strings);
			Collections.sort(list);
			for(String str : list) {
				builder.append(str);
			}
		}
		return idForString(builder.toString());
	}

	public static String idForString(String string) {
		CRC32 crc32 = new CRC32();
		crc32.update(string.getBytes(StandardCharsets.UTF_8));
		return String.valueOf(crc32.getValue());
	}

	public static String uuid() {
		return UUID.randomUUID().toString().replace("-", "");
	}
}
