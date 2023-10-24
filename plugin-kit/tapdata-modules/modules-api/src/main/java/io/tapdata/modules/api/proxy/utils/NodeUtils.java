package io.tapdata.modules.api.proxy.utils;

public class NodeUtils {
	public static String typeConnectionId(String type, String connectionId) {
		return type + "#" + connectionId;
	}
}
