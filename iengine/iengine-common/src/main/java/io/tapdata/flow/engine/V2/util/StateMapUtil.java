package io.tapdata.flow.engine.V2.util;

/**
 * @author samuel
 * @Description
 * @create 2024-04-01 21:29
 **/
public class StateMapUtil {
	public static String encodeDotAndDollar(String input) {
		return null == input ? null : input.replace(".", "\\u002e").replace("$", "\\u0024");
	}

	public static String decodeDotAndDollar(String input) {
		return null == input ? null : input.replace("\\u002e", ".").replace("\\u0024", "$");
	}
}
