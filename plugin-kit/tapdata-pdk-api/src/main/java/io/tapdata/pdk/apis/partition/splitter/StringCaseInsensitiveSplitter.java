package io.tapdata.pdk.apis.partition.splitter;

/**
 * @author aplomb
 */
public class StringCaseInsensitiveSplitter extends StringSplitter {
	public static StringCaseInsensitiveSplitter INSTANCE = new StringCaseInsensitiveSplitter();

	@Override
	public boolean caseInsensitive() {
		return true;
	}
}
