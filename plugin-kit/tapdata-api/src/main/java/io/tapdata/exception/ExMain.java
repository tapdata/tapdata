package io.tapdata.exception;

/**
 * @author samuel
 * @Description
 * @create 2023-02-23 11:19
 **/
public class ExMain {
	public static void main(String[] args) {
		throw new TapCodeException(SourcePdkEx.DOWNLOAD_PDK_FAILED, "Cannot download pdk: 404");
	}
}
