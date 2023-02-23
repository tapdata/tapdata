package io.tapdata.exception;

/**
 * @author samuel
 * @Description
 * @create 2023-02-23 10:18
 **/
@TapExClass(code = 10, module = "SourcePdk")
public interface SourcePdkEx {
	@TapExCode(
			recoverable = true,
			describe = "Error downloading PDK data source plugin",
			solution = "Please check your network and try again"
	)
	String DOWNLOAD_PDK_FAILED = "10001";
}
