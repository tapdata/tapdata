package io.tapdata.exception;

/**
 * @author samuel
 * @Description
 * @create 2023-02-23 10:18
 **/
@TapExClass(code = 11, module = "SourcePdk")
public interface SourcePdkEx_11 {
	@TapExCode(
			recoverable = true,
			describe = "Error downloading PDK data source plugin",
			describeCN = "下载PDK数据源插件出错",
			solution = "Please check network and try again",
			solutionCN = "请检查网络并重试",
			type = TapExType.IO,
			level = TapExLevel.NORMAL
	)
	String DOWNLOAD_PDK_FAILED = "11001";

	@TapExCode
	String BATCH_READ_FROM_PDK = "11002";
}
