package com.tapdata.constant;

import com.tapdata.tm.worker.WorkerSingletonException;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * @author samuel
 * @Description
 * @create 2021-08-27 12:18
 **/
public class StartResultUtil {

	private static Logger logger = LogManager.getLogger(StartResultUtil.class);
	private final static String AGENT_START_RESULT_JSON = ".agentStartMsg.json";

	public static void writeStartResult(String workDir, String daasVersion, Exception e) {
		Map<String, String> result = new HashMap<>();
		if (StringUtils.isNotBlank(daasVersion)) {
			result.put("version", daasVersion);
		} else {
			result.put("version", "-");
		}
		if (e == null) {
			result.put("status", "ok");
			result.put("msg", "");
		} else if (e instanceof WorkerSingletonException) {
			result.put("status", "failed");
			result.put("msg", e.getMessage());
		} else {
			result.put("status", "failed");
			result.put("msg", e.getMessage() + "\n  " + Log4jUtil.getStackString(e));
		}

		String filePath;
		if (StringUtils.isNotBlank(workDir)) {
			filePath = workDir + File.separator + AGENT_START_RESULT_JSON;
		} else {
			filePath = ".agentStartMsg";
		}
		File file = new File(filePath);
		if (file.exists()) {
			file.delete();
		}
		try (FileOutputStream fileOutputStream = new FileOutputStream(file)) {
			String json = JSONUtil.map2Json(result);
			logger.info("Write start result to file: " + filePath + "\n  " + json);
			fileOutputStream.write(json.getBytes(StandardCharsets.UTF_8));
			fileOutputStream.flush();
		} catch (Exception exception) {
			String err = "Write start result failed, work dir: " + workDir + ", version: " + daasVersion + ", e: " + e == null ? "-" : e.getMessage() + ", cause: " + exception.getMessage();
			throw new RuntimeException(err);
		}
	}
}
