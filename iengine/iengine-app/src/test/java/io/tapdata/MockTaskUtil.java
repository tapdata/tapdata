package io.tapdata;

import com.tapdata.constant.JSONUtil;
import com.tapdata.tm.commons.task.dto.TaskDto;

import java.io.File;
import java.io.IOException;
import java.net.URL;

/**
 * @author samuel
 * @Description Help mock task data
 * @create 2023-11-16 12:18
 **/
public class MockTaskUtil {
	private final static String TASK_JSON_DIRECTORY = "task/json";

	public static TaskDto setUpTaskDtoByJsonFile(String jsonFileName) {
		String pathInResources = TASK_JSON_DIRECTORY + File.separator + jsonFileName;
		URL taskJsonFileURL = MockTaskUtil.class.getClassLoader().getResource(pathInResources);
		if (null == taskJsonFileURL) {
			throw new RuntimeException(String.format("Cannot get url: '%s', check your json file name and path is correct", pathInResources));
		}
		TaskDto taskDto;
		try {
			taskDto = JSONUtil.json2POJO(taskJsonFileURL, TaskDto.class);
		} catch (IOException e) {
			throw new RuntimeException("Parse json file to task dto failed, url: " + taskJsonFileURL, e);
		}
		return taskDto;
	}

	public static TaskDto setUpTaskDtoByJsonFile() {
		return setUpTaskDtoByJsonFile("dummy2dummy.json");
	}
}
