package io.tapdata.flow.engine.V2.task.preview;

import com.tapdata.constant.Log4jUtil;
import org.apache.http.HttpStatus;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author samuel
 * @Description
 * @create 2024-09-23 14:24
 **/
public class TaskPreviewNodeResultVO {
	private static final String TAG = TaskPreviewNodeResultVO.class.getName();
	protected int code;
	protected volatile List<Map<String, Object>> data;
	protected String errorMsg;
	protected String errorStack;

	public TaskPreviewNodeResultVO() {
		this.code = HttpStatus.SC_OK;
	}

	public TaskPreviewNodeResultVO data(Map<String, Object> d) {
		if (null == data) {
			synchronized (this) {
				if (null == data) {
					data = new ArrayList<>();
				}
			}
		}
		data.add(d);
		return this;
	}

	public TaskPreviewNodeResultVO failed(Exception e) {
		this.code = HttpStatus.SC_INTERNAL_SERVER_ERROR;
		this.errorMsg = e.getMessage();
		this.errorStack = Log4jUtil.getStackString(e);
		return this;
	}

	public TaskPreviewNodeResultVO invalid(String message) {
		this.code = HttpStatus.SC_UNPROCESSABLE_ENTITY;
		this.errorMsg = message;
		return this;
	}

	public int getCode() {
		return code;
	}

	public void setCode(int code) {
		this.code = code;
	}

	public List<Map<String, Object>> getData() {
		return data;
	}

	public String getErrorMsg() {
		return errorMsg;
	}

	public String getErrorStack() {
		return errorStack;
	}
}
