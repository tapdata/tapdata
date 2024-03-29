/**
 * @title: DataFlowResetAllResDto
 * @description:
 * @author lk
 * @date 2021/9/13
 */
package com.tapdata.tm.dataflow.dto;

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.collections4.CollectionUtils;

public class DataFlowResetAllResDto {

	private List<SuccessInfo> success;

	private List<FailInfo> fail;

	public List<SuccessInfo> getSuccess() {
		return success;
	}

	public List<FailInfo> getFail() {
		return fail;
	}

	public void setSuccess(List<SuccessInfo> success) {
		this.success = success;
	}

	public void setFail(List<FailInfo> fail) {
		this.fail = fail;
	}

	public void addSuccess(String id){
		if (CollectionUtils.isEmpty(success)){
			success = new ArrayList<>();
		}
		success.add(new SuccessInfo(id));
	}

	public void addFail(String id, String reason){
		if (CollectionUtils.isEmpty(fail)){
			fail = new ArrayList<>();
		}
		fail.add(new FailInfo(id, reason));
	}

	public static class SuccessInfo{

		private String id;

		SuccessInfo(String id) {
			this.id = id;
		}

		public String getId() {
			return id;
		}
	}

	public static class FailInfo{

		private String id;

		private String reason;

		FailInfo(String id, String reason) {
			this.id = id;
			this.reason = reason;
		}

		public String getId() {
			return id;
		}

		public String getReason() {
			return reason;
		}
	}
}
