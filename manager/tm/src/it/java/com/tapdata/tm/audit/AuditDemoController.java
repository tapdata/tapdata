package com.tapdata.tm.audit;

import com.tapdata.tm.base.dto.MutiResponseMessage;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.base.exception.BizException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** 用于 L2 用例的控制器替身：方法声明与产品控制器相同的 @Audited 写法，经真实切面代理调用。 */
public class AuditDemoController {
	/** 模拟删除操作在执行中移除对象。 */
	private Runnable deleteHook = () -> {
	};

	public void setDeleteHook(Runnable deleteHook) {
		this.deleteHook = deleteHook;
	}

	@Audited(value = AuditAction.TASK_CREATE, objectId = "#result?.get('id')", objectName = "#name")
	public ResponseMessage<Map<String, String>> create(String name, Map<String, Object> body) {
		ResponseMessage<Map<String, String>> result = new ResponseMessage<>();
		result.setData(Map.of("id", "64b000000000000000000001"));
		return result;
	}

	@Audited(value = AuditAction.DATASOURCE_DELETE, objectId = "#id")
	public ResponseMessage<Void> delete(String id) {
		deleteHook.run();
		return new ResponseMessage<>();
	}

	@Audited(value = AuditAction.DATASOURCE_UPDATE, objectId = "#id")
	public ResponseMessage<Void> update(String id, Map<String, Object> body) {
		throw new BizException("Datasource.NotFound");
	}

	@Audited(value = AuditAction.USER_UPDATE, objectId = "#id")
	public ResponseMessage<Void> softFail(String id) {
		ResponseMessage<Void> result = new ResponseMessage<>();
		result.setCode("Account.disabled");
		return result;
	}

	@Audited(value = AuditAction.TASK_START, objectId = "#id")
	public ResponseMessage<Void> start(String id) {
		return new ResponseMessage<>();
	}

	@Audited(value = AuditAction.TASK_START, objectId = "#id")
	public ResponseMessage<Void> startRejected(String id) {
		throw new BizException("NotAuthorized");
	}

	@Audited(value = AuditAction.TASK_STOP, objectId = "#id", alternateWhen = "#force", alternate = AuditAction.TASK_FORCE_STOP)
	public ResponseMessage<Void> stop(String id, boolean force) {
		return new ResponseMessage<>();
	}

	@Audited(value = AuditAction.TASK_START, objectId = "#ids")
	public ResponseMessage<List<MutiResponseMessage>> batchStart(List<String> ids, List<String> failedIds) {
		List<MutiResponseMessage> items = new ArrayList<>();
		for (String id : ids) {
			MutiResponseMessage item = new MutiResponseMessage();
			item.setId(id);
			if (failedIds.contains(id)) {
				item.setCode("Task.StatusError");
				item.setMessage("task is running");
			}
			items.add(item);
		}
		ResponseMessage<List<MutiResponseMessage>> result = new ResponseMessage<>();
		result.setData(items);
		return result;
	}

	@Audited(value = AuditAction.TASK_DELETE, objectId = "#ids")
	public ResponseMessage<List<MutiResponseMessage>> batchDelete(List<String> ids) {
		List<MutiResponseMessage> items = new ArrayList<>();
		for (String id : ids) {
			MutiResponseMessage item = new MutiResponseMessage();
			item.setId(id);
			item.setCode("Task.DeleteFailed");
			items.add(item);
		}
		ResponseMessage<List<MutiResponseMessage>> result = new ResponseMessage<>();
		result.setData(items);
		return result;
	}

	@Audited(value = AuditAction.TASK_DOWNLOAD_LOG, objectId = "#taskId")
	public void downloadLog(String taskId) {
	}

	@Audited(value = AuditAction.DATASOURCE_UPDATE, objectId = "#id", objectName = "#body['name']")
	public ResponseMessage<Void> updateWithSecret(String id, Map<String, Object> body) {
		return new ResponseMessage<>();
	}

	@Audited(value = AuditAction.SETTINGS_UPDATE, action = "#internal ? '-' : 'settings.update'")
	public ResponseMessage<Void> maybeInternal(boolean internal) {
		return new ResponseMessage<>();
	}
}
