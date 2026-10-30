package com.tapdata.tm.audit.query;

import com.tapdata.tm.audit.AuditAction;
import com.tapdata.tm.audit.AuditAspect;
import com.tapdata.tm.audit.AuditEvent;
import com.tapdata.tm.audit.AuditEventService;
import com.tapdata.tm.audit.AuditOutcome;
import com.tapdata.tm.audit.AuditRetentionManager;
import com.tapdata.tm.audit.service.AuditEventServiceImpl;
import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.permissions.constants.DataPermissionEnumsName;
import com.tapdata.tm.permissions.service.ModuleAccess;
import com.tapdata.tm.utils.MessageUtil;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.servlet.http.HttpServletResponse;
import org.apache.commons.lang3.StringUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/** 审计日志查询、详情与导出。查看需要 v2_audit_log；导出另需 v2_audit_log_export，导出本身产生审计事件。 */
@Tag(name = "AuditEvents", description = "审计日志")
@RestController
@RequestMapping("/api/audit-events")
public class AuditEventController extends BaseController {
	private final AuditQueryService queryService;
	private final AuditEventService auditEventService;
	private final ModuleAccess moduleAccess;
	private final AuditRetentionManager retentionManager;

	public AuditEventController(AuditQueryService queryService, AuditEventService auditEventService, ModuleAccess moduleAccess,
								AuditRetentionManager retentionManager) {
		this.queryService = queryService;
		this.auditEventService = auditEventService;
		this.moduleAccess = moduleAccess;
		this.retentionManager = retentionManager;
	}

	@Operation(summary = "审计日志列表（逻辑操作）")
	@GetMapping
	public ResponseMessage<Page<AuditOperationVo>> list(@ModelAttribute AuditQuery query) {
		UserDetail user = getLoginUser();
		moduleAccess.require(user, DataPermissionEnumsName.V2_AUDIT_LOG);
		return success(queryService.list(query, scope(user)));
	}

	@Operation(summary = "审计事件详情（按 Event ID 或 Operation ID）")
	@GetMapping("/{id}")
	public ResponseMessage<List<AuditOperationVo>> detail(@PathVariable("id") String id) {
		UserDetail user = getLoginUser();
		moduleAccess.require(user, DataPermissionEnumsName.V2_AUDIT_LOG);
		return success(queryService.detail(id, scope(user)));
	}

	@Operation(summary = "审计能力状态：保留天数、导出上限与本实例审计写入失败情况")
	@GetMapping("/status")
	public ResponseMessage<Map<String, Object>> status() {
		UserDetail user = getLoginUser();
		moduleAccess.require(user, DataPermissionEnumsName.V2_AUDIT_LOG);
		Map<String, Object> status = new LinkedHashMap<>();
		status.put("retentionDays", retentionManager.currentDays());
		status.put("maxExport", AuditQueryService.MAX_EXPORT);
		status.put("exportAllowed", moduleAccess.allowed(user, DataPermissionEnumsName.V2_AUDIT_LOG_EXPORT));
		if (auditEventService instanceof AuditEventServiceImpl impl) {
			status.put("writeFailures", impl.getFailureCount());
			status.put("lastWriteFailureAt", impl.getLastFailureAt());
		}
		return success(status);
	}

	@Operation(summary = "导出当前查询条件命中的审计记录（CSV，原始事件粒度）")
	@GetMapping("/export")
	public void export(@ModelAttribute AuditQuery query, @RequestParam(value = "timeZone", required = false) String timeZone,
					   HttpServletResponse response) throws IOException {
		UserDetail user = getLoginUser();
		moduleAccess.require(user, DataPermissionEnumsName.V2_AUDIT_LOG);
		String summary = summary(query);
		try {
			moduleAccess.require(user, DataPermissionEnumsName.V2_AUDIT_LOG_EXPORT);
		} catch (BizException e) {
			recordExport(AuditOutcome.FAILED, summary, null, e.getErrorCode(), user);
			throw e;
		}
		AtomicLong rows = new AtomicLong();
		try {
			long expected = queryService.count(query, scope(user));
			if (expected > AuditQueryService.MAX_EXPORT) {
				throw new BizException("Audit.Export.TooMany", expected, AuditQueryService.MAX_EXPORT);
			}
			String name = "audit-logs-" + new SimpleDateFormat("yyyyMMddHHmmss").format(new Date()) + ".csv";
			response.setContentType("text/csv;charset=UTF-8");
			response.setHeader("Content-Disposition", "attachment; filename=\"" + name + "\"");
			Locale locale = MessageUtil.getLocale();
			Writer writer = new OutputStreamWriter(response.getOutputStream(), StandardCharsets.UTF_8);
			AuditCsvWriter csv = new AuditCsvWriter(writer, zone(timeZone), locale);
			csv.header();
			queryService.export(query, scope(user), event -> {
				try {
					csv.row(event);
					rows.incrementAndGet();
				} catch (IOException e) {
					throw new UncheckedIOException(e);
				}
			});
			writer.flush();
			recordExport(AuditOutcome.SUCCESS, summary, rows.get(), null, user);
		} catch (RuntimeException | IOException e) {
			recordExport(AuditOutcome.FAILED, summary, rows.get(), AuditAspect.reasonOfPublic(e), user);
			throw e;
		}
	}

	private void recordExport(AuditOutcome outcome, String summary, Long count, String reason, UserDetail user) {
		AuditEvent event = AuditEvent.of(AuditAction.AUDIT_EXPORT, outcome);
		event.object(null, "audit_log");
		event.setSummary(summary);
		event.setReason(reason);
		if (count != null) {
			event.getContext().put("record_count", String.valueOf(count));
		}
		auditEventService.record(event, user);
	}

	private String scope(UserDetail user) {
		return moduleAccess.allData(user, DataPermissionEnumsName.V2_AUDIT_LOG) ? null : user.getUserId();
	}

	static String summary(AuditQuery q) {
		Map<String, Object> parts = new LinkedHashMap<>();
		parts.put("start", q.getStart());
		parts.put("end", q.getEnd());
		parts.put("eventType", q.getEventType());
		parts.put("user", q.getUser());
		parts.put("source", q.getSource());
		parts.put("sourceIp", q.getSourceIp());
		parts.put("action", q.getAction());
		parts.put("object", q.getObject());
		parts.put("outcome", q.getOutcome());
		parts.put("eventId", q.getEventId());
		parts.put("operationId", q.getOperationId());
		parts.put("keyword", q.getKeyword());
		StringBuilder sb = new StringBuilder();
		parts.forEach((k, v) -> {
			if (v != null && StringUtils.isNotBlank(String.valueOf(v))) {
				sb.append(sb.length() == 0 ? "" : "; ").append(k).append('=').append(v);
			}
		});
		return sb.length() == 0 ? "all" : sb.toString();
	}

	private static ZoneId zone(String timeZone) {
		try {
			return StringUtils.isBlank(timeZone) ? ZoneId.systemDefault() : ZoneId.of(timeZone);
		} catch (Exception e) {
			return ZoneId.systemDefault();
		}
	}
}
