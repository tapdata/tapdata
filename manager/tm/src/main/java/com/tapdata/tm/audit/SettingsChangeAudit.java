package com.tapdata.tm.audit;

import com.tapdata.tm.Settings.dto.SettingsDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.permissions.service.ModuleAccess;
import org.bson.Document;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * 系统设置保存的审计：系统设置是标量键值，可以可靠取得前后值，因此记录字段差异（秘密字段只记状态）；
 * 审计日志保留期单独记为保留策略修改，保存前完成范围校验和 TTL 调整，任一失败都拒绝保存并记录失败事件。
 */
@Component
public class SettingsChangeAudit {
	static final String SETTINGS_PERMISSION = "v2_system_settings_menu";
	private static final String MASKED = "*****";

	private final AuditEventService auditEventService;
	private final MongoTemplate mongoTemplate;
	private final AuditRetentionManager retentionManager;
	private final ModuleAccess moduleAccess;

	public SettingsChangeAudit(AuditEventService auditEventService, MongoTemplate mongoTemplate,
							   AuditRetentionManager retentionManager, ModuleAccess moduleAccess) {
		this.auditEventService = auditEventService;
		this.mongoTemplate = mongoTemplate;
		this.retentionManager = retentionManager;
		this.moduleAccess = moduleAccess;
	}

	public void save(List<SettingsDto> settings, UserDetail user, Runnable save) {
		List<SettingsDto> items = settings == null ? List.of() : settings.stream().filter(Objects::nonNull).toList();
		Map<String, Object> before = currentValues(items);
		SettingsDto retention = items.stream().filter(s -> AuditRetentionManager.SETTING_KEY.equals(s.getKey())).findFirst().orElse(null);
		Integer oldDays = null;
		Integer newDays = null;
		if (retention != null && !sameValue(before.get(key(retention)), retention.getValue())) {
			oldDays = retentionManager.currentDays();
			AuditChange change = AuditRedactor.change(AuditRetentionManager.SETTING_KEY, oldDays, retention.getValue());
			try {
				moduleAccess.require(user, SETTINGS_PERMISSION);
				newDays = AuditRetentionManager.parseDays(retention.getValue());
				retentionManager.apply(newDays);
			} catch (RuntimeException e) {
				record(AuditAction.AUDIT_RETENTION_UPDATE, AuditOutcome.FAILED, change == null ? List.of() : List.of(change), AuditAspect.reasonOf(e), user);
				throw e;
			}
		}
		List<AuditChange> changes = new ArrayList<>();
		for (SettingsDto item : items) {
			if (item == retention || isMasked(item)) {
				continue;
			}
			AuditChange change = AuditRedactor.change(item.getCategory() + "." + item.getKey(), before.get(key(item)), item.getValue());
			if (change != null) {
				changes.add(change);
			}
		}
		try {
			save.run();
		} catch (RuntimeException e) {
			if (newDays != null) {
				safeRevert(oldDays);
			}
			record(AuditAction.SETTINGS_UPDATE, AuditOutcome.FAILED, changes, AuditAspect.reasonOf(e), user);
			throw e;
		}
		if (newDays != null) {
			record(AuditAction.AUDIT_RETENTION_UPDATE, AuditOutcome.SUCCESS,
					List.of(new AuditChange(AuditRetentionManager.SETTING_KEY, String.valueOf(oldDays), String.valueOf(newDays), null)), null, user);
		}
		if (!changes.isEmpty() || newDays == null) {
			record(AuditAction.SETTINGS_UPDATE, AuditOutcome.SUCCESS, changes, null, user);
		}
	}

	private void record(AuditAction action, AuditOutcome outcome, List<AuditChange> changes, String reason, UserDetail user) {
		AuditEvent event = AuditEvent.of(action, outcome);
		event.setChanges(new ArrayList<>(changes));
		event.setReason(reason);
		event.object(null, "settings");
		if (changes.isEmpty()) {
			event.setSummary("audit_summary_settings_no_field_changed");
		}
		auditEventService.record(event, user);
	}

	private Map<String, Object> currentValues(List<SettingsDto> items) {
		Map<String, Object> values = new HashMap<>();
		List<String> keys = items.stream().map(SettingsDto::getKey).filter(Objects::nonNull).toList();
		if (keys.isEmpty()) {
			return values;
		}
		for (Document doc : mongoTemplate.find(Query.query(Criteria.where("key").in(keys)), Document.class, "Settings")) {
			values.put(doc.getString("category") + "|" + doc.getString("key"), doc.get("value"));
		}
		return values;
	}

	private void safeRevert(Integer days) {
		try {
			if (days != null) {
				retentionManager.apply(days);
			}
		} catch (RuntimeException ignore) {
			// 保持原错误；启动对齐会再次以设置值为准
		}
	}

	private static String key(SettingsDto dto) {
		return dto.getCategory() + "|" + dto.getKey();
	}

	private static boolean isMasked(SettingsDto dto) {
		return dto.getValue() != null && MASKED.equals(String.valueOf(dto.getValue()));
	}

	static boolean sameValue(Object a, Object b) {
		return Objects.equals(a == null ? null : String.valueOf(a), b == null ? null : String.valueOf(b));
	}
}
