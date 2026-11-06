package com.tapdata.tm.audit;

import com.tapdata.tm.base.exception.BizException;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Component;

/**
 * 审计日志保留期（需求 15 章）：系统设置 audit.log.retention.days，默认 90，范围 30–3650。
 * 生效方式是调整 UserLogs 上 last_updated 的 TTL 索引（collMod）；调整失败时拒绝保存，保持原 TTL。
 * 到期记录由 MongoDB 自动删除，不产生逐条或批次清理事件。
 */
@Slf4j
@Component
public class AuditRetentionManager {
	public static final String SETTING_KEY = "audit.log.retention.days";
	public static final int DEFAULT_DAYS = 90;
	public static final int MIN_DAYS = 30;
	public static final int MAX_DAYS = 3650;
	static final String COLLECTION = "UserLogs";
	static final String TTL_INDEX = "last_updated_1";

	private final MongoTemplate mongoTemplate;

	public AuditRetentionManager(MongoTemplate mongoTemplate) {
		this.mongoTemplate = mongoTemplate;
	}

	/** 解析并校验天数；非整数或超出范围时抛出 Audit.Retention.OutOfRange。 */
	public static int parseDays(Object value) {
		try {
			double number = Double.parseDouble(String.valueOf(value).trim());
			if (number != Math.floor(number) || number < MIN_DAYS || number > MAX_DAYS) {
				throw new BizException("Audit.Retention.OutOfRange", MIN_DAYS, MAX_DAYS);
			}
			return (int) number;
		} catch (NumberFormatException e) {
			throw new BizException("Audit.Retention.OutOfRange", MIN_DAYS, MAX_DAYS);
		}
	}

	public int currentDays() {
		try {
			Document setting = mongoTemplate.findOne(Query.query(Criteria.where("key").is(SETTING_KEY)), Document.class, "Settings");
			if (setting == null || setting.get("value") == null) {
				return DEFAULT_DAYS;
			}
			return parseDays(setting.get("value"));
		} catch (Exception e) {
			return DEFAULT_DAYS;
		}
	}

	/** 将 TTL 调整为指定天数；索引不存在时创建。失败抛出 Audit.Retention.ApplyFailed。 */
	public void apply(int days) {
		long seconds = days * 24L * 3600L;
		try {
			Document command = new Document("collMod", COLLECTION)
					.append("index", new Document("name", TTL_INDEX).append("expireAfterSeconds", seconds));
			mongoTemplate.getDb().runCommand(command);
		} catch (Exception e) {
			if (!indexMissing(e)) {
				log.error("Apply audit log retention {} days failed", days, e);
				throw new BizException("Audit.Retention.ApplyFailed");
			}
			try {
				Document create = new Document("createIndexes", COLLECTION).append("indexes", java.util.List.of(
						new Document("name", TTL_INDEX).append("key", new Document("last_updated", 1))
								.append("expireAfterSeconds", seconds).append("background", true)));
				mongoTemplate.getDb().runCommand(create);
			} catch (Exception createError) {
				log.error("Create audit log TTL index failed", createError);
				throw new BizException("Audit.Retention.ApplyFailed");
			}
		}
	}

	/** 启动时以设置值为准对齐 TTL，修正手工修改或升级前遗留的差异。 */
	@EventListener(ApplicationReadyEvent.class)
	public void reconcile() {
		try {
			apply(currentDays());
		} catch (Exception e) {
			log.warn("Reconcile audit log retention failed: {}", e.getMessage());
		}
	}

	private static boolean indexMissing(Exception e) {
		String message = String.valueOf(e.getMessage());
		return message.contains("IndexNotFound") || message.contains("cannot find index") || message.contains("index not found");
	}
}
