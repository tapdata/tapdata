package com.tapdata.tm.audit.service;

import com.tapdata.tm.audit.AuditChange;
import com.tapdata.tm.audit.AuditEvent;
import com.tapdata.tm.audit.AuditEventService;
import com.tapdata.tm.audit.AuditRedactor;
import com.tapdata.tm.audit.AuditRequestContext;
import com.tapdata.tm.audit.AuditSource;
import com.tapdata.tm.audit.AuditValues;
import com.tapdata.tm.accessToken.dto.AuthType;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.userLog.constant.UserLogType;
import com.tapdata.tm.userLog.dto.User;
import com.tapdata.tm.userLog.entity.UserLogs;
import com.tapdata.tm.utils.ThreadLocalUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@Service
public class AuditEventServiceImpl implements AuditEventService {
	private static final long FAILURE_LOG_INTERVAL_MS = 60_000L;

	private final MongoTemplate mongoTemplate;
	private final AtomicLong failureCount = new AtomicLong();
	private final AtomicLong lastFailureAt = new AtomicLong();
	private final AtomicLong lastFailureLoggedAt = new AtomicLong();

	public AuditEventServiceImpl(MongoTemplate mongoTemplate) {
		this.mongoTemplate = mongoTemplate;
	}

	@Override
	public String record(AuditEvent event, UserDetail userDetail) {
		if (event == null || event.getAction() == null || event.getOutcome() == null) {
			return null;
		}
		try {
			UserLogs doc = toDocument(event, userDetail);
			mongoTemplate.insert(doc);
			AuditRequestContext context = AuditRequestContext.current();
			if (context != null) {
				context.setRecorded(true);
			}
			return doc.getEventId();
		} catch (Exception e) {
			onFailure(event, e);
			return null;
		}
	}

	@Override
	public String currentOperationId() {
		AuditRequestContext context = AuditRequestContext.current();
		if (context != null && StringUtils.isNotBlank(context.getOperationId())) {
			return context.getOperationId();
		}
		String reqId = ThreadLocalUtils.get(ThreadLocalUtils.REQUEST_ID);
		return StringUtils.isNotBlank(reqId) ? reqId : UUID.randomUUID().toString();
	}

	/** 自上次启动以来的审计写入失败次数，供审计页面提示。 */
	public long getFailureCount() {
		return failureCount.get();
	}

	public Date getLastFailureAt() {
		long at = lastFailureAt.get();
		return at == 0 ? null : new Date(at);
	}

	UserLogs toDocument(AuditEvent event, UserDetail userDetail) {
		AuditRequestContext context = AuditRequestContext.current();
		Date now = event.getTimestamp() != null ? event.getTimestamp() : new Date();
		UserLogs doc = new UserLogs();
		doc.setSchemaVersion(AuditValues.SCHEMA_VERSION);
		doc.setType(AuditValues.SCHEMA_TYPE);
		doc.setEventId(StringUtils.defaultIfBlank(event.getEventId(), new ObjectId().toHexString()));
		doc.setOperationId(StringUtils.defaultIfBlank(event.getOperationId(), currentOperationId()));
		doc.setPhase(event.getPhase().name());
		doc.setAction(event.getAction().getKey());
		doc.setEventType(event.getAction().getEventType().name());
		doc.setOutcome(event.getOutcome().name());
		doc.setDetailLevel(event.getDetailLevel() == null ? event.getAction().getDetailLevel().name() : event.getDetailLevel().name());
		doc.setTimestamp(now);
		doc.setCreateAt(now);
		doc.setLastUpdAt(now);

		fillActor(doc, event, userDetail);
		fillSource(doc, event, context);

		doc.setObjectType(StringUtils.defaultIfBlank(event.getObjectType(), event.getAction().getObjectType()));
		doc.setObjectId(event.getObjectId());
		doc.setObjectName(AuditRedactor.text(event.getObjectName()));
		doc.setReason(AuditRedactor.text(event.getReason()));
		doc.setSummary(AuditRedactor.text(event.getSummary()));
		doc.setChanges(sanitize(event.getChanges()));
		if ("DIFF".equals(doc.getDetailLevel()) && doc.getChanges() == null) {
			// 没有可靠取得字段差异时如实降级为基础审计，页面显示「该操作暂不提供字段级差异」
			doc.setDetailLevel("BASIC");
		}
		doc.setBatch(event.getBatch());
		doc.setNode(event.getNode());
		Map<String, String> ctx = new LinkedHashMap<>();
		if (event.getContext() != null) {
			event.getContext().forEach((k, v) -> ctx.put(k, AuditRedactor.isSecretKey(k) ? AuditRedactor.MASK : AuditRedactor.text(v)));
		}
		doc.setContext(ctx.isEmpty() ? null : ctx);
		if (ObjectId.isValid(String.valueOf(event.getObjectId()))) {
			doc.setSourceId(new ObjectId(event.getObjectId()));
		}
		return doc;
	}

	private void fillActor(UserLogs doc, AuditEvent event, UserDetail userDetail) {
		if (event.getSource() == AuditSource.SYSTEM || AuditValues.SYSTEM.equals(event.getUserId())) {
			doc.setUserId(AuditValues.SYSTEM);
			doc.setUsername(AuditValues.SYSTEM);
			return;
		}
		if (userDetail != null) {
			doc.setUserId(userDetail.getUserId());
			String name = StringUtils.defaultIfBlank(userDetail.getUsername(), userDetail.getEmail());
			doc.setUsername(StringUtils.defaultIfBlank(event.getUsername(), name));
			doc.setAuthType(StringUtils.defaultIfBlank(event.getAuthType(), userDetail.getAuthType()));
			User user = new User();
			user.setUserId(userDetail.getUserId());
			user.setEmail(userDetail.getEmail());
			user.setUsername(userDetail.getUsername());
			doc.setUser(user);
			doc.setCustomId(userDetail.getCustomerId());
		} else if (StringUtils.isNotBlank(event.getUserId())) {
			doc.setUserId(event.getUserId());
			doc.setUsername(StringUtils.defaultIfBlank(event.getUsername(), event.getUserId()));
			doc.setAuthType(event.getAuthType());
		} else {
			doc.setUserId(AuditValues.UNAUTHENTICATED);
			doc.setUsername(AuditValues.UNAUTHENTICATED);
			doc.setAuthType(event.getAuthType());
		}
		doc.setAttemptedAccount(event.getAttemptedAccount());
		doc.setCredentialRef(event.getCredentialRef());
	}

	private void fillSource(UserLogs doc, AuditEvent event, AuditRequestContext context) {
		AuditSource source = event.getSource();
		if (source == null) {
			source = AuthType.ACCESS_CODE.getValue().equals(doc.getAuthType()) || "openapi".equals(doc.getAuthType())
					? AuditSource.OPENAPI : (context == null ? AuditSource.SYSTEM : AuditSource.UI);
		}
		doc.setSource(source.name());
		if (source == AuditSource.SYSTEM && event.getSourceIp() == null) {
			String nodeIp = event.getNode() == null ? null : event.getNode().getIp();
			doc.setSourceIp(StringUtils.defaultIfBlank(nodeIp, AuditValues.LOCAL_SYSTEM));
			return;
		}
		if (event.getSourceIp() != null) {
			doc.setSourceIp(event.getSourceIp());
			doc.setSourceIpNote(event.getSourceIpNote());
		} else if (context != null) {
			doc.setSourceIp(StringUtils.defaultIfBlank(context.getSourceIp(), AuditValues.UNAVAILABLE));
			doc.setSourceIpNote(context.getSourceIpNote());
			doc.setForwardedFor(AuditRedactor.text(context.getForwardedFor()));
			doc.setUserAgent(AuditRedactor.text(context.getUserAgent()));
		} else {
			doc.setSourceIp(AuditValues.UNAVAILABLE);
			doc.setSourceIpNote("audit_ip_note_unavailable");
		}
	}

	private List<AuditChange> sanitize(List<AuditChange> changes) {
		if (changes == null || changes.isEmpty()) {
			return null;
		}
		List<AuditChange> result = new ArrayList<>();
		for (AuditChange change : changes) {
			if (change == null) {
				continue;
			}
			if (change.getSecretState() != null || AuditRedactor.isSecretKey(change.getField())) {
				String state = change.getSecretState() != null ? change.getSecretState()
						: AuditRedactor.secretState(change.getBefore(), change.getAfter());
				AuditChange secret = AuditChange.secret(change.getField(), state);
				secret.setLabel(change.getLabel());
				result.add(secret);
			} else {
				AuditChange normal = new AuditChange(change.getField(), AuditRedactor.text(change.getBefore()), AuditRedactor.text(change.getAfter()), null);
				normal.setLabel(change.getLabel());
				result.add(normal);
			}
		}
		return result;
	}

	private void onFailure(AuditEvent event, Exception e) {
		failureCount.incrementAndGet();
		long now = System.currentTimeMillis();
		lastFailureAt.set(now);
		long last = lastFailureLoggedAt.get();
		if (now - last >= FAILURE_LOG_INTERVAL_MS && lastFailureLoggedAt.compareAndSet(last, now)) {
			log.error("Audit event write failed (total failures since start: {}), action={}, outcome={}",
					failureCount.get(), event.getAction().getKey(), event.getOutcome(), e);
		}
	}

	/** 兼容旧类型标记，供查询区分。 */
	public static String legacyType() {
		return UserLogType.USER_OPERATION.getValue();
	}
}
