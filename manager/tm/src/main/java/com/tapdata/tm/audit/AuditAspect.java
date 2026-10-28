package com.tapdata.tm.audit;

import com.tapdata.tm.base.dto.MutiResponseMessage;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.base.security.LoginUserResolver;
import com.tapdata.tm.config.security.UserDetail;
import jakarta.servlet.http.HttpServletRequest;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.springframework.context.expression.MethodBasedEvaluationContext;
import org.springframework.core.DefaultParameterNameDiscoverer;
import org.springframework.core.annotation.Order;
import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.stereotype.Component;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 统一审计切面：按 @Audited 声明记录一次业务操作的结果。审计异常一律吞掉并由服务计数，不改变业务返回。
 * 同步操作一条最终事件；异步操作记录受理事件并登记 Operation ID，最终结果由 SYSTEM 事件补齐；
 * 请求校验阶段失败只产生一条失败事件。
 */
@Slf4j
@Aspect
@Component
@Order(10)
public class AuditAspect {
	private static final ExpressionParser PARSER = new SpelExpressionParser();
	private static final DefaultParameterNameDiscoverer NAMES = new DefaultParameterNameDiscoverer();
	private final Map<String, Expression> expressions = new ConcurrentHashMap<>();

	private final AuditEventService auditEventService;
	private final AuditObjectResolver objectResolver;
	private final AuditOperationRegistry operationRegistry;

	public AuditAspect(AuditEventService auditEventService, AuditObjectResolver objectResolver, AuditOperationRegistry operationRegistry) {
		this.auditEventService = auditEventService;
		this.objectResolver = objectResolver;
		this.operationRegistry = operationRegistry;
	}

	/** 动态 Action 表达式返回该值时表示本次调用不是用户业务操作（例如引擎自身的状态上报），不记录。 */
	public static final String SKIP = "-";

	@Around("@annotation(audited)")
	public Object around(ProceedingJoinPoint joinPoint, Audited audited) throws Throwable {
		if (isSkipped(joinPoint, audited)) {
			return joinPoint.proceed();
		}
		AuditAction action = resolveAction(joinPoint, audited);
		List<String> ids = safeIds(joinPoint, audited, null);
		Map<String, String> names = prefetchNames(action, ids, joinPoint, audited);
		AuditRequestContext context = AuditRequestContext.current();
		if (context != null) {
			context.setAuditedDepth(context.getAuditedDepth() + 1);
		}
		Object result;
		try {
			result = joinPoint.proceed();
		} catch (Throwable e) {
			safeRecord(() -> recordFailure(action, joinPoint, audited, ids, names, e));
			throw e;
		} finally {
			if (context != null) {
				context.setAuditedDepth(context.getAuditedDepth() - 1);
			}
		}
		safeRecord(() -> recordResult(action, joinPoint, audited, ids, names, result));
		return result;
	}

	private boolean isSkipped(ProceedingJoinPoint jp, Audited audited) {
		if (StringUtils.isBlank(audited.action())) {
			return false;
		}
		try {
			return SKIP.equals(String.valueOf(evaluate(audited.action(), jp, null)));
		} catch (Exception e) {
			return false;
		}
	}

	private AuditAction resolveAction(ProceedingJoinPoint jp, Audited audited) {
		if (StringUtils.isNotBlank(audited.action())) {
			try {
				AuditAction dynamic = AuditAction.ofKey(String.valueOf(evaluate(audited.action(), jp, null)));
				if (dynamic != null) {
					return dynamic;
				}
			} catch (Exception e) {
				log.debug("Evaluate audit action failed: {}", audited.action(), e);
			}
		}
		if (StringUtils.isBlank(audited.alternateWhen())) {
			return audited.value();
		}
		try {
			return Boolean.TRUE.equals(evaluate(audited.alternateWhen(), jp, null)) ? audited.alternate() : audited.value();
		} catch (Exception e) {
			return audited.value();
		}
	}

	private void recordFailure(AuditAction action, ProceedingJoinPoint jp, Audited audited, List<String> ids, Map<String, String> names, Throwable e) {
		AuditEvent event = AuditEvent.of(action, AuditOutcome.FAILED);
		applyObject(event, ids, names);
		event.setReason(reasonOf(e));
		event.setSummary(summary(jp, audited, null));
		auditEventService.record(event, loginUser());
	}

	private void recordResult(AuditAction action, ProceedingJoinPoint jp, Audited audited, List<String> ids, Map<String, String> names, Object result) {
		Object data = result instanceof ResponseMessage<?> rm ? rm.getData() : result;
		List<String> resolvedIds = ids.isEmpty() ? safeIds(jp, audited, data) : ids;
		AuditEvent event;
		if (result instanceof ResponseMessage<?> rm && !ResponseMessage.OK.equals(rm.getCode())) {
			event = AuditEvent.of(action, AuditOutcome.FAILED);
			event.setReason(StringUtils.defaultIfBlank(rm.getCode(), rm.getMessage()));
			applyObject(event, resolvedIds, names);
		} else if (isBatchResult(data)) {
			event = batchEvent(action, data, names);
		} else {
			event = AuditEvent.of(action, action.isAsync() ? AuditOutcome.ACCEPTED : AuditOutcome.SUCCESS);
			applyObject(event, resolvedIds, names);
		}
		if (action.isAsync() && event.getOutcome() != AuditOutcome.FAILED) {
			event.setPhase(AuditPhase.REQUEST);
		}
		event.setSummary(summary(jp, audited, data));
		String operationId = auditEventService.currentOperationId();
		event.setOperationId(operationId);
		auditEventService.record(event, loginUser());
		if (event.getPhase() == AuditPhase.REQUEST) {
			registerPending(action, event, operationId);
		}
	}

	private void registerPending(AuditAction action, AuditEvent event, String operationId) {
		if (event.getBatch() != null) {
			for (AuditBatchItem item : event.getBatch().getItems()) {
				if (item.getOutcome() != AuditOutcome.FAILED) {
					operationRegistry.register(action.getObjectType(), item.getObjectId(), operationId, action, item.getObjectName());
				}
			}
		} else if (event.getObjectId() != null) {
			for (String id : event.getObjectId().split(",")) {
				operationRegistry.register(action.getObjectType(), id, operationId, action, event.getObjectName());
			}
		}
	}

	private AuditEvent batchEvent(AuditAction action, Object data, Map<String, String> names) {
		AuditBatch batch = new AuditBatch();
		for (Object element : (Collection<?>) data) {
			MutiResponseMessage item = (MutiResponseMessage) element;
			boolean ok = ResponseMessage.OK.equals(item.getCode());
			String name = names.getOrDefault(item.getId(), objectResolver.resolveName(action.getObjectType(), item.getId()));
			batch.add(new AuditBatchItem(item.getId(), name,
					ok ? (action.isAsync() ? AuditOutcome.ACCEPTED : AuditOutcome.SUCCESS) : AuditOutcome.FAILED,
					ok ? null : AuditRedactor.text(StringUtils.defaultIfBlank(item.getMessage(), item.getCode()))));
		}
		AuditOutcome outcome = batch.outcome();
		if (outcome == AuditOutcome.SUCCESS && action.isAsync()) {
			outcome = AuditOutcome.ACCEPTED;
		}
		AuditEvent event = AuditEvent.of(action, outcome);
		event.setBatch(batch);
		return event;
	}

	static boolean isBatchResult(Object data) {
		if (!(data instanceof Collection<?> collection) || collection.isEmpty()) {
			return false;
		}
		return collection.stream().allMatch(MutiResponseMessage.class::isInstance);
	}

	private void applyObject(AuditEvent event, List<String> ids, Map<String, String> names) {
		if (ids.isEmpty()) {
			return;
		}
		if (ids.size() == 1) {
			String id = ids.get(0);
			event.object(id, names.getOrDefault(id, objectResolver.resolveName(event.getObjectType(), id)));
			return;
		}
		AuditBatch batch = new AuditBatch();
		for (String id : ids) {
			batch.add(new AuditBatchItem(id, names.getOrDefault(id, objectResolver.resolveName(event.getObjectType(), id)),
					event.getOutcome(), event.getOutcome() == AuditOutcome.FAILED ? event.getReason() : null));
		}
		event.setBatch(batch);
	}

	private Map<String, String> prefetchNames(AuditAction action, List<String> ids, ProceedingJoinPoint jp, Audited audited) {
		Map<String, String> names = new HashMap<>();
		try {
			if (StringUtils.isNotBlank(audited.objectName()) && ids.size() == 1) {
				Object value = evaluate(audited.objectName(), jp, null);
				if (value != null) {
					names.put(ids.get(0), String.valueOf(value));
				}
			}
			for (String id : ids) {
				names.computeIfAbsent(id, key -> objectResolver.resolveName(action.getObjectType(), key));
			}
			names.values().removeIf(v -> v == null);
		} catch (Exception e) {
			log.debug("Prefetch audit object name failed", e);
		}
		return names;
	}

	private List<String> safeIds(ProceedingJoinPoint jp, Audited audited, Object data) {
		List<String> ids = new ArrayList<>();
		if (StringUtils.isBlank(audited.objectId())) {
			return ids;
		}
		try {
			Object value = evaluate(audited.objectId(), jp, data);
			if (value instanceof Collection<?> collection) {
				collection.stream().filter(v -> v != null).forEach(v -> ids.add(String.valueOf(v)));
			} else if (value instanceof Object[] array) {
				for (Object v : array) {
					if (v != null) {
						ids.add(String.valueOf(v));
					}
				}
			} else if (value != null && StringUtils.isNotBlank(String.valueOf(value))) {
				for (String id : String.valueOf(value).split(",")) {
					if (StringUtils.isNotBlank(id)) {
						ids.add(id.trim());
					}
				}
			}
		} catch (Exception e) {
			log.debug("Evaluate audit object id failed: {}", audited.objectId(), e);
		}
		return ids;
	}

	private String summary(ProceedingJoinPoint jp, Audited audited, Object data) {
		if (StringUtils.isBlank(audited.summary())) {
			return null;
		}
		try {
			Object value = evaluate(audited.summary(), jp, data);
			return value == null ? null : AuditRedactor.text(String.valueOf(value));
		} catch (Exception e) {
			log.debug("Evaluate audit summary failed: {}", audited.summary(), e);
			return null;
		}
	}

	private Object evaluate(String expression, ProceedingJoinPoint jp, Object data) {
		MethodSignature signature = (MethodSignature) jp.getSignature();
		MethodBasedEvaluationContext context = new MethodBasedEvaluationContext(jp.getTarget(), signature.getMethod(), jp.getArgs(), NAMES);
		context.setVariable("result", data);
		context.setVariable("request", currentRequest());
		return expressions.computeIfAbsent(expression, PARSER::parseExpression).getValue(context);
	}

	public static String reasonOfPublic(Throwable e) {
		return reasonOf(e);
	}

	static String reasonOf(Throwable e) {
		if (e instanceof BizException biz && StringUtils.isNotBlank(biz.getErrorCode())) {
			return biz.getErrorCode();
		}
		return e == null ? null : e.getClass().getSimpleName();
	}

	static HttpServletRequest currentRequest() {
		if (RequestContextHolder.getRequestAttributes() instanceof ServletRequestAttributes attributes) {
			return attributes.getRequest();
		}
		return null;
	}

	public static UserDetail loginUser() {
		HttpServletRequest request = currentRequest();
		Object user = request == null ? null : request.getAttribute(LoginUserResolver.LOGIN_USER_ATTRIBUTE);
		return user instanceof UserDetail detail ? detail : null;
	}

	private void safeRecord(Runnable runnable) {
		try {
			runnable.run();
		} catch (Exception e) {
			log.error("Audit record failed", e);
		}
	}
}
