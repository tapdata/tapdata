package com.tapdata.tm.audit;

import lombok.Data;

/** 一次请求的审计上下文，由请求过滤器建立，切面与业务代码只读使用。 */
@Data
public class AuditRequestContext {
	private static final ThreadLocal<AuditRequestContext> HOLDER = new ThreadLocal<>();

	private String operationId;
	private String sourceIp;
	/** sourceIp 为 UNAVAILABLE 或经可信代理解析时的说明。 */
	private String sourceIpNote;
	/** 客户端提交的原始转发头，未经验证，仅供调查参考。 */
	private String forwardedFor;
	private String userAgent;
	/** 请求携带了凭据（token、access code 或签名），用于区分认证失败与匿名访问。 */
	private boolean credentialPresented;
	/** 本次请求已由业务代码显式记录审计事件，切面不再重复记录。 */
	private boolean recorded;
	/** 当前处于 @Audited 方法内的层数；旧 addUserLog 在作用域内不再重复记录。 */
	private int auditedDepth;

	public boolean inAuditedScope() {
		return auditedDepth > 0;
	}

	public static AuditRequestContext current() {
		return HOLDER.get();
	}

	public static void set(AuditRequestContext context) {
		HOLDER.set(context);
	}

	public static void clear() {
		HOLDER.remove();
	}
}
