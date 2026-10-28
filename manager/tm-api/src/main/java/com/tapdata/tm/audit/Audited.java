package com.tapdata.tm.audit;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 标记一个会产生审计事件的接口。结果由切面按返回码、异常和批量明细判定；对象标识与名称使用 SpEL，
 * 变量为方法参数名、#result（返回值的 data）与 #request。表达式只取业务字段，不得引用请求体整体。
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Audited {
	AuditAction value();

	/** 对象 ID 表达式，如 "#id"；批量接口可返回集合。 */
	String objectId() default "";

	/** 对象名称表达式；为空时按对象类型从库中解析，删除操作在执行前预取。 */
	String objectName() default "";

	/** 额外的安全上下文摘要（如导入文件名、表数量），返回 Map 或字符串。 */
	String summary() default "";

	/** 条件表达式为 true 时改用 alternate 动作，例如 stop 接口的 force=true 记为强制停止。 */
	String alternateWhen() default "";

	AuditAction alternate() default AuditAction.ACCESS_DENIED;

	/** 返回 Action key 的表达式；结果是已登记的 Action 时覆盖 value()，用于一个接口对应多个业务动作的情况。 */
	String action() default "";
}
