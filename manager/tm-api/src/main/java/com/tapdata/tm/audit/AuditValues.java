package com.tapdata.tm.audit;

/** 字段的明确替代值，避免以空值伪装字段完整（需求 7.3 / 10.3 / 10.4）。 */
public final class AuditValues {
	public static final String SCHEMA_TYPE = "auditEvent";
	public static final int SCHEMA_VERSION = 2;
	public static final String UNAUTHENTICATED = "UNAUTHENTICATED";
	public static final String SYSTEM = "SYSTEM";
	public static final String UNAVAILABLE = "UNAVAILABLE";
	public static final String LOCAL_SYSTEM = "LOCAL_SYSTEM";
	public static final String SECRET_SET = "SET";
	public static final String SECRET_REPLACED = "REPLACED";
	public static final String SECRET_CLEARED = "CLEARED";
	public static final String SECRET_UNCHANGED = "UNCHANGED";

	private AuditValues() {
	}
}
