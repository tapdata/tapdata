package com.tapdata.tm.module.dto;

import java.util.Locale;

/**
 * 落地失败的归类与人话化（纯函数）。TAP-12057 · P3-5（方案 §4 风险表）。
 *
 * <p>驱动原文直接端给人等于没说：prod 上最常见的那一种失败是「目标账号只有读写权限、没有 DDL」，
 * 它长成 {@code Command failed with error 13 (Unauthorized)} —— 看的人不知道该去改什么。方案 §4 给的
 * 兜底是「落地前权限探测<b>或</b>首次失败时明确报『需 DDL 权限』而非裸抛驱动错误」，这里走后者：
 * 探测要么得先建后删（对目标库有副作用、且本期无 drop 动作），要么得扩引擎载荷，代价都高于收益。</p>
 *
 * <p><b>只认两件事</b>：权限不足（要人去授权）、连接器不支持建索引（方案 §4「非 MongoDB 目标」：
 * 记 skipped-unsupported，<b>不算部署失败</b>）。其余一律 {@link Kind#UNKNOWN} 并<b>原样保留</b>原文
 * ——猜错比不猜更坏，唯一约束违约（11000）就该以它自己的样子出现在报告里。</p>
 *
 * <p>纯函数、不触库不触 Spring：可离线 TDD（{@code ServingIndexFailuresTest}）。</p>
 */
public final class ServingIndexFailures {

	/** 失败类别。 */
	public enum Kind {
		/** 目标账号没有建索引（DDL）权限——人去授权即可恢复，重跑幂等。 */
		PERMISSION_DENIED,
		/** 连接器没有 {@code CreateIndexFunction}——记 skipped-unsupported，不算部署失败（方案 §4）。 */
		UNSUPPORTED,
		/** 认不出：原文原样带走，交给人判断。 */
		UNKNOWN
	}

	/** 权限不足的措辞；PDK 是多方言抽象，故不止 Mongo 那一句。 */
	private static final String[] PERMISSION_MARKERS = {
			"not authorized", "unauthorized", "command denied", "access denied", "permission denied"
	};

	/** 引擎侧 {@code PdkIndexService#createIndex} 在连接器无 {@code CreateIndexFunction} 时抛的原话。 */
	private static final String UNSUPPORTED_MARKER = "does not support create index";

	private ServingIndexFailures() {
	}

	/**
	 * @param message 驱动/引擎给出的失败原文（可为 null）
	 * @return 失败类别；认不出即 {@link Kind#UNKNOWN}
	 */
	public static Kind classify(String message) {
		if (message == null || message.trim().isEmpty()) {
			return Kind.UNKNOWN;
		}
		String lower = message.toLowerCase(Locale.ROOT);
		if (lower.contains(UNSUPPORTED_MARKER)) {
			return Kind.UNSUPPORTED;
		}
		for (String marker : PERMISSION_MARKERS) {
			if (lower.contains(marker)) {
				return Kind.PERMISSION_DENIED;
			}
		}
		return Kind.UNKNOWN;
	}

	/**
	 * 归类后的人话，<b>原文一个字不吞</b>——排查最终还是要靠驱动原文（服务器地址、库名、命令都在里面）。
	 *
	 * @return 认得出的加一句「要去做什么」再接原文；认不出的原样返回（null → 空串）
	 */
	public static String explain(String message) {
		switch (classify(message)) {
			case PERMISSION_DENIED:
				return "target account lacks createIndex (DDL) privilege, grant it on the target connection "
						+ "or create the index manually: " + message;
			case UNSUPPORTED:
				return "connector cannot create indexes, recorded as unsupported (not a deploy failure): " + message;
			default:
				return message == null ? "" : message;
		}
	}
}
