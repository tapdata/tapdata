package com.tapdata.tm.module.dto;

import lombok.Data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * 一个 {@code (连接, 集合)} 的分桶报告。TAP-12057 · P3-4（方案 §3.4 / <b>ADR-0008</b>）。
 */
@Data
public class ServingIndexTargetReport implements Serializable {

	private static final long serialVersionUID = 1L;

	/** MongoDB 单集合索引数硬上限。 */
	public static final int HARD_LIMIT = 64;
	/** 告警阈值（方案：≥50 提示、≥60 严重）。 */
	public static final int WARN_AT = 50;
	public static final int SEVERE_AT = 60;

	/** 索引总数水位。 */
	public enum IndexCountLevel {
		OK,
		/** ≥50：提示。 */
		APPROACHING,
		/** ≥60：严重，离 64 上限很近。 */
		SEVERE,
		/** 落地后将达到或超过 64：<b>预检失败</b>，该建的建不上（ADR-0008）。 */
		EXCEEDS_LIMIT
	}

	private String connectionId;

	private String connectionName;

	private String tableName;

	/** 将创建。 */
	private List<ServingIndexReportEntry> create = new ArrayList<>();

	/** 将跳过（目标已有同字段集索引）。 */
	private List<ServingIndexReportEntry> skip = new ArrayList<>();

	/** 目标多出：<b>仅列出，绝不删</b>（ADR-0005 只加不删）。 */
	private List<ServingIndexReportEntry> extra = new ArrayList<>();

	/** 目标现有索引数（= 将跳过 + 目标多出）。 */
	private int existingCount;

	/** 落地后预计索引数（= 现有 + 将创建）。 */
	private int projectedCount;

	private IndexCountLevel level = IndexCountLevel.OK;

	/** 本 target 读回/建索引失败时的原因；成功为 {@code null}（失败时三桶可能为空，别读成「无事可做」）。 */
	private String error;
}
