package com.tapdata.tm.module.dto;

/**
 * 加载面板里一条读回索引的归因。TAP-12057 · P2-2（方案 §3.8.1 / §3.8.3）。
 *
 * <p>归因只影响「可勾 / 默认勾」与展示标注，<b>绝不</b>把索引从清单里拿掉（全表可见，§3.8.1）。
 * 取值顺序即默认勾选的「首个命中即定」优先级（§3.8.3）。</p>
 */
public enum LoadedIndexAttribution {

	/** {@code _id_} 默认索引：MongoDB 自建，灰置不可勾。 */
	SYSTEM_INDEX,

	/** text / wildcard（{@code _fts}/{@code _ftsx}/{@code $**}）：超出支持范围，灰置不可勾、仅报告不采纳（ADR-0004）。 */
	UNSUPPORTED,

	/** 匹配本 API 的 Path 查询模式：默认勾（即便同时是流水线索引，§3.8.3 行2）。 */
	MATCHES_API,

	/** 上次已收录进本 API：默认勾（保持）。 */
	COLLECTED_BY_THIS_API,

	/** 已被别的 API 收录（他 Module）：默认不勾，标注「已被 API X 收录」（双写无意义）。 */
	COLLECTED_BY_OTHER_API,

	/** 其余（纯流水线不匹配 / 手工建但不匹配 Path / 未分类）：默认不勾，可见可手勾。 */
	UNCLASSIFIED
}
