package com.tapdata.tm.module.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * 加载面板里的一条读回索引 + 归因/勾选建议。TAP-12057 · P2-2（方案 §3.8）。
 *
 * <p>由 {@link ServingIndexLoadPlanner} 产出、回给前端加载全表勾选面板（P2-6）。{@link #index} 是映射后的
 * 平台声明（供展示与采纳）；{@link #attribution} + {@link #checkable} + {@link #defaultChecked} 承载
 * §3.8.3 的默认勾选策略；{@link #attributionApi} 仅在「已被别的 API 收录」时给出来源 API 名。</p>
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class LoadedServingIndex implements Serializable {

	private static final long serialVersionUID = 1L;

	/** 映射后的平台声明（{@code name}/{@code unique}/有序字段含方向），供展示与采纳。 */
	private ServingIndex index;

	/** 归因（决定可勾/默认勾与标注）。 */
	private LoadedIndexAttribution attribution;

	/** 是否可勾选：{@code _id_} 与超范围项为 {@code false}（灰置）。 */
	private boolean checkable;

	/** 默认是否勾选（§3.8.3 策略；最终以人工为准）。 */
	private boolean defaultChecked;

	/** 「已被别的 API 收录」时的来源 API 名；其余为 {@code null}。 */
	private String attributionApi;
}
