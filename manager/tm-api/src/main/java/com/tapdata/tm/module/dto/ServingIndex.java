package com.tapdata.tm.module.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;

/**
 * 平台收录的一条服务型索引声明。TAP-12057 · P2-1。
 *
 * <p>存 {@link ModulesDto#getServingIndexes()}，作为人工编写的产物随 Module 进导出包 /
 * CICD（见 <b>ADR-0001</b>）。</p>
 *
 * <p><b>身份 = 有序字段 + 方向（仅此）</b>：{@code name} 仅用于展示、{@code unique} 仅作创建索引时的参数，
 * 二者都不参与索引身份比对（项目红线）。刻意不含 Mongo 原始元信息（{@code v}/{@code ns} 等），
 * 以免污染 Module 全量 diff（ADR-0001 follow-up）。</p>
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ServingIndex implements Serializable {

	private static final long serialVersionUID = 1L;

	/** 索引名，仅展示，不参与身份比对。 */
	private String name;

	/** 是否唯一索引，仅作创建时参数，不参与身份比对。 */
	private Boolean unique;

	/** 有序字段（含方向）。顺序是语义、不可重排。 */
	private List<ServingIndexField> fields;
}
