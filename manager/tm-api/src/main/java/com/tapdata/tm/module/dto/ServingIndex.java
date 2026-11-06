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

	/**
	 * 是否被本 API <b>声明</b>（= 抽屉里勾选）。
	 *
	 * <p>Module 里存的不止声明：读回后<b>未勾选</b>的物理索引也留在列表里，好让下次打开无需重新加载
	 * （2026-07-31 用户决定）。因此需要这一位来分辨——<b>只有 {@code true} 才是声明</b>：
	 * 部署（P3）只创建它，归因「已被本/他 API 收录」也只认它。</p>
	 *
	 * <p><b>{@code null} 按 true 解释</b>：此前只存勾选项、历史数据没有这个字段，默认真值才能保持语义不变。
	 * 判读一律走 {@link ServingIndexes#isCollected}，别在各处自行 {@code Boolean.TRUE.equals(...)}。</p>
	 */
	private Boolean collected;

	/**
	 * 上次读回时的归因（{@code LoadedIndexAttribution} 名，如 {@code MATCHES_API}）。<b>纯展示</b>：
	 * 不参与索引身份、不参与部署判定。存下来是为了 load + save 之后<b>下次打开无需再加载即可看到标签</b>
	 * （2026-07-31 用户决定）。
	 *
	 * <p><b>会过期</b>：它是「上次加载那一刻」的判断——别的 API 后来也收录了同一条索引、或查询条件被改，
	 * 都要等下次加载才刷新。判定性用途一律现算，别读这里。</p>
	 */
	private String attribution;

	/** 归因为「已被别的 API 收录」时的来源 API 名。纯展示，同样可能过期。 */
	private String attributionApi;

	/**
	 * 三参构造：其余字段留空（{@code collected} 空 = 按已收录解释，见字段注释）。
	 * 显式保留它是为了不打断既有调用面——{@code @AllArgsConstructor} 会随字段增减改变元数。
	 */
	public ServingIndex(String name, Boolean unique, List<ServingIndexField> fields) {
		this(name, unique, fields, null, null, null);
	}
}
