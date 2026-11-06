package com.tapdata.tm.module.dto;

import lombok.Data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * 分桶报告里的一条索引。TAP-12057 · P3-4。
 *
 * <p>三个桶（将创建 / 将跳过 / 目标多出）共用这一条形状，靠所在桶区分动作——报告是给人看的，
 * 同一列表里三种形状会让人对不上账。</p>
 */
@Data
public class ServingIndexReportEntry implements Serializable {

	private static final long serialVersionUID = 1L;

	/** 索引名：「将创建」是推导名，其余是目标环境的实际名。 */
	private String name;

	/** 有序字段（含方向）——身份本身，报告里必须显示，名字靠不住。 */
	private List<ServingIndexField> fields = new ArrayList<>();

	/** 唯一索引：报告里要高亮（建失败风险最高的一类，存量违约会 11000）。 */
	private boolean unique;

	/**
	 * 仅「将跳过」桶：目标已有同字段集索引但 {@code unique} 与声明不一致。
	 * <b>只是注记</b>——只加不删语义下平台从不 drop 重建去修 unique（ADR-0005）。
	 */
	private boolean uniqueMismatch;

	/** 声明这条索引的 API 名；「目标多出」桶为空（它不是任何 API 声明的）。 */
	private List<String> sourceApis = new ArrayList<>();
}
