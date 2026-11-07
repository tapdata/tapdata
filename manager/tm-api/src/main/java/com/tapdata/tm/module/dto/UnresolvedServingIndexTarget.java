package com.tapdata.tm.module.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

/**
 * 一条<b>有声明却落不了地</b>的记录。TAP-12057 · P3-1。
 *
 * <p>唯一的正确处置是「记下来、报出去」：目标连接猜不出来就不能建索引——猜错的代价是把索引建到别的库
 * （<b>ADR-0002</b> 的错库红线）。P3-5 据此响亮报错并汇总。</p>
 *
 * <p>没有声明的 API 不进这里：它本就无事可做，连接解析不出来也不该拖累导入。</p>
 */
@Data
@AllArgsConstructor
public class UnresolvedServingIndexTarget implements Serializable {

	private static final long serialVersionUID = 1L;

	/** 落不了地的原因。 */
	public enum Reason {
		/** conMap 里没有这个连接——导出包的连接没随包进来 / 目标环境没有它。 */
		CONNECTION_UNRESOLVED,
		/** Module 上没有表名，推不出目标集合。 */
		TABLE_NAME_MISSING
	}

	/** API（Module）名，报错时给人看的第一现场。 */
	private String apiName;

	/** Module id。 */
	private String moduleId;

	/** 解析失败时用的连接 id（原样保留，便于对着导出包排查）。 */
	private String connectionId;

	/** 集合名（缺失时为原值，可能是 null/空白）。 */
	private String tableName;

	/** 本该落地的声明条数。 */
	private int indexCount;

	private Reason reason;
}
