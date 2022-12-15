package com.tapdata.entity.inspect;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;
import java.util.List;

/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2020/9/7 10:26 上午
 * @description
 */
@Getter
@Setter
@ToString
@EqualsAndHashCode
public class InspectDataSource implements Serializable {

	private String connectionId;  // "",  // 连接ID
	private String connectionName;  // 连接名称
	private String table;      // "IV_JW_MATRL",  // 不能带 owner，默认使用connection的owner
	private String sortColumn;    // "INVNT_ID",  // 必填，提示使用索引字段
	private String direction;    // "ASC",  // 固定值 ASC，后端强制ASC
	private List<String> columns;  // [  // 必须带有排序字段，前端可以做限制，后端要自动适配   "INVNT_ID", "...", // 需要校验比对的列名称，必填，顺序与目标字段一致
	private int limit;        // null, // 取多少行数据，默认全部
	private int skip;        // null,  // 跳过多少行数据
	private String where;      // "",   // sql 查询条件，直接拼接到sql中
	private String initialOffset;      // null,   // 自定义 sql 偏移量参数

	// 增量校验时使用，增量运行配置
	private InspectCdcRunProfiles cdcRunProfiles;
	private String nodeId;
	private String nodeName;
}
