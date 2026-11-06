package com.tapdata.tm.module.dto;

import lombok.Data;

import java.io.Serializable;

/**
 * CICD 部署计划表里的<b>一行</b>——一条将要（或已经）建到目标库的索引。TAP-12057 · P4-1（方案 §3.5）。
 *
 * <p><b>字段全是标量，且声明顺序即列顺序</b>：worker 的通用渲染脚本对「全标量」的条目走表格分支
 * （复杂对象会退化成逐条 {@code <details>}），列顺序取第一条的 {@code keys_unsorted}，
 * 而 Jackson 按字段声明顺序序列化。改字段顺序 = 改部署计划表的列顺序。</p>
 *
 * <p>{@link #keys} 把字段与方向拼成 {@code CUSTOMER_ID:-1} 这样的可读串：<b>方向必须一眼可见</b>——
 * 「声明降序、建出升序」正是 P0 那个缺陷（§2.4），审阅计划表的人得能当场看出来。</p>
 */
@Data
public class ServingIndexPlanRow implements Serializable {

	private static final long serialVersionUID = 1L;

	/** 目标环境的连接名。 */
	private String connection;

	/** 集合（表）名。 */
	private String table;

	/** 索引名（仅展示，不参与身份比对——身份 = 有序字段 + 方向）。 */
	private String name;

	/** 有序字段与方向，如 {@code CUSTOMER_ID:-1, CITY:1}。 */
	private String keys;

	/** 是否唯一索引。 */
	private boolean unique;

	/** 声明它的 API，多个以逗号相连。 */
	private String declaredBy;
}
