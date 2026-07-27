package com.tapdata.tm.module.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * 服务型索引的单个字段：字段名 + 方向。TAP-12057 · P2-1。
 *
 * <p>方向语义与 PDK {@code TapIndexField.fieldAsc} 及 P0 连接器修复一致：
 * {@code asc == FALSE → 降序(-1)}，{@code TRUE / null → 升序(1)}。复合索引里字段的先后是语义、不可重排。</p>
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ServingIndexField implements Serializable {

	private static final long serialVersionUID = 1L;

	/** 字段名。 */
	private String field;

	/** 方向：{@code FALSE}=降序(-1)，{@code TRUE}/{@code null}=升序(1)（同 P0 语义）。 */
	private Boolean asc;
}
