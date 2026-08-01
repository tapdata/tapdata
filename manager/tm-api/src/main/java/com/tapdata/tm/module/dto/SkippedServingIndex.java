package com.tapdata.tm.module.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * 一条「将跳过」的声明：目标环境已有同「有序字段+方向」的索引。TAP-12057 · P3-2。
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class SkippedServingIndex implements Serializable {

	private static final long serialVersionUID = 1L;

	private ServingIndex declared;

	private ServingIndex existing;

	private boolean uniqueMismatch;
}
