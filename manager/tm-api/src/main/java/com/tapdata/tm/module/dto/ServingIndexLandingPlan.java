package com.tapdata.tm.module.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * 一个 (连接, 集合) 上的落地计划：将创建 / 将跳过 / 目标多出。TAP-12057 · P3-2。
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ServingIndexLandingPlan implements Serializable {

	private static final long serialVersionUID = 1L;

	private List<ServingIndex> create = new ArrayList<>();

	private List<SkippedServingIndex> skip = new ArrayList<>();

	private List<ServingIndex> extra = new ArrayList<>();
}
