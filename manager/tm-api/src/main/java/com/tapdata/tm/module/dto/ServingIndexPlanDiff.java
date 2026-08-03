package com.tapdata.tm.module.dto;

import lombok.Data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * 索引腿的部署计划表，形状对齐 worker 通用脚本读的 {@code add / update / delete}。
 * TAP-12057 · P4-1（方案 §3.5）。
 *
 * <p><b>{@link #update} 与 {@link #delete} 恒空</b>，且这是设计而非未实现：索引身份 = 有序字段 + 方向，
 * 改一个字就是另一条索引，没有「原地改」这回事；目标多出的索引<b>只列出、绝不删</b>
 * （只加不删，<b>ADR-0005</b>；回滚侧同样零处理，<b>ADR-0008</b>）。两个空桶留在响应里是为了
 * 让 worker 那套通用渲染照常工作，也让读计划表的人看见「这一腿不会删任何东西」。</p>
 */
@Data
public class ServingIndexPlanDiff implements Serializable {

	private static final long serialVersionUID = 1L;

	/** 将创建（preview）／已建成（import）的索引。 */
	private List<ServingIndexPlanRow> add = new ArrayList<>();

	/** 恒空，见类注释。 */
	private List<ServingIndexPlanRow> update = new ArrayList<>();

	/** 恒空，见类注释。 */
	private List<ServingIndexPlanRow> delete = new ArrayList<>();
}
