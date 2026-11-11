package com.tapdata.tm.modules.param;

import com.tapdata.tm.module.dto.ServingIndex;
import lombok.Data;

import java.util.List;

/**
 * {@code PATCH /api/Modules/{id}/serving-indexes} 的请求体（TAP-12057 · 索引 tab 与编辑态解绑）。
 *
 * <p>刻意包一层对象而不是裸 {@code List<ServingIndex>}：留出后续加字段（如乐观锁版本号）的余地，
 * 且与 {@code LoadServingIndexesRequest} 同形。</p>
 *
 * <p>{@code null} 与空列表都表示「本 API 一条索引都不收录」——由
 * {@code ModulesService.updateServingIndexes} 统一写成空列表，不能当成「没传」跳过写入，
 * 否则取消最后一条勾选就存不下去。</p>
 */
@Data
public class ServingIndexesParam {
    List<ServingIndex> servingIndexes;
}
