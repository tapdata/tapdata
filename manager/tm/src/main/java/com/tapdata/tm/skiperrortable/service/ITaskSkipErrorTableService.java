package com.tapdata.tm.skiperrortable.service;

import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.skiperrortable.SkipErrorTableStatusEnum;
import com.tapdata.tm.skiperrortable.dto.TaskSkipErrorTableDto;
import com.tapdata.tm.skiperrortable.vo.SkipErrorTableStatusVo;

import java.util.List;

/**
 * 任务-错误表跳过-服务接口
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/9/1 17:09 Create
 */
public interface ITaskSkipErrorTableService {

    /**
     * 删除任务错误表记录
     *
     * @param taskId       任务编号
     * @param sourceTables 要删除的源表名
     * @return 修改记录数
     */
    long deleteByTaskId(String taskId, List<String> sourceTables);

    /**
     * 添加错误跳过表记录
     *
     * @param dto 跳过实体数据
     * @return 返回更新后的对象
     */
    TaskSkipErrorTableDto addSkipTable(TaskSkipErrorTableDto dto);

    /**
     * 分页查询任务错误表信息
     *
     * @param taskId      任务编号
     * @param tableFilter 源表或目标表名过滤
     * @param skip        分页控制-跳过数量
     * @param limit       分页控制-返回数量
     * @param order       排序设置
     * @return 分页信息
     */
    Page<TaskSkipErrorTableDto> pageOfTaskId(String taskId, String tableFilter, long skip, int limit, String order);

    /**
     * 查询任务所有跳过表状态
     *
     * @param taskId 任务名
     * @return 表状态集合
     */
    List<SkipErrorTableStatusVo> listTableStatus(String taskId);

    /**
     * 变更错误表状态
     * @param taskId 任务编号
     * @param sourceTables 源表名
     * @param status 目标状态
     * @return 变更记录数
     */
    long changeTableStatus(String taskId, List<String> sourceTables, SkipErrorTableStatusEnum status);
}
