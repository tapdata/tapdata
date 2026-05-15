package com.tapdata.tm.skiperrortable.service;

import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.skiperrortable.SkipErrorTableStatusEnum;
import com.tapdata.tm.skiperrortable.dto.TaskSkipErrorTableDto;
import com.tapdata.tm.skiperrortable.repository.TaskSkipErrorTableRepository;
import com.tapdata.tm.skiperrortable.vo.SkipErrorTableStatusVo;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * 任务-错误表跳过-实体
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/8/21 15:23 Create
 */
@Service
public class TaskSkipErrorTableServiceImpl implements ITaskSkipErrorTableService {

    private final TaskSkipErrorTableRepository repository;

    public TaskSkipErrorTableServiceImpl(TaskSkipErrorTableRepository repository) {
        this.repository = repository;
    }

    @Override
    public long deleteByTaskId(String taskId, List<String> sourceTables) {
        assert null != taskId;
        Criteria criteria = Criteria.where(TaskSkipErrorTableDto.FIELD_TASK_ID).is(taskId);
        if (null != sourceTables && ! sourceTables.isEmpty()) {
            criteria.and(TaskSkipErrorTableDto.FIELD_SOURCE_TABLE).in(sourceTables);
        }
        Query query = Query.query(criteria);
        return repository.deleteAll(query);
    }

    @Override
    public TaskSkipErrorTableDto addSkipTable(TaskSkipErrorTableDto dto) {
        return repository.addSkipTable(dto);
    }

    @Override
    public Page<TaskSkipErrorTableDto> pageOfTaskId(String taskId, String tableFilter, long skip, int limit, String order) {
        return repository.pageOfTaskId(taskId, tableFilter, skip, limit, order);
    }

    @Override
    public List<SkipErrorTableStatusVo> listTableStatus(String taskId) {
        return repository.getAllRecoverTableNames(taskId);
    }

    @Override
    public long changeTableStatus(String taskId, List<String> sourceTables, SkipErrorTableStatusEnum status) {
        return repository.changeTableStatus(taskId, sourceTables, status);
    }
}
