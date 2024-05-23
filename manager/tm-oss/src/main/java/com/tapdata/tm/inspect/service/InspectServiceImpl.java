package com.tapdata.tm.inspect.service;

import com.mongodb.client.result.UpdateResult;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.dto.Where;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.inspect.bean.Task;
import com.tapdata.tm.inspect.constant.InspectStatusEnum;
import com.tapdata.tm.inspect.dto.InspectDto;
import com.tapdata.tm.inspect.repository.InspectRepository;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

/**
 * @Author:
 * @Date: 2021/09/14
 * @Description:
 */
@Service
@Slf4j
public class InspectServiceImpl extends InspectService {
    public InspectServiceImpl(@NonNull InspectRepository repository) {
        super(repository);
    }

    @Override
    protected void beforeSave(InspectDto dto, UserDetail userDetail) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    @Override
    public Map<String, Long> delete(String id, UserDetail user) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    @Override
    public Page<InspectDto> list(Filter filter, UserDetail userDetail) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    @Override
    public InspectDto findById(Filter filter, UserDetail userDetail) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    @Override
    public void saveInspect(TaskDto taskDto, UserDetail userDetail) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    @Override
    public InspectDto createCheckByTask(TaskDto taskDto, UserDetail userDetail) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    @Override
    public List<InspectDto> findByTaskIdList(List<String> taskIdList, UserDetail userDetail) {
        Query query = Query.query(Criteria.where("flowId").in(taskIdList).and("is_deleted").ne(true));
        return findAllDto(query, userDetail);
    }

    @Override
    public UpdateResult deleteByTaskId(String taskId, UserDetail userDetail) {
        return null;
    }

    @Override
    public InspectDto updateInspectByWhereFromEngine(Where where, InspectDto updateDto, UserDetail user) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    @Override
    public InspectDto executeInspect(Where where, InspectDto updateDto, UserDetail user) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    @Override
    public InspectDto updateById(ObjectId objectId, InspectDto inspectDto, UserDetail userDetail) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    @Override
    public List<InspectDto> findByName(String name) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    @Override
    public void importData(String json, String upsert, UserDetail userDetail) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    @Override
    public void setRepeatInspectTask() {
    }

    @Override
    public UpdateResult updateStatusById(String id, InspectStatusEnum inspectStatusEnum) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    @Override
    public UpdateResult updateStatusByIds(List<ObjectId> idList, InspectStatusEnum status) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    @Override
    public Map<String, Integer> inspectPreview(UserDetail user) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    @Override
    public List<InspectDto> findByStatus(InspectStatusEnum inspectStatusEnum) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    @Override
    public List<InspectDto> findByResult(boolean passed, UserDetail userDetail) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    @Override
    public void cleanDeadInspect() {
    }

    @Override
    public void supplementAlarm(InspectDto inspectDto, UserDetail userDetail) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    @Override
    public List<InspectDto> findAllByIds(List<String> inspectIds) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    @Override
    public void fieldHandler(List<Task> tasks,UserDetail userDetail) {
        throw new BizException("TapOssNonSupportFunctionException");
    }
}
