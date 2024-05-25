package com.tapdata.tm.inspect.service;

import com.mongodb.client.result.UpdateResult;
import com.tapdata.tm.base.dto.Field;
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
import com.tapdata.tm.webhook.enums.ConstVariable;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.bson.types.ObjectId;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

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
    public Supplier<InspectDto> dataPermissionFindById(ObjectId taskId, Field fields) {
        throw new BizException(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION);
    }

    @Override
    protected void beforeSave(InspectDto dto, UserDetail userDetail) {
        throw new BizException(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION);
    }

    @Override
    public Map<String, Long> delete(String id, UserDetail user) {
        throw new BizException(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION);
    }

    @Override
    public Page<InspectDto> list(Filter filter, UserDetail userDetail) {
        throw new BizException(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION);
    }

    @Override
    public InspectDto findById(Filter filter, UserDetail userDetail) {
        throw new BizException(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION);
    }

    @Override
    public void saveInspect(TaskDto taskDto, UserDetail userDetail) {
        throw new BizException(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION);
    }

    @Override
    public InspectDto createCheckByTask(TaskDto taskDto, UserDetail userDetail) {
        throw new BizException(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION);
    }

    @Override
    public List<InspectDto> findByTaskIdList(List<String> taskIdList) {
        throw new BizException(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION);
    }

    @Override
    public UpdateResult deleteByTaskId(String taskId) {
        throw new BizException(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION);
    }

    @Override
    public InspectDto doExecuteInspect(Where where, InspectDto updateDto, UserDetail user) {
        throw new BizException(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION);
    }

    @Override
    public InspectDto updateById(ObjectId objectId, InspectDto inspectDto, UserDetail userDetail) {
        throw new BizException(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION);
    }

    @Override
    public List<InspectDto> findByName(String name) {
        throw new BizException(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION);
    }

    @Override
    public void importData(String json, String upsert, UserDetail userDetail) {
        throw new BizException(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION);
    }

    @Override
    public void setRepeatInspectTask() {
        throw new BizException(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION);
    }

    @Override
    public UpdateResult updateStatusById(String id, InspectStatusEnum inspectStatusEnum) {
        throw new BizException(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION);
    }

    @Override
    public UpdateResult updateStatusByIds(List<ObjectId> idList, InspectStatusEnum status) {
        throw new BizException(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION);
    }

    @Override
    public Map<String, Integer> inspectPreview(UserDetail user) {
        throw new BizException(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION);
    }

    @Override
    public List<InspectDto> findByStatus(InspectStatusEnum inspectStatusEnum) {
        throw new BizException(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION);
    }

    @Override
    public List<InspectDto> findByResult(boolean passed) {
        throw new BizException(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION);    }

    @Override
    public void cleanDeadInspect() {
        throw new BizException(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION);
    }

    @Override
    public void supplementAlarm(InspectDto inspectDto, UserDetail userDetail) {
        throw new BizException(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION);
    }

    @Override
    public List<InspectDto> findAllByIds(List<String> inspectIds) {
        throw new BizException(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION);
    }

    @Override
    public void fieldHandler(List<Task> tasks,UserDetail userDetail) {
        throw new BizException(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION);
    }
}
