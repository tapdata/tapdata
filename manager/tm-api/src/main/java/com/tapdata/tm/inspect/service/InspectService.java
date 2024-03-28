package com.tapdata.tm.inspect.service;

import com.mongodb.client.result.UpdateResult;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.dto.Where;
import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.inspect.bean.Task;
import com.tapdata.tm.inspect.constant.InspectStatusEnum;
import com.tapdata.tm.inspect.dto.InspectDto;
import com.tapdata.tm.inspect.entity.InspectEntity;
import com.tapdata.tm.inspect.repository.InspectRepository;
import lombok.NonNull;
import org.bson.types.ObjectId;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Map;

public abstract class InspectService extends BaseService<InspectDto, InspectEntity, ObjectId, InspectRepository> {
    public InspectService(@NonNull InspectRepository repository) {
        super(repository, InspectDto.class, InspectEntity.class);
    }
    @Transactional
    public abstract Map<String, Long> delete(String id, UserDetail user);

    public abstract Page<InspectDto> list(Filter filter, UserDetail userDetail);

    public abstract InspectDto findById(Filter filter, UserDetail userDetail);

    public InspectDto save(InspectDto inspectDto, UserDetail user){
        return super.save(inspectDto, user);
    }

    public abstract void saveInspect(TaskDto taskDto, UserDetail userDetail);

    public abstract InspectDto createCheckByTask(TaskDto taskDto, UserDetail userDetail);

    public abstract List<InspectDto> findByTaskIdList(List<String> taskIdList);

    public abstract UpdateResult deleteByTaskId(String taskId);

    public abstract InspectDto updateInspectByWhere(Where where, InspectDto updateDto, UserDetail user);

    public abstract InspectDto executeInspect(Where where, InspectDto updateDto, UserDetail user);

    public abstract InspectDto updateById(ObjectId objectId, InspectDto inspectDto, UserDetail userDetail);

    public abstract List<InspectDto> findByName(String name);

    public abstract void importData(String json, String upsert, UserDetail userDetail);

    public abstract void setRepeatInspectTask();

    public abstract UpdateResult updateStatusById(String id, InspectStatusEnum inspectStatusEnum);

    public abstract UpdateResult updateStatusByIds(List<ObjectId> idList, InspectStatusEnum status);

    public abstract Map<String, Integer> inspectPreview(UserDetail user);

    public abstract List<InspectDto> findByStatus(InspectStatusEnum inspectStatusEnum);

    public abstract List<InspectDto> findByResult(boolean passed);

    public abstract void cleanDeadInspect();

    public abstract void supplementAlarm(InspectDto inspectDto, UserDetail userDetail);

    public long updateByWhere(Where where, InspectDto dto, UserDetail userDetail){
        return super.updateByWhere(where, dto, userDetail);
    }

    public abstract List<InspectDto> findAllByIds(List<String> inspectIds);

    public abstract void fieldHandler(List<Task> tasks,UserDetail userDetail);
}
