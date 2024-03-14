package com.tapdata.tm.inspect.service;

import com.mongodb.client.result.UpdateResult;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.dto.Where;
import com.tapdata.tm.base.service.IBaseService;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.inspect.constant.InspectStatusEnum;
import com.tapdata.tm.inspect.dto.InspectDto;
import com.tapdata.tm.inspect.entity.InspectEntity;
import com.tapdata.tm.inspect.repository.InspectRepository;
import org.bson.types.ObjectId;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Map;

public interface InspectService extends IBaseService<InspectDto, InspectEntity, ObjectId, InspectRepository> {
    @Transactional
    Map<String, Long> delete(String id, UserDetail user);

    Page<InspectDto> list(Filter filter, UserDetail userDetail);

    InspectDto findById(Filter filter, UserDetail userDetail);

    InspectDto save(InspectDto inspectDto, UserDetail user);

    void saveInspect(TaskDto taskDto, UserDetail userDetail);

    InspectDto createCheckByTask(TaskDto taskDto, UserDetail userDetail);

    List<InspectDto> findByTaskIdList(List<String> taskIdList);

    UpdateResult deleteByTaskId(String taskId);

    InspectDto updateInspectByWhere(Where where, InspectDto updateDto, UserDetail user);

    InspectDto executeInspect(Where where, InspectDto updateDto, UserDetail user);

    InspectDto updateById(ObjectId objectId, InspectDto inspectDto, UserDetail userDetail);

    List<InspectDto> findByName(String name);

    void importData(String json, String upsert, UserDetail userDetail);

    void setRepeatInspectTask();

    UpdateResult updateStatusById(String id, InspectStatusEnum inspectStatusEnum);

    UpdateResult updateStatusByIds(List<ObjectId> idList, InspectStatusEnum status);

    Map<String, Integer> inspectPreview(UserDetail user);

    List<InspectDto> findByStatus(InspectStatusEnum inspectStatusEnum);

    List<InspectDto> findByResult(boolean passed);

    void cleanDeadInspect();

    void supplementAlarm(InspectDto inspectDto, UserDetail userDetail);

    long updateByWhere(Where where, InspectDto dto, UserDetail userDetail);

    List<InspectDto> findAllByIds(List<String> inspectIds);
}
