package com.tapdata.tm.scheduleTasks.service;

import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.metadatainstance.entity.MetadataInstancesEntity;
import com.tapdata.tm.metadatainstance.repository.MetadataInstancesRepository;
import com.tapdata.tm.metadatainstance.service.MetadataInstancesService;
import com.tapdata.tm.scheduleTasks.dto.ScheduleTasksDto;
import com.tapdata.tm.scheduleTasks.entity.ScheduleTasksEntity;
import com.tapdata.tm.scheduleTasks.repository.ScheduleTasksRepository;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.Date;
import java.util.Map;

@Slf4j
@Service
public class ScheduleTasksService extends BaseService<ScheduleTasksDto, ScheduleTasksEntity, ObjectId, ScheduleTasksRepository> {

    @Autowired
    MetadataInstancesService metadataInstancesService;

    @Autowired
    MetadataInstancesRepository metadataInstancesRepository;

    public ScheduleTasksService(@NonNull ScheduleTasksRepository repository) {
        super(repository, ScheduleTasksDto.class, ScheduleTasksEntity.class);
    }

    @Override
    protected void beforeSave(ScheduleTasksDto dto, UserDetail userDetail) {

    }


    public ScheduleTasksDto save(ScheduleTasksDto scheduleTasksDto, UserDetail userDetail) {
        super.save(scheduleTasksDto, userDetail);
        //更新metaInstance表
        String meta_id = null;
        Map taskData = scheduleTasksDto.getTask_data();
        if (null != taskData && null != taskData.get("meta_id")) {
            meta_id = (String) taskData.get("meta_id");
            Query query = Query.query(Criteria.where("id").is(meta_id));
            if ("MONGODB_CREATE_INDEX".equals(scheduleTasksDto.getTask_type())) {
                //todo agentId  怎么分配  ping_time 由谁来更新
                scheduleTasksDto.setLast_updated(new Date());

                Update update = new Update();
                taskData.put("status", "creating");
                taskData.put("create_by", userDetail.getUsername());
                Document document = new Document();
                document.put("$each", Collections.singletonList(taskData));

                update.push("indexes", document);
                metadataInstancesRepository.getMongoOperations().updateFirst(query, update, MetadataInstancesEntity.class);
            } else if ("MONGODB_UPDATE_INDEX".equals(scheduleTasksDto.getTask_type())) {
                //更新metaInstance表
                Update update = new Update();
                update.setOnInsert("indexes", taskData);
                metadataInstancesRepository.getMongoOperations().updateFirst(query, update, MetadataInstancesEntity.class);
            } else if ("MONGODB_DROP_INDEX".equals(scheduleTasksDto.getTask_type())) {


            }
        }
        return scheduleTasksDto;
    }


}
