package com.tapdata.tm.scheduleTasks.service;

import cn.hutool.core.util.EnumUtil;
import com.tapdata.tm.BaseJunit;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.task.constant.TaskStatusEnum;
import com.tapdata.tm.task.entity.TaskEntity;
import com.tapdata.tm.task.repository.TaskRepository;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.utils.MongoUtils;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import java.util.*;


class ScheduleTasksServiceTest extends BaseJunit {

    @Autowired
    TaskService taskService;

    @Autowired
    TaskRepository taskRepository;

    @Autowired
    ScheduleTasksService scheduleTasksService;

    @Test
    void beforeSave() {
    }

    @Test
    void save() {
    /*    SaveSchduleTaskParam saveSchduleTaskParam = new SaveSchduleTaskParam();
        Map<String, Object> taskData = new HashMap<>();
        taskData.put("collection_name", "AUTO_POLICY_CUSTOMER_OR2MG_IC");
        taskData.put("meta_id", "61dbf9eba27c8f00524dafc1");
        taskData.put("name", "");
        taskData.put("data_type", "h");
        taskData.put("type_data", 31104000);
        taskData.put("expireAfterSeconds", 1213131);
        taskData.put("unique", false);
        taskData.put("create_by", "admin@admin.com");
        taskData.put("ttl", true);
        taskData.put("status", "creating");
        taskData.put("uri", "mongodb://47.115.163.10:27017/source");
        saveSchduleTaskParam.setTask_data(taskData);
        saveSchduleTaskParam.setTask_name("mongodb_create_index");
        saveSchduleTaskParam.setTask_type("MONGODB_CREATE_INDEX");
        saveSchduleTaskParam.setStatus("waiting");*/
    }

    @Test
    void dataDevelopPreview() {
//        printResult(taskService.dataDevelopPreview(getUser("6193700c1516f86b493d21f2")));
        UserDetail userDetail = getUser("6193700c1516f86b493d21f2");
        Map<String, Object> resultChart = new HashMap<>();
        Map dataCopyPreview = new HashMap();
        Map dataDevPreview = new HashMap();
        Criteria criteria = Criteria.where("user_id").is(userDetail.getUserId())
                .andOperator(Criteria.where("status").exists(true), Criteria.where("status").ne(null));
        Query query = Query.query(criteria);
        List<TaskDto> taskDtoList = taskService.findAll(query);
        printResult(taskDtoList);

    }

    @Test
    public void test() {
        Map map = EnumUtil.getEnumMap(TaskStatusEnum.class);
        printResult(map);
    }


    @Test
    public void findAll() {
        Filter filter = new Filter();
        Page page = taskService.find(filter, getUser("6193700c1516f86b493d21f2"));
        printResult(page.getItems());
    }


    @Test
    public void getAllStatuses() {
/*        List<TaskDto> synList = taskService.findAll(new Query(Criteria.where("syncType").is(SyncType.SYNC.getValue())));
        List<SubStatus> subStatuses = synList.stream().map(TaskDto::getStatuses).collect(Collectors.toList());
        printResult(subStatuses);*/
    }

    @Test
    public void orTest() {

        List<String> listStatusList = new ArrayList();
        listStatusList.add("edit");
        listStatusList.add("running");

        Criteria criteria = Criteria.where("is_deleted").ne(true);


        Criteria statusesCriteria = new Criteria().orOperator(Criteria.where("statuses").is(new ArrayList()),
                Criteria.where("statuses.status").in(listStatusList));


        Criteria nameCriteria = Criteria.where("name").regex("vi");
        Criteria tableNameCriteria = Criteria.where("stages.tableName").regex("vi");
        List<Criteria> nameCriteraiList = new ArrayList();
        nameCriteraiList.add(nameCriteria);
        nameCriteraiList.add(tableNameCriteria);

        Criteria namesCriteria = new Criteria().orOperator(nameCriteraiList);
//        criteria.andOperator(statusesCriteria);
        criteria.andOperator(namesCriteria);

        Query query = Query.query(criteria);
        query.fields().exclude("dag");
        List<TaskEntity> taskDtoList = taskRepository.getMongoOperations().find(query, TaskEntity.class);
        System.out.println("total  " + taskDtoList.size());
        printResult(taskDtoList);
    }


    @Test
    public void parseOrToCriteria() {
        Criteria criteria = Criteria.where("is_deleted").ne(true).and("user_id").is("62172cfc49b865ee5379d3ed");

        Filter filter = parseFilter("{\"order\":\"createTime DESC\",\"limit\":20,\"fields\":{\"id\":true,\"name\":true,\"status\":true,\"last_updated\":true,\"createTime\":true,\"user_id\":true,\"startTime\":true,\"agentId\":true,\"statuses\":true,\"type\":true},\"skip\":0,\"where\":{\"syncType\":\"sync\",\"or\":[{\"name\":{\"like\":\"ma\",\"options\":\"i\"}},{\"stages.tableName\":{\"like\":\"ma\",\"options\":\"i\"}},{\"stages.name\":{\"like\":\"ma\",\"options\":\"i\"}}]}}");
        Criteria Orcriteria = taskService.parseOrToCriteria(filter.getWhere());
        List<String> statues = new ArrayList<>();
        statues.addAll(TaskService.stopStatus);
        Criteria notRunningCri = Criteria.where("statuses.status").in(statues);
        Criteria emptyStatus = Criteria.where("statuses").is(new ArrayList());
        List<Criteria> statusCriList = new ArrayList<>();

        statusCriList.add(notRunningCri);
        statusCriList.add(emptyStatus);
        Criteria statusesCriteria = new Criteria().orOperator(statusCriList);

        criteria.andOperator(Orcriteria, statusesCriteria);
        Query query = Query.query(criteria);

        List<TaskEntity> taskDtoList = taskRepository.getMongoOperations().find(query, TaskEntity.class);
        printResult(taskDtoList.size());
        printResult(taskDtoList);
    }


    @Test
    public void findByIds() {
        List<ObjectId> ids = new ArrayList<>();
        ids.add(MongoUtils.toObjectId("6267b816cd214826edde06dd"));
        ids.add(MongoUtils.toObjectId("62184512ae53313ba32f56f6"));
        List<TaskEntity> list = taskService.findByIds(ids);

        for (TaskEntity t : list) {
            printResult(t);
        }
    }

    @Test
    public void chart() {
        Map map = taskService.chart(getUser());
        printResult(map);
    }

}