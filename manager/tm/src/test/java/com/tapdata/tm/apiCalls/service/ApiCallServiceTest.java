package com.tapdata.tm.apiCalls.service;

import cn.hutool.core.date.DateUtil;
import com.tapdata.tm.BaseJunit;
import com.tapdata.tm.apiCalls.entity.ApiCallEntity;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import java.util.Date;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ApiCallServiceTest extends BaseJunit {

    @Autowired
    ApiCallService apiCallService;

    @Autowired
    MongoTemplate mongoOperations;

    @Test
    void findOne() {
    }

    @Test
    void upsertByWhere() {
    }

    @Test
    void updateByWhere() {
    }

    @Test
    void deleteLogicsById() {
    }

    @Test
    void findById() {
    }

    @Test
    void updateById() {
    }

    @Test
    void find() {
    }

    @Test
    void save() {

        List<ApiCallEntity> apiCallEntityList = mongoOperations.findAll(ApiCallEntity.class);
        int i=1;
        for (ApiCallEntity apiCallEntity : apiCallEntityList) {
            apiCallEntity.setCreateAt(new Date());
            Update update = new Update().set("createTime", DateUtil.offsetMinute(new Date(),i));
            mongoOperations.updateFirst(Query.query(Criteria.where("id").is(apiCallEntity.getId())), update, ApiCallEntity.class);
            i++;
        }

    }

    @Test
    void testFindOne() {
    }

    @Test
    void getVisitTotalCount() {
    }

    @Test
    void getTransmitTotal() {
    }

    @Test
    void findByModuleIds() {
    }

    @Test
    void getVisitTotalLine() {
    }

    @Test
    void findClients() {
    }

    @Test
    void findByClientId() {
    }

    @Test
    void findByModuleIdAndTimePeriod() {
    }
}