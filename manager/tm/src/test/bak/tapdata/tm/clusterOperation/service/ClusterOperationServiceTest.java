package com.tapdata.tm.clusterOperation.service;

import com.tapdata.tm.BaseJunit;
import com.tapdata.tm.clusterOperation.constant.AgentStatusEnum;
import com.tapdata.tm.clusterOperation.dto.ClusterOperationDto;
import com.tapdata.tm.utils.MongoUtils;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ClusterOperationServiceTest extends BaseJunit {

    @Autowired
    ClusterOperationService clusterOperationService;

    @Test
    void beforeSave() {
    }

    @Test
    void sendOperation() {
        clusterOperationService.sendOperation();

    }

    @Test
    void find() {
        List<String> uuidList = Arrays.asList("f8c40fc6-59c9-4d3b-9f30-f027dbd9cefd");
        Query query = Query.query(Criteria.where("uuid").in(uuidList).and("status").is(AgentStatusEnum.NEED_UPDATE.getValue()));
        List<ClusterOperationDto> dtoToUpdateList = clusterOperationService.findAll(query);
        printResult(dtoToUpdateList);
    }


    @Test
    void cleanOperation() {
    }

    @Test
    void changeStatus() {
    }

    @Test
    void updateMsg() {

        ClusterOperationDto clusterOperationDto = clusterOperationService.findById(MongoUtils.toObjectId("61946df8664994258c139a66"));
        System.out.println(clusterOperationService.getSendObj(clusterOperationDto));

    }
}