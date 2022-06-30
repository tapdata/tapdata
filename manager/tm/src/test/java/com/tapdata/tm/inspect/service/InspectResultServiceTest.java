package com.tapdata.tm.inspect.service;

import com.tapdata.tm.BaseJunit;
import com.tapdata.tm.inspect.dto.InspectResultDto;
import com.tapdata.tm.utils.MongoUtils;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class InspectResultServiceTest extends BaseJunit {

    @Autowired
    InspectResultService inspectResultService;

    @Test
    void beforeSave() {
    }

    @Test
    void list() {
        Criteria criteria = Criteria.where("inspect_id").is("605eea0f5dbaaa00100b51b5");
        Query query = new Query(criteria);
        query.with(Sort.by(Sort.Direction.DESC, "_id"));

        List<InspectResultDto> resultDtos = inspectResultService.findAll(query);
        printResult(resultDtos);

    }

    @Test
    void findById() {
        InspectResultDto inspectResultDto =inspectResultService.findById(MongoUtils.toObjectId("605edfd05dbaaa00100b4a11"));
        printResult(inspectResultDto);

    }



    @Test
    void joinResult() {
    }

    @Test
    void fillInspectInfo() {
    }

    @Test
    void setSourceConnectName() {
    }

    @Test
    void createAndPatch() {
    }
}