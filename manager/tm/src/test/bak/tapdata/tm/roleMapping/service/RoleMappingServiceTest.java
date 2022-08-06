package com.tapdata.tm.roleMapping.service;

import com.tapdata.tm.BaseJunit;
import com.tapdata.tm.roleMapping.dto.RoleMappingDto;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import java.util.List;

class RoleMappingServiceTest extends BaseJunit {
    @Autowired
    RoleMappingService roleMappingService;

    @Test
    void beforeSave() {
    }

    @Test
    void findByPrincipleTypeAndPrincipleId() {
        Query query = Query.query(Criteria.where("principalId").is("604f4b7ce1ca905fa754520c").and("principalType").is("USER"));
        List<RoleMappingDto> roleMappingEntities = roleMappingService.findAll(query);
        printResult(roleMappingEntities);
    }

    @Test
    void findByid() {
        RoleMappingDto roleMappingEntities = roleMappingService.findById(new ObjectId("604f4b7fe1ca905fa754521f"));
        printResult(roleMappingEntities);
    }

}