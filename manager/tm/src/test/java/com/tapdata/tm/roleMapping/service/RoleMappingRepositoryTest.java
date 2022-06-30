package com.tapdata.tm.roleMapping.service;

import com.tapdata.tm.BaseJunit;
import com.tapdata.tm.roleMapping.dto.RoleMappingDto;
import com.tapdata.tm.roleMapping.entity.RoleMappingEntity;
import com.tapdata.tm.roleMapping.repository.RoleMappingRepository;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import java.util.List;

class RoleMappingRepositoryTest extends BaseJunit {
    @Autowired
    RoleMappingRepository repository;

    @Test
    void beforeSave() {
    }

    @Test
    void findByPrincipleTypeAndPrincipleId() {
        RoleMappingEntity roleMappingEntities = repository.findById("604f4b7fe1ca905fa754521f").get();
        printResult(roleMappingEntities);
    }

}