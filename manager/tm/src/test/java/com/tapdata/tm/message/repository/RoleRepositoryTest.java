package com.tapdata.tm.message.repository;

import com.tapdata.tm.BaseJunit;
import com.tapdata.tm.role.entity.RoleEntity;
import com.tapdata.tm.role.repository.RoleRepository;
import com.tapdata.tm.roleMapping.entity.RoleMappingEntity;
import com.tapdata.tm.roleMapping.repository.RoleMappingRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

class RoleRepositoryTest extends BaseJunit {
    @Autowired
    RoleRepository repository;

    @Test
    void beforeSave() {
    }

    @Test
    void findById() {
        RoleEntity entity = repository.findById("5d31ae1ab953565ded04badd").get();
        printResult(entity);
    }

}