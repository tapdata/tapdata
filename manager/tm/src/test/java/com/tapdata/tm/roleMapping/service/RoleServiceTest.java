package com.tapdata.tm.roleMapping.service;

import com.tapdata.tm.BaseJunit;
import com.tapdata.tm.role.dto.RoleDto;
import com.tapdata.tm.role.service.RoleService;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

class RoleServiceTest extends BaseJunit {
    @Autowired
    RoleService roleService;

    @Test
    void beforeSave() {
    }



    @Test
    void findByid() {
        RoleDto roleDto = roleService.findById(new ObjectId("5d31ae1ab953565ded04badd"));
        printResult(roleDto);
    }

}