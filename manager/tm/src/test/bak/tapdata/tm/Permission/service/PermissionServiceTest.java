package com.tapdata.tm.Permission.service;

import cn.hutool.core.collection.ListUtil;
import com.tapdata.tm.BaseJunit;
import com.tapdata.tm.Permission.entity.PermissionEntity;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.query.Query;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class PermissionServiceTest extends BaseJunit {

    @Autowired
    PermissionService permissionService;

    @Test
    void getCurrentPermission() {
    }

    @Test
    void getByNames() {
    }

    @Test
    void find() {
        Query query = new Query();
        query.with(Sort.by(Sort.Order.desc("_id")
        ));
        List<PermissionEntity> permissionEntityList = permissionService.permissionRepository.getMongoOperations().find(query, PermissionEntity.class);
        List<String> studentsSortName = permissionEntityList.stream().map(PermissionEntity::getId).collect(Collectors.toList());

        studentsSortName.forEach(nma
                -> {
            printResult(nma);
        });


        ListUtil.sortByProperty(studentsSortName,"id");
        printResult(studentsSortName);

    }


    @Test
    void count() {
    }
}