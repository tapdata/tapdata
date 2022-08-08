package com.tapdata.tm.ds.service.impl;

import com.tapdata.tm.BaseJunit;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import static org.junit.jupiter.api.Assertions.*;

class DataSourceServiceTest extends BaseJunit {

    @Autowired
    DataSourceService dataSourceService;

    @Test
    void add() {
    }

    @Test
    void update() {
    }

    @Test
    void updateCheck() {
    }

    @Test
    void list() {
    }

    @Test
    void getById() {
    }

    @Test
    void updateTag() {
    }

    @Test
    void delete() {
    }

    @Test
    void copy() {
        dataSourceService.copy(getUser("6050575762ed301e55add7fb"),"605f044bf57164001027b704");
    }

    @Test
    void customQuery() {
    }

    @Test
    void beforeSave() {
    }

    @Test
    void deleteTags() {
    }

    @Test
    void checkConn() {
    }

    @Test
    void testUpdate() {
    }

    @Test
    void updateAfter() {
    }

    @Test
    void testUpdateAfter() {
    }
}