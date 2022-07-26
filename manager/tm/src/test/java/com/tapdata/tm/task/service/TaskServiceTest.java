//package com.tapdata.tm.task.service;
//
//import com.tapdata.tm.BaseJunit;
//import com.tapdata.tm.commons.cache.Cache;
//import com.tapdata.tm.commons.task.dto.ShareCacheTaskDto;
//import com.tapdata.tm.utils.Lists;
//import org.junit.jupiter.api.Test;
//import org.springframework.beans.factory.annotation.Autowired;
//
//class TaskServiceTest extends BaseJunit {
//
//    @Autowired
//    TaskService taskService;
//
//
//    @Test
//    void create() {
//    }
//
//    @Test
//    void createShareCacheTask() {
//        ShareCacheTaskDto shareCacheTaskDto = new ShareCacheTaskDto();
//        Cache cache = new Cache();
//        cache.setFields(Lists.newArrayList("ready", "fail"));
//        cache.setMaxRows(1000L);
//        cache.setTableName("table1");
//
//        shareCacheTaskDto.setCache(cache);
//        shareCacheTaskDto.setCreateUser(getUser().getUsername());
//        shareCacheTaskDto.setIncreaseReadSize(50);
//        shareCacheTaskDto.setName("shareCacheTest1");
//        taskService.create(shareCacheTaskDto, getUser());
//    }
//
//    @Test
//    void beforeSave() {
//    }
//
//    @Test
//    void updateById() {
//    }
//
//    @Test
//    void checkTaskName() {
//    }
//
//    @Test
//    void confirmById() {
//    }
//
//    @Test
//    void remove() {
//    }
//
//    @Test
//    void flushStatus() {
//    }
//
//    @Test
//    void copy() {
//    }
//
//    @Test
//    void queryTaskRunHistory() {
//    }
//
//    @Test
//    void start() {
//    }
//
//    @Test
//    void pause() {
//    }
//
//    @Test
//    void renew() {
//    }
//
//    @Test
//    void stop() {
//    }
//
//    @Test
//    void testStop() {
//    }
//
//    @Test
//    void checkExistById() {
//    }
//
//    @Test
//    void batchStart() {
//    }
//
//    @Test
//    void batchStop() {
//    }
//
//    @Test
//    void batchDelete() {
//    }
//
//    @Test
//    void batchRenew() {
//    }
//
//    @Test
//    void find() {
//    }
//
//    @Test
//    void printInfos() {
//    }
//
//    @Test
//    void searchLogCollector() {
//    }
//}