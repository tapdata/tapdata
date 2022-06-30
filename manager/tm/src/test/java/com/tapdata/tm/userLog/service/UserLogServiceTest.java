package com.tapdata.tm.userLog.service;

import cn.hutool.core.date.DateUtil;
import com.tapdata.tm.BaseJunit;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.userLog.entity.UserLogs;
import com.tapdata.tm.userLog.repository.UserLogRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import java.util.Date;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class UserLogServiceTest extends BaseJunit {

    @Autowired
    UserLogService userLogService;

    @Autowired
    UserLogRepository userLogRepository;

    @Test
    void findAll() {

        Criteria criteria = Criteria.where("userId").is("61407a8cfa67f20019f68f9f");
//        criteria.andOperator(Criteria.where("createTime").gte(1636351739000L), Criteria.where("createTime").lte(1636473600000L));


        Date oneDayBefore = DateUtil.offsetDay(new Date(), -1);

        Query query = Query.query(Criteria.where("createTime").gt(new Date(1636351739000L)));
        List list = userLogRepository.getMongoOperations().find(query, UserLogs.class);
        printResult(list);
    }

    @Test
    void findById() {
        System.out.println(new Date(1636351739000L));
        System.out.println(new Date(1636473600000L));

    }

    @Test
    void add() {
    }

    @Test
    void findPage() {
        String s = "{\"order\":\"createTime DESC\",\"limit\":20,\"skip\":0,\"where\":{\"type\":\"userOperation\",\"and\":[{\"createTime\":{\"gte\":\"2022-05-11 00:00:00\"}}]}}";
        Filter filter = parseFilter(s);
        Page page = userLogService.find(filter);
        printResult(filter.getWhere());

        printResult(page);

    }
}