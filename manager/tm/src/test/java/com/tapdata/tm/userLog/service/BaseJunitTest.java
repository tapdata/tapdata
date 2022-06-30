package com.tapdata.tm.userLog.service;

import com.alibaba.fastjson.JSON;
import com.tapdata.tm.TMApplication;
import lombok.extern.slf4j.Slf4j;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@Slf4j
@RunWith(SpringRunner.class)
@SpringBootTest(classes = {TMApplication.class})
public class BaseJunitTest {

    protected void printResult(Object o) {
        log.info(JSON.toJSONString(o));
    }
}
