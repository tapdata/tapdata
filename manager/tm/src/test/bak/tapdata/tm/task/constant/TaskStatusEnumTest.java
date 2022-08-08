package com.tapdata.tm.task.constant;

import cn.hutool.core.util.EnumUtil;
import com.tapdata.tm.BaseJunit;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class TaskStatusEnumTest extends BaseJunit {

    @Test
    public void test(){
       Map map = EnumUtil.getEnumMap(TaskStatusEnum.class);
        printResult(map);

    }
}