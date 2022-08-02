package com.tapdata.tm.utils;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.bean.copier.CopyOptions;
import cn.hutool.core.map.MapUtil;
import com.tapdata.tm.BaseJunit;
import com.tapdata.tm.log.dto.LogDto;
import org.junit.jupiter.api.Test;
import org.springframework.beans.BeanUtils;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class BeanUtilTest extends BaseJunit {


    @Test
    public void mapToBean() {
        Map map = new HashMap();
        map.put("threadName", "asdas");
        map.put("date", new Date());


        LogDto logDto = BeanUtil.mapToBean(map, LogDto.class, false, CopyOptions.create());
        printResult(logDto);

    }
}