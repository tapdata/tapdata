package com.tapdata.tm.modules.util;

import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.modules.dto.ApiViewUtil;
import com.tapdata.tm.modules.dto.ModulesDto;
import com.tapdata.tm.modules.dto.Param;
import com.tapdata.tm.modules.entity.Path;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.util.*;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class ApiViewUtilTest {
    @Test
    void test1(){
        try(MockedStatic<ApiViewUtil> apiViewUtilMockedStatic = mockStatic(ApiViewUtil.class)){
            String ip="http://127.0.0.1:3080";
            Map<String, List<ModulesDto>> modules=new HashMap<>();
            List<ModulesDto> modulesDtos=new ArrayList<>();
            ModulesDto modulesDto = new ModulesDto();
            modulesDto.setName("Mysql");
            Path path=new Path();
            path.setPath("/api/qbl7pd2tzwu");
            Field field=new Field();
            field.setFieldName("name");
            field.setDataType("string");
            field.setComment("name 字段描述");
            path.setFields(Arrays.asList(field));
            modulesDto.setPaths(Arrays.asList(path));
            Param param = new Param();
            param.setName("page");
            param.setType("number");
            param.setDescription("分页编号");
            param.setDefaultvalue("1");
            path.setParams(Arrays.asList(param));
            modulesDtos.add(modulesDto);
            modules.put("API",modulesDtos);
            doCallRealMethod().when(ApiViewUtil.convert(anyMap(),anyString(),anyString()));
            when(ApiViewUtil.doRequest(any(),any())).thenReturn("成功返回数据");
            ApiViewUtil.convert(modules,ip,"access_toekn");
        }


    }
}
