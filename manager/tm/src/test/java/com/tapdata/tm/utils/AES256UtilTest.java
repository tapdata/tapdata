package com.tapdata.tm.utils;

import com.tapdata.tm.BaseJunit;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.modules.constant.ModuleStatusEnum;
import com.tapdata.tm.modules.dto.ModulesDto;
import com.tapdata.tm.modules.service.ModulesService;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class AES256UtilTest extends BaseJunit {

    @Autowired
    ModulesService modulesService;

    @Autowired
    DataSourceService dataSourceService;

    @Test
    void aes256Encode() {
    }

    @Test
    void aes256Decode() {
        List<ModulesDto> apis = modulesService.findAllActiveApi(ModuleStatusEnum.ACTIVE);
        List<ObjectId> connections = apis.stream().map(ModulesDto::getConnection).collect(Collectors.toList());
        Query query = Query.query(Criteria.where("id").in(connections));
        List<DataSourceConnectionDto> dataSourceConnectionDtoList = dataSourceService.findAll(query);
        dataSourceConnectionDtoList.forEach(dataSourceConnectionDto -> {
            String password=dataSourceConnectionDto.getDatabase_password();
            System.out.println(password+":------  "+AES256Util.Aes256Decode(password));
        });

    }

    @Test
    void initialize() {
    }

    @Test
    void getKey() {
    }
}