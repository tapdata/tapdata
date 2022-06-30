package com.tapdata.tm.modules.service;

import com.tapdata.tm.BaseJunit;
import com.tapdata.tm.apiCalls.entity.ApiCallEntity;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.utils.MongoUtils;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class ModulesServiceTest extends BaseJunit {

    @Autowired
    ModulesService modulesService;


    @Test
    public void find() {
        String s = "{\"where\":{\"basePath\":{\"like\":\"mysql\"}},\"limit\":20,\"skip\":0,\"order\":\"createTime desc\"}";
        Filter filter = parseFilter(s);
        printResult(modulesService.find(filter, getUser("613f37e5a703840012b36d15")));
    }

    @Test
    public void idTest(){
        Filter filter=new Filter();
            List<ObjectId> moduleIdList=new ArrayList<>();
            moduleIdList.add(MongoUtils.toObjectId("622f09a3f421fb6cc8189f81"));
            Map notDeleteMap = new HashMap();
            notDeleteMap.put("in", moduleIdList);
            filter.getWhere().put("id", notDeleteMap);

        Page page = modulesService.find(filter,getUser("62172cfc49b865ee5379d3ed"));
        printResult(page);
    }

    @Test
    public void add(){
        Filter filter=new Filter();
        List<ObjectId> moduleIdList=new ArrayList<>();
        moduleIdList.add(MongoUtils.toObjectId("622f09a3f421fb6cc8189f81"));
        Map notDeleteMap = new HashMap();
        notDeleteMap.put("in", moduleIdList);
        filter.getWhere().put("id", notDeleteMap);

        Page page = modulesService.find(filter,getUser("62172cfc49b865ee5379d3ed"));
        printResult(page);
    }


}