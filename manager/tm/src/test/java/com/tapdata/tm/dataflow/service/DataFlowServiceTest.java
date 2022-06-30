package com.tapdata.tm.dataflow.service;

import com.tapdata.manager.common.utils.JsonUtil;
import com.tapdata.tm.BaseJunit;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Where;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.dataflow.dto.DataFlowDto;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class DataFlowServiceTest extends BaseJunit {

    @Autowired
    DataFlowService dataFlowService;

    @Test
    void beforeSave() {
    }

    @Test
    void updateById() {
    }

    @Test
    void patch() {
    }

    @Test
    void save() {
        Map<String ,Object> map=new HashMap<>();
        map.put("test","test");
        map.put("name","test name");
//        dataFlowService.save(map,getUser("613f37e5a703840012b36d15"));
    }

    @Test
    void updateOne() {
    }

    @Test
    void copyDataFlow() {
        dataFlowService.copyDataFlow("605eb19ea86ab40010f41b53",getUser("6050575762ed301e55add7fb"));
    }

    @Test
    void resetDataFlow() {

    }

    @Test
    public void parsetest() throws UnsupportedEncodingException {
        String s="{\"id\":{\"inq\":[\"6061390882560e001051aec1\"]}}";
        Where where = parseWhere(s);
        UserDetail userDetail = getUser("6050575762ed301e55add7fb");
        List<DataFlowDto> dataFlowDtoList = dataFlowService.findAll(where, userDetail);
        printResult(dataFlowDtoList);
    }
    public Where parseWhere(String whereJson) {
        replaceLoopBack(whereJson);
        return JsonUtil.parseJson(whereJson, Where.class);
    }

    public String replaceLoopBack(String json) {
        if (com.tapdata.manager.common.utils.StringUtils.isNotBlank(json)) {
            json = json.replace("\"like\"", "\"$regex\"");
            json = json.replace("\"options\"", "\"$options\"");
            json = json.replace("\"$inq\"", "\"$in\"");
            json = json.replace("\"in\"", "\"$in\"");
        }
        return json;
    }

    public Filter parseFilter(String filterJson) {
        replaceLoopBack(filterJson);
        return JsonUtil.parseJson(filterJson, Filter.class);
    }
}