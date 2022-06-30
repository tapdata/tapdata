package com.tapdata.tm.function;

import com.tapdata.tm.BaseJunit;
import com.tapdata.tm.inspect.dto.InspectResultDto;
import com.tapdata.tm.inspect.service.InspectResultService;
import com.tapdata.tm.javascript.dto.FunctionsDto;
import com.tapdata.tm.javascript.service.FunctionService;
import com.tapdata.tm.utils.MongoUtils;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import java.util.List;

class FunctionServiceTest extends BaseJunit {

    @Autowired
    FunctionService functionService;

    @Test
    void beforeSave() {
    }

    @Test
    void save() {
        FunctionsDto functionsDto=new FunctionsDto();
        functionsDto.setFunctionName("fun3");
        functionsDto.setFunctionBody("{\n" +  "}");
        functionsDto.setParameters("");
        functionService.save(functionsDto,getUser("61306d94725cec27ed3401e3"));

    }

    @Test
    void findPage() {
     /*   FunctionsDto functionsDto=new FunctionsDto();
        functionsDto.setFunctionName("fun3");
        functionsDto.setFunctionBody("{\n" +  "}");
        functionsDto.setParameters("");
        functionService.findPage(functionsDto,getUser("61306d94725cec27ed3401e3"));*/

    }


    @Test
    void findById() {

    }



    @Test
    void joinResult() {
    }

    @Test
    void fillInspectInfo() {
    }

    @Test
    void setSourceConnectName() {
    }

    @Test
    void createAndPatch() {
    }
}