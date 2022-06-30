package com.tapdata.tm.insights.service;

import com.tapdata.tm.BaseJunit;
import com.tapdata.tm.base.dto.Field;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.insights.repository.InsightsRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class InsightsServiceTest extends BaseJunit {

    @Autowired
    InsightsService insightsService;

    @Autowired
    InsightsRepository insightsRepository;

    @Test
    void beforeSave() {
    }

    @Test
    public void find() {
        Filter filter = parseFilter("{\"order\":\"data.api_calls DESC\",\"limit\":20,\"skip\":0,\"where\":{\"stats_name\":\"ALL-time:ALL-user:EVERY-api\",\"or\":[{\"data.api_method\":{\"like\":\"mysql\",\"options\":\"i\"}},{\"data.api_path\":{\"like\":\"mysql\",\"options\":\"i\"}}]}}");

//        filter.getFields().put("data", false);
        printResult(insightsService.find(filter));


    }

}