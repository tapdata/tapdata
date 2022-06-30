package com.tapdata.tm.worker.service;

import com.tapdata.manager.common.utils.JsonUtil;
import com.tapdata.tm.BaseJunit;
import com.tapdata.tm.config.security.SimpleGrantedAuthority;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.inspect.dto.InspectDto;
import com.tapdata.tm.worker.entity.Worker;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import java.util.Collections;
import java.util.List;
import java.util.Map;

class WorkerServiceTest extends BaseJunit {

    @Autowired
    WorkerService workerService;

    @Test
    void scheduleTaskToEngine() {
        InspectDto entity = new InspectDto();
        UserDetail user = new UserDetail("5f32027814fbc2d8530c5fe3","","admin@admin.com","admin", Collections.singleton(new SimpleGrantedAuthority("admin")));
        entity.setUserId("5f32027814fbc2d8530c5fe3");
        entity.setName("test123");
        workerService.scheduleTaskToEngine(entity,user);
    }

    @Test
    void deleteById() {
        Query query=   Query.query(Criteria.where("isDeleted").ne(true));
/*        query.addCriteria(Criteria.where("stopping").ne("true"));
        query.addCriteria(Criteria.where("worker_type").is("connector"));*/
        query.addCriteria(Criteria.where("user_id").is("61407a8cfa67f20019f68f9f"));
        List<Worker> workerList=workerService.findAll(query,getUser("61407a8cfa67f20019f68f9f"));
        System.out.println(workerList.size());


        System.out.println("被删除数量"+workerService.deleteAll(query)+"");
    }


    @Test
    public void updateMsg(){
        String s="{\n" +
                "    \"type\": \"updateMsg\",\n" +
                "    \"timestamp\": 1637132667302,\n" +
                "    \"data\": {\n" +
                "        \"process_id\": \"619b61f5889c1f49e12e5ac2-1fl3fda07\",\n" +
                "        \"status\": \"downloading\",\n" +
                "        \"progres\": \"69.72\",\n" +
                "        \"msg\": \"Downloading tapdata-agent 39.44% 159.00 MB, Total size: 403.09 MB, avg speed: 618 KB/s, remaining time: 6m 43s     \"\n" +
                "    }\n" +
                "}";
        Map map= JsonUtil.parseJson(s,Map.class);
        workerService.updateMsg(map);
    }


}
