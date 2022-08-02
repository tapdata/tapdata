package com.tapdata.tm.inspect.service;

import cn.hutool.core.date.DateUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.gson.internal.LinkedTreeMap;
import com.tapdata.manager.common.utils.JsonUtil;
import com.tapdata.tm.BaseJunit;
import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.dto.Where;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.commons.schema.bean.PlatformInfo;
import com.tapdata.tm.inspect.bean.Task;
import com.tapdata.tm.inspect.constant.InspectStatusEnum;
import com.tapdata.tm.inspect.dto.InspectDto;
import com.tapdata.tm.inspect.repository.InspectRepository;
import com.tapdata.tm.tcm.dto.UserInfoDto;
import com.tapdata.tm.tcm.service.TcmService;
import com.tapdata.tm.utils.MongoUtils;
import com.tapdata.tm.utils.UUIDUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import java.util.*;

@Slf4j
class InspectServiceTest extends BaseJunit {

    @Autowired
    InspectService inspectService;
    @Autowired
    InspectRepository inspectRepository;

    @Autowired
    TcmService tcmService;

    @Autowired
    SettingsService settingsService;


    @Test
    void beforeSave() {
    }

    @Test
    void list() {
//                        {"order":"lastStartTime ASC","limit":20,"skip":140,"where":{}}
        String filterJson = "{\"order\":\"lastStartTime ASC\",\"limit\":20,\"skip\":140,\"where\":{}}";
        Filter filter = parseFilter(filterJson);
        Page<InspectDto> page = inspectService.list(filter, getUser());
        List<InspectDto> items = page.getItems();
        for (InspectDto inspectDto : items) {
            log.info("{}  --{}", inspectDto.getName(), DateUtil.date(inspectDto.getLastStartTime()).toDateStr());
        }


    }

    @Test
    void findOne() {
/*        InspectDto inspectResultDto =inspectService.findById(MongoUtils.toObjectId("616ff5dda4eb010e030a1962"));
        printResult(inspectResultDto);*/

        String filterStr = "{\"where\":{\"id\":{\"$inq\":[\"6172668517e4396fb056da1b\"]}}}";
        Filter filter = parseFilter(filterStr);
        LinkedTreeMap ids = (LinkedTreeMap) filter.getWhere().get("id");
        List<String> idList = (List<String>) ids.get("$inq");
        idList.forEach(id -> {
            System.out.println(id);
        });
        printResult(ids);
    }

    @Test
    void findById() {
//        InspectDto inspectResultDto =inspectService.findById(MongoUtils.toObjectId("616ff5dda4eb010e030a1962"));

        Filter filter = new Filter();
        String s = "{\"where\":{\"id\":\"616ff5dda4eb010e030a1962\"}}";
        Where where = parseWhere(s);
        filter.setWhere(where);

        InspectDto inspectDto = inspectService.findOne(filter, getUser("613f37dbb043b8350a668f4d"));
        printResult(inspectDto);

    }


    @Test
    void joinResult() {
    }

    @Test
    void fillInspectInfo() {
        List<InspectDto> inspectDtoList = inspectService.findByName("pg 2 tom gp3");
        printResult(inspectDtoList);
       /* if (CollectionUtils.isNotEmpty(inspectDtoList)) {
            System.out.println("not");
        } else {
            System.out.println("yes");
        }
        System.out.println(inspectDtoList.stream().filter(m -> m.getName().equals("1to2-1asdddd")).findAny().isPresent());*/
    }

    @Test
    void setSourceConnectName() {
        Query query = Query.query(Criteria.where("name").is("1to2-1zxasdf"));

        List<InspectDto> inspectDtoList = inspectService.findAll(query);
        System.out.println(inspectDtoList.size());
        printResult(inspectDtoList);
//        System.out.println( inspectService.existedName("1to2-1asdddd","61794b8c6a00b03260f84ac6"));
    }

    @Test
    void startInspectTask() throws JsonProcessingException {
        InspectDto inspectDto = inspectService.findById(MongoUtils.toObjectId("61128a12cc2ed50011780caf"));
//        inspectService.startInspectTask(inspectDto,"");

    }

    @Test
    public void deleteLogics() {
     /*   InspectDto inspectDto=inspectService.findById(MongoUtils.toObjectId("605edfd05dbaaa00100b4a11"));
        printResult(inspectDto);*/
        inspectService.deleteLogicsById("605edfd05dbaaa00100b4a11");
    }

    @Test
    public void JsonTest() {
        String filterStr = "{\"order\":\"createTime DESC\",\"limit\":10,\"skip\":0,\"where\":{\"inspectMethod\":\"row_count\",\"mode\":\"manual\"}}";
        String filterStr2 = "{\"where\":{\"id\":{\"$inq\":[\"617a436e2d3f8658970349a5\",\"617a43602d3f8658970349a2\",\"61793c1c2733805c295dd6d9\"]}}}";
        Filter filter = parseFilter(filterStr);
        JSONObject jsonObject = JSONUtil.parseObj(filterStr2);
        JSONObject whereJsonObject = jsonObject.getJSONObject("where");
        Iterator iter = whereJsonObject.entrySet().iterator();


    }


    @Test
    public void fixData() {
        InspectDto inspectDto = inspectService.findById(MongoUtils.toObjectId("6172a25628b2af04f764870c"));
        List<Task> taskList = inspectDto.getTasks();
        if (CollectionUtils.isNotEmpty(taskList)) {
            taskList.forEach(task -> {
                if (null == task.getTaskId()) {
                    task.setTaskId(UUIDUtil.getUUID());

                }
            });
        }
//        inspectService.updateById(MongoUtils.toObjectId("6172a25628b2af04f764870c"), inspectDto, getUser("61407a8cfa67f20019f68f9f"));
    }

    @Test
    public void upsertByWhere() {
        String s = "{ \"id\" : \"619b830b3d207d6acb0fbf90\" }";
        String dto = "{\"differenceNumber\":0,\"enabled\":true,\"errorMsg\":\"\",\"result\":\"passed\",\"status\":\"error\"}";
        Where where = parseWhere(s);
        InspectDto inspectDto = JsonUtil.parseJson(dto, InspectDto.class);
        inspectService.upsertByWhere(where, inspectDto, getUser("61407a8cfa67f20019f68f9f"));
    }

    @Test
    public void save() {
        String s = "{ \"id\" : \"619b830b3d207d6acb0fbf90\" }";
        String dto = "{\"differenceNumber\":0,\"enabled\":true,\"errorMsg\":\"\",\"result\":\"passed\",\"status\":\"error\"}";
        Where where = parseWhere(s);
        InspectDto inspectDto = JsonUtil.parseJson(dto, InspectDto.class);
        UserDetail userDetail = getUser();
        UserInfoDto userInfoDto = tcmService.getUserInfo(userDetail.getUserId());

        List<String> agentTags = new ArrayList<>();
        inspectDto.setAgentTags(agentTags);
        inspectDto.setName("sadas87d");


        PlatformInfo platformInfo = new PlatformInfo();
        platformInfo.setAgentType("private");
        inspectDto.setPlatformInfo(platformInfo);
        inspectService.save(inspectDto, userDetail);
    }


    @Test
    public void updateInspectByWhere() throws Exception {
        String s = "{ \"id\" : \"61d55ebee6800c3f3d0895db\" }";
        Where where = parseWhere(s);
        InspectDto inspectDto = new InspectDto();
        inspectDto.setErrorMsg("");
        inspectDto.setStatus("done");
        inspectService.updateInspectByWhere(where, inspectDto, getUser("61408608c4e5c40012663090"));
    }


    @Test
    public void updateById() {
        inspectService.updateStatusById("614ae1a2cba1d90019d56bbe", InspectStatusEnum.SCHEDULING);
    }

    @Test
    public void setRepeatInspectTask() {
        inspectService.setRepeatInspectTask();
    }


    @Test
    public void cleanTimeOut() {
        inspectService.cleanDeadInspect();

    }

}