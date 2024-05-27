package com.tapdata.tm.featurecheck;

import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.featurecheck.controller.FeatureCheckController;
import com.tapdata.tm.featurecheck.dto.FeatureCheckDto;
import com.tapdata.tm.featurecheck.dto.FeatureCheckResult;
import com.tapdata.tm.featurecheck.repository.FeatureCheckRepository;
import com.tapdata.tm.featurecheck.service.FeatureCheckService;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.worker.entity.Worker;
import com.tapdata.tm.worker.service.WorkerService;
import org.apache.commons.collections.CollectionUtils;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.*;

public class FeatureCheckTest {

   static FeatureCheckController featureCheckController;

    static FeatureCheckService featureCheckService;

    static UserDetail user;

    static WorkerService workerService;

    static TaskService taskService;
    @BeforeAll
    static  void  init(){
        user = mock(UserDetail.class);
        featureCheckController = mock(FeatureCheckController.class);
        when(featureCheckController.getLoginUser()).thenReturn(user);
        featureCheckService = spy(new FeatureCheckService(mock(FeatureCheckRepository.class)));
        ReflectionTestUtils.setField(featureCheckController,"featureCheckService",featureCheckService);
        workerService = mock(WorkerService.class);
        taskService = mock(TaskService.class);
        ReflectionTestUtils.setField(featureCheckService,"workerService",workerService);
        ReflectionTestUtils.setField(featureCheckService,"taskService",taskService);


    }



    @Test
    void testCompareVersion() {
        String version1 = "v3.5-97be16";
        String version2 = "v3.5.12-f8be16";
        int actualData1 = FeatureCheckService.compareVersion(version1, version2);
        Assertions.assertTrue(actualData1 < 0);

        String version3 = "v3.5.13-97be16";
        String version4 = "v3.5.12-f8be16";
        int actualData2 = FeatureCheckService.compareVersion(version3, version4);
        Assertions.assertTrue(actualData2 > 0);

        String version5 = "v3.5.12-97be16";
        String version6 = "v3.5.12-f8be16";
        int actualData3 = FeatureCheckService.compareVersion(version5, version6);
        Assertions.assertTrue(actualData3 == 0);

    }

    @Test
    void testQueryFeatureCheckParamEmpty() {
        try {
            ReflectionTestUtils.invokeMethod(featureCheckService, "checkVersionDependencies", new ArrayList<>(), user);
        }catch (BizException e){
            Assertions.assertEquals("featureCheck.param.empty", e.getErrorCode());
        }

    }

    @Test
    void testQueryFeatureCheck() {
        List<FeatureCheckDto> featureCheckDtoList = new ArrayList<>();
        FeatureCheckDto featureCheckDto = new FeatureCheckDto();
        featureCheckDto.setFeatureType("connector");
        featureCheckDto.setFeatureCode("Mysql");
        featureCheckDtoList.add(featureCheckDto);
        List<Worker> workers = new ArrayList<>();
        Worker worker = new Worker();
        worker.setProcessId("test");
        worker.setVersion("v3.5.15-f8be16");
        worker.setId(ObjectId.get());
        workers.add(worker);
        when(workerService.findAvailableAgent(user)).thenReturn(workers);
        when(workerService.getLimitTaskNum(any(),any())).thenReturn(5);
        when(taskService.runningTaskNum(any(),any())).thenReturn(2);
        List<FeatureCheckDto> featureCheckDtoTemp = new ArrayList<>();
        FeatureCheckDto featureCheckDto1 =new FeatureCheckDto();
        featureCheckDto1.setFeatureType("connector");
        featureCheckDto1.setFeatureCode("Mysql1");
        featureCheckDto1.setMinAgentVersion("3.5.14");
        featureCheckDto1.setDescription("有新的功能");
        featureCheckDtoTemp.add(featureCheckDto1);
        Mockito.doReturn(featureCheckDtoTemp).when(featureCheckService).findAll(any(Query.class));
        FeatureCheckResult actualData =  ReflectionTestUtils.invokeMethod(featureCheckService, "checkVersionDependencies", featureCheckDtoList, user);
        Assertions.assertTrue(CollectionUtils.isNotEmpty(actualData.getEligibleAgents()));
        Assertions.assertTrue(actualData.getResult().get(0).getMinAgentVersion() ==null);

    }


}
