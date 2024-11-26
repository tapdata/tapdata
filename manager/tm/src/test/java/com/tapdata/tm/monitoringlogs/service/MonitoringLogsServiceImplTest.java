package com.tapdata.tm.monitoringlogs.service;

import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.commons.schema.MonitoringLogsDto;
import com.tapdata.tm.monitoringlogs.entity.MonitoringLogsEntity;
import com.tapdata.tm.monitoringlogs.param.MonitoringLogQueryParam;
import com.tapdata.tm.monitoringlogs.repository.MonitoringLogsRepository;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2024/11/20 14:02
 */
public class MonitoringLogsServiceImplTest {

    @Test
    void testQuery() throws ExecutionException, InterruptedException {

        MonitoringLogsRepository monitoringLogsRepository = mock(MonitoringLogsRepository.class);
        MongoTemplate mongoOperations = mock(MongoTemplate.class);

        when(mongoOperations.count(any(Query.class), eq(MonitoringLogsEntity.class))).thenReturn(0L);

        MonitoringLogsServiceImpl monitoringLogsService =
                new MonitoringLogsServiceImpl(monitoringLogsRepository, CompletableFuture.completedFuture(mongoOperations));

        MonitoringLogQueryParam param = new MonitoringLogQueryParam();
        param.setTaskId("673d825967ac3c7c5785ae02");
        param.setStart(1732421532000L);
        param.setType("testRun");
        param.setOrder("asc");

        param.setIncludeLogTags(Arrays.asList("tag1", "tag2"));

        Page<MonitoringLogsDto> page = monitoringLogsService.query(param);


        param.setIncludeLogTags(null);
        param.setExcludeLogTags(Arrays.asList("tag1", "tag2"));

        page = monitoringLogsService.query(param);

        verify(mongoOperations, times(2)).count(any(Query.class), eq(MonitoringLogsEntity.class));

    }

}
