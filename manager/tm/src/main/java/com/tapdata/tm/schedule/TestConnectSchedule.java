package com.tapdata.tm.schedule;

import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.entity.DataSourceEntity;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.user.service.UserService;
import lombok.Setter;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@Component
@Setter(onMethod_ = {@Autowired})
public class TestConnectSchedule {

    private DataSourceService dataSourceService;
    private UserService userService;

    @Scheduled(initialDelay = 60 * 1000, fixedDelay = 30 * 1000)
    @SchedulerLock(name ="TestConnectSchedule_retry_lock", lockAtMostFor = "10s", lockAtLeastFor = "10s")
    public void retry() {
        Thread.currentThread().setName("taskSchedule-TestConnectSchedule-retry");
        Criteria criteria = Criteria.where("status").is(DataSourceEntity.STATUS_TESTING)
                .orOperator(Criteria.where("testCount").lt(15), Criteria.where("testCount").exists(false));

        Query query = Query.query(criteria);
        query.with(Sort.by("testTime").ascending());

        List<DataSourceConnectionDto> connectionDtos = dataSourceService.findAll(query);

        if (CollectionUtils.isEmpty(connectionDtos)) {
            return;
        }

        List<String> userIds = connectionDtos.stream().map(DataSourceConnectionDto::getUserId).distinct().collect(Collectors.toList());
        List<UserDetail> userByIdList = userService.getUserByIdList(userIds);
        Map<String, UserDetail> userDetailMap = userByIdList.stream().collect(Collectors.toMap(UserDetail::getUserId, Function.identity(), (e1, e2) -> e1));

        connectionDtos.forEach(connectionDto -> {
            dataSourceService.sendTestConnection(connectionDto, false, connectionDto.getSubmit(), userDetailMap.get(connectionDto.getUserId()));
        });
    }
}
