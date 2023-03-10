package com.tapdata.tm.schedule;

import com.tapdata.tm.commons.base.dto.BaseDto;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.user.service.UserService;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

@Component
public class ConnectionRecoveryStatusSchedule {

    @Autowired
    private DataSourceService dataSourceService;

    @Autowired
    private UserService userService;


    @Scheduled(fixedDelay = 1000 * 60)
    @SchedulerLock(name ="connection_recovery_status_lock", lockAtMostFor = "10s", lockAtLeastFor = "10s")
    public void recoveryStatus() {
        Criteria criteria = Criteria.where("status").is(DataSourceConnectionDto.STATUS_TESTING)
                .and("testTime").lt(System.currentTimeMillis() - 90 * 1000);
        Query query = new Query(criteria);
        List<DataSourceConnectionDto> all = dataSourceService.findAll(query);
        List<String> userIdList = all.stream().map(BaseDto::getUserId).distinct().collect(Collectors.toList());

        if (CollectionUtils.isEmpty(userIdList)) {
            return;
        }

        Map<String, UserDetail> userMap = userService.getUserMapByIdList(userIdList);

        for (DataSourceConnectionDto connectionDto : all) {
            if (userMap.get(connectionDto.getUserId()) == null) {
                continue;
            }
            Update update = Update.update("status", Optional.ofNullable(connectionDto.getLastStatus()).orElse(DataSourceConnectionDto.STATUS_INVALID))
                    .set("testTime", System.currentTimeMillis());
            dataSourceService.updateByIdNotChangeLast(connectionDto.getId(), update, userMap.get(connectionDto.getUserId()));
        }
    }
}
