package com.tapdata.tm.schedule;

import com.tapdata.tm.Settings.constant.SettingsEnum;
import com.tapdata.tm.commons.base.dto.BaseDto;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.schema.ScheduleTimeEnum;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.entity.DataSourceEntity;
import com.tapdata.tm.ds.repository.DataSourceRepository;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.metadatainstance.service.MetadataInstancesService;
import com.tapdata.tm.metadatainstance.vo.SourceTypeEnum;
import com.tapdata.tm.user.service.UserService;
import lombok.extern.slf4j.Slf4j;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.apache.commons.collections.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@Component
public class LoadSchemaSchedule {

    @Autowired
    private DataSourceRepository repository;


    @Autowired
    private DataSourceService dataSourceService;

    @Autowired
    private UserService userService;

    @Autowired
    private MetadataInstancesService metadataInstancesService;

    /** 每小时执行一次 */
    @Scheduled(cron = "0 0 0/1 * * ?")
    @SchedulerLock(name ="timedReloadSchema_lock", lockAtMostFor = "20s", lockAtLeastFor = "20s")
    public void timedReloadSchema() {
        //查询所有的数据源信息
        Query query = new Query();
        query.with(Sort.by("createTime").descending());
        List<DataSourceEntity> dataSourceEntities = repository.findAll(query);
        List<DataSourceConnectionDto> dataSourceConnectionDtos = dataSourceService.convertToDto(dataSourceEntities, DataSourceConnectionDto.class);
        List<String> userList = dataSourceConnectionDtos.stream().map(BaseDto::getUserId).collect(Collectors.toList());

        if (CollectionUtils.isEmpty(userList)) {
            return;
        }
        Map<String, UserDetail> userDetailMap = userService.getUserMapByIdList(userList);

        if (userDetailMap == null || userDetailMap.isEmpty()) {
            return;
        }

        int countThreshold = SettingsEnum.SCHEDULED_LOAD_SCHEMA_COUNT_THRESHOLD.getIntValue(10000);

        int oneDay = 1000 * 60 * 60 * 24;

        for (DataSourceConnectionDto dataSource : dataSourceConnectionDtos) {
            UserDetail user = userDetailMap.get(dataSource.getUserId());

            Criteria criteria = Criteria.where("is_deleted").ne(true)
                    .and("source._id").is(dataSource.getId().toHexString())
                    .and("sourceType").is(SourceTypeEnum.SOURCE.name())
                    .and("meta_type").ne("database")
                    .and("taskId").exists(false);
            long count = metadataInstancesService.count(new Query(criteria));
            long sleepTime = count / 1000;
            if (count < countThreshold) {
                dataSourceService.sendTestConnection(dataSource, true, true, user);
                if (sleepTime > 0) {
                    try {
                        //比较大的表，需要sleep一下
                        Thread.sleep(sleepTime * 1000);
                    } catch (InterruptedException e) {
                    }
                }
                continue;
            }

            int time = ScheduleTimeEnum.getHour(dataSource.getSchemaUpdateHour());
            if (time == ScheduleTimeEnum.FALSE.getValue()) {
                //不需要加载
                continue;
            }
            LocalDateTime now = LocalDateTime.now();
            int hour = now.getHour();
            if (time == hour || System.currentTimeMillis() - dataSource.getLoadSchemaTime().getTime() > oneDay) {
                dataSourceService.sendTestConnection(dataSource, true, true, user);
                try {
                    //比较大的表，需要sleep一下
                    sleepTime = sleepTime > 60 ? 60 : sleepTime;
                    Thread.sleep(sleepTime * 1000);
                } catch (InterruptedException e) {
                }
            }
        }
    }

}
