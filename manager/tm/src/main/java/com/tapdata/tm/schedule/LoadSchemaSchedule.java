package com.tapdata.tm.schedule;

import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.commons.base.dto.BaseDto;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.entity.DataSourceEntity;
import com.tapdata.tm.ds.repository.DataSourceRepository;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.metadatainstance.service.MetadataInstancesService;
import com.tapdata.tm.schedule.util.LoadSchemaScheduleUtil;
import com.tapdata.tm.user.service.UserService;
import lombok.extern.slf4j.Slf4j;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.apache.commons.collections.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
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

    @Autowired
    private SettingsService settingsService;

    @Autowired
    private LoadSchemaScheduleUtil scheduleUtil;

    private final Map<String, Object> settingMap = new ConcurrentHashMap<>();

    /** 每分钟刷新一次全局配置，
     * 这里仅获取模型加载的配置：
     * connection_schema_update_hour 和 connection_schema_update_interval
     * */
    @Scheduled(cron = "0 0/1 * * * ?")
    public void loadSettings() {
			Thread.currentThread().setName(getClass().getSimpleName() + "-loadSettings");
        scheduleUtil.loadSettings(settingsService, settingMap);
    }

    /** 每小时执行一次
     *  数据源配置的时间不是false，取全局配置的频率在配置的时间加载一下模型
     *  数据源配置的时间是false时，取全局的时间和频率在这个时间加载一次模型
     * */
    @Scheduled(cron = "0 0 0/1 * * ?")
    @SchedulerLock(name ="timedReloadSchema_lock", lockAtMostFor = "20s", lockAtLeastFor = "20s")
    public void timedReloadSchema() {
			Thread.currentThread().setName(getClass().getSimpleName() + "-timedReloadSchema");
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

        for (DataSourceConnectionDto dataSource : dataSourceConnectionDtos) {
            scheduleUtil.doLoadSchema(dataSource, userDetailMap, metadataInstancesService, settingMap, dataSourceService);
        }
    }

}
