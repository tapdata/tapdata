package com.tapdata.tm.task.service;

import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.commons.dag.logCollector.LogCollecotrConnConfig;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.service.impl.DataSourceDefinitionService;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.task.bean.*;
import com.tapdata.tm.task.param.TableLogCollectorParam;
import com.tapdata.tm.utils.Lists;

import javax.annotation.Nullable;
import java.util.*;

public interface LogCollectorService {
    @Nullable
    static Date getAttrsValues(String sourceId, String targetId, String type, Map<String, Object> attrs) {
        try {
            if (attrs == null) {
                return null;
            }
            Object syncProgress = attrs.get("syncProgress");
            if (syncProgress == null) {
                return null;
            }

            Map syncProgressMap = (Map) syncProgress;
            List<String> key = Lists.newArrayList(sourceId, targetId);

            String valueMapString = (String) syncProgressMap.get(JsonUtil.toJsonUseJackson(key));
            LinkedHashMap valueMap = JsonUtil.parseJson(valueMapString, LinkedHashMap.class);
            if (valueMap == null) {
                return null;
            }

            Object o = valueMap.get(type);
            if (o == null) {
                return null;
            }

            return new Date(((Double) o).longValue());

        } catch (Exception e) {
            return null;
        }
    }

    static void main(String[] args) {
        Map<String, LogCollecotrConnConfig> logCollectorConnConfigMap = new HashMap<String, LogCollecotrConnConfig>() {{
            put("connectionId_1", new LogCollecotrConnConfig("connectionId_1",
                    new ArrayList<String>() {{
                        add("table_1");
                        add("table_2");
                        add("table_3");
                    }},
                    new ArrayList<String>() {{
                        add("table_3");
                        add("table_4");
                        add("table_5");
                        add("table_6");
                    }}
            ));
        }};
        List<TableLogCollectorParam> addParams = new ArrayList<TableLogCollectorParam>() {{
            add(new TableLogCollectorParam("connectionId_1", new HashSet<String>() {{
                add("table_4");
                add("table_5");
            }}));
        }};

        List<TableLogCollectorParam> exclusionParams = new ArrayList<TableLogCollectorParam>() {{
            add(new TableLogCollectorParam("connectionId_1", new HashSet<String>() {{
                add("table_1");
                add("table_2");
            }}));
        }};

//		Map<String, LogCollecotrConnConfig> logCollecotrConnConfigMap = new LogCollectorService().addTables(logCollectorConnConfigMap, addParams);
//		System.out.println(logCollecotrConnConfigMap);


    }

    Page<LogCollectorVo> find(Filter filter, UserDetail user);

    List<TaskDto> findSyncTaskById(TaskDto taskDto, UserDetail user);

    List<LogCollectorVo> findByTaskId(String taskId, UserDetail user);

    List<LogCollectorVo> findBySubTaskId(String taskId, UserDetail user);

    Page<LogCollectorVo> findByConnectionName(String name, String connectionName, UserDetail user, int skip, int limit, List<String> sort);

    boolean checkCondition(UserDetail user);

    void update(LogCollectorEditVo logCollectorEditVo, UserDetail user);

    LogCollectorDetailVo findDetail(String id, UserDetail user);

    LogSystemConfigDto findSystemConfig(UserDetail loginUser);

    void updateSystemConfig(LogSystemConfigDto logSystemConfigDto, UserDetail user);

    Boolean checkUpdateConfig(UserDetail user);

    Boolean checkUpdateConfig(String connectionId, UserDetail user);

    Page<Map<String, String>> findTableNames(String taskId, int skip, int limit, UserDetail user);

    Page<Map<String, String>> findCallTableNames(String taskId, String callSubId, int skip, int limit, UserDetail user);

    void logCollector(UserDetail user, TaskDto oldTaskDto);

    void startConnHeartbeat(UserDetail user, TaskDto taskDto);

    void endConnHeartbeat(UserDetail user, TaskDto taskDto);

    void cancelMerge(String taskId, String connectionId, UserDetail user);

    Page<ShareCdcTableInfo> tableInfos(String taskId, String connectionId, String keyword, Integer page, Integer size, UserDetail user);

    Page<ShareCdcTableInfo> excludeTableInfos(String taskId, String connectionId, String keyword, Integer page, Integer size, UserDetail user);

    void configTables(String taskId, List<TableLogCollectorParam> params, String type, UserDetail user);

    List<ShareCdcConnectionInfo> getConnectionIds(String taskId, UserDetail user);

    void clear();

    void removeTask();
}
