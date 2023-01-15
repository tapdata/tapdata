package com.tapdata.tm.dataflow.service;

import com.tapdata.manager.common.utils.StringUtils;
import com.tapdata.tm.Settings.constant.SettingsEnum;
import com.tapdata.tm.base.dto.Where;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.dataflow.dto.DataFlowDto;
import com.tapdata.tm.dataflow.entity.DataFlow;
import com.tapdata.tm.inspect.dto.InspectDto;
import com.tapdata.tm.inspect.service.InspectService;
import com.tapdata.tm.task.service.TaskService;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.GroupOperation;
import org.springframework.data.mongodb.core.aggregation.MatchOperation;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

import static com.tapdata.tm.dataflow.dto.DataFlowStatus.*;

/**
 * @Author: Zed
 * @Date: 2021/10/22
 * @Description:
 */
@Service
public class DataflowChartService {
    @Autowired
    private DataFlowService dataFlowService;

    @Autowired
    private MongoTemplate mongoTemplate;

    @Autowired
    private InspectService inspectService;


    @Autowired
    private TaskService taskService;

    private static final String [] statues = {"cdc", "Lag", "initialized", "initializing"};


    /**
     * {
     *     "chart1": {
     *        // 迁移
     *         "total": 33,
     *         "items": [
     *             {
     *                 "_id": "running",
     *                 "count": 1
     *             },
     *             {
     *                 "_id": "paused",
     *                 "count": 1
     *             },
     *             {
     *                 "_id": "draft",
     *                 "count": 1
     *             },
     *             {
     *                 "_id": "error",
     *                 "count": 1
     *             }
     *         ]
     *     },
     *     "chart2": {   // 迁移状态
     *         "Lag": 4,   增量之后
     *         "cdc": 138,  增量中
     *         "initialized": 89,
     *         "initializing": 5
     *     },
     *     "chart3": {        // 同步
     *         "total": 33,
     *         "items": [
     *             {
     *                 "_id": "running",
     *                 "count": 1
     *             },
     *             {
     *                 "_id": "paused",
     *                 "count": 1
     *             },
     *             {
     *                 "_id": "draft",
     *                 "count": 1
     *             },
     *             {
     *                 "_id": "error",
     *                 "count": 1
     *             }
     *         ]
     *     },
     *     "chart4": {
     *        // 同步状态
     *         "Lag": 4,
     *         "cdc": 138,
     *         "initialized": 89,
     *         "initializing": 5
     *     },
     *     "chart5": {
     *         "countDiff": 1,
     *         "error": 0,
     *         "passed": 0,
     *         "total": 1,
     *         "valueDiff": 0
     *     },
     *     "chart6": [
     *         {
     *             "totalDelete": 0,
     *             "totalInput": 581593,
     *             "totalInsert": 260760,
     *             "totalOutput": 581593,
     *             "totalUpdate": 178026
     *         }
     *     ]
     * }
     * dataflow 数据复制
     * task      数据开发
     * @param user
     * @return
     */

    /**
     *  数据复制预览
     * @param user
     * @return
     */
  /*  private Map<String, Object> dataCopyPreview(UserDetail user) {
        Criteria criteria = Criteria.where("status").exists(true).ne(null)
                .and("mappingTemplate").is("cluster-clone");
        long count = dataFlowService.count(new Query(criteria), user);

        criteria.and("user_id").is(user.getUserId());
        MatchOperation match = Aggregation.match(criteria);
        GroupOperation group = Aggregation.group("status").count().as("count");
        Aggregation aggregation = Aggregation.newAggregation(match, group);
        List<Char1Group> dataFlowGroups = mongoTemplate.aggregate(aggregation, "DataFlows", Char1Group.class).getMappedResults();
        Map<String, Char1Group> groupMap = dataFlowGroups.stream().collect(Collectors.toMap(Char1Group::get_id, d -> d));

        initDataFlowStatus(groupMap, stopping.v);
        if (groupMap.get(force_stopping.v) != null) {
            Char1Group forceStoppingGroup = groupMap.get(force_stopping.v);
            Char1Group dataFlowGroup = groupMap.get(stopping.v);
            dataFlowGroup.setCount(dataFlowGroup.getCount() + forceStoppingGroup.getCount());
        }

        initDataFlowStatus(groupMap, draft.v);
        initDataFlowStatus(groupMap, paused.v);
        initDataFlowStatus(groupMap, error.v);
        initDataFlowStatus(groupMap, running.v);

        List<Char1Group> values = new ArrayList<>(groupMap.values());
        values.sort(Comparator.comparing(Char1Group::get_id));
        Map<String, Object> chart1Result = new HashMap<>();
        chart1Result.put("total", count);
        chart1Result.put("items", values);
        return chart1Result;
    }*/

    private static void initDataFlowStatus(Map<String, Char1Group> groupMap, String status) {
        if (groupMap.get(status) == null) {
            groupMap.put(status, new Char1Group(status, 0));
        }
    }




    private List<Char2Group> chart2(UserDetail user) {
        Criteria userCriteria = Criteria.where("user_id").is(user.getUserId());
        MatchOperation match = Aggregation.match(userCriteria);
        GroupOperation group = Aggregation.group()
                .sum("stats.input.rows").as("totalInput")
                .sum("stats.output.rows").as("totalOutput")
                .sum("stats.input.dataSize").as("totalInputDataSize")
                .sum("stats.output.dataSize").as("totalOutputDataSize")
                .sum("stats.insert.rows").as("totalInsert")
                .sum("stats.insert.dataSize").as("totalInsertSize")
                .sum("stats.update.rows").as("totalUpdate")
                .sum("stats.update.dataSize").as("totalUpdateSize")
                .sum("stats.delete.rows").as("totalDelete")
                .sum("stats.delete.dataSize").as("totalDeleteSize");
        Aggregation aggregation = Aggregation.newAggregation(match, group);
        List<Char2Group> dataFlowGroups = mongoTemplate.aggregate(aggregation, "DataFlows", Char2Group.class).getMappedResults();
        if (CollectionUtils.isEmpty(dataFlowGroups)) {
            dataFlowGroups = new ArrayList<>();
            dataFlowGroups.add(new Char2Group());
        }

        return dataFlowGroups;
    }


  /*  private List<DataFlowDto> chart3(UserDetail user) {
        Query query = new Query();
        query.fields().include("id", "name", "status", "executeMode", "category", "stopOnError", "last_updated", "createTime", "children", "stats", "checked", "stages", "stages.id", "stages.name", "setting", "user_id", "startTime", "listtags");
        query.skip(0);
        query.limit(10);
        query.with(Sort.by(Sort.Direction.DESC, "createTime"));

        List<DataFlow> dataFlows = dataFlowService.findAll(query, user);
        return dataFlowService.convertToDto(dataFlows, DataFlowDto.class);
    }
*/

    /**
     * 复制任务状态
     * @param user
     * @return
     */
    private Map<String, Integer> copyTaskStatus(UserDetail user) {
        Criteria criteria = Criteria.where("mappingTemplate").is("cluster-clone");
        Query query = new Query(criteria);
        query.fields().include("stats", "setting");

        List<DataFlow> dataFlows = dataFlowService.findAll(query, user);
        List<DataFlowDto> dataFlowDtos = dataFlowService.convertToDto(dataFlows, DataFlowDto.class);


        Map<String, String> rsObj = new HashMap<>();
        for (DataFlowDto dataFlowDto : dataFlowDtos) {
            String id = dataFlowDto.getId().toHexString();
            int userLag = 0;
            if (dataFlowDto.getSetting() != null && dataFlowDto.getSetting().get("userSetLagTime") != null
                    && dataFlowDto.getSetting().get("lagTimeFalg") != null && dataFlowDto.getSetting().get("lagTime") != null) {
                userLag = (int)dataFlowDto.getSetting().get("lagTime");
            } else {
                userLag = SettingsEnum.JOB_LAG_TIME.getIntValue(0);
            }

            if (dataFlowDto.getStats() != null && dataFlowDto.getStats().get("stagesMetrics") != null) {

                List<Map<String, Object>> stagesMetrics = (List<Map<String, Object>>) dataFlowDto.getStats().get("stagesMetrics");
                for (Map<String, Object> stagesMetric : stagesMetrics) {
                    String value = rsObj.get(id);
                    String status = (String) stagesMetric.get("status");
                    if ("cdc".equals(status) && ((int)stagesMetric.get("replicationLag")) > userLag
                            && (value == null || "cdc".equals(value))) {
                        rsObj.put(id, "Lag");
                    } else if ("cdc".equals(status) && value == null) {
                        rsObj.put(id, "cdc");
                    } else if ("initialized".equals(status) && value == null) {
                        rsObj.put("id", "initialized");
                    } else if ("initializing".equals(status) && (value == null || "initialized".equals(value))) {
                        rsObj.put("id", "initializing");
                    }
                }
            }
        }

        Map<String, List<String>> group = rsObj.values().stream().collect(Collectors.groupingBy(s -> s));
        Map<String, Integer> char4 = new HashMap<>();
        for (String status : statues) {
            List<String> list = group.get(status);
            char4.put(status, list == null ? 0 : list.size());
        }
        char4.put("cdc", char4.get("cdc") + char4.get("Lag"));
        return char4;
    }


    private Map<String, Object> chart5(UserDetail user) {
        Criteria criteria = Criteria.where("status").exists(true).ne(null)
                .and("mappingTemplate").is("custom");
        long count = dataFlowService.count(new Query(criteria), user);

        criteria.and("user_id").is(user.getUserId());
        MatchOperation match = Aggregation.match(criteria);
        GroupOperation group = Aggregation.group("status").count().as("count");
        Aggregation aggregation = Aggregation.newAggregation(match, group);
        List<Char1Group> dataFlowGroups = mongoTemplate.aggregate(aggregation, "DataFlows", Char1Group.class).getMappedResults();
        Map<String, Char1Group> groupMap = dataFlowGroups.stream().collect(Collectors.toMap(Char1Group::get_id, d -> d));

        initDataFlowStatus(groupMap, stopping.v);
        if (groupMap.get(force_stopping.v) != null) {
            Char1Group forceStoppingGroup = groupMap.get(force_stopping.v);
            Char1Group dataFlowGroup = groupMap.get(stopping.v);
            dataFlowGroup.setCount(dataFlowGroup.getCount() + forceStoppingGroup.getCount());
        }

        initDataFlowStatus(groupMap, draft.v);
        initDataFlowStatus(groupMap, paused.v);
        initDataFlowStatus(groupMap, error.v);
        initDataFlowStatus(groupMap, running.v);

        List<Char1Group> values = new ArrayList<>(groupMap.values());
        values.sort(Comparator.comparing(Char1Group::get_id));
        Map<String, Object> chart5Result = new HashMap<>();
        chart5Result.put("totalDataFlows", count);
        chart5Result.put("statusCount", values);
        return chart5Result;
    }


    private Map<String, Integer> chart6(UserDetail user) {
        Criteria criteria = Criteria.where("mappingTemplate").is("custom");
        Query query = new Query(criteria);
        query.fields().include("stats", "setting");

        List<DataFlow> dataFlows = dataFlowService.findAll(query, user);
        List<DataFlowDto> dataFlowDtos = dataFlowService.convertToDto(dataFlows, DataFlowDto.class);


        Map<String, String> rsObj = new HashMap<>();
        for (DataFlowDto dataFlowDto : dataFlowDtos) {
            String id = dataFlowDto.getId().toHexString();
            int userLag = 0;
            if (dataFlowDto.getSetting() != null && dataFlowDto.getSetting().get("userSetLagTime") != null && dataFlowDto.getSetting().get("lagTimeFalg") != null) {
                userLag = dataFlowDto.getSetting().get("lagTime") == null ? 0 : ((int)dataFlowDto.getSetting().get("lagTime"));
            } else {
                userLag = SettingsEnum.JOB_LAG_TIME.getIntValue(0);
            }

            if (dataFlowDto.getStats() != null && dataFlowDto.getStats().get("stagesMetrics") != null) {

                List<Map<String, Object>> stagesMetrics = (List<Map<String, Object>>) dataFlowDto.getStats().get("stagesMetrics");
                for (Map<String, Object> stagesMetric : stagesMetrics) {
                    String value = rsObj.get(id);
                    String status = (String) stagesMetric.get("status");
                    if ("cdc".equals(status) && ((int)stagesMetric.get("replicationLag")) > userLag
                            && (value == null || "cdc".equals(value))) {
                        rsObj.put(id, "Lag");
                    } else if ("cdc".equals(status) && value == null) {
                        rsObj.put(id, "cdc");
                    } else if ("initialized".equals(status) && value == null) {
                        rsObj.put("id", "initialized");
                    } else if ("initializing".equals(status) && (value == null || "initialized".equals(value))) {
                        rsObj.put("id", "initializing");
                    }
                }
            }
        }
        Map<String, List<String>> group = rsObj.values().stream().collect(Collectors.groupingBy(s -> s));
        Map<String, Integer> char6 = new HashMap<>();
        for (String statue : statues) {
            List<String> list = group.get(statue);
            char6.put(statue, list == null ? 0 : list.size());

        }
        char6.put("cdc", char6.get("cdc") + char6.get("Lag"));
        return char6;
    }


   /* private Map<String, Integer> chart7(UserDetail user) {
        List<InspectDto> inspectDtos = inspectService.findAll(new Where(), user);
        if (CollectionUtils.isNotEmpty(inspectDtos)) {
            inspectService.joinResult(inspectDtos);
        }

        int error = 0;
        int passed = 0;
        int countDiff = 0;
        int valueDiff = 0;
        for (InspectDto inspectDto : inspectDtos) {
            if ("error".equals(inspectDto.getStatus())) {
                error++;
            } else if ("passed".equals(inspectDto.getResult())) {
                passed++;
            } else if ("row_count".equals(inspectDto.getInspectMethod())) {
                countDiff++;
            } else {
                valueDiff++;
            }
        }
        Map<String, Integer> chart7 = new HashMap<>();
        chart7.put("total", inspectDtos.size());
        chart7.put("error", error);
        chart7.put("passed", passed);
        chart7.put("countDiff", countDiff);
        chart7.put("valueDiff", valueDiff);
        return chart7;

    }*/

   /* private Map<String, Object> chart8(UserDetail user) {
        Criteria criteria = Criteria.where("user_id").is(user.getUserId());
        MatchOperation match = Aggregation.match(criteria);
        GroupOperation group = Aggregation.group("status").count().as("count");
        Aggregation aggregation = Aggregation.newAggregation(match, group);
        List<Char1Group> dataFlowGroups = mongoTemplate.aggregate(aggregation, "Connections", Char1Group.class).getMappedResults();
        Map<String, Object> chart8 = new HashMap<>();
        int total = 0;
        for (Char1Group dataFlowGroup : dataFlowGroups) {
            if (StringUtils.isNotBlank(dataFlowGroup.get_id())) {
                chart8.put(dataFlowGroup.get_id(), dataFlowGroup.getCount());
                total += dataFlowGroup.getCount();
            }
        }
        chart8.put("total", total);
        return chart8;
    }*/

    private Map<String, Object> chart9(UserDetail user) {
        Criteria criteria = Criteria.where("user_id").is(user.getUserId());
        MatchOperation match = Aggregation.match(criteria);

        GroupOperation group = Aggregation.group("setting.sync_type").count().as("count");
        Aggregation aggregation = Aggregation.newAggregation(match, group);
        List<Char1Group> dataFlowGroups = mongoTemplate.aggregate(aggregation, "DataFlows", Char1Group.class).getMappedResults();
        Map<String, Object> chart9 = new HashMap<>();
        int total = 0;
        for (Char1Group dataFlowGroup : dataFlowGroups) {
            if (StringUtils.isNotBlank(dataFlowGroup.get_id())) {
                chart9.put(dataFlowGroup.get_id(), dataFlowGroup.getCount());
                total += dataFlowGroup.getCount();
            }
        }
        chart9.put("total", total);
        return chart9;
    }



    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Char1Group {
        private String _id;
        private long count;
    }

    @Data
    public static class Char2Group {
        private long totalInput = 0;
        private long totalOutput = 0;
        private long totalInputDataSize = 0;
        private long totalOutputDataSize = 0;
        private long totalInsert = 0;
        private long totalInsertSize = 0;
        private long totalUpdate = 0;
        private long totalUpdateSize = 0;
        private long totalDelete = 0;
        private long totalDeleteSize = 0;
    }
}
