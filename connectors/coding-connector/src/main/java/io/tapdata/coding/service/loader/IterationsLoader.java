package io.tapdata.coding.service.loader;

import cn.hutool.json.JSONUtil;
import io.tapdata.coding.CodingConnector;
import io.tapdata.coding.entity.CodingOffset;
import io.tapdata.coding.entity.ContextConfig;
import io.tapdata.coding.entity.param.IterationParam;
import io.tapdata.coding.entity.param.Param;
import io.tapdata.coding.enums.CodingEvent;
import io.tapdata.coding.service.schema.SchemaStart;
import io.tapdata.coding.utils.collection.MapUtil;
import io.tapdata.coding.utils.http.CodingHttp;
import io.tapdata.coding.utils.http.HttpEntity;
import io.tapdata.coding.utils.tool.Checker;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectionContext;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

import static io.tapdata.coding.enums.TapEventTypes.*;

public class IterationsLoader extends CodingStarter implements CodingLoader<IterationParam> {
    private static final String TAG = IterationsLoader.class.getSimpleName();
    public static final String TABLE_NAME = "Iterations";
    Map<String, Object> queryMap;
    private final long streamExecutionGap = 5000;//util: ms
    private int batchReadPageSize = 500;//coding page 1~500,

    private Long lastTimePoint;
    private Set<String> lastTimeSplitIterationCode = new HashSet<>();//hash code list
    int tableSize;

    @Override
    public CodingStarter connectorInit(CodingConnector codingConnector) {
        this.lastTimeSplitIterationCode.addAll(codingConnector.lastTimeSplitIterationCode());
        this.lastTimePoint = codingConnector.iterationsLastTimePoint();
        return super.connectorInit(codingConnector);
    }

    @Override
    public CodingStarter connectorOut() {
        this.codingConnector.lastTimeSplitIterationCode(this.lastTimeSplitIterationCode);
        this.codingConnector.iterationsLastTimePoint(this.lastTimePoint);
        return super.connectorOut();
    }

    public static IterationsLoader create(TapConnectionContext tapConnectionContext, AtomicReference<String> accessToken, Map<String, Object> queryMap) {
        return new IterationsLoader(tapConnectionContext, accessToken, queryMap);
    }

    public IterationsLoader(TapConnectionContext tapConnectionContext, AtomicReference<String> accessToken, Map<String, Object> queryMap) {
        super(tapConnectionContext, accessToken);
        this.queryMap = queryMap;
    }

    public IterationsLoader(TapConnectionContext tapConnectionContext, AtomicReference<String> accessToken) {
        super(tapConnectionContext, accessToken);
    }

    public HttpEntity<String, Object> commandSetter(String command, HttpEntity<String, Object> requestBody) {
        if (Checker.isEmpty(command)) {
            throw new CoreException("Command must be not null or not empty.");
        }
        if (Checker.isEmpty(requestBody)) requestBody = HttpEntity.create();
        DataMap connectionConfig = this.tapConnectionContext.getConnectionConfig();
        if (Checker.isEmpty(this.queryMap)) {
            throw new CoreException("QueryMap is must be null or empty");
        }
        Object pageObj = this.queryMap.get("page");
        if (Checker.isEmpty(pageObj)) {
            throw new CoreException("Page is must be null or empty");
        }
        Object sizeObj = this.queryMap.get("size");
        if (Checker.isEmpty(sizeObj)) {
            throw new CoreException("Size is must be null or empty");
        }
        Integer page = Integer.parseInt(pageObj.toString());//第几页，从一开始
        Integer size = Integer.parseInt(sizeObj.toString());
        Object keyWordsObj = this.queryMap.get("key");
        switch (command) {
            case "DescribeIterationList": {
                requestBody.builder("Limit", size)
                        .builder("Offset", page - 1)
                        .builder("Action", "DescribeIterationList");
                if (Checker.isNotEmpty(keyWordsObj)) {
                    requestBody.builder("keywords", String.valueOf(keyWordsObj).trim());
                }
                break;
            }
            case "DescribeCodingProjects": {
                requestBody.builder("PageSize", size)
                        .builder("PageNumber", page)
                        .builder("Action", "DescribeCodingProjects");
                if (Checker.isNotEmpty(keyWordsObj)) {
                    requestBody.builder("ProjectName", String.valueOf(keyWordsObj).trim());
                }
                break;
            }
            default:
                throw new CoreException("Command only support [DescribeIterationList] or [DescribeCodingProjects] now.");
        }
        return requestBody;
    }

    private List<Map> queryAllIteration(int tableSize) throws Exception {
        DataMap connectionConfig = tapConnectionContext.getConnectionConfig();
        String projectName = connectionConfig.getString("projectName");
        String token = this.accessToken().get();
        String teamName = connectionConfig.getString("teamName");

        int currentQueryCount = 0, queryIndex = 0;

        List<Map> matterList = new ArrayList<>();
        do {
            HttpEntity<String, String> header = HttpEntity.create()
                    .builder("Authorization", token);
            HttpEntity<String, Object> body = HttpEntity.create()
                    .builder("Action", "DescribeIterationList")
                    .builder("ProjectName", projectName)
                    .builder("Limit", tableSize)
                    .builder("Offset", ++queryIndex);
            Map<String, Object> resultMap = CodingHttp.create(
                    header.getEntity(),
                    body.getEntity(),
                    String.format(OPEN_API_URL, teamName)
            ).post();
            Object response = resultMap.get("Response");
            Map<String, Object> responseMap = null != response ? JSONUtil.parseObj(response) : null;
            if (null == response) {
                if (queryIndex > 1) {
                    queryIndex -= 1;
                    break;
                } else {
                    throw new Exception("discover error. " + Optional.ofNullable(responseMap.get(CodingHttp.ERROR_KEY)).orElse(""));
                }
            }

            currentQueryCount = Integer.parseInt(String.valueOf(resultMap.get("PageSize")));
            Map<String, Object> dataMap = null != responseMap.get("data") ? JSONUtil.parseObj(responseMap.get("data")) : null;
            if (null == dataMap || null == dataMap.get("List")) {
                break;
            }
            List<Map> list = JSONUtil.toList(JSONUtil.parseArray(dataMap.get("List")), Map.class);
            matterList.addAll(list);
        } while (currentQueryCount < tableSize);

        return matterList;
    }

    @Override
    public Long streamReadTime() {
        return 5 * 60 * 1000l;
    }

    @Override
    public List<Map<String, Object>> list(IterationParam param) {
        return list(this.codingHttp(param));
    }

    public List<Map<String, Object>> list(CodingHttp http) {
        Map<String, Object> resultMap = http.post();
        Object response = resultMap.get("Response");
        if (null == response) {
            throw new CoreException("can not get iteration list, the 'Response' is empty. " + Optional.ofNullable(resultMap.get(CodingHttp.ERROR_KEY)).orElse(""));
        }
        Map<String, Object> responseMap = (Map<String, Object>) response;
        Object dataObj = responseMap.get("data");
        if (null == dataObj) {
            throw new CoreException("can not get iteration list, the 'data' is empty.");
        }
        Map<String, Object> data = (Map<String, Object>) dataObj;
        Object listObj = data.get("List");
        return null != listObj ? (List<Map<String, Object>>) listObj : null;
    }

    @Override
    public List<Map<String, Object>> all(IterationParam param) {
        CodingHttp codingHttp = this.codingHttp(param);
        int offset = 0;//从0开始
        List<Map<String, Object>> result = new ArrayList<>();
        while (true) {
            List<Map<String, Object>> list = list(codingHttp);
            if (Checker.isEmpty(list) || list.isEmpty()) {
                break;
            }
            result.addAll(list);
            if (list.size() < param.limit()) {
                break;
            }
            offset++;
            codingHttp.buildBody("Offset", offset);
        }
        return result;
    }

    @Override
    public CodingHttp codingHttp(IterationParam param) {
        ContextConfig contextConfig = this.veryContextConfigAndNodeConfig();
        final int maxLimit = 500;//@TODO 最大分页数
        if (param.limit() > maxLimit) param.limit(maxLimit);
        HttpEntity<String, String> header = HttpEntity.create()
                .builder("Authorization", this.accessToken().get());
        HttpEntity<String, Object> body = HttpEntity.create()
                .builderIfNotAbsent("Action", "DescribeIterationList")
                .builder("ProjectName", contextConfig.getProjectName())
                .builder("Offset", param.offset())
                .builder("Limit", param.limit())
                .builderIfNotAbsent("Assignee", param.assignee())
                .builderIfNotAbsent("Status", param.status());
        return CodingHttp.create(
                header.getEntity(),
                body.getEntity(),
                String.format(OPEN_API_URL, contextConfig.getTeamName()));
    }

    @Override
    public void batchRead(Object offset, int batchCount, BiConsumer<List<TapEvent>, Object> consumer) {
        Long readEnd = System.currentTimeMillis();
        CodingOffset codingOffset = new CodingOffset();
        //current read end as next read begin
        codingOffset.setTableUpdateTimeMap(new HashMap<String, Long>() {{
            put(TABLE_NAME, readEnd);
        }});
        this.read(offset, null, readEnd, batchCount, consumer, false);
    }

    @Override
    public int batchCount() throws Throwable {
        Param param = IterationParam.create().limit(1).offset(1);
        Map<String, Object> resultMap = this.codingHttp((IterationParam) param).post();
        Object response = resultMap.get("Response");
        if (null == response) {
            return 0;
        }
        Map<String, Object> responseMap = (Map<String, Object>) response;
        Object dataObj = responseMap.get("data");
        if (null == dataObj) {
            return 0;
        }
        Map<String, Object> data = (Map<String, Object>) dataObj;
        Object totalRowObj = data.get("TotalRow");
        return null != totalRowObj ? (Integer) totalRowObj : 0;
    }

    @Override
    public void streamRead(List<String> tableList, Object offsetState, int recordSize, StreamReadConsumer consumer) {
        CodingOffset codingOffset = null != offsetState && offsetState instanceof CodingOffset
                ? (CodingOffset) offsetState : new CodingOffset();
        Map<String, Long> tableUpdateTimeMap = codingOffset.getTableUpdateTimeMap();
        if (null == tableUpdateTimeMap || tableUpdateTimeMap.isEmpty()) {
            TapLogger.warn(TAG, "offsetState is Empty or not Exist!");
            return;
        }
        String currentTable = tableList.get(0);
        long current = tableUpdateTimeMap.get(currentTable);
        Long last = Long.MAX_VALUE;
        this.read(codingOffset, current, last, recordSize, consumer, true);
    }

    @Override
    public List<TapEvent> rawDataCallbackFilterFunction(Map<String, Object> issueEventData) {
        CodingEvent issueEvent = this.getRowDataCallBackEvent(issueEventData);
        if (Checker.isEmpty(issueEvent)) return null;
        if (!TABLE_NAME.equals(issueEvent.getEventGroup())) return null;//拒绝处理非此表相关事件

        String eventType = issueEvent.getEventType();
        TapEvent event = null;

        Object iterationObj = issueEventData.get("iteration");
        if (Checker.isEmpty(iterationObj)) {
            return null;
        }
        Map<String, Object> iteration = (Map<String, Object>) iterationObj;

        Map<String, Object> iterationMap = SchemaStart.getSchemaByName(TABLE_NAME, accessToken()).autoSchema(iteration);

        Object referenceTimeObj = iterationMap.get("UpdatedAt");
        Long referenceTime = Checker.isEmpty(referenceTimeObj) ? System.currentTimeMillis() : (Long) referenceTimeObj;

        switch (eventType) {
            case DELETED_EVENT: {
                event = TapSimplify.deleteDMLEvent(iterationMap, TABLE_NAME).referenceTime(referenceTime);
            }
            ;
            break;
            case UPDATE_EVENT: {
                event = TapSimplify.updateDMLEvent(null, iterationMap, TABLE_NAME).referenceTime(referenceTime);
            }
            ;
            break;
            case CREATED_EVENT: {
                event = TapSimplify.insertRecordEvent(iterationMap, TABLE_NAME).referenceTime(referenceTime);
            }
            ;
            break;
        }
        TapLogger.debug(TAG, "From WebHook coding completed a event [{}] for [{}] table: event data is - {}", eventType, TABLE_NAME, iterationMap);
        return Collections.singletonList(event);
    }

    private void read(Object offsetState,
                      Long readStartTime,
                      Long readEndTime,
                      int batchCount,
                      BiConsumer<List<TapEvent>, Object> consumer, boolean isStreamRead) {
        if (Checker.isEmpty(offsetState)) {
            offsetState = new CodingOffset();
        }
        if (this.lastTimePoint == null) this.lastTimePoint = 0L;
        CodingOffset offset = (CodingOffset) offsetState;
        long startPage = Optional.ofNullable(offset.getTableUpdateTimeMap().get(TABLE_NAME)).orElse(0L);
        Param param = IterationParam.create()
                .startDate(readStartTime)
                .endDate(readEndTime)
                .limit(batchCount)
                .offset((int) startPage);
        CodingHttp codingHttp = this.codingHttp((IterationParam) param);
        List<TapEvent> events = new ArrayList<>();
        while (this.sync()) {
            Map<String, Object> resultMap = codingHttp.buildBody("Offset", startPage).post();
            Object response = resultMap.get("Response");
            if (null == response) {
                return;
            }
            Map<String, Object> responseMap = (Map<String, Object>) response;
            Object dataObj = responseMap.get("Data");
            if (null == dataObj) {
                return;
            }
            Map<String, Object> data = (Map<String, Object>) dataObj;
            Object listObj = data.get("List");
            if (Checker.isNotEmpty(listObj)) {
                List<Map<String, Object>> result = (List<Map<String, Object>>) listObj;
                for (Map<String, Object> iteration : result) {
                    Long referenceTime = (Long) iteration.get("UpdatedAt");
                    Long createdAt = (Long) iteration.get("CreatedAt");
                    String iterationHash = this.key(iteration, createdAt, referenceTime);
                    Long currentTimePoint = referenceTime - referenceTime % (24 * 60 * 60 * 1000);//时间片段
                    if (isStreamRead) {
                        if (currentTimePoint >= this.lastTimePoint && !lastTimeSplitIterationCode.contains(iterationHash)) {
                            if (referenceTime > createdAt) {
                                events.add(TapSimplify.updateDMLEvent(null, iteration, TABLE_NAME).referenceTime(System.currentTimeMillis()));
                            } else {
                                events.add(TapSimplify.insertRecordEvent(iteration, TABLE_NAME).referenceTime(System.currentTimeMillis()));
                            }
                            if (!currentTimePoint.equals(this.lastTimePoint)) {
                                this.lastTimePoint = currentTimePoint;
                                lastTimeSplitIterationCode = new HashSet<>();
                            }
                            lastTimeSplitIterationCode.add(iterationHash);
                        }
                    }else {
                        events.add(TapSimplify.insertRecordEvent(iteration, TABLE_NAME).referenceTime(System.currentTimeMillis()));
                        if (!currentTimePoint.equals(this.lastTimePoint)) {
                            this.lastTimePoint = currentTimePoint;
                            lastTimeSplitIterationCode = new HashSet<>();
                        }
                        lastTimeSplitIterationCode.add(iterationHash);
                    }
                    if (Checker.isEmpty(offset)) {
                        offset = new CodingOffset();
                    }
                    if (events.size() == batchCount) {
                        consumer.accept(events, offset);
                        events = new ArrayList<>();
                    }
                }
                if (result.size() < param.limit()) {
                    break;
                }
                startPage++;
                offset.getTableUpdateTimeMap().put(TABLE_NAME, startPage);
            } else {
                break;
            }
        }
        if (!this.sync()) {
            this.connectorOut();
        }
        if (events.size() > 0) consumer.accept(events, offset);
    }

    public String key(Map<String, Object> iteration, Long createTime, Long updateTime) {
        String code = String.valueOf(iteration.get("Code"));
        return code + createTime + updateTime;
    }
}
