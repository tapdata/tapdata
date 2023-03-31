package io.tapdata.coding.service.loader;

import io.tapdata.coding.CodingConnector;
import io.tapdata.coding.entity.ContextConfig;
import io.tapdata.coding.entity.param.Param;
import io.tapdata.coding.entity.param.ProjectMemberParam;
import io.tapdata.coding.enums.CodingEvent;
import io.tapdata.coding.service.schema.SchemaStart;
import io.tapdata.coding.utils.http.CodingHttp;
import io.tapdata.coding.utils.http.HttpEntity;
import io.tapdata.coding.utils.tool.Checker;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectionContext;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

import static io.tapdata.coding.enums.TapEventTypes.*;
import static io.tapdata.entity.simplify.TapSimplify.toJson;

public class ProjectMembersLoader extends CodingStarter implements CodingLoader<ProjectMemberParam> {
    public static final String TABLE_NAME = "ProjectMembers";
    private Integer currentProjectId;
    private Long lastTimePoint;
    private Set<String> lastTimeProjectMembersCode = new HashSet<>();//hash code list

    public ProjectMembersLoader(TapConnectionContext tapConnectionContext, AtomicReference<String> accessToken) {
        super(tapConnectionContext, accessToken);
        Map<String, Object> project = ProjectsLoader.create(tapConnectionContext, accessToken).get(null);
        if (Checker.isNotEmpty(project)) {
            this.currentProjectId = (Integer) project.get("Id");
        }
    }

    public CodingStarter connectorInit(CodingConnector codingConnector) {
        super.connectorInit(codingConnector);
        this.lastTimeProjectMembersCode.addAll(codingConnector.lastTimeProjectMembersCode());
        return this;
    }

    public CodingStarter connectorOut() {
        this.codingConnector.lastTimeProjectMembersCode(this.lastTimeProjectMembersCode);
        return super.connectorOut();
    }

    public static ProjectMembersLoader create(TapConnectionContext tapConnectionContext, AtomicReference<String> accessToken) {
        return new ProjectMembersLoader(tapConnectionContext, accessToken);
    }

    @Override
    public Long streamReadTime() {
        return 5 * 60 * 1000l;
    }

    @Override
    public List<Map<String, Object>> list(ProjectMemberParam param) {
        Map<String, Object> resultMap = this.codingHttp(param).post();
        Object response = resultMap.get("Response");
        if (null == response) {
            throw new CoreException("can not get project members list, the 'Response' is empty. " + Optional.ofNullable(resultMap.get(CodingHttp.ERROR_KEY)).orElse(""));
        }
        Map<String, Object> responseMap = (Map<String, Object>) response;
        Object dataObj = responseMap.get("Data");
        if (null == dataObj) {
            throw new CoreException("can not get project members list, the 'Data' is empty.");
        }
        Map<String, Object> data = (Map<String, Object>) dataObj;
        Object projectMembersObj = data.get(TABLE_NAME);
        return null != projectMembersObj ? (List<Map<String, Object>>) projectMembersObj : null;
    }

    @Override
    public List<Map<String, Object>> all(ProjectMemberParam param) {
        return null;
    }

    @Override
    public CodingHttp codingHttp(ProjectMemberParam param) {
        final int maxLimit = 500;//@TODO 最大分页数
        if (param.limit() > maxLimit) param.limit(maxLimit);
        ContextConfig contextConfig = this.veryContextConfigAndNodeConfig();
        HttpEntity<String, String> header = HttpEntity.create()
                .builder("Authorization", this.accessToken().get());
        HttpEntity<String, Object> body = HttpEntity.create()
                .builder("Action", "DescribeProjectMembers")
                .builder("ProjectId", this.currentProjectId)
                .builder("PageNumber", param.offset())
                .builder("PageSize", param.limit())
                .builderIfNotAbsent("RoleId", param.roleId());
        return CodingHttp.create(
                header.getEntity(),
                body.getEntity(),
                String.format(OPEN_API_URL, contextConfig.getTeamName()));
    }

    @Override
    public void batchRead(Object offset, int batchCount, BiConsumer<List<TapEvent>, Object> consumer) {
        this.read(offset, batchCount, consumer, true);
    }

    @Override
    public int batchCount() throws Throwable {
        Param param = ProjectMemberParam.create().limit(1).offset(1);
        Map<String, Object> resultMap = this.codingHttp((ProjectMemberParam) param).post();
        Object response = resultMap.get("Response");
        if (null == response) {
            return 0;
        }
        Map<String, Object> responseMap = (Map<String, Object>) response;
        Object dataObj = responseMap.get("Data");
        if (null == dataObj) {
            return 0;
        }
        Map<String, Object> data = (Map<String, Object>) dataObj;
        Object totalCountObj = data.get("TotalCount");
        if (Checker.isEmpty(totalCountObj)) return 0;
        return (Integer) totalCountObj;
    }

    private void read(Object offsetState, int recordSize, BiConsumer<List<TapEvent>, Object> consumer, boolean isStreamRead) {
        int startPage = 1;
        Param param = ProjectMemberParam.create()
                .limit(recordSize)
                .offset(startPage);
        CodingHttp codingHttp = this.codingHttp((ProjectMemberParam) param);
        List<TapEvent> events = new ArrayList<>();
        while (this.sync()) {
            Map<String, Object> resultMap = codingHttp.buildBody("PageNumber", startPage).post();
            Object response = resultMap.get("Response");
            if (null == response) {
                throw new CoreException("can not get the project members's page which 'PageNumber' is " + startPage + ", the 'Response' is empty. " + Optional.ofNullable(resultMap.get(CodingHttp.ERROR_KEY)).orElse(""));
            }
            Map<String, Object> responseMap = (Map<String, Object>) response;
            Object dataObj = responseMap.get("Data");
            if (null == dataObj) {
                throw new CoreException("can not get the project members's page which 'PageNumber' is " + startPage + ", the 'Data' is empty.");
            }
            Map<String, Object> data = (Map<String, Object>) dataObj;
            Object teamMembersObj = data.get(TABLE_NAME);
            if (Checker.isNotEmpty(teamMembersObj)) {
                List<Map<String, Object>> teamMembers = (List<Map<String, Object>>) teamMembersObj;
                for (Map<String, Object> teamMember : teamMembers) {
                    Long updatedAt = System.currentTimeMillis();
                    Long currentTimePoint = updatedAt - updatedAt % (24 * 60 * 60 * 1000);//时间片段
                    String membersHash = this.key(teamMember, null, updatedAt);
                    if (!lastTimeProjectMembersCode.contains(membersHash)) {
                        events.add(TapSimplify.insertRecordEvent(teamMember, TABLE_NAME).referenceTime(System.currentTimeMillis()));
                        if (!currentTimePoint.equals(this.lastTimePoint)) {
                            this.lastTimePoint = currentTimePoint;
                            lastTimeProjectMembersCode = new HashSet<>();
                        }
                        lastTimeProjectMembersCode.add(membersHash);
                    }
                    if (teamMembers.size() == recordSize) {
                        consumer.accept(events, offsetState);
                        events = new ArrayList<>();
                    }
                }
                if (teamMembers.size() < param.limit()) {
                    break;
                }
                startPage += param.limit();
            } else {
                break;
            }
        }
        if (!sync()) {
            this.connectorOut();
        }
        if (events.size() > 0) consumer.accept(events, offsetState);
    }

    @Override
    public void streamRead(List<String> tableList, Object offsetState, int recordSize, StreamReadConsumer consumer) {
        this.read(offsetState, recordSize, consumer, false);
    }

    @Override
    public List<TapEvent> rawDataCallbackFilterFunction(Map<String, Object> issueEventData) {
        CodingEvent issueEvent = this.getRowDataCallBackEvent(issueEventData);
        if (Checker.isEmpty(issueEvent)) return null;
        if (!TABLE_NAME.equals(issueEvent.getEventGroup())) return null;//拒绝处理非此表相关事件

        String eventType = issueEvent.getEventType();
        TapEvent event = null;

        Object memberObj = issueEventData.get("member");
        if (Checker.isEmpty(memberObj)) {
            TapLogger.info(TAG, "A event lack member data in project members row data call back method's param.");
            return null;
        }
        Map<String, Object> member = (Map<String, Object>) memberObj;
        Object teamObj = issueEventData.get("team");
        if (!Checker.isEmpty(teamObj)) {
            Object teamId = ((Map<String, Object>) teamObj).get("team_id");
            member.put("team_id", teamId);
        }
        member.put("project_id", this.currentProjectId);
        Map<String, Object> schemaMap = SchemaStart.getSchemaByName(TABLE_NAME, accessToken()).autoSchema(member);
        Object referenceTimeObj = schemaMap.get("UpdatedAt");
        Long referenceTime = Checker.isEmpty(referenceTimeObj) ? System.currentTimeMillis() : (Long) referenceTimeObj;

        switch (eventType) {
            case DELETED_EVENT: {
                event = TapSimplify.deleteDMLEvent(schemaMap, TABLE_NAME).referenceTime(referenceTime);
            }
            ;
            break;
            case UPDATE_EVENT: {
                event = TapSimplify.updateDMLEvent(null, schemaMap, TABLE_NAME).referenceTime(referenceTime);
            }
            ;
            break;
            case CREATED_EVENT: {
                event = TapSimplify.insertRecordEvent(schemaMap, TABLE_NAME).referenceTime(referenceTime);
            }
            ;
            break;
        }
        TapLogger.debug(TAG, "From WebHook coding completed a event [{}] for [{}] table: event data is - {}", eventType, TABLE_NAME, schemaMap);
        return Collections.singletonList(event);
    }

    public String key(Map<String, Object> iteration, Long createTime, Long updateTime) {
        return toJson(iteration);
    }
}
