package io.tapdata.coding.service.loader;

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

public class ProjectMembersLoader extends CodingStarter implements CodingLoader<ProjectMemberParam> {
    //OverlayQueryEventDifferentiator overlayQueryEventDifferentiator = new OverlayQueryEventDifferentiator();
    public static final String TABLE_NAME = "ProjectMembers";
    private Integer currentProjectId;

    public ProjectMembersLoader(TapConnectionContext tapConnectionContext, AtomicReference<String> accessToken) {
        super(tapConnectionContext, accessToken);
        Map<String, Object> project = ProjectsLoader.create(tapConnectionContext, accessToken).get(null);
        if (Checker.isNotEmpty(project)) {
            this.currentProjectId = (Integer) project.get("Id");
        }
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

    private void read(Object offsetState, int recordSize, BiConsumer<List<TapEvent>, Object> consumer, boolean batchFlag) {
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
                    Object updatedAtObj = teamMember.get("UpdatedAt");
                    Long updatedAt = Checker.isEmpty(updatedAtObj) ? System.currentTimeMillis() : (Long) updatedAtObj;
//                    if (!batchFlag) {
//                        Integer teamMemberId = (Integer) teamMember.get("Id");
//                        Integer teamMemberHash = MapUtil.create().hashCode(teamMember);
//                        switch (overlayQueryEventDifferentiator.createOrUpdateEvent(teamMemberId, teamMemberHash)) {
//                            case CREATED_EVENT:
//                                events.add(TapSimplify.insertRecordEvent(teamMember, TABLE_NAME).referenceTime(updatedAt));
//                                break;
//                            case UPDATE_EVENT:
//                                events.add(TapSimplify.updateDMLEvent(null, teamMember, TABLE_NAME).referenceTime(updatedAt));
//                                break;
//                        }
//                    }else {
                    events.add(TapSimplify.insertRecordEvent(teamMember, TABLE_NAME).referenceTime(updatedAt));
//                    }
                    //((CodingOffset)offsetState).offset().put(TABLE_NAME,updatedAt);
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
//        if (!batchFlag) {
//            List<TapEvent> delEvents = overlayQueryEventDifferentiator.delEvent(TABLE_NAME, "Id");
//            if (!delEvents.isEmpty()) {
//                events.addAll(delEvents);
//            }
//        }
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

    /**
     *
     * {
     *     "action": "added",
     *     "event": "MEMBER_CREATED",
     *     "eventName": "添加项目成员",
     *     "sender": {
     *         "id": 8613060,
     *         "login": "cxRfjXWiUi",
     *         "avatar_url": "https://coding-net-production-static-ci.codehub.cn/3b111f6b-f929-4f16-838c-f29ff2c97eb6.jpg?imageView2/1/w/0/h/0",
     *         "url": "https://testhookgavin.coding.net/api/user/key/cxRfjXWiUi",
     *         "html_url": "https://testhookgavin.coding.net/u/cxRfjXWiUi",
     *         "name": "TestHookGavin",
     *         "name_pinyin": "TestHookGavin"
     *     },
     *     "member": {
     *         "id": 8647278,
     *         "login": "vMBtGCrzEP",
     *         "avatar_url": "https://coding-net-production-static-ci.codehub.cn/a0835321-9657-48ce-950a-d196b75e4ed3.png?imageView2/1/w/0/h/0",
     *         "url": "https://testhookgavin.coding.net/api/user/key/vMBtGCrzEP",
     *         "html_url": "https://testhookgavin.coding.net/u/vMBtGCrzEP",
     *         "name": "邱迎豪",
     *         "name_pinyin": "qyh|qiuyinghao"
     *     },
     *     "project": {
     *         "id": 11021181,
     *         "icon": "/static/project_icon/scenery-version-2-12.svg",
     *         "url": "https://testhookgavin.coding.net/p/testissue",
     *         "description": "测试Hookhello",
     *         "name": "testissue",
     *         "display_name": "TestIssue"
     *     },
     *     "team": {
     *         "id": 3862723,
     *         "domain": "testhookgavin",
     *         "url": "https://testhookgavin.coding.net",
     *         "introduction": "",
     *         "name": "TestHookGavin",
     *         "name_pinyin": "TestHookGavin",
     *         "avatar": "https://coding-net-production-pp-ci.codehub.cn/9837b4a6-442b-4513-b51d-a2030c4a6ede.png"
     *     },
     *     "hook": {
     *         "id": "17fb4cb3-c679-4fb1-829c-1f02ffe2a815",
     *         "name": "web",
     *         "type": "Repository",
     *         "active": false,
     *         "events": [
     *             "CI_JOB_FINISHED",
     *             "CI_JOB_DELETED",
     *             "ARTIFACTS_REPO_DELETED",
     *             "WIKI_CREATED",
     *             "GIT_MR_NOTE",
     *             "WIKI_DELETED",
     *             "ITERATION_DELETED",
     *             "FILE_RESTORED_FROM_RECYCLE_BIN",
     *             "ISSUE_ITERATION_CHANGED",
     *             "GIT_MR_CLOSED",
     *             "FILE_COPIED",
     *             "ARTIFACTS_VERSION_DOWNLOADED",
     *             "MEMBER_CREATED",
     *             "ISSUE_ASSIGNEE_CHANGED",
     *             "ITERATION_CREATED",
     *             "ISSUE_STATUS_UPDATED",
     *             "CI_JOB_UPDATED",
     *             "ARTIFACTS_VERSION_DOWNLOAD_FORBIDDEN",
     *             "ISSUE_COMMENT_CREATED",
     *             "FILE_CREATED",
     *             "MEMBER_DELETED",
     *             "FILE_UPDATED",
     *             "GIT_MR_CREATED",
     *             "ARTIFACTS_VERSION_CREATED",
     *             "ARTIFACTS_VERSION_DOWNLOAD_BLOCKED",
     *             "ARTIFACTS_REPO_UPDATED",
     *             "WIKI_SHARE_UPDATED",
     *             "GIT_MR_MERGED",
     *             "WIKI_COPIED",
     *             "MEMBER_ROLE_UPDATED",
     *             "WIKI_UPDATED",
     *             "ARTIFACTS_VERSION_DOWNLOAD_ALLOWED",
     *             "FILE_MOVED_TO_RECYCLE_BIN",
     *             "ITERATION_PLANNED",
     *             "ISSUE_CREATED",
     *             "ARTIFACTS_REPO_CREATED",
     *             "ISSUE_UPDATED",
     *             "WIKI_MOVED_TO_RECYCLE_BIN",
     *             "ARTIFACTS_VERSION_RELEASED",
     *             "FILE_MOVED",
     *             "WIKI_ACCESS_UPDATED",
     *             "ISSUE_DELETED",
     *             "CI_JOB_STARTED",
     *             "ARTIFACTS_VERSION_DELETED",
     *             "WIKI_RESTORED_FROM_RECYCLE_BIN",
     *             "ISSUE_RELATIONSHIP_CHANGED",
     *             "CI_JOB_CREATED",
     *             "ITERATION_UPDATED",
     *             "GIT_MR_UPDATED",
     *             "GIT_PUSHED",
     *             "ARTIFACTS_VERSION_UPDATED",
     *             "ISSUE_HOUR_RECORD_UPDATED",
     *             "FILE_RENAMED",
     *             "FILE_SHARE_UPDATED",
     *             "FILE_DELETED",
     *             "WIKI_MOVED"
     *         ],
     *         "config": {
     *             "content_type": "application/json",
     *             "url": "http://183.238.176.18:9909/api/"
     *         },
     *         "created_at": 1665197725000,
     *         "updated_at": 1665313386000
     *     },
     *     "hook_id": "17fb4cb3-c679-4fb1-829c-1f02ffe2a815"
     * }
     * */

//    public Map<Integer,Integer> recordHashMap = new HashMap<>();
//
//    //用来记录上一轮事件完成后的全部记录的主键
//    //如果某个记录的主键在这个Set中存在，并且在recordHashMap中存在，那么表示上一轮操作和本论操作都存在这个记录，否则表示这个记录被删除了
//    public Set<Integer> currentBatchSet = new HashSet<>();
//    public String createOrUpdateEvent(Integer code, Integer hash){
//        String event = recordHashMap.containsKey(code) ?
//                (!hash.equals(recordHashMap.get(code)) ? UPDATE_EVENT : null)
//                : CREATED_EVENT;
//        if (Checker.isNotEmpty(event)) {
//            currentBatchSet.add(code);
//            recordHashMap.put(code, hash);
//            return event;
//        }
//        return "NOT_EVENT";
//    }
//    public List<TapEvent> delEvent(String tableName,String primaryKeyName){
//        List<TapEvent> events = new ArrayList<>();
//        recordHashMap.forEach((iterationCode,hash)->{
//            if (!currentBatchSet.contains(iterationCode)){
//                events.add(TapSimplify.deleteDMLEvent(
//                        new HashMap<String,Object>(){{put(primaryKeyName,iterationCode);}}
//                        ,tableName
//                ).referenceTime(System.currentTimeMillis()));
//            }
//        });
//        currentBatchSet.clear();
//        return events;
//    }
}
