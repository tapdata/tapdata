package io.tapdata.coding.service.loader;

import io.tapdata.coding.entity.CodingOffset;
import io.tapdata.coding.entity.ContextConfig;
import io.tapdata.coding.entity.param.CommentParam;
import io.tapdata.coding.utils.http.CodingHttp;
import io.tapdata.coding.utils.http.HttpEntity;
import io.tapdata.coding.utils.tool.Checker;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectionContext;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

/**
 * 事项评论
 */
public class CommentsLoader extends CodingStarter implements CodingLoader<CommentParam> {
    private static final String TAG = CommentsLoader.class.getSimpleName();

    public CommentsLoader(TapConnectionContext tapConnectionContext, AtomicReference<String> accessToken) {
        super(tapConnectionContext, accessToken);
    }

    public static CommentsLoader create(TapConnectionContext tapConnectionContext, AtomicReference<String> accessToken) {
        return new CommentsLoader(tapConnectionContext, accessToken);
    }

    @Override
    public Long streamReadTime() {
        long streamReadTime = 5 * 60 * 1000;
        return streamReadTime;
    }

    @Override
    public List<Map<String, Object>> list(CommentParam param) {
        Map<String, Object> resultMap = this.codingHttp(param).post();
        Object response = resultMap.get("CommentList");
        return null != response ? (List<Map<String, Object>>) response : null;
    }

    @Override
    public List<Map<String, Object>> all(CommentParam param) {
        CodingHttp codingHttp = this.codingHttp(param);
        List<Integer> issueCodes = param.issueCodes();
        List<Map<String, Object>> result = new ArrayList<>();
        if (Checker.isEmpty(issueCodes)) {
            return result;
        }
        issueCodes.forEach(issueCode -> {
            param.issueCode(issueCode);
            Map<String, Object> resultMap = codingHttp.buildBody("IssueCode", issueCode).post();
            Object response = resultMap.get("CommentList");
            if (Checker.isNotEmpty(response)) {
                result.addAll((List<Map<String, Object>>) response);
            }
        });
        return result;
    }

    @Override
    public CodingHttp codingHttp(CommentParam param) {
        ContextConfig contextConfig = this.veryContextConfigAndNodeConfig();
        param.action("DescribeIterationList");
        HttpEntity<String, String> header = HttpEntity.create()
                .builder("Authorization", contextConfig.getToken());
        HttpEntity<String, Object> body = HttpEntity.create()
                .builderIfNotAbsent("Action", "DescribeIssueCommentList")
                .builder("ProjectName", contextConfig.getProjectName())
                .builder("IssueCode", param.issueCode());
        return CodingHttp.create(
                header.getEntity(),
                body.getEntity(),
                String.format(OPEN_API_URL, contextConfig.getTeamName()));
    }

    @Override
    public void batchRead(Object offset, int batchCount, BiConsumer<List<TapEvent>, Object> consumer) {
        //@TODO 节点配置需要具备事项选择
        ContextConfig contextConfig = this.veryContextConfigAndNodeConfig();
        CommentParam param = CommentParam.create();
        CodingHttp codingHttp = this.codingHttp(param);
        List<Integer> issueCodes = contextConfig.issueCodes();
        List<TapEvent> events = new ArrayList<>();
        if (Checker.isEmpty(issueCodes)) {
            throw new CoreException("Please select at least one issues to get the comment list.");
        }
        for (Integer issueCode : issueCodes) {
            if (!this.sync()) {
                this.connectorOut();
                break;
            }
            param.issueCode(issueCode);
            Map<String, Object> resultMap = codingHttp.buildBody("IssueCode", issueCode).post();
            Object response = resultMap.get("CommentList");
            if (Checker.isNotEmpty(response)) {
                List<Map<String, Object>> result = (List<Map<String, Object>>) response;
                for (Map<String, Object> stringObjectMap : result) {
                    Object updatedAtObj = stringObjectMap.get("UpdatedAt");
                    Long updatedAt = Checker.isEmpty(updatedAtObj) ? System.currentTimeMillis() : (Long) updatedAtObj;
                    events.add(TapSimplify.insertRecordEvent(stringObjectMap, "Comments").referenceTime(updatedAt));
                    CodingOffset codingOffset = (CodingOffset) offset;
                    Map<Object, Object> offset1 = codingOffset.offset();
                    if (Checker.isEmpty(offset1)) {
                        codingOffset.offset(new HashMap<>());
                    }
                    codingOffset.offset().put(issueCode, updatedAt);
                    codingOffset.getTableUpdateTimeMap().put("Comments", updatedAt);
                    if (result.size() == batchCount) {
                        consumer.accept(events, offset);
                        events = new ArrayList<>();
                    }
                }
            }
        }
        if (events.size() > 0) consumer.accept(events, offset);
    }

    @Override
    public int batchCount() throws Throwable {
        return 0;
    }

    @Override
    public void streamRead(List<String> tableList, Object offsetState, int recordSize, StreamReadConsumer consumer) {
        return;
    }

    @Override
    public List<TapEvent> rawDataCallbackFilterFunction(Map<String, Object> issueEventData) {
        return null;
    }
    /**
     * {
     *     "action": "comment",
     *     "event": "ISSUE_COMMENT_CREATED",
     *     "eventName": "增加评论",
     *     "sender": {
     *         "id": 8613060,
     *         "login": "cxRfjXWiUi",
     *         "avatar_url": "https://coding-net-production-static-ci.codehub.cn/3b111f6b-f929-4f16-838c-f29ff2c97eb6.jpg?imageView2/1/w/0/h/0",
     *         "url": "https://testhookgavin.coding.net/api/user/key/cxRfjXWiUi",
     *         "html_url": "https://testhookgavin.coding.net/u/cxRfjXWiUi",
     *         "name": "TestHookGavin",
     *         "name_pinyin": "TestHookGavin"
     *     },
     *     "project": {
     *         "id": 11021181,
     *         "icon": "/static/project_icon/scenery-version-2-12.svg",
     *         "url": "https://testhookgavin.coding.net/p/testissue",
     *         "description": "测试Hook",
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
     *     "issueComment": {
     *         "html_url": "https://testhookgavin.coding.net/p/testissue/requirements/issues/71/detail#comment-4556442",
     *         "issue": {
     *             "html_url": "https://testhookgavin.coding.net/p/testissue/requirements/issues/71/detail",
     *             "type": "REQUIREMENT",
     *             "project_id": 11021181,
     *             "code": 71,
     *             "parent_code": 0,
     *             "title": "萨达三分大赛",
     *             "creator": {
     *                 "id": 8613060,
     *                 "login": "cxRfjXWiUi",
     *                 "avatar_url": "https://coding-net-production-static-ci.codehub.cn/3b111f6b-f929-4f16-838c-f29ff2c97eb6.jpg?imageView2/1/w/0/h/0",
     *                 "url": "https://testhookgavin.coding.net/api/user/key/cxRfjXWiUi",
     *                 "html_url": "https://testhookgavin.coding.net/u/cxRfjXWiUi",
     *                 "name": "TestHookGavin",
     *                 "name_pinyin": "TestHookGavin"
     *             },
     *             "status": "未开始",
     *             "assignee": {
     *                 "id": 8613060,
     *                 "login": "cxRfjXWiUi",
     *                 "avatar_url": "https://coding-net-production-static-ci.codehub.cn/3b111f6b-f929-4f16-838c-f29ff2c97eb6.jpg?imageView2/1/w/0/h/0",
     *                 "url": "https://testhookgavin.coding.net/api/user/key/cxRfjXWiUi",
     *                 "html_url": "https://testhookgavin.coding.net/u/cxRfjXWiUi",
     *                 "name": "TestHookGavin",
     *                 "name_pinyin": "TestHookGavin"
     *             },
     *             "priority": 3,
     *             "start_date": 1661961600000,
     *             "due_date": 1664380799000,
     *             "iteration": {
     *                 "title": "#1ISSUE",
     *                 "goal": "测试Hook",
     *                 "html_url": "https://testhookgavin.coding.net/p/testissue/iterations/1",
     *                 "project_id": 11021181,
     *                 "code": 1,
     *                 "creator": {
     *                     "id": 8613060,
     *                     "login": "cxRfjXWiUi",
     *                     "avatar_url": "https://coding-net-production-static-ci.codehub.cn/3b111f6b-f929-4f16-838c-f29ff2c97eb6.jpg?imageView2/1/w/0/h/0",
     *                     "url": "https://testhookgavin.coding.net/api/user/key/cxRfjXWiUi",
     *                     "html_url": "https://testhookgavin.coding.net/u/cxRfjXWiUi",
     *                     "name": "TestHookGavin",
     *                     "name_pinyin": "TestHookGavin"
     *                 },
     *                 "assignee": {
     *                     "id": 8613060,
     *                     "login": "cxRfjXWiUi",
     *                     "avatar_url": "https://coding-net-production-static-ci.codehub.cn/3b111f6b-f929-4f16-838c-f29ff2c97eb6.jpg?imageView2/1/w/0/h/0",
     *                     "url": "https://testhookgavin.coding.net/api/user/key/cxRfjXWiUi",
     *                     "html_url": "https://testhookgavin.coding.net/u/cxRfjXWiUi",
     *                     "name": "TestHookGavin",
     *                     "name_pinyin": "TestHookGavin"
     *                 },
     *                 "watchers": [
     *                     {
     *                         "id": 8613060,
     *                         "login": "cxRfjXWiUi",
     *                         "avatar_url": "https://coding-net-production-static-ci.codehub.cn/3b111f6b-f929-4f16-838c-f29ff2c97eb6.jpg?imageView2/1/w/0/h/0",
     *                         "url": "",
     *                         "html_url": "",
     *                         "name": "TestHookGavin",
     *                         "name_pinyin": "TestHookGavin"
     *                     }
     *                 ],
     *                 "status": "PROCESSING",
     *                 "plan_issue_number": 48,
     *                 "start_at": 1661702400000,
     *                 "end_at": 1662825599000,
     *                 "created_at": 1661745521000,
     *                 "updated_at": 1661753667000
     *             },
     *             "description": "\n## HTML5 在线编辑器 \n\n>\n\nUAT2, 升级到1.24.33-33版本； ss\n升级后部分任务失败调整。 \nUAT2环境， \nShare_Log_S_local_DAAS_4 \n日志 \n[ERROR]  2022-09-02 12:46:45 [Connector runner-Share_Log_S_local_DAAS_4_1-[62fdfe41b67a7a1067eccfae]] io.tapdata.LogCollectSource - Initial job config failed io.tapdata.exception.SourceException: Init log collector setting error, message: java.lang.String cannot be cast to java.util.Map, need stop: true.; Will stop job \n堆栈信息， \n[ERROR] 2022-09-02 12:46:45.896 [Connector runner-Share_Log_S_local_DAAS_4_1-[62fdfe41b67a7a1067eccfae]] LogCollectSource - Initial job config failed io.tapdata.exception.SourceException: Init log collector setting error, message: java.lang.String cannot be cast to java.util.Map, need stop: true.; Will stop job \nio.tapdata.exception.SourceException: io.tapdata.exception.SourceException: Init log collector setting error, message: java.lang.String cannot be cast to java.util.Map \nat io.tapdata.common.Connector.lambda$runSource$10(Connector.java:308) ~[classes!/:0.5.2-SNAPSHOT] \nat java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149) [?:1.8.0_231] \nat java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624) [?:1.8.0_231] \nat java.lang.Thread.run(Thread.java:748) [?:1.8.0_231] \nCaused by: io.tapdata.exception.SourceException: Init log collector setting error, message: java.lang.String cannot be cast to java.util.Map \nat io.tapdata.LogCollectSource.sourceInit(LogCollectSource.java:68) ~[log-collect-lib-0.5.2-SNAPSHOT.jar!/:0.5.2-SNAPSHOT] \nat io.tapdata.common.Connector.lambda$runSource$10(Connector.java:304) ~[classes!/:0.5.2-SNAPSHOT] \n... 3 more \nCaused by: java.lang.ClassCastException: java.lang.String cannot be cast to java.util.Map \nat io.tapdata.LogCollectSource.lambda$initLogCollectorSettings$0(LogCollectSource.java:108) ~[log-collect-lib-0.5.2-SNAPSHOT.jar!/:0.5.2-SNAPSHOT] \nat java.util.Iterator.forEachRemaining(Iterator.java:116) ~[?:1.8.0_231] \nat java.util.Spliterators$IteratorSpliterator.forEachRemaining(Spliterators.java:1801) ~[?:1.8.0_231] \nat java.util.stream.ReferencePipeline$Head.forEach(ReferencePipeline.java:580) ~[?:1.8.0_231] \nat io.tapdata.LogCollectSource.initLogCollectorSettings(LogCollectSource.java:108) ~[log-collect-lib-0.5.2-SNAPSHOT.jar!/:0.5.2-SNAPSHOT] \nat io.tapdata.LogCollectSource.sourceInit(LogCollectSource.java:66) ~[log-collect-lib-0.5.2-SNAPSHOT.jar!/:0.5.2-SNAPSHOT] \nat io.tapdata.common.Connector.lambda$runSource$10(Connector.java:304) ~[classes!/:0.5.2-SNAPSHOT] \n... 3 more \n    ![](/api/km/v1/spaces/2/page/0c332bdb338d44448f7d5ae4acfec8fe/blocks/d392821f05f247888ce35a0fdbb4da50/imagePreview)\n",
     *             "created_at": 1663154015000,
     *             "updated_at": 1663753857000,
     *             "issue_status": {
     *                 "id": 43900887,
     *                 "name": "未开始",
     *                 "type": "TODO"
     *             },
     *             "watchers": [
     *                 {
     *                     "id": 8613060,
     *                     "login": "cxRfjXWiUi",
     *                     "avatar_url": "https://coding-net-production-static-ci.codehub.cn/3b111f6b-f929-4f16-838c-f29ff2c97eb6.jpg?imageView2/1/w/0/h/0",
     *                     "url": "",
     *                     "html_url": "",
     *                     "name": "TestHookGavin",
     *                     "name_pinyin": "TestHookGavin"
     *                 }
     *             ],
     *             "labels": [
     *                 {
     *                     "id": 12951196,
     *                     "name": "StreamRead"
     *                 }
     *             ]
     *         },
     *         "creator": {
     *             "id": 8613060,
     *             "login": "cxRfjXWiUi",
     *             "avatar_url": "https://coding-net-production-static-ci.codehub.cn/3b111f6b-f929-4f16-838c-f29ff2c97eb6.jpg?imageView2/1/w/0/h/0",
     *             "url": "https://testhookgavin.coding.net/api/user/key/cxRfjXWiUi",
     *             "html_url": "https://testhookgavin.coding.net/u/cxRfjXWiUi",
     *             "name": "TestHookGavin",
     *             "name_pinyin": "TestHookGavin"
     *         },
     *         "content": "<p>AAA</p>",
     *         "created_at": 1665304451630,
     *         "updated_at": 1665304451630
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
     *         "updated_at": 1665213930000
     *     },
     *     "hook_id": "17fb4cb3-c679-4fb1-829c-1f02ffe2a815"
     * }
     *
     * */
}
