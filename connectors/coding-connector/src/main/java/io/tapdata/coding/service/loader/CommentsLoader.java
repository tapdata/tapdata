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
                .builder("Authorization", this.accessToken().get());
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

}
