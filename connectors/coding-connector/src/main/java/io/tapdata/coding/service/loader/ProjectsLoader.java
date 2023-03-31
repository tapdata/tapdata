package io.tapdata.coding.service.loader;

import io.tapdata.coding.entity.CodingOffset;
import io.tapdata.coding.entity.ContextConfig;
import io.tapdata.coding.entity.param.Param;
import io.tapdata.coding.entity.param.ProjectParam;
import io.tapdata.coding.utils.http.CodingHttp;
import io.tapdata.coding.utils.http.HttpEntity;
import io.tapdata.coding.utils.tool.Checker;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.utils.FormatUtils;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectionContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;


public class ProjectsLoader extends CodingStarter implements CodingLoader<ProjectParam> {
    public static final String TABLE_NAME = "Projects";

    public ProjectsLoader(TapConnectionContext tapConnectionContext, AtomicReference<String> accessToken) {
        super(tapConnectionContext, accessToken);
    }

    public static ProjectsLoader create(TapConnectionContext tapConnectionContext, AtomicReference<String> accessToken) {
        return new ProjectsLoader(tapConnectionContext, accessToken);
    }

    public List<Map<String, Object>> myProjectList() {
        Map<String, Object> user = this.myselfInfo();
        Object userIdObj = user.get("Id");
        if (Checker.isEmpty(userIdObj))
            throw new RuntimeException(FormatUtils.format("User Id can not be parsed"));
        return this.userProjectList((Integer) userIdObj);
    }

    public List<Map<String, Object>> userProjectList(Integer userId) {
        ContextConfig contextConfig = this.veryContextConfigAndNodeConfig();
        HttpEntity<String, String> header = HttpEntity.create()
                .builder("Authorization", this.accessToken().get());
        HttpEntity<String, Object> body = HttpEntity.create()
                .builderIfNotAbsent("Action", "DescribeUserProjects")
                .builder("userId", userId);
        Map<String, Object> post = CodingHttp.create(
                header.getEntity(),
                body.getEntity(),
                String.format(OPEN_API_URL, contextConfig.getTeamName())).post();
        if (null == post || post.isEmpty()) {
            throw new RuntimeException(FormatUtils.format("Coding request return empty body, url {}", String.format(OPEN_API_URL, contextConfig.getTeamName())));
        }
        Object responseObj = post.get("Response");
        if (Checker.isEmptyCollection(responseObj))
            throw new RuntimeException(String.format("Response can not be parsed. %s.", Optional.ofNullable(post.get(CodingHttp.ERROR_KEY)).orElse("")));

        Object projectListObj = ((Map<String, Object>) responseObj).get("ProjectList");
        if (Checker.isNotEmptyCollection(projectListObj)) {
            return (List<Map<String, Object>>) projectListObj;
        } else {
            throw new RuntimeException(FormatUtils.format("Project List can not be parsed"));
        }
        //return Checker.isNotEmptyCollection(projectListObj)?(List<Map<String, Object>>)projectListObj:null;
    }

    public Map<String, Object> myselfInfo() {
        ContextConfig contextConfig = this.veryContextConfigAndNodeConfig();
        HttpEntity<String, String> header = HttpEntity.create()
                .builder("Authorization", this.accessToken().get());
        HttpEntity<String, Object> body = HttpEntity.create()
                .builderIfNotAbsent("Action", "DescribeCodingCurrentUser");
        Map<String, Object> post = CodingHttp.create(
                header.getEntity(),
                body.getEntity(),
                String.format(OPEN_API_URL, contextConfig.getTeamName())).post();
        if (Checker.isEmptyCollection(post)) {
            throw new RuntimeException(String.format("Coding request return empty body, url %s", String.format(OPEN_API_URL, contextConfig.getTeamName())));
        }
        Object responseObj = post.get("Response");
        if (Checker.isEmptyCollection(responseObj))
            throw new RuntimeException(String.format("Response can not be parsed. %s.", Optional.ofNullable(post.get(CodingHttp.ERROR_KEY)).orElse("")));

        Object userObj = ((Map<String, Object>) responseObj).get("User");
        if (Checker.isNotEmptyCollection(userObj)) {
            return (Map<String, Object>) userObj;
        } else {
            throw new RuntimeException(FormatUtils.format("User can not be parsed"));
        }
    }

    @Override
    public Long streamReadTime() {
        return 5 * 60 * 1000l;
    }

    @Override
    public List<Map<String, Object>> list(ProjectParam param) {
        Map<String, Object> resultMap = this.codingHttp(param).post();
        Object response = resultMap.get("Response");
        if (null == response) {
            throw new RuntimeException(String.format("Response can not be parsed. %s.", Optional.ofNullable(resultMap.get(CodingHttp.ERROR_KEY)).orElse("")));
        }
        Map<String, Object> responseMap = (Map<String, Object>) response;
        Object dataObj = responseMap.get("Data");
        if (null == dataObj) {
            throw new RuntimeException(FormatUtils.format("Data can not be parsed."));
        }
        Map<String, Object> data = (Map<String, Object>) dataObj;
        Object listObj = data.get("ProjectList");
        if (listObj == null)
            throw new RuntimeException(FormatUtils.format("ProjectList can not be parsed"));
        return (List<Map<String, Object>>) listObj;
    }

    @Override
    public Map<String, Object> get(ProjectParam param) {
        ContextConfig contextConfig = this.veryContextConfigAndNodeConfig();
        HttpEntity<String, String> header = HttpEntity.create()
                .builder("Authorization", this.accessToken().get());
        HttpEntity<String, Object> body = HttpEntity.create()
                .builderIfNotAbsent("Action", "DescribeProjectByName")
                .builder("ProjectName", contextConfig.getProjectName());
        Map<String, Object> resultMap = CodingHttp.create(
                header.getEntity(),
                body.getEntity(),
                String.format(OPEN_API_URL, contextConfig.getTeamName())).post();
        Object response = resultMap.get("Response");
        if (null == response) {
            throw new RuntimeException(String.format("Response can not be parsed. %s.", Optional.ofNullable(resultMap.get(CodingHttp.ERROR_KEY)).orElse("")));
        }
        Map<String, Object> responseMap = (Map<String, Object>) response;
        Object dataObj = responseMap.get("Project");
        if (dataObj == null)
            throw new RuntimeException(FormatUtils.format("Project can not be parsed"));
        return (Map<String, Object>) dataObj;
    }

    @Override
    public List<Map<String, Object>> all(ProjectParam param) {
        return null;
    }

    @Override
    public CodingHttp codingHttp(ProjectParam param) {
        final int maxLimit = 500;//@TODO 最大分页数
        if (param.limit() > maxLimit) param.limit(maxLimit);
        ContextConfig contextConfig = this.veryContextConfigAndNodeConfig();
        HttpEntity<String, String> header = HttpEntity.create()
                .builder("Authorization", this.accessToken().get());
        HttpEntity<String, Object> body = HttpEntity.create()
                .builderIfNotAbsent("Action", "DescribeCodingProjects")
                .builder("ProjectName", contextConfig.getProjectName())
                .builderIfNotAbsent("PageNumber", param.offset())
                .builderIfNotAbsent("PageSize", param.limit());
        return CodingHttp.create(
                header.getEntity(),
                body.getEntity(),
                String.format(OPEN_API_URL, contextConfig.getTeamName()));
    }

    @Override
    public void batchRead(Object offset, int batchCount, BiConsumer<List<TapEvent>, Object> consumer) {
        this.read(offset, batchCount, consumer);
    }

    @Override
    public int batchCount() throws Throwable {
        Param param = ProjectParam.create().limit(1).offset(1);
        Map<String, Object> resultMap = this.codingHttp((ProjectParam) param).post();
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
        return null != totalCountObj ? (Integer) totalCountObj : 0;
    }

    private void read(Object offset, int batchCount, BiConsumer<List<TapEvent>, Object> consumer) {
        ContextConfig contextConfig = this.veryContextConfigAndNodeConfig();
        int startPage = 0;
        Param param = ProjectParam.create()
                .limit(batchCount)
                .offset(startPage);
        CodingHttp codingHttp = this.codingHttp((ProjectParam) param);
        List<TapEvent> events = new ArrayList<>();
        //List<Integer> issueCodes = contextConfig.issueCodes();
        //if (Checker.isEmpty(issueCodes)){
        //    throw new RuntimeException(FormatUtils.format("issueCodes is empty"));
        //}
        while (this.sync()) {
            Map<String, Object> resultMap = codingHttp.buildBody("Offset", startPage).post();
            Object response = resultMap.get("Response");
            if (null == response) {
                throw new RuntimeException(String.format("Response can not be parsed %s.", Optional.ofNullable(resultMap.get(CodingHttp.ERROR_KEY)).orElse("")));
            }
            Map<String, Object> responseMap = (Map<String, Object>) response;
            Object dataObj = responseMap.get("Data");
            if (null == dataObj) {
                throw new RuntimeException(FormatUtils.format("Data can not be parsed"));
            }
            Map<String, Object> data = (Map<String, Object>) dataObj;
            Object listObj = data.get("ProjectList");
            if (Checker.isNotEmpty(listObj)) {
                List<Map<String, Object>> result = (List<Map<String, Object>>) listObj;
                for (Map<String, Object> stringObjectMap : result) {
                    Object updatedAtObj = stringObjectMap.get("UpdatedAt");
                    Long updatedAt = Checker.isEmpty(updatedAtObj) ? System.currentTimeMillis() : (Long) updatedAtObj;
                    events.add(TapSimplify.insertRecordEvent(stringObjectMap, TABLE_NAME).referenceTime(updatedAt));
                    ((CodingOffset) offset).getTableUpdateTimeMap().put(TABLE_NAME, updatedAt);
                    if (result.size() == batchCount) {
                        consumer.accept(events, offset);
                        events = new ArrayList<>();
                    }
                }
                if (result.size() < param.limit()) {
                    break;
                }
                startPage += param.limit();
            } else {
                throw new RuntimeException(FormatUtils.format("ProjectList can not be parsed"));
            }
        }
        if (!this.sync()) {
            this.connectorOut();
        }
        if (events.size() > 0) consumer.accept(events, offset);
    }

    @Override
    public void streamRead(List<String> tableList, Object offsetState, int recordSize, StreamReadConsumer consumer) {
        this.read(offsetState, recordSize, consumer);
    }

    @Override
    public List<TapEvent> rawDataCallbackFilterFunction(Map<String, Object> issueEventData) {
        return null;//项目不支持WebHook
    }
}
