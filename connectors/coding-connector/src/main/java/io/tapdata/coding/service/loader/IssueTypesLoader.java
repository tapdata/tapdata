package io.tapdata.coding.service.loader;

import io.tapdata.coding.entity.ContextConfig;
import io.tapdata.coding.entity.param.IssueTypeParam;
import io.tapdata.coding.utils.collection.MapUtil;
import io.tapdata.coding.utils.http.CodingHttp;
import io.tapdata.coding.utils.http.HttpEntity;
import io.tapdata.coding.utils.tool.Checker;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectionContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

import static io.tapdata.coding.enums.TapEventTypes.CREATED_EVENT;
import static io.tapdata.coding.enums.TapEventTypes.UPDATE_EVENT;

public class IssueTypesLoader extends CodingStarter implements CodingLoader<IssueTypeParam> {
    OverlayQueryEventDifferentiator overlayQueryEventDifferentiator = new OverlayQueryEventDifferentiator();
    public static final String TABLE_NAME = "IssueTypes";

    public IssueTypesLoader(TapConnectionContext tapConnectionContext, AtomicReference<String> accessToken) {
        super(tapConnectionContext, accessToken);
    }

    public static IssueTypesLoader create(TapConnectionContext context, AtomicReference<String> accessToken) {
        return new IssueTypesLoader(context, accessToken);
    }

    @Override
    public Long streamReadTime() {
        return 5 * 60 * 1000L;
    }

    @Override
    public List<Map<String, Object>> list(IssueTypeParam param) {
        Map<String, Object> resultMap = this.codingHttp(param).post();
        Object response = resultMap.get("Response");
        if (Checker.isEmpty(response)) {
            throw new CoreException("can not get issue type list, the 'Response' is empty. " + Optional.ofNullable(resultMap.get(CodingHttp.ERROR_KEY)).orElse(""));
        }
        Object IssueTypesObj = ((Map<String, Object>) response).get("IssueTypes");
        if (Checker.isEmpty(IssueTypesObj)) {
            throw new CoreException("can not get issue type list, the 'IssueTypes' is empty.");
        }
        return (List<Map<String, Object>>) IssueTypesObj;
    }

    @Override
    public List<Map<String, Object>> all(IssueTypeParam param) {
        return null;
    }

    @Override
    public CodingHttp codingHttp(IssueTypeParam param) {
        ContextConfig contextConfig = this.veryContextConfigAndNodeConfig();
        HttpEntity<String, String> header = HttpEntity.create()
                .builder("Authorization", this.accessToken().get());
        HttpEntity<String, Object> body = HttpEntity.create()
                .builderIfNotAbsent("Action", "DescribeProjectIssueTypeList")
                .builder("ProjectName", contextConfig.getProjectName());
        return CodingHttp.create(
                header.getEntity(),
                body.getEntity(),
                String.format(OPEN_API_URL, contextConfig.getTeamName()));
    }

    @Override
    public void batchRead(Object offset, int batchCount, BiConsumer<List<TapEvent>, Object> consumer) {
        this.read(offset, batchCount, consumer);
    }

    private void read(Object offset, int batchCount, BiConsumer<List<TapEvent>, Object> consumer) {
        List<Map<String, Object>> list = list(null);
        if (null == list || list.isEmpty()) {
            throw new CoreException("can not get issue type list, the list is empty.");
        }
        List<TapEvent> events = new ArrayList<>();
        for (Map<String, Object> issueType : list) {
            if (!this.sync()) {
                this.connectorOut();
                break;
            }
            Integer issueTypeId = (Integer) issueType.get("Id");
            Integer issueTypeHash = MapUtil.create().hashCode(issueType);
            switch (overlayQueryEventDifferentiator.createOrUpdateEvent(issueTypeId, issueTypeHash)) {
                case CREATED_EVENT:
                    events.add(TapSimplify.insertRecordEvent(issueType, TABLE_NAME).referenceTime(System.currentTimeMillis()));
                    break;
                case UPDATE_EVENT:
                    events.add(TapSimplify.updateDMLEvent(null, issueType, TABLE_NAME).referenceTime(System.currentTimeMillis()));
                    break;
            }
            //events.add(TapSimplify.insertRecordEvent(issueType, TABLE_NAME).referenceTime(System.currentTimeMillis()));
            if (events.size() == batchCount) {
                consumer.accept(events, offset);
                events = new ArrayList<>();
            }
        }

        List<TapEvent> delEvents = overlayQueryEventDifferentiator.delEvent(TABLE_NAME, "Id");
        if (!delEvents.isEmpty()) {
            events.addAll(delEvents);
        }
        if (events.size() > 0) consumer.accept(events, offset);
    }

    @Override
    public void streamRead(List<String> tableList, Object offsetState, int recordSize, StreamReadConsumer consumer) {
        this.read(offsetState, recordSize, consumer);
    }

    @Override
    public int batchCount() throws Throwable {
        List<Map<String, Object>> list = list(null);
        if (null == list) {
            return 0;
        }
        return list.size();
    }

    @Override
    public List<TapEvent> rawDataCallbackFilterFunction(Map<String, Object> issueEventData) {
        return null;
    }

}
