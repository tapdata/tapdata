package io.tapdata.zoho.service.zoho.schemaLoader;

import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.zoho.ZoHoConnector;
import io.tapdata.zoho.entity.ContextConfig;
import io.tapdata.zoho.entity.webHook.EventBaseEntity;
import io.tapdata.zoho.service.connectionMode.ConnectionMode;
import io.tapdata.zoho.service.zoho.loader.TicketLoader;
import io.tapdata.zoho.utils.Checker;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class TickersSchema implements SchemaLoader {
    private static final String TAG = TickersSchema.class.getSimpleName();
    TicketLoader ticketLoader;

    @Override
    public TickersSchema configSchema(TapConnectionContext context) {
        this.ticketLoader = TicketLoader.create(context);
        return this;
    }

    @Override
    public List<TapEvent> rawDataCallbackFilterFunction(Map<String, Object> issueEventData){
        if (Checker.isEmpty(issueEventData)){
            TapLogger.debug(TAG,"WebHook of ZoHo patch body is empty, Data callback has been over.");
            return null;
        }
        Object listObj = issueEventData.get("data");
        if (Checker.isEmpty(listObj) || !(listObj instanceof List)){
            TapLogger.debug(TAG,"WebHook of ZoHo patch body is empty, Data callback has been over.");
            return null;
        }
        List<Map<String,Object>> dataEventList = (List<Map<String, Object>>)listObj;
        final List<TapEvent>[] events = new List[]{new ArrayList<>()};
        //@TODO BiConsumer<List<TapEvent>, Object> consumer;
        //@TODO 获取筛选条件
        ContextConfig contextConfig = ticketLoader.veryContextConfigAndNodeConfig();
        TapConnectionContext context = ticketLoader.getContext();
        String modeName = contextConfig.connectionMode();
        ConnectionMode instance = ConnectionMode.getInstanceByName(context, modeName);
        if (null == instance){
            throw new CoreException("Connection Mode must be not empty or not null.");
        }
        dataEventList.forEach(eventMap->{
            EventBaseEntity instanceByEventType = EventBaseEntity.getInstanceByEventType(eventMap);
            if (Checker.isEmpty(instanceByEventType)){
                TapLogger.debug(TAG,"An event type with unknown origin was found and cannot be processed .");
                return;
            }
            events[0].add(instanceByEventType.outputTapEvent("Tickets",instance));
        });
        return events[0];
    }

    @Override
    public void streamRead(Object offsetState, int recordSize, StreamReadConsumer consumer) {

    }

    @Override
    public Object timestampToStreamOffset(Long time) {
        return null;
    }

    @Override
    public ConnectionOptions connectionTest(Consumer<TestItem> consumer) throws Throwable {
        return null;
    }

    @Override
    public void batchRead(Object offset, int batchCount, BiConsumer<List<TapEvent>, Object> consumer) {

    }

    @Override
    public long batchCount() throws Throwable {
        return 0;
    }

    @Override
    public int tableCount() throws Throwable {
        return 0;
    }

}
