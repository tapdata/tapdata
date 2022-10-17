package io.tapdata.zoho.service.zoho.schemaLoader;

import cn.hutool.core.date.DateUtil;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.zoho.entity.ContextConfig;
import io.tapdata.zoho.entity.HttpEntity;
import io.tapdata.zoho.entity.ZoHoOffset;
import io.tapdata.zoho.entity.webHook.EventBaseEntity;
import io.tapdata.zoho.service.connectionMode.ConnectionMode;
import io.tapdata.zoho.service.zoho.loader.TicketLoader;
import io.tapdata.zoho.service.zoho.schema.Schemas;
import io.tapdata.zoho.utils.Checker;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

public class TickersSchema implements SchemaLoader {
    private static final String TAG = TickersSchema.class.getSimpleName();
    TicketLoader ticketLoader;

    private final long streamExecutionGap = 5000;//util: ms
    private int batchReadMaxPageSize = 100;//ZoHo ticket page size 1~100,
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
        Object listObj = issueEventData.get("array");
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
    public void batchRead(Object offset, int batchCount, BiConsumer<List<TapEvent>, Object> consumer) {
        Long readEnd = System.currentTimeMillis();
        ZoHoOffset zoHoOffset =  new ZoHoOffset();
        //current read end as next read begin
        zoHoOffset.setTableUpdateTimeMap(new HashMap<String,Long>(){{ put(Schemas.Tickets.getTableName(),readEnd);}});
        this.read(batchCount,zoHoOffset,consumer);
    }

    //分页接口没有返回总数，也没有单独提供count Api
    @Override
    public long batchCount() throws Throwable {
        return 0;
    }

    /**
     * 分页读取事项列表，并依次查询事项详情
     * @param readSize
     * @param consumer
     *
     *
     * ZoHo提供的分页查询条件
     * -from                     integer                 Index number, starting from which the tickets must be fetched
     * -limit                    integer[1-100]          Number of tickets to fetch  ,pageSize
     * -departmentId             long                    ID of the department from which the tickets must be fetched (Please note that this key will be deprecated soon and replaced by the departmentIds key.)
     * -departmentIds            List<Long>              Departments from which the tickets need to be queried'
     * -viewId                   long                    ID of the view to apply while fetching the resources
     * -assignee                 string(MaxLen:100)      assignee - Key that filters tickets by assignee. Values allowed are Unassigned or a valid assigneeId. Multiple assigneeIds can be passed as comma-separated values.
     * -channel                  string(MaxLen:100)      Filter by channel through which the tickets originated. You can include multiple values by separating them with a comma
     * -status                   string(MaxLen:100)      Filter by resolution status of the ticket. You can include multiple values by separating them with a comma
     * -sortBy                   string(MaxLen:100)      Sort by a specific attribute: responseDueDate or customerResponseTime or createdTime or ticketNumber. The default sorting order is ascending. A - prefix denotes descending order of sorting.
     * -receivedInDays           integer                 Fetches recent tickets, based on customer response time. Values allowed are 15, 30 , 90.
     * -include                  string(MaxLen:100)      Additional information related to the tickets. Values allowed are: contacts, products, departments, team, isRead and assignee. You can pass multiple values by separating them with commas in the API request.
     * -fields                   string(MaxLen:100)      Key that returns the values of mentioned fields (both pre-defined and custom) in your portal. All field types except multi-text are supported. Standard, non-editable fields are supported too. These fields include: statusType, webUrl, layoutId. Maximum of 30 fields is supported as comma separated values.
     * -priority                 string(MaxLen:100)      Key that filters tickets by priority. Multiple priority levels can be passed as comma-separated values.
     */
    public void read(int readSize, Object offsetState, BiConsumer<List<TapEvent>, Object> consumer ){
        final List<TapEvent>[] events = new List[]{new ArrayList<>()};
        int pageSize = Math.min(readSize, this.batchReadMaxPageSize);
        HttpEntity<String, Object> tickPageParam = ticketLoader.getTickPageParam()
                .build("limit", pageSize);//分页数
        int fromPageIndex = 1;//从第几个工单开始分页
        TapConnectionContext context = this.ticketLoader.getContext();
        String modeName = context.getConnectionConfig().getString("connectionMode");
        ConnectionMode connectionMode = ConnectionMode.getInstanceByName(context, modeName);
        if (null == connectionMode){
            throw new CoreException("Connection Mode is not empty or not null.");
        }
        String tableName =  Schemas.Tickets.getTableName();
        while (true){
            tickPageParam.build("from", fromPageIndex);
            List<Map<String, Object>> list = ticketLoader.list(tickPageParam);
            if (Checker.isNotEmpty(list) && !list.isEmpty()){
                fromPageIndex += pageSize;
                list.stream().forEach(ticket->{
                    Map<String, Object> oneTicket = connectionMode.attributeAssignment(ticket,tableName,ticketLoader);
                    if (Checker.isNotEmpty(oneTicket) && !oneTicket.isEmpty()){
                        Object modifiedTimeObj = oneTicket.get("modifiedTime");
                        long referenceTime = System.currentTimeMillis();
                        if (Checker.isNotEmpty(modifiedTimeObj) && modifiedTimeObj instanceof String) {
                            String referenceTimeStr = (String) modifiedTimeObj;
                            referenceTime = DateUtil.parse(
                                    referenceTimeStr.replaceAll("Z", "").replaceAll("T", " "),
                                    "yyyy-MM-dd HH:mm:ss.SSS").getTime();
                            ((ZoHoOffset) offsetState).getTableUpdateTimeMap().put(tableName, referenceTime);
                        }
                        events[0].add(TapSimplify.insertRecordEvent(oneTicket,tableName).referenceTime(referenceTime));
                        if (events[0].size() == readSize){
                            consumer.accept(events[0], offsetState);
                            events[0] = new ArrayList<>();
                        }
                    }
                });
                if (events[0].size()>0){
                    consumer.accept(events[0], offsetState);
                }
            }else {
                break;
            }
        }
    }

}
