package io.tapdata.zoho.service.zoho.schemaLoader;

import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.zoho.ZoHoConnector;
import io.tapdata.zoho.entity.ContextConfig;
import io.tapdata.zoho.entity.HttpEntity;
import io.tapdata.zoho.entity.ZoHoOffset;
import io.tapdata.zoho.service.zoho.webHook.EventBaseEntity;
import io.tapdata.zoho.service.connectionMode.ConnectionMode;
import io.tapdata.zoho.service.zoho.loader.TicketLoader;
import io.tapdata.zoho.service.zoho.schema.Schemas;
import io.tapdata.zoho.utils.Checker;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import static io.tapdata.entity.simplify.TapSimplify.map;

public class TicketsSchema extends Schema implements SchemaLoader {
    private static final String TAG = TicketsSchema.class.getSimpleName();
    TicketLoader ticketLoader;

    private final long streamExecutionGap = 5000;//util: ms
    private int batchReadMaxPageSize = 100;//ZoHo ticket page size 1~100,

    @Override
    public TicketsSchema configSchema(TapConnectionContext context) {
        this.ticketLoader = TicketLoader.create(context);
        return this;
    }


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
    public void batchRead(Object offset, int batchCount, BiConsumer<List<TapEvent>, Object> consumer) {
        Long readEnd = System.currentTimeMillis();
        ZoHoOffset zoHoOffset =  new ZoHoOffset();
        //current read end as next read begin
        zoHoOffset.setTableUpdateTimeMap(new HashMap<String,Long>(){{ put(Schemas.Tickets.getTableName(),readEnd);}});
        this.read(batchCount,zoHoOffset,consumer,false);
    }

    @Override
    public long batchCount() throws Throwable {
        return ticketLoader.count();
    }

    private String sortKey(boolean isStreamRead){
        return isStreamRead?"UPDATED_AT":"CREATED_AT";
    }
//
//    public void defineHttpAttributes(Long readStartTime, Long readEndTime, int readSize, HttpEntity<String, String> header, HttpEntity<String, Object> pageBody,boolean isStreamRead) {
//        List<Map<String, Object>> coditions = io.tapdata.entity.simplify.TapSimplify.list(map(
//                entry("Key", this.sortKey(isStreamRead)),
//                entry("Value", this.longToDateStr(readStartTime) + "_" + this.longToDateStr(readEndTime)))
//        );
//        header.builder("Authorization", contextConfig.getToken());
//        String projectName = contextConfig.getProjectName();
//        String iterationCodes = contextConfig.getIterationCodes();
//        if (null != iterationCodes && !"".equals(iterationCodes) && !",".equals(iterationCodes) && !"-1".equals(iterationCodes)) {
//            //-1时表示全选
//            //String[] iterationCodeArr = iterationCodes.split(",");
//            //@TODO 输入的迭代编号需要验证，否则，查询事项列表时作为查询条件的迭代不存在时，查询会报错
//            //选择的迭代编号不需要验证
//            coditions.add(map(entry("Key", "ITERATION"), entry("Value", iterationCodes)));
//        }
//        pageBody.builder("Action", "DescribeIssueListWithPage")
//                .builder("ProjectName", projectName)
//                .builder("SortKey", this.sortKey(isStreamRead))
//                .builder("PageSize", readSize)
//                .builder("SortValue", "ASC")
//                .builder("IssueType",
//                        Checker.isNotEmpty(contextConfig) && Checker.isNotEmpty(contextConfig.getIssueType()) ?
//                                IssueType.verifyType(contextConfig.getIssueType().getName())
//                                : "ALL")
//                .builder("Conditions", coditions);
//
//    }
//
//    public void defineHttpAttributesV2(int readSize, HttpEntity<String, String> header, HttpEntity<String, Object> pageBody,boolean isStreamRead) {
//        List<Map<String, Object>> coditions = io.tapdata.entity.simplify.TapSimplify.list();
//        header.builder("Authorization", contextConfig.getToken());
//        String projectName = contextConfig.getProjectName();
//        String iterationCodes = contextConfig.getIterationCodes();
//        if (null != iterationCodes && !"".equals(iterationCodes) && !",".equals(iterationCodes) && !"-1".equals(iterationCodes)) {
//            //-1时表示全选
//            //String[] iterationCodeArr = iterationCodes.split(",");
//            //@TODO 输入的迭代编号需要验证，否则，查询事项列表时作为查询条件的迭代不存在时，查询会报错
//            //选择的迭代编号不需要验证
//            coditions.add(map(entry("Key", "ITERATION"), entry("Value", iterationCodes)));
//            pageBody.builder("Conditions", coditions);
//        }
//        pageBody.builder("Action", "DescribeIssueListWithPage")
//                .builder("ProjectName", projectName)
//                .builder("SortKey", this.sortKey(isStreamRead))
//                .builder("PageSize", readSize)
//                .builder("PageNumber", 1)
//                .builder("SortValue", "ASC")
//                .builder("IssueType",
//                        Checker.isNotEmpty(contextConfig) && Checker.isNotEmpty(contextConfig.getIssueType()) ?
//                                IssueType.verifyType(contextConfig.getIssueType().getName())
//                                : "ALL");
//    }
//
//    public void readV2(
//            Long readStartTime,
//            Long readEndTime,
//            int readSize,
//            Object offsetState,
//            BiConsumer<List<TapEvent>, Object> consumer) {
//        final int MAX_THREAD = 20;
//        Queue<Integer> queuePage = new ConcurrentLinkedQueue<>();
//        Queue<Map<String, Object>> queueItem = new ConcurrentLinkedQueue<>();
//        AtomicInteger itemThreadCount = new AtomicInteger(0);
//
//        String teamName = contextConfig.getTeamName();
//        List<TapEvent> events = new ArrayList<>();
//        HttpEntity<String, String> header = HttpEntity.create();
//        HttpEntity<String, Object> pageBody = HttpEntity.create();
//        this.defineHttpAttributes(readStartTime, readEndTime, readSize, header, pageBody,false);
//        ZoHoOffset offset = (ZoHoOffset) (Checker.isEmpty(offsetState) ? new ZoHoOffset() : offsetState);
//        AtomicInteger total = new AtomicInteger(-1);
//        //分页线程
//        Thread pageThread = new Thread(() -> {
//            int currentQueryCount = batchReadMaxPageSize, queryIndex = 1;
//            while (currentQueryCount >= batchReadMaxPageSize && this.sync()) {
//                /**
//                 * start page ,and add page to queuePage;
//                 * */
//                pageBody.build("PageNumber", queryIndex++);
//                Map<String, Object> dataMap = this.getIssuePage(header.getEntity(), pageBody.getEntity(), String.format(CodingStarter.OPEN_API_URL, teamName));
//                if (null == dataMap || null == dataMap.get("List")) {
//                    TapLogger.error(TAG, "Paging result request failed, the Issue list is empty: page index = {}", queryIndex);
//                    throw new RuntimeException("Paging result request failed, the Issue list is empty: " + CodingStarter.OPEN_API_URL + "?Action=DescribeIssueListWithPage");
//                }
//                List<Map<String, Object>> resultList = (List<Map<String, Object>>) dataMap.get("List");
//                currentQueryCount = resultList.size();
//                batchReadMaxPageSize = null != dataMap.get("PageSize") ? (int) (dataMap.get("PageSize")) : batchReadMaxPageSize;
//                if (total.get() < 0) {
//                    total.set((int) (dataMap.get("TotalCount")));
//                }
//                queuePage.addAll(resultList.stream().map(obj -> (Integer) (obj.get("Code"))).collect(Collectors.toList()));
//                //pageCount.getAndAdd(1);
//            }
//        }, "PAGE_THREAD");
//        pageThread.start();
//
//        //详情查询线程
//        while ( this.sync() ) {
//            if (!pageThread.isAlive() && queuePage.isEmpty()) break;
//            if (!queuePage.isEmpty()) {
//                int threadCount = total.get() / 500 ;
//                threadCount = Math.min(threadCount, MAX_THREAD);
//                threadCount = Math.max(threadCount,1);
//                final ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(threadCount + 1, run -> {
//                    Thread thread = new Thread(run);
//                    return thread;
//                });
//                for (int i = 0; i < threadCount; i++) {
//                    executor.schedule(() -> {
//                        itemThreadCount.getAndAdd(1);
//                        /**
//                         * start page ,and add page to queuePage;
//                         * */
//                        try {
//                            while ((!queuePage.isEmpty() || pageThread.isAlive())) {
//                                synchronized (codingConnector){
//                                    if (!codingConnector.isAlive()){
//                                        break;
//                                    }
//                                }
//                                Integer peekId = queuePage.poll();
//                                if (Checker.isEmpty(peekId)) continue;
//                                Map<String, Object> issueDetail = this.get(IssueParam.create().issueCode(peekId));
//                                if (Checker.isEmpty(issueDetail)) continue;
//                                queueItem.add(issueDetail);
//                            }
//                        } catch (Exception e) {
//                            throw e;
//                        } finally {
//                            itemThreadCount.getAndAdd(-1);
//                        }
//                    }, 1, TimeUnit.SECONDS);
//                }
//                break;
//            }
//        }
//
//        //主线程生成事件
//        while ((!queuePage.isEmpty() || pageThread.isAlive() || itemThreadCount.get() > 0 || !queueItem.isEmpty()) ) {
//            if ( !this.sync()){
//                this.connectorOut();
//                break;
//            }
//            /**
//             * 从queueItem取数据生成事件
//             * **/
//            if (queueItem.isEmpty()) {
//                continue;
//            }
//            Map<String, Object> issueDetail = queueItem.poll();
//            if (Checker.isEmptyCollection(issueDetail)) continue;
//
//            Long referenceTime = (Long) issueDetail.get("CreatedAt");
//            Long currentTimePoint = referenceTime - referenceTime % (24 * 60 * 60 * 1000);//时间片段
//            Integer issueDetailHash = MapUtil.create().hashCode(issueDetail);
//            //issueDetial的更新时间字段值是否属于当前时间片段，并且issueDetail的hashcode是否在上一次批量读取同一时间段内
//            //如果不在，说明时全新增加或修改的数据，需要在本次读取这条数据
//            //如果在，说明上一次批量读取中以及读取了这条数据，本次不在需要读取 !currentTimePoint.equals(lastTimePoint) &&
//
//            if (!lastTimeSplitIssueCode.contains(issueDetailHash)) {
//                events.add(TapSimplify.insertRecordEvent(issueDetail, TABLE_NAME).referenceTime(System.currentTimeMillis()));
//                //eventCount.getAndAdd(1);
//                if (null == currentTimePoint || !currentTimePoint.equals(lastTimePoint)) {
//                    lastTimePoint = currentTimePoint;
//                    lastTimeSplitIssueCode = new ArrayList<Integer>();
//                }
//                lastTimeSplitIssueCode.add(issueDetailHash);
//            }
//            offset.getTableUpdateTimeMap().put(TABLE_NAME, referenceTime);
//            if (events.size() != readSize) continue;
//            consumer.accept(events, offset);
//            events = new ArrayList<>();
//        }
//        //TapLogger.info(TAG,"Issues batch read - {} pages, {} issues, output {} events. ",pageCount.get(),itemCount.get(),eventCount.get());
//        if (events.isEmpty()) return;
//        consumer.accept(events, offset);
//    }

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
    public void read(int readSize, Object offsetState, BiConsumer<List<TapEvent>, Object> consumer,boolean isStreamRead ){
        final List<TapEvent>[] events = new List[]{new ArrayList<>()};
        int pageSize = Math.min(readSize, this.batchReadMaxPageSize);

        TapConnectionContext context = this.ticketLoader.getContext();
        ContextConfig contextConfig = ticketLoader.veryContextConfigAndNodeConfig();

        HttpEntity<String, Object> tickPageParam = ticketLoader.getTickPageParam().build("limit", pageSize);

        tickPageParam.build("sortBy", (contextConfig.sortType() ? "-" : "" )+ (isStreamRead ? "modifiedTime" : "createdTime"));
        tickPageParam.build("include", "contacts,products,departments,team,isRead,assignee");

        int fromPageIndex = 1;//从第几个工单开始分页
        String modeName = context.getConnectionConfig().getString("connectionMode");
        ConnectionMode connectionMode = ConnectionMode.getInstanceByName(context, modeName);
        if (null == connectionMode){
            throw new CoreException("Connection Mode is not empty or not null.");
        }
        String tableName =  Schemas.Tickets.getTableName();
        if (Checker.isEmpty(offsetState)) offsetState = ZoHoOffset.create(new HashMap<>());
        final Object offset = offsetState;

        boolean finalNeedDetail = contextConfig.needDetailObj();

        String fields = contextConfig.fields();

        while (isAlive()){
            tickPageParam.build("from", fromPageIndex);
            List<Map<String, Object>> list = ticketLoader.list(tickPageParam);
            if (Checker.isEmpty(list) || list.isEmpty()) break;
            Map<String, List<Map<String, Object>>> idGroupOfCf = new HashMap<>();
            if (!finalNeedDetail) {
                Optional.ofNullable(fields).ifPresent(f -> tickPageParam.build("fields", f));
                List<Map<String, Object>> cfList = ticketLoader.list(tickPageParam);
                tickPageParam.remove("fields");
                idGroupOfCf.putAll(cfList.stream().filter(Objects::nonNull).collect(Collectors.groupingBy(x -> String.valueOf(x.get("id")))));
            }
            fromPageIndex += pageSize;
            list.stream().filter(Objects::nonNull).forEach(ticket -> {
                Object id = ticket.get("id");
                List<Map<String, Object>> cfMaps = idGroupOfCf.get(String.valueOf(id));

                boolean thisRecordNeedDetail = null == cfMaps || cfMaps.isEmpty();
                if (!finalNeedDetail && !thisRecordNeedDetail) {
                    for (Map<String, Object> cfMap : cfMaps) {
                        if (null != cfMap && !cfMap.isEmpty() && null != cfMap.get("cf")){
                            ticket.putAll(cfMap);
                        }
                    }
                }

                if (!isAlive()) return;
                    Map<String, Object> oneTicket = finalNeedDetail || thisRecordNeedDetail ?
                            connectionMode.attributeAssignment(ticket, tableName, ticketLoader)
                            : ticket;
                    if (Checker.isEmpty(oneTicket) || oneTicket.isEmpty()) return;
                    Object modifiedTimeObj = isStreamRead ? oneTicket.get("modifiedTime") : oneTicket.get("createdTime");//stream read is sort by "modifiedTime",batch read is sort by "createdTime"
                    long referenceTime = System.currentTimeMillis();
                    if (Checker.isNotEmpty(modifiedTimeObj) && modifiedTimeObj instanceof String) {
                        referenceTime = this.parseZoHoDatetime((String) modifiedTimeObj);
                        ((ZoHoOffset) offset).getTableUpdateTimeMap().put(tableName, referenceTime);
                    }
                    events[0].add(TapSimplify.insertRecordEvent(oneTicket,tableName).referenceTime(referenceTime));

                if (events[0].size() != readSize) return;
                consumer.accept(events[0], offset);
                events[0] = new ArrayList<>();
            });
        }
        if (events[0].isEmpty()) return;
        consumer.accept(events[0], offset);
    }

}
