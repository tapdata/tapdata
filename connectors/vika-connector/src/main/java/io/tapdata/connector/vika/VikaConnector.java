package io.tapdata.connector.vika;

import cn.vika.client.api.VikaApiClient;
import cn.vika.client.api.http.ApiCredential;
import cn.vika.client.api.http.ApiHttpClient;
import cn.vika.client.api.model.ApiQueryParam;
import cn.vika.client.api.model.CreateDatasheetRequest;
import cn.vika.client.api.model.CreateFieldRequest;
import cn.vika.client.api.model.CreateRecordRequest;
import cn.vika.client.api.model.FieldKey;
import cn.vika.client.api.model.Node;
import cn.vika.client.api.model.Pager;
import cn.vika.client.api.model.Record;
import cn.vika.client.api.model.RecordMap;
import cn.vika.client.api.model.UpdateRecord;
import cn.vika.client.api.model.UpdateRecordRequest;
import cn.vika.client.api.model.field.FieldTypeEnum;
import cn.vika.client.api.model.field.property.DateTimeFieldProperty;
import cn.vika.client.api.model.field.property.NumberFieldProperty;
import cn.vika.client.api.model.field.property.SingleTextFieldProperty;
import cn.vika.client.api.model.field.property.TextFieldProperty;
import cn.vika.client.api.model.field.property.option.DateFormatEnum;
import cn.vika.client.api.model.field.property.option.PrecisionEnum;
import cn.vika.client.api.model.field.property.option.TimeFormatEnum;
import cn.vika.core.utils.CollectionUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.tapdata.base.ConnectorBase;
import io.tapdata.connector.vika.field.Field;
import io.tapdata.connector.vika.field.FieldApi;
import io.tapdata.connector.vika.limit.Restrictor;
import io.tapdata.connector.vika.space.SpaceApi;
import io.tapdata.connector.vika.space.SpaceRespone;
import io.tapdata.connector.vika.view.DataSheetView;
import io.tapdata.connector.vika.view.DataSheetViewApi;
import io.tapdata.connector.vika.view.GetDatasheetViewRespone;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.ddl.table.TapClearTableEvent;
import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.TapNumber;
import io.tapdata.entity.schema.value.TapArrayValue;
import io.tapdata.entity.schema.value.TapDateTimeValue;
import io.tapdata.entity.schema.value.TapDateValue;
import io.tapdata.entity.schema.value.TapMapValue;
import io.tapdata.entity.schema.value.TapRawValue;
import io.tapdata.entity.schema.value.TapTimeValue;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.connector.target.CreateTableOptions;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;

@TapConnectorClass("spec_vika.json")
public class VikaConnector extends ConnectorBase {
    private VikaApiClient vikaApiClient;
    private ApiHttpClient apiHttpClient;
    private volatile SpaceApi spaceApi;
    private volatile DataSheetViewApi dataSheetViewApi;
    private volatile FieldApi fieldApi;
    private String spaceId;

    private final Integer groupNum = 10;

    // {api: {count: 5, time:45395435345}}
//    private final Map<String, Pair<Integer, Long>> limitMap = new ConcurrentHashMap<>();

    @Override
    public void onStart(TapConnectionContext connectionContext) throws Throwable {
        DataMap config = connectionContext.getConnectionConfig();
        String hostUrl = config.getString("hostUrl");
        String credential = config.getString("credential");
        spaceId = config.getString("spaceId");

        if (EmptyKit.isNotEmpty(hostUrl)) {
            vikaApiClient = new VikaApiClient(hostUrl, new ApiCredential(credential));
        } else if (EmptyKit.isNotEmpty(credential)) {
            vikaApiClient = new VikaApiClient(new ApiCredential(credential));
        }

        apiHttpClient = new ApiHttpClient(vikaApiClient.getApiVersion(), hostUrl, new ApiCredential(credential));
    }

    @Override
    public void onStop(TapConnectionContext connectionContext) throws Throwable {

    }

    @Override
    public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecsRegistry codecRegistry) {
        connectorFunctions.supportErrorHandleFunction(this::errorHandle);
        connectorFunctions.supportWriteRecord(this::writeRecord);
        connectorFunctions.supportCreateTableV2(this::createTableV2);
        connectorFunctions.supportClearTable(this::clearTable);
        //connectorFunctions.supportDropTable(this::dropTable);

        codecRegistry.registerFromTapValue(TapRawValue.class, FieldTypeEnum.Text.name(), tapRawValue -> {
            if (tapRawValue != null && tapRawValue.getValue() != null) return tapRawValue.getValue().toString();
            return "null";
        });
        codecRegistry.registerFromTapValue(TapArrayValue.class, FieldTypeEnum.Text.name(), TapArrayValue -> {
            if (TapArrayValue != null && TapArrayValue.getValue() != null) return TapArrayValue.getValue().toString();
            return "null";
        });
        codecRegistry.registerFromTapValue(TapMapValue.class, FieldTypeEnum.Text.name(), tapMapValue -> {
            if (tapMapValue != null && tapMapValue.getValue() != null) return toJson(tapMapValue.getValue());
            return "null";
        });
        codecRegistry.registerFromTapValue(TapTimeValue.class, tapTimeValue -> formatTapDateTime(tapTimeValue.getValue(), "HH:mm:ss"));
        codecRegistry.registerFromTapValue(TapDateValue.class, tapDateValue -> formatTapDateTime(tapDateValue.getValue(), "yyyy-MM-dd"));
        codecRegistry.registerFromTapValue(TapDateTimeValue.class, tapDateValue -> formatTapDateTime(tapDateValue.getValue(), "yyyy-MM-dd HH:mm:ss"));
    }

    @Override
    public void discoverSchema(TapConnectionContext connectionContext, List<String> tables, int tableSize, Consumer<List<TapTable>> consumer) throws Throwable {
        List<Node> nodes = (List<Node>) Restrictor.limitRule(() -> vikaApiClient.getNodeApi().getNodes(spaceId));
        if (EmptyKit.isNotEmpty(nodes)) {
            List<Node> nodeList = nodes.stream().filter(node -> "Datasheet".equals(node.getType())).collect(Collectors.toList());
            List<List<Node>> partition = Lists.partition(nodeList, tableSize);

            List<TapTable> tapTableList = list();
            for (List<Node> list : partition) {
                for (Node node : list) {

                    String datasheetId = node.getId();
                    List<DataSheetView> views = getDataSheetViewApi(apiHttpClient).getViews(datasheetId).getViews();
                    if (EmptyKit.isEmpty(views)) {
                        continue;
                    }
                    for (DataSheetView view : views) {
                        TapTable tapTable = table(node.getName());
                        List<Field> fields = getFieldApi(apiHttpClient).getFields(datasheetId, view.getId()).getFields();
                        for (Field field : fields) {
                            TapField tapField = new TapField(field.getName(), field.getType());
                            tapTable.add(tapField);
                        }
                        tapTableList.add(tapTable);
                    }
                }
            }
            consumer.accept(tapTableList);
        }
    }

    @Override
    public ConnectionOptions connectionTest(TapConnectionContext connectionContext, Consumer<TestItem> consumer) throws Throwable {
        DataMap config = connectionContext.getConnectionConfig();
        String hostUrl = config.getString("hostUrl");
        String credential = config.getString("credential");
        String spaceId = config.getString("spaceId");

        VikaApiClient vikaApiClient;
        if (EmptyKit.isNotEmpty(hostUrl)) {
            vikaApiClient = new VikaApiClient(hostUrl, new ApiCredential(credential));
        } else {
            vikaApiClient = new VikaApiClient(new ApiCredential(credential));
        }

        ConnectionOptions connectionOptions = ConnectionOptions.create();
        StringJoiner joiner = new StringJoiner("/");
        joiner.add(hostUrl);
        joiner.add(spaceId);
        connectionOptions.connectionString(joiner.toString());

        ApiHttpClient apiHttpClient = new ApiHttpClient(vikaApiClient.getApiVersion(), hostUrl, new ApiCredential(credential));

        TestItem testConnect;
        try {
            getSpaceApi(apiHttpClient).getSpaces();
            testConnect = testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_SUCCESSFULLY);
        } catch (Exception e) {
            testConnect = testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_FAILED, e.getMessage());
        }

        consumer.accept(testConnect);
        return connectionOptions;
    }

    @Override
    public int tableCount(TapConnectionContext connectionContext) throws Throwable {
        SpaceRespone spaces = getSpaceApi(apiHttpClient).getSpaces();
        if (EmptyKit.isNotEmpty(spaces.getSpaces())) {

            AtomicInteger count = new AtomicInteger();

            List<Node> nodes = (List<Node>) Restrictor.limitRule(() -> vikaApiClient.getNodeApi().getNodes(spaceId));
            if (EmptyKit.isNotEmpty(nodes)) {
                List<String> datasheetIds = nodes.stream().filter(node -> "Datasheet".equals(node.getType())).map(Node::getId).collect(Collectors.toList());
                if (EmptyKit.isNotEmpty(datasheetIds)) {
                    for (String datasheetId : datasheetIds) {
                        GetDatasheetViewRespone views = getDataSheetViewApi(apiHttpClient).getViews(datasheetId);
                        if (EmptyKit.isNotEmpty(views.getViews())) {
                            count.getAndAdd(views.getViews().size());
                        }
                    }
                }
            }

            return count.get();
        }

        return 0;
    }

    public SpaceApi getSpaceApi(ApiHttpClient apiHttpClient) {
        if (this.spaceApi == null) {
            synchronized (this) {
                if (this.spaceApi == null) {
                    this.spaceApi = new SpaceApi(apiHttpClient);
                }
            }
        }
        return this.spaceApi;
    }

    public DataSheetViewApi getDataSheetViewApi(ApiHttpClient apiHttpClient) {
        if (this.dataSheetViewApi == null) {
            synchronized (this) {
                if (this.dataSheetViewApi == null) {
                    this.dataSheetViewApi = new DataSheetViewApi(apiHttpClient);
                }
            }
        }
        return this.dataSheetViewApi;
    }

    public FieldApi getFieldApi(ApiHttpClient apiHttpClient) {
        if (this.fieldApi == null) {
            synchronized (this) {
                if (this.fieldApi == null) {
                    this.fieldApi = new FieldApi(apiHttpClient);
                }
            }
        }
        return this.fieldApi;
    }

    private CreateTableOptions createTableV2(TapConnectorContext tapConnectorContext, TapCreateTableEvent tapCreateTableEvent) throws Throwable {
        CreateTableOptions createTableOptions = new CreateTableOptions();
        TapTable tapTable = tapCreateTableEvent.getTable();
        String tableId = tapTable.getId();

        List<Node> nodes = (List<Node>) Restrictor.limitRule(() -> vikaApiClient.getNodeApi().getNodes(spaceId));
        boolean match = nodes.stream().anyMatch(node -> tableId.equals(node.getName()));
        createTableOptions.setTableExists(match);
        if (!match) {
            CreateDatasheetRequest createDatasheetRequest = new CreateDatasheetRequest();
            createDatasheetRequest.setName(tableId);

            List<CreateFieldRequest<?>> fields = Lists.newArrayList();
            for (TapField value : tapTable.getNameFieldMap().values()) {
                String type = value.getDataType().startsWith("Number") ? value.getDataType() : FieldTypeEnum.valueOf(value.getDataType()).name();
                if (null != type && FieldTypeEnum.SingleText.name().equals(type)) {
                    CreateFieldRequest<SingleTextFieldProperty> fieldRequest = new CreateFieldRequest<>();
                    SingleTextFieldProperty property = new SingleTextFieldProperty();
                    property.setDefaultValue(Objects.isNull(value.getDefaultValue()) ? "" : value.getDefaultValue().toString());
                    fieldRequest.setProperty(property);
                    fieldRequest.setName(value.getName());
                    fieldRequest.setType(type);
                    fields.add(fieldRequest);
                } else if (null != type && type.startsWith(FieldTypeEnum.Number.name())) {
                    CreateFieldRequest<NumberFieldProperty> fieldRequest = new CreateFieldRequest<>();
                    NumberFieldProperty property = new NumberFieldProperty();
                    property.setDefaultValue(Objects.isNull(value.getDefaultValue()) ? "" : value.getDefaultValue().toString());
                    //TapNumber tapType = (TapNumber) value.getTapType();
                    PrecisionEnum precisionEnum;
                    switch (type) {
                        case "Number0" : precisionEnum = PrecisionEnum.POINT0;break;
                        case "Number1": precisionEnum = PrecisionEnum.POINT1;break;
                        case "Number2" : precisionEnum = PrecisionEnum.POINT2;break;
                        case "Number3": precisionEnum = PrecisionEnum.POINT3;break;
                        default: precisionEnum = PrecisionEnum.POINT4;
                    }
                    property.setPrecision(precisionEnum);
                    fieldRequest.setProperty(property);
                    fieldRequest.setName(value.getName());
                    fieldRequest.setType("Number");
                    fields.add(fieldRequest);
                }  else if (null != type && type.startsWith("Date")) {
                    CreateFieldRequest<DateTimeFieldProperty> fieldRequest = new CreateFieldRequest<>();
                    DateTimeFieldProperty property = new DateTimeFieldProperty();
                    property.setDateFormat(DateFormatEnum.DATE);
                    property.setIncludeTime(FieldTypeEnum.DateTime.name().equals(type));
                    property.setTimeFormat(TimeFormatEnum.HOUR_MINUTE_24);
                    fieldRequest.setProperty(property);
                    fieldRequest.setName(value.getName());
                    fieldRequest.setType(type);
                    fields.add(fieldRequest);
                } else {
                    CreateFieldRequest<TextFieldProperty> fieldRequest = new CreateFieldRequest<>();
                    fieldRequest.setName(value.getName());
                    fieldRequest.setType(type);
                    fields.add(fieldRequest);
                }
            }

            createDatasheetRequest.setFields(fields);

            Restrictor.limitRule0(() -> vikaApiClient.getDatasheetApi().addDatasheet(spaceId, createDatasheetRequest));
        }
        return createTableOptions;
    }

    /**
     * 频率限制
     * 同一个用户对同一张表的 API 请求频率上限为 5 次/秒。
     * 请求频率超过限制时，会提示错误“操作太频繁”（错误状态码 429）。
     * 接口限制
     * 获取记录接口：一次最多获取 1000 行记录。
     * 比如想批量获取 10000 行记录，至少需要调用 10 次获取记录接口。
     * 创建记录接口：一次最多创建 10 行记录。
     * 比如想批量创建 1000 行记录，至少需要调用 100 次创建记录接口。
     * 更新记录接口：一次最多更新 10 行记录。
     * 比如想批量更新 1000 行记录，至少需要调用 100 次更新记录接口。
     * 删除记录接口：一次最多删除 10 行记录。
     * 比如想批量删除 1000 行记录，至少需要调用 100 次删除记录接口。
     * 上传附件接口：一次只可上传 1 个附件。
     * 如果需要上传多份文件，需要重复调用此接口。
     *
     */
    private void writeRecord(TapConnectorContext connectorContext, List<TapRecordEvent> tapRecordEvents, TapTable tapTable, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer) throws Throwable, Exception {
        String tableId = tapTable.getId();
        Object fieldsObj = connectorContext.getNodeConfig().get("ignoreFields");
        Set<String> ignoreValueFields = new HashSet<>();
        Optional.ofNullable(fieldsObj).ifPresent(fields -> {
            if (fields instanceof Collection){
                ignoreValueFields.addAll((Collection<String>) fieldsObj);
            }
        });
        List<Node> nodes = (List<Node>) Restrictor.limitRule(() -> vikaApiClient.getNodeApi().getNodes(spaceId));
        Node nodeTemp = nodes.stream().filter(node -> tableId.equals(node.getName())).findAny().orElse(null);
        boolean match = Objects.nonNull(nodeTemp);
        if (match) {
            String datasheetId = nodeTemp.getId();
            Map<Integer, List<TapRecordEvent>> listMap = tapRecordEvents.stream().collect(Collectors.groupingBy(TapRecordEvent::getType));
            for (Map.Entry<Integer, List<TapRecordEvent>> entry : listMap.entrySet()) {
                if (TapInsertRecordEvent.TYPE == entry.getKey()) {
                    List<TapRecordEvent> collect = entry.getValue().stream().map(event -> (TapInsertRecordEvent) event).collect(Collectors.toList());
                    List<List<TapRecordEvent>> partition = Lists.partition(collect, groupNum);
                    for (int i = 1; i <= partition.size(); i++) {
                        List<Map<String, List<?>>> recordList = new ArrayList<>();
                        List<TapRecordEvent> insertRecordEvents = partition.get(i - 1);

                        List<Record> allRecords = getRecordFromVika(tapTable, insertRecordEvents, datasheetId);
                        Collection<String> primaryKeys = tapTable.primaryKeys(true);
                        Map<String, List<Record>> groupRecords = allRecords.stream().filter(Objects::nonNull).collect(Collectors.groupingBy(entity ->
                            asKey(primaryKeys, tapTable, entity.getFields())
                        ));
                        String isChangeStatus = "I";
                        String itemStatus = "I";
                        Map<String, List<?>> map = new HashMap<>();
                        List<Object> objects = new ArrayList<>();
                        for (TapRecordEvent event : insertRecordEvents) {
                            Map<String, Object> after = ((TapInsertRecordEvent)event).getAfter();
                            List<Record> all = groupRecords.get(asKey(primaryKeys, tapTable, event));
                            List<Object> recordItem = new ArrayList<>();
                            if (null == all || all.isEmpty()) {
                                isChangeStatus = "I";
                                //没有查出记录做新增
                                recordItem.add(new RecordMap().withFields(after));
                            } else {
                                //@TODO 查出记录了， 做更新或者丢弃
                                isChangeStatus = "U";
                                for (Record record : all) {
                                    if (null == record) continue;
                                    UpdateRecord updateRecord = new UpdateRecord();
                                    updateRecord.withRecordId(record.getRecordId());
                                    Map<String, Object> itemMap = new HashMap<>(after);
                                    //指定的字段存在值时不更新这些字段，直接使用存在的值
                                    ignoreValueFields.stream().filter(Objects::nonNull).forEach(field ->
                                            Optional.ofNullable(record.getFields().get(field)).ifPresent(value -> itemMap.put(field, value))
                                    );
                                    updateRecord.setFields(itemMap);
                                    recordItem.add(updateRecord);
                                }
                            }
                            if (isChangeStatus.equals(itemStatus)) {
                                objects.addAll(recordItem);
                            } else {
                                map.put(itemStatus, objects);
                                recordList.add(map);
                                map = new HashMap<>();
                                objects = new ArrayList<>(recordItem);
                                //map.put(itemStatus, objects);
                                itemStatus = isChangeStatus;
                            }
                        }
                        if (!objects.isEmpty()){
                            map.put(itemStatus, objects);
                            recordList.add(map);
                        }

                        recordList.forEach(ent -> {
                            ent.keySet().stream().filter(Objects::nonNull).forEach(key -> {
                                List<?> event = ent.get(key);
                                switch (key) {
                                        case "U" :
                                            UpdateRecordRequest request = new UpdateRecordRequest();
                                            request.setFieldKey(FieldKey.Name);
                                            request.setRecords((List<UpdateRecord>) event);
                                            Restrictor.limitRule(() -> vikaApiClient.getRecordApi().updateRecords(datasheetId, request));
                                            writeListResultConsumer.accept(new WriteListResult<TapRecordEvent>().modifiedCount(request.getRecords().size()));
                                            break;
                                        case "I" :
                                            CreateRecordRequest record = new CreateRecordRequest();
                                            record.setFieldKey(FieldKey.Name);
                                            record.setRecords((List<RecordMap>) event);
                                            Restrictor.limitRule(() -> vikaApiClient.getRecordApi().addRecords(datasheetId, record));
                                            writeListResultConsumer.accept(new WriteListResult<TapRecordEvent>().insertedCount(event.size()));
                                            break;
                                    }
                            });
                        });
                    }
                } else if (TapUpdateRecordEvent.TYPE == entry.getKey()) {
                    List<TapUpdateRecordEvent> collect = entry.getValue().stream().map(event -> (TapUpdateRecordEvent) event).collect(Collectors.toList());

                    List<UpdateRecord> recordList = Lists.newArrayList();
                    for (int i = 1; i <= collect.size(); i++) {
                        TapUpdateRecordEvent event = collect.get(i - 1);

                        Map<String, Object> after = event.getAfter();
                        List<Record> all = getRecordFromVika(tapTable, list(event), datasheetId);
                        //ignoreFields
                        if (null == all || all.isEmpty()) {
                            //@TODO 不存在时插入 或 丢弃

                        } else {
                            all.forEach(l -> {
                                UpdateRecord updateRecord = new UpdateRecord();
                                updateRecord.withRecordId(l.getRecordId());
                                Map<String, Object> itemMap = new HashMap<>(after);

                                //指定的字段存在值时不更新这些字段，直接使用存在的值
                                ignoreValueFields.stream().filter(Objects::nonNull).forEach(field ->
                                        Optional.ofNullable(l.getFields().get(field)).ifPresent(value -> itemMap.put(field, value))
                                );

                                updateRecord.setFields(itemMap);
                                recordList.add(updateRecord);
                            });
                        }
                    }

                    List<List<UpdateRecord>> partition = Lists.partition(recordList, 10);
                    for (int i = 1; i <= partition.size(); i++) {
                        UpdateRecordRequest request = new UpdateRecordRequest();
                        request.setFieldKey(FieldKey.Name);
                        request.setRecords(partition.get(i - 1));

                        Restrictor.limitRule(() -> vikaApiClient.getRecordApi().updateRecords(datasheetId, request));
                        writeListResultConsumer.accept(new WriteListResult<TapRecordEvent>().modifiedCount(request.getRecords().size()));
                        //updateCount.addAndGet(request.getRecords().size());
                    }

                } else if (TapDeleteRecordEvent.TYPE == entry.getKey()) {
                    List<TapDeleteRecordEvent> collect = entry.getValue().stream().map(event -> (TapDeleteRecordEvent) event).collect(Collectors.toList());

                    for (int i = 1; i <= collect.size(); i++) {

                        TapDeleteRecordEvent event = collect.get(i - 1);
                        Map<String, Object> before = event.getBefore();

                        List<String> querys = Lists.newArrayList();
                        for (Map.Entry<String, Object> ent : before.entrySet()) {
                            String key = ent.getKey();
                            Object value = ent.getValue();
                            querys.add(key + "=\"" + value.toString() + "\"");
                        }

                        ApiQueryParam queryParam = new ApiQueryParam();
                        queryParam.withFilter(StringUtils.join(querys, "&&"));

                        Pager<Record> records = (Pager<Record>) Restrictor.limitRule(() -> vikaApiClient.getRecordApi().getRecords(datasheetId, queryParam));
                        if (records.getTotalItems() > 0) {
                            List<String> recordList = records.stream().map(Record::getRecordId).collect(Collectors.toList());
                            List<List<String>> splitList = CollectionUtil.splitListParallel(recordList, 10);
                            for (List<String> list : splitList) {
                                Restrictor.limitRule0(() -> vikaApiClient.getRecordApi().deleteRecords(datasheetId, list));

                                //deleteCount.addAndGet(list.size());
                                writeListResultConsumer.accept(new WriteListResult<TapRecordEvent>().removedCount(list.size()));
                            }
                        }
                    }

                }
            }
        }
    }

    private String asKey(Collection<String> primaryKeys, TapTable tapTable, TapRecordEvent event) {
        StringJoiner querySub = new StringJoiner("&&");
        Map<String, Object> eventData = new HashMap<>(Optional.ofNullable(
                event instanceof TapInsertRecordEvent ? ((TapInsertRecordEvent) event).getAfter() : (
                        event instanceof TapUpdateRecordEvent ? ((TapUpdateRecordEvent) event).getAfter() : ((TapDeleteRecordEvent) event).getBefore()
                )).orElse(new HashMap<>()));
        return asKey(primaryKeys, tapTable, eventData);
    }
    private String asKey(Collection<String> primaryKeys, TapTable tapTable, Map<String, Object> eventData) {
        StringJoiner querySub = new StringJoiner("&&");
        for (String key : ( null == primaryKeys || primaryKeys.isEmpty() ? tapTable.getNameFieldMap().keySet() : primaryKeys)) {
            Object value = eventData.get(key);
            querySub.add(key + "=\"" + value.toString() + "\"");
        }
        return querySub.toString();
    }

    private List<Record> getRecordFromVika(TapTable tapTable, List<TapRecordEvent> events, String datasheetId){
        //Map<String, Object> before = event.getBefore();
        StringJoiner query = new StringJoiner(",");
        Collection<String> primaryKeys = tapTable.primaryKeys(true);
        for (TapRecordEvent event : events) {
            String querySub = asKey(primaryKeys, tapTable, event);
            if (querySub.length() > 0) {
                query.add(querySub.toString());
            }
        }


        /*
        通过主键查询, 但是主键不一定是唯一的, 有可能是联合主键, 所以这里需要查询出来, 然后再进行更新
        Collection<String> fields = tapTable.primaryKeys(true);
        Map<String, Object> match = collect.get(0).getFilter(fields);
        Map.Entry<String, Object> entry = match.entrySet().stream().findFirst().get();

        vikaApiClient.getRecordApi().getRecords(datasheetId, ApiQueryParam.newInstance().withFilter("{订单编号} = 'B230528135433'"));

         */
        String queryStr = "OR(" + query.toString() + ")";
        int first = 1;
        ApiQueryParam queryParam = new ApiQueryParam(first, 1000);
        queryParam.withFilter(queryStr);
        Pager<Record> recordPager = (Pager<Record>) Restrictor.limitRule(() -> vikaApiClient.getRecordApi().getRecords(datasheetId, queryParam));
        List<Record> all = recordPager.all();
        while (first < recordPager.getTotalPages()) {
            first++;
            ApiQueryParam quertTemp = new ApiQueryParam(first, 1000);
            quertTemp.withFilter(queryStr);

            Pager<Record> recordPagerTemp = (Pager<Record>) Restrictor.limitRule(() -> vikaApiClient.getRecordApi().getRecords(datasheetId, quertTemp));
            all.addAll(recordPagerTemp.all());
        }

        return all;
    }

    private void clearTable(TapConnectorContext tapConnectorContext, TapClearTableEvent tapClearTableEvent) throws Throwable {
        String tableId = tapClearTableEvent.getTableId();
        List<Node> nodes = (List<Node>) Restrictor.limitRule(() -> vikaApiClient.getNodeApi().getNodes(spaceId));
        Node nodeTemp = nodes.stream().filter(node -> tableId.equals(node.getName())).findAny().orElse(null);
        if (Objects.nonNull(nodeTemp)) {
            Restrictor.limitRule0(() -> vikaApiClient.getRecordApi().deleteAllRecords(nodeTemp.getId()));
        }
    }
}
