package io.tapdata.connector.vika;

import cn.vika.client.api.VikaApiClient;
import cn.vika.client.api.http.ApiCredential;
import cn.vika.client.api.http.ApiHttpClient;
import cn.vika.client.api.model.*;
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
import io.tapdata.entity.schema.type.TapDateTime;
import io.tapdata.entity.schema.type.TapNumber;
import io.tapdata.entity.schema.type.TapType;
import io.tapdata.entity.schema.value.*;
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

import java.util.List;
import java.util.Map;
import java.util.Objects;
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
        codecRegistry.registerFromTapValue(TapTimeValue.class, FieldTypeEnum.Text.name(), tapTimeValue -> formatTapDateTime(tapTimeValue.getValue(), "HH:mm:ss"));
        codecRegistry.registerFromTapValue(TapDateValue.class, FieldTypeEnum.Text.name(), tapDateValue -> formatTapDateTime(tapDateValue.getValue(), "yyyy-MM-dd"));
        codecRegistry.registerFromTapValue(TapDateTimeValue.class, FieldTypeEnum.Text.name(), tapDateValue -> formatTapDateTime(tapDateValue.getValue(), "yyyy-MM-dd HH:mm:ss"));
    }

    @Override
    public void discoverSchema(TapConnectionContext connectionContext, List<String> tables, int tableSize, Consumer<List<TapTable>> consumer) throws Throwable {
        List<Node> nodes = vikaApiClient.getNodeApi().getNodes(spaceId);
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

            List<Node> nodes = vikaApiClient.getNodeApi().getNodes(spaceId);
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

        List<Node> nodes = vikaApiClient.getNodeApi().getNodes(spaceId);
        boolean match = nodes.stream().anyMatch(node -> tableId.equals(node.getName()));
        if (match) {
            createTableOptions.setTableExists(true);
        } else {
            CreateDatasheetRequest createDatasheetRequest = new CreateDatasheetRequest();
            createDatasheetRequest.setName(tableId);

            List<CreateFieldRequest<?>> fields = Lists.newArrayList();
            for (TapField value : tapTable.getNameFieldMap().values()) {
                String type = FieldTypeEnum.valueOf(value.getDataType()).name();

                if (FieldTypeEnum.SingleText.name().equals(type)) {
                    CreateFieldRequest<SingleTextFieldProperty> fieldRequest = new CreateFieldRequest<>();
                    SingleTextFieldProperty property = new SingleTextFieldProperty();
                    property.setDefaultValue(Objects.isNull(value.getDefaultValue()) ? "" : value.getDefaultValue().toString());
                    fieldRequest.setProperty(property);
                    fieldRequest.setName(value.getName());
                    fieldRequest.setType(type);
                    fields.add(fieldRequest);
                } else if (FieldTypeEnum.Number.name().equals(type)) {
                    CreateFieldRequest<NumberFieldProperty> fieldRequest = new CreateFieldRequest<>();
                    NumberFieldProperty property = new NumberFieldProperty();
                    property.setDefaultValue(Objects.isNull(value.getDefaultValue()) ? "" : value.getDefaultValue().toString());
                    TapNumber tapType = (TapNumber) value.getTapType();
                    PrecisionEnum precisionEnum;
                    if (Objects.isNull(tapType.getScale())) {
                        precisionEnum = PrecisionEnum.POINT0;
                    } else if (tapType.getScale() == PrecisionEnum.POINT1.getValue()){
                        precisionEnum = PrecisionEnum.POINT1;
                    } else if (tapType.getScale() == PrecisionEnum.POINT2.getValue()){
                        precisionEnum = PrecisionEnum.POINT2;
                    } else if (tapType.getScale() == PrecisionEnum.POINT3.getValue()){
                        precisionEnum = PrecisionEnum.POINT3;
                    } else {
                        precisionEnum = PrecisionEnum.POINT4;
                    }
                    property.setPrecision(precisionEnum);
                    fieldRequest.setProperty(property);
                    fieldRequest.setName(value.getName());
                    fieldRequest.setType(type);
                    fields.add(fieldRequest);
                }  else if (FieldTypeEnum.DateTime.name().equals(type)) {
                    CreateFieldRequest<DateTimeFieldProperty> fieldRequest = new CreateFieldRequest<>();
                    DateTimeFieldProperty property = new DateTimeFieldProperty();
                    property.setDateFormat(DateFormatEnum.DATE);
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

            vikaApiClient.getDatasheetApi().addDatasheet(spaceId, createDatasheetRequest);

            createTableOptions.setTableExists(false);
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
        List<Node> nodes = vikaApiClient.getNodeApi().getNodes(spaceId);
        Node nodeTemp = nodes.stream().filter(node -> tableId.equals(node.getName())).findAny().orElse(null);
        boolean match = Objects.nonNull(nodeTemp);
        if (match) {
            if (tapRecordEvents.size() > 4) {
                Thread.sleep(1000);
            }

            String datasheetId = nodeTemp.getId();
            WriteListResult<TapRecordEvent> listResult = new WriteListResult<>();
            AtomicInteger insertCount = new AtomicInteger(0);
            AtomicInteger updateCount = new AtomicInteger(0);
            AtomicInteger deleteCount = new AtomicInteger(0);

            Map<Integer, List<TapRecordEvent>> listMap = tapRecordEvents.stream().collect(Collectors.groupingBy(TapRecordEvent::getType));
            for (Map.Entry<Integer, List<TapRecordEvent>> entry : listMap.entrySet()) {
                if (TapInsertRecordEvent.TYPE == entry.getKey()) {
                    List<TapInsertRecordEvent> collect = entry.getValue().stream().map(event -> (TapInsertRecordEvent) event).collect(Collectors.toList());
                    List<List<TapInsertRecordEvent>> partition = Lists.partition(collect, groupNum);
                    for (int i = 1; i <= partition.size(); i++) {
                        if (i % 5 == 0) {
                            Thread.sleep(1000);
                        }

                        List<TapInsertRecordEvent> insertRecordEvents = partition.get(i - 1);

                        List<RecordMap> records = Lists.newArrayList();
                        for (TapInsertRecordEvent event : insertRecordEvents) {
                            Map<String, Object> after = event.getAfter();

                            Map<String, Object> fields = Maps.newHashMap();
                            for (Map.Entry<String, Object> ent : after.entrySet()) {
                                String fieldName = ent.getKey();
                                Object value = ent.getValue();
                                fields.put(fieldName, value);
                            }

                            records.add(new RecordMap().withFields(fields));
                        }

                        CreateRecordRequest record = new CreateRecordRequest();
                        record.setFieldKey(FieldKey.Name);
                        record.setRecords(Lists.newArrayList(records));

                        vikaApiClient.getRecordApi().addRecords(datasheetId, record);
                        insertCount.addAndGet(records.size());
                    }

                } else if (TapUpdateRecordEvent.TYPE == entry.getKey()) {
                    List<TapUpdateRecordEvent> collect = entry.getValue().stream().map(event -> (TapUpdateRecordEvent) event).collect(Collectors.toList());

                    List<UpdateRecord> recordList = Lists.newArrayList();
                    for (int i = 1; i <= collect.size(); i++) {
                        if (i % 3 == 0) {
                            Thread.sleep(1000);
                        }

                        TapUpdateRecordEvent event = collect.get(i - 1);
//                        Map<String, Object> before = event.getBefore();
                        List<String> querys = Lists.newArrayList();
                        for (String key : tapTable.primaryKeys()) {
                            Object value = event.getAfter().get(key);
                            querys.add(key + "=\"" + value.toString() + "\"");
                        }

                        int first = 1;
                        ApiQueryParam queryParam = new ApiQueryParam(first, 1000);
                        queryParam.withFilter(StringUtils.join(querys, "&&"));
                        Pager<Record> recordPager = vikaApiClient.getRecordApi().getRecords(datasheetId, queryParam);
                        List<Record> all = recordPager.all();
                        while (first < recordPager.getTotalPages()) {
                            if (first % 5 == 0) {
                                Thread.sleep(1000);
                            }

                            first++;
                            ApiQueryParam quertTemp = new ApiQueryParam(first, 1000);
                            quertTemp.withFilter(StringUtils.join(querys, "&&"));
                            Pager<Record> recordPagerTemp = vikaApiClient.getRecordApi().getRecords(datasheetId, quertTemp);
                            all.addAll(recordPagerTemp.all());
                        }

                        Map<String, Object> after = event.getAfter();
                        all.forEach(l -> {
                            UpdateRecord updateRecord = new UpdateRecord();
                            updateRecord.withRecordId(l.getRecordId());
                            updateRecord.setFields(after);
                            recordList.add(updateRecord);
                        });
                    }

                    List<List<UpdateRecord>> partition = Lists.partition(recordList, 10);
                    for (int i = 1; i <= partition.size(); i++) {
                        if (i % 5 == 0) {
                            Thread.sleep(1000);
                        }
                        UpdateRecordRequest request = new UpdateRecordRequest();
                        request.setFieldKey(FieldKey.Name);
                        request.setRecords(partition.get(i - 1));

                        vikaApiClient.getRecordApi().updateRecords(datasheetId, request);
                        updateCount.addAndGet(request.getRecords().size());
                    }

                } else if (TapDeleteRecordEvent.TYPE == entry.getKey()) {
                    List<TapDeleteRecordEvent> collect = entry.getValue().stream().map(event -> (TapDeleteRecordEvent) event).collect(Collectors.toList());

                    for (int i = 1; i <= collect.size(); i++) {
                        if (i % 3 == 0) {
                            Thread.sleep(1000);
                        }

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
                        Pager<Record> records = vikaApiClient.getRecordApi().getRecords(datasheetId, queryParam);
                        if (records.getTotalItems() > 0) {
                            List<String> recordList = records.stream().map(Record::getRecordId).collect(Collectors.toList());
                            List<List<String>> splitList = CollectionUtil.splitListParallel(recordList, 10);
                            splitList.forEach(list -> {
                                vikaApiClient.getRecordApi().deleteRecords(datasheetId, list);
                                deleteCount.addAndGet(list.size());
                            });
                        }
                    }

                }
            }

            writeListResultConsumer.accept(listResult.insertedCount(insertCount.get())
                    .modifiedCount(updateCount.get())
                    .removedCount(deleteCount.get()));
        }
    }

    private void clearTable(TapConnectorContext tapConnectorContext, TapClearTableEvent tapClearTableEvent) throws Throwable {
        String tableId = tapClearTableEvent.getTableId();
        List<Node> nodes = vikaApiClient.getNodeApi().getNodes(spaceId);
        Node nodeTemp = nodes.stream().filter(node -> tableId.equals(node.getName())).findAny().orElse(null);
        boolean match = Objects.nonNull(nodeTemp);
        if (match) {
            vikaApiClient.getRecordApi().deleteAllRecords(nodeTemp.getId());
        }
    }
}
