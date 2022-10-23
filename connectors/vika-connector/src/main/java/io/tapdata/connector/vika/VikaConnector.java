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
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.concurrent.ConcurrentHashMap;
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

    // {api: {count: 5, time:45395435345}}
    private final Map<String, Pair<Integer, Long>> limitMap = new ConcurrentHashMap<>();

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
                    property.setPrecision(PrecisionEnum.POINT4);
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

    private void writeRecord(TapConnectorContext connectorContext, List<TapRecordEvent> tapRecordEvents, TapTable tapTable, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer) throws Throwable {
        String tableId = tapTable.getId();
        List<Node> nodes = vikaApiClient.getNodeApi().getNodes(spaceId);
        Node nodeTemp = nodes.stream().filter(node -> tableId.equals(node.getName())).findAny().orElse(null);
        boolean match = Objects.nonNull(nodeTemp);
        if (match) {
            String datasheetId = nodeTemp.getId();
            WriteListResult<TapRecordEvent> listResult = new WriteListResult<>();
            long insertCount = 0L;
            long updateCount = 0L;
            long deleteCount = 0L;

            List<List<TapRecordEvent>> partition = Lists.partition(tapRecordEvents, 4);

            for (List<TapRecordEvent> recordEvents : partition) {
                for (TapRecordEvent recordEvent : recordEvents) {
                    if (recordEvent instanceof TapInsertRecordEvent) {
                        TapInsertRecordEvent event = (TapInsertRecordEvent) recordEvent;
                        Map<String, Object> after = event.getAfter();

                        List<RecordMap> records = Lists.newArrayList();
                        RecordMap recordMap = new RecordMap();
                        for (Map.Entry<String, Object> entry : after.entrySet()) {
                            String fieldName = entry.getKey();
                            Object value = entry.getValue();

                            Map<String, Object> fields = Maps.newHashMap();
                            fields.put(fieldName, value);
                            recordMap.withFields(fields);
                        }
                        records.add(recordMap);

                        CreateRecordRequest record = new CreateRecordRequest();
                        record.setFieldKey(FieldKey.Name);
                        record.setRecords(records);

                        try {
                            vikaApiClient.getRecordApi().addRecords(datasheetId, record);
                            insertCount ++;
                        } catch (Exception e) {
                            listResult.addError(recordEvent, e);
                        }
                    } else if (recordEvent instanceof TapUpdateRecordEvent) {
                        TapUpdateRecordEvent event = (TapUpdateRecordEvent) recordEvent;
                        Map<String, Object> after = event.getAfter();

                        List<UpdateRecord> records = Lists.newArrayList();
                        UpdateRecord updateRecord = new UpdateRecord();
                        for (Map.Entry<String, Object> entry : after.entrySet()) {
                            String fieldName = entry.getKey();
                            Object value = entry.getValue();

                            updateRecord.withField(fieldName, value);
                        }
                        records.add(updateRecord);

                        UpdateRecordRequest record = new UpdateRecordRequest();
                        record.setFieldKey(FieldKey.Name);
                        record.setRecords(records);

                        try {
                            vikaApiClient.getRecordApi().updateRecords(tableId, record);
                            insertCount ++;
                        } catch (Exception e) {
                            listResult.addError(recordEvent, e);
                        }
                    } else if (recordEvent instanceof TapDeleteRecordEvent) {
                        TapDeleteRecordEvent event = (TapDeleteRecordEvent) recordEvent;
                        Map<String, Object> before = event.getBefore();

                        List<String> querys = Lists.newArrayList();
                        for (Map.Entry<String, Object> entry : before.entrySet()) {
                            String key = entry.getKey();
                            Object value = entry.getValue();
                            StringJoiner joiner = new StringJoiner("=", key, "\"" + value.toString() + "\"");
                            querys.add(joiner.toString());
                        }

                        ApiQueryParam queryParam = new ApiQueryParam();
                        queryParam.withFilter(StringUtils.join(querys, "&&"));
                        Pager<Record> records = vikaApiClient.getRecordApi().getRecords(tableId, queryParam);
                        if (records.getTotalItems() > 0) {
                            try {
                                List<String> recordList = records.stream().map(Record::getRecordId).collect(Collectors.toList());
                                vikaApiClient.getRecordApi().deleteRecords(tableId, recordList);
                                insertCount ++;
                            } catch (Exception e) {
                                listResult.addError(recordEvent, e);
                            }
                        }
                    }
                }

                Thread.sleep(900);
            }


            writeListResultConsumer.accept(listResult.insertedCount(insertCount)
                    .modifiedCount(updateCount)
                    .removedCount(deleteCount));
        }
    }

    private void clearTable(TapConnectorContext tapConnectorContext, TapClearTableEvent tapClearTableEvent) throws Throwable {
        String tableId = tapClearTableEvent.getTableId();
        vikaApiClient.getRecordApi().deleteAllRecords(tableId);
    }

    private void calculateApiLimit(String datasheetId) {

    }
}
