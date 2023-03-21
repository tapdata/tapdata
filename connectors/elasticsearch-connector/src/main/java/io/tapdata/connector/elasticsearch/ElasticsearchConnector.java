package io.tapdata.connector.elasticsearch;

import com.google.common.collect.Lists;
import io.tapdata.base.ConnectorBase;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.ddl.table.TapClearTableEvent;
import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.entity.event.ddl.table.TapDropTableEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.value.*;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.*;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.GetAliasesResponse;
import org.elasticsearch.client.IndicesClient;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetMappingsRequest;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

@TapConnectorClass("spec_elasticsearch.json")
public class ElasticsearchConnector extends ConnectorBase {

    private ElasticsearchHttpContext elasticsearchHttpContext;
    private ElasticsearchConfig elasticsearchConfig;
    private String elasticsearchVersion;
    private static final String TAG = ElasticsearchConnector.class.getSimpleName();

    private void initConnection(TapConnectionContext connectorContext) {
        elasticsearchConfig = new ElasticsearchConfig().load(connectorContext.getConnectionConfig());
        elasticsearchHttpContext = new ElasticsearchHttpContext(elasticsearchConfig);
        elasticsearchVersion = elasticsearchHttpContext.queryVersion();
    }

    @Override
    public void onStart(TapConnectionContext connectionContext) {
        initConnection(connectionContext);
    }

    @Override
    public void onStop(TapConnectionContext connectionContext) throws Throwable {
        elasticsearchHttpContext.finish();
    }

    @Override
    public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecsRegistry codecRegistry) {
        connectorFunctions.supportErrorHandleFunction(this::errorHandle);
        connectorFunctions.supportWriteRecord(this::writeRecord);
        connectorFunctions.supportCreateTable(this::createTable);
        connectorFunctions.supportClearTable(this::clearTable);
        connectorFunctions.supportDropTable(this::dropTable);
        connectorFunctions.supportQueryByFilter(this::queryByFilter);

        codecRegistry.registerFromTapValue(TapRawValue.class, "text", tapRawValue -> {
            if (tapRawValue != null && tapRawValue.getValue() != null) return tapRawValue.getValue().toString();
            return "null";
        });
        codecRegistry.registerFromTapValue(TapArrayValue.class, "text", TapArrayValue -> {
            if (TapArrayValue != null && TapArrayValue.getValue() != null) return TapArrayValue.getValue().toString();
            return "null";
        });
        codecRegistry.registerFromTapValue(TapMapValue.class, "text", tapMapValue -> {
            if (tapMapValue != null && tapMapValue.getValue() != null) return toJson(tapMapValue.getValue());
            return "null";
        });
        codecRegistry.registerFromTapValue(TapTimeValue.class, tapTimeValue -> formatTapDateTime(tapTimeValue.getValue(), "HH:mm:ss"));
        codecRegistry.registerFromTapValue(TapDateTimeValue.class, tapDateTimeValue -> formatTapDateTime(tapDateTimeValue.getValue(), "yyyy-MM-dd HH:mm:ss.SSSSSS"));
        codecRegistry.registerFromTapValue(TapDateValue.class, tapDateValue -> formatTapDateTime(tapDateValue.getValue(), "yyyy-MM-dd"));
    }

    @Override
    public void discoverSchema(TapConnectionContext connectionContext, List<String> tables, int tableSize, Consumer<List<TapTable>> consumer) throws Throwable {
        IndicesClient indicesClient = elasticsearchHttpContext.getElasticsearchClient().indices();
        List<String> indices = list();
        if (EmptyKit.isEmpty(tables)) {
            GetAliasesRequest request = new GetAliasesRequest();
            GetAliasesResponse getAliasesResponse = indicesClient.getAlias(request, RequestOptions.DEFAULT);
            Map<String, Set<AliasMetaData>> map = getAliasesResponse.getAliases();
            indices.addAll(map.keySet());
        } else {
            indices.addAll(tables.stream().map(String::toLowerCase).collect(Collectors.toList()));
        }
        indices.sort(Comparator.naturalOrder());
        List<List<String>> indexLists = Lists.partition(indices, tableSize);
        for (List<String> indexList : indexLists) {
            List<TapTable> tapTableList = list();
            GetMappingsRequest mappingsRequest = new GetMappingsRequest();
            mappingsRequest.indices(indexList.toArray(new String[0]));
            Map<String, MappingMetaData> mappings = indicesClient.getMapping(mappingsRequest, RequestOptions.DEFAULT).mappings();
            indexList.forEach(index -> {
                TapTable tapTable = table(index);
                LinkedHashMap<String, LinkedHashMap> fields = (LinkedHashMap) mappings.get(index).getSourceAsMap().get("properties");
                if (EmptyKit.isEmpty(fields)) {
                    TapLogger.warn(TAG, "discover schema no fields, index: " + index);
                    tapTableList.add(tapTable);
                    return;
                }
                fields.forEach((key, value) -> {
                    TapField tapField = new TapField();
                    tapField.setName(key);
                    tapField.setDataType(EmptyKit.isNull(value.get("type")) ? "object" : value.get("type").toString());
                    tapTable.add(tapField);
                });
                tapTableList.add(tapTable);
            });
            consumer.accept(tapTableList);
        }
    }

    @Override
    public ConnectionOptions connectionTest(TapConnectionContext connectionContext, Consumer<TestItem> consumer) throws Throwable {
        elasticsearchConfig = new ElasticsearchConfig().load(connectionContext.getConnectionConfig());
        ElasticsearchTest elasticsearchTest = new ElasticsearchTest(elasticsearchConfig);
        TestItem testHostPort = elasticsearchTest.testHostPort();
        consumer.accept(testHostPort);
        if (testHostPort.getResult() == TestItem.RESULT_FAILED) {
            return null;
        }
        TestItem testConnect = elasticsearchTest.testConnect();
        consumer.accept(testConnect);
        if (testConnect.getResult() == TestItem.RESULT_FAILED) {
            return null;
        }
        elasticsearchTest.close();
        return null;
    }

    @Override
    public int tableCount(TapConnectionContext connectionContext) throws Throwable {
        return elasticsearchHttpContext.countIndices();
    }

    private void writeRecord(TapConnectorContext connectorContext, List<TapRecordEvent> tapRecordEvents, TapTable tapTable, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer) throws Throwable {
        new ElasticsearchRecordWriter(elasticsearchHttpContext, tapTable).write(tapRecordEvents, writeListResultConsumer);
    }

    private void createTable(TapConnectorContext tapConnectorContext, TapCreateTableEvent tapCreateTableEvent) throws Throwable {
        TapTable tapTable = tapCreateTableEvent.getTable();
        if (!elasticsearchHttpContext.existsIndex(tapTable.getId().toLowerCase())) {
            int chunkSize = 1;
            CreateIndexRequest indexRequest = new CreateIndexRequest(tapTable.getId().toLowerCase());
            indexRequest.settings(Settings.builder()
                    .put("number_of_shards", chunkSize)
                    .put("number_of_replicas", 1));
            XContentBuilder xContentBuilder = XContentFactory.jsonBuilder();
            xContentBuilder.startObject();
            xContentBuilder.field("dynamic", "true");
            xContentBuilder.startObject("properties");
            for (TapField field : tapTable.getNameFieldMap().values()) {
                String dataType = field.getDataType();
                if (null == dataType) {
                    continue;
                }
                switch (dataType) {
                    case "text":
                        xContentBuilder.startObject(field.getName())
                                .field("type", "text")
                                .startObject("fields").startObject("keyword")
                                .field("ignore_above", 256)
                                .field("type", "keyword")
                                .endObject().endObject()
                                .endObject();
                        break;
                    case "unsigned_long":
                        xContentBuilder.startObject(field.getName())
                                .field("type", "unsigned_long")
                                .endObject();
                        break;
                    case "scaled_float":
                        xContentBuilder.startObject(field.getName())
                                .field("type", "scaled_float")
                                .field("scaling_factor", 100)
                                .endObject();
                        break;
                    case "date":
                        xContentBuilder.startObject(field.getName())
                                .field("type", "date")
                                .field("format", "yyyy-MM-dd||yyyy-MM-dd HH:mm:ss.SSSSSS||HH:mm:ss||epoch_millis")
                                .endObject();
                        break;
                    default:
                        xContentBuilder.startObject(field.getName())
                                .field("type", dataType)
                                .endObject();
                        break;
                }
            }
            xContentBuilder.endObject();
            xContentBuilder.endObject();
            indexRequest.mapping(xContentBuilder);

            elasticsearchHttpContext.getElasticsearchClient().indices().create(indexRequest, RequestOptions.DEFAULT);
        }
    }

    private void clearTable(TapConnectorContext tapConnectorContext, TapClearTableEvent tapClearTableEvent) throws Throwable {
        String index = tapClearTableEvent.getTableId().toLowerCase();
        if (elasticsearchHttpContext.existsIndex(index)) {
            DeleteByQueryRequest deleteByQueryRequest = new DeleteByQueryRequest(index);
            deleteByQueryRequest.setQuery(QueryBuilders.matchAllQuery());
            elasticsearchHttpContext.getElasticsearchClient().deleteByQuery(deleteByQueryRequest, RequestOptions.DEFAULT);
        }
    }

    private void dropTable(TapConnectorContext tapConnectorContext, TapDropTableEvent tapDropTableEvent) throws Throwable {
        String index = tapDropTableEvent.getTableId().toLowerCase();
        if (elasticsearchHttpContext.existsIndex(index)) {
            DeleteIndexRequest deleteRequest = new DeleteIndexRequest(index);
            elasticsearchHttpContext.getElasticsearchClient().indices().delete(deleteRequest, RequestOptions.DEFAULT);
        }
    }

    private void queryByFilter(TapConnectorContext connectorContext, List<TapFilter> filters, TapTable tapTable, Consumer<List<FilterResult>> listConsumer) throws Throwable {
        Set<String> columnNames = tapTable.getNameFieldMap().keySet();
        List<FilterResult> filterResults = new LinkedList<>();
        MultiSearchRequest multiSearchRequest = new MultiSearchRequest();
        for (TapFilter filter : filters) {
            SearchRequest searchRequest = new SearchRequest();
            searchRequest.indices(tapTable.getId().toLowerCase());
            SearchSourceBuilder builder = new SearchSourceBuilder();
            filter.getMatch().forEach((key, value) -> builder.query(QueryBuilders.termQuery(key, value)));
            searchRequest.source(builder);
            multiSearchRequest.add(searchRequest);
        }
        MultiSearchResponse multiSearchResponse = elasticsearchHttpContext.getElasticsearchClient().msearch(multiSearchRequest, RequestOptions.DEFAULT);
        multiSearchResponse.forEach(search -> {
            FilterResult filterResult = new FilterResult();
            if (search.isFailure()) {
                filterResult.setError(search.getFailure());
            } else {
                SearchHits hits = search.getResponse().getHits();
                if (hits.getHits().length < 1) {
                    filterResult.setResult(null);
                } else {
                    filterResult.setResult(Arrays.stream(hits.getHits()).findFirst().get().getSourceAsMap());
                }
            }
            filterResults.add(filterResult);
        });
        listConsumer.accept(filterResults);
    }
}
