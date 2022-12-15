package io.tapdata.quickapi;

import cn.hutool.json.JSONUtil;
import io.tapdata.base.ConnectorBase;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.api.APIFactory;
import io.tapdata.pdk.apis.api.APIResponse;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.quickapi.common.QuickApiConfig;
import io.tapdata.quickapi.server.TestQuickApi;
import io.tapdata.quickapi.server.enums.QuickApiTestItem;
import io.tapdata.quickapi.support.APIFactoryImpl;
import io.tapdata.quickapi.support.postman.PostManAnalysis;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

@TapConnectorClass("spec.json")
public class QuickApiConnector extends ConnectorBase {
	private static final String TAG = QuickApiConnector.class.getSimpleName();
	private final Object streamReadLock = new Object();

	private QuickApiConfig config;
	private PostManAnalysis invoker;
	private APIFactory apiFactory;

	@Override
	public void onStart(TapConnectionContext connectionContext) throws Throwable {
		DataMap connectionConfig = connectionContext.getConnectionConfig();
		config = QuickApiConfig.create();
		if (Objects.nonNull(connectionConfig)) {
			String apiType = connectionConfig.getString("apiType");
			if (Objects.isNull(apiType)) apiType = "POST_MAN";
			String jsonTxt = connectionConfig.getString("jsonTxt");
			if (Objects.isNull(jsonTxt)){
				TapLogger.error(TAG,"API JSON must be not null or not empty. ");
			}
			if (!JSONUtil.isJson(jsonTxt)){
				TapLogger.error(TAG,"API JSON only JSON format. ");
			}
			config.apiConfig(apiType)
					.jsonTxt(jsonTxt);

			apiFactory = new APIFactoryImpl();
			invoker = (PostManAnalysis)apiFactory.loadAPI(jsonTxt, apiType, null);
			invoker.setAPIResponseInterceptor((response, urlOrName, method, params)->{
				APIResponse interceptorResponse = APIResponse.create();

				return interceptorResponse;
			});
		}
	}

	@Override
	public void onStop(TapConnectionContext connectionContext) throws Throwable {
		synchronized (this) {
			this.notify();
		}
		TapLogger.info(TAG, "Stop connector");
	}

	@Override
	public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecsRegistry codecRegistry) {
		if(Objects.nonNull(connectorFunctions)) {
			connectorFunctions.supportBatchCount(this::batchCount)
					.supportBatchRead(this::batchRead)
					.supportTimestampToStreamOffset(this::timestampToStreamOffset);
		}else{
			TapLogger.error(TAG,"ConnectorFunctions must be not null or not be empty. ");
		}
	}


	private Object timestampToStreamOffset(TapConnectorContext tapConnectorContext, Long time) {
		return null;
	}

	public void batchRead(TapConnectorContext connectorContext,
						  TapTable table,
						  Object offset,
						  int batchCount,
						  BiConsumer<List<TapEvent>, Object> consumer){
	}
	private long batchCount(TapConnectorContext tapConnectorContext, TapTable tapTable) throws Throwable {
		return 0L;
	}

	@Override
	public void discoverSchema(TapConnectionContext connectionContext, List<String> tables, int tableSize, Consumer<List<TapTable>> consumer) throws Throwable {
		List<String> schema = ((PostManAnalysis)invoker).apiContext().tapTable();
		List<TapTable> tableList = new ArrayList<>();
		schema.stream().filter(Objects::nonNull).forEach(name->tableList.add(table(name,name)));
		consumer.accept( tableList );
	}

	@Override
	public ConnectionOptions connectionTest(TapConnectionContext connectionContext, Consumer<TestItem> consumer) throws Throwable {
		ConnectionOptions connectionOptions = ConnectionOptions.create();
		TestQuickApi testQuickApi = null;
		try {
			testQuickApi = TestQuickApi.create(connectionContext);
			consumer.accept(testItem(QuickApiTestItem.TEST_PARAM.testName(),TestItem.RESULT_SUCCESSFULLY));
		}catch (Exception e){
			consumer.accept(testItem(QuickApiTestItem.TEST_PARAM.testName(),TestItem.RESULT_FAILED,e.getMessage()));
		}

		//TestItem testJSON = testQuickApi.testJSON();
		//consumer.accept(testJSON);
		//if (Objects.isNull(testJSON) || Objects.equals(testJSON.getResult(),TestItem.RESULT_FAILED)){
		//	return connectionOptions;
		//}
		TestItem testTapTableTag = testQuickApi.testTapTableTag();
		consumer.accept(testTapTableTag);
		if (Objects.isNull(testTapTableTag) || Objects.equals(testTapTableTag.getResult(),TestItem.RESULT_FAILED)){
			return connectionOptions;
		}

		TestItem testTokenConfig = testQuickApi.testTokenConfig();
		consumer.accept(testTokenConfig);
		if (Objects.isNull(testTokenConfig) || Objects.equals(testTokenConfig.getResult(),TestItem.RESULT_FAILED)){
			return connectionOptions;
		}

		TestItem testItem = testQuickApi.testApi();
		consumer.accept(testItem);
		if (Objects.isNull(testItem) || Objects.equals(testItem.getResult(),TestItem.RESULT_FAILED)){
			return connectionOptions;
		}
		return connectionOptions;
	}

	@Override
	public int tableCount(TapConnectionContext connectionContext) throws Throwable {
		return 0;
	}
}
