package io.tapdata.quickapi;

import io.tapdata.base.ConnectorBase;
import io.tapdata.common.APIFactoryImpl;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.common.support.APIFactory;
import io.tapdata.common.support.APIInvoker;
import io.tapdata.quickapi.common.QuickApiConfig;
import io.tapdata.quickapi.server.QuickAPIResponseInterceptor;
import io.tapdata.quickapi.server.TestQuickApi;
import io.tapdata.quickapi.server.enums.QuickApiTestItem;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

@TapConnectorClass("spec.json")
public class QuickApiConnector extends ConnectorBase {
	private static final String TAG = QuickApiConnector.class.getSimpleName();
	private final Object streamReadLock = new Object();

	private QuickApiConfig config;
	private APIInvoker invoker;
	private APIFactory apiFactory;
	private Map<String,Object> apiParam = new HashMap<>();

	private AtomicBoolean task = new AtomicBoolean(true);

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
			try {
				toJson(jsonTxt);
			}catch (Exception e){
				TapLogger.error(TAG,"API JSON only JSON format. ");
			}
			String expireStatus = connectionConfig.getString("expireStatus");
			String tokenParams = connectionConfig.getString("tokenParams");
			config.apiConfig(apiType)
					.jsonTxt(jsonTxt)
					.expireStatus(expireStatus)
					.tokenParams(tokenParams);
			apiFactory = new APIFactoryImpl();
			invoker = apiFactory.loadAPI(jsonTxt, apiParam);
			invoker.setAPIResponseInterceptor(QuickAPIResponseInterceptor.create(config,invoker));
		}
	}

	@Override
	public void onStop(TapConnectionContext connectionContext) throws Throwable {
		this.task.set(false);
	}

	@Override
	public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecsRegistry codecRegistry) {
		if(Objects.nonNull(connectorFunctions)) {
			connectorFunctions.supportBatchCount(this::batchCount)
					.supportBatchRead(this::batchRead)
					.supportTimestampToStreamOffset(this::timestampToStreamOffset)
					.supportErrorHandleFunction(this::errorHandle);
		}else{
			TapLogger.error(TAG,"ConnectorFunctions must be not null or not be empty. ");
		}
	}

	private void streamRead(TapConnectorContext context, List<String> strings, Object o, int i, StreamReadConsumer streamReadConsumer) {
		TapLogger.info(TAG,"QuickAPIConnector does not support StreamRead at the moment. Please manually set the task to incremental only or wait 3 seconds for the task to enter the completion state.");
		try {
			this.wait(3000);
		} catch (InterruptedException ignored) {
		}
	}


	private Object timestampToStreamOffset(TapConnectorContext tapConnectorContext, Long time) {
		return Objects.isNull(time)?System.currentTimeMillis():time;
	}

	public void batchRead(TapConnectorContext context,
						  TapTable table,
						  Object offset,
						  int batchCount,
						  BiConsumer<List<TapEvent>, Object> consumer){
		invoker.pageStage(context,table,offset,batchCount,task,consumer);
	}
	private long batchCount(TapConnectorContext tapConnectorContext, TapTable tapTable) throws Throwable {
		return 0L;
	}

	@Override
	public void discoverSchema(TapConnectionContext connectionContext, List<String> tables, int tableSize, Consumer<List<TapTable>> consumer) throws Throwable {
		List<String> schema = invoker.tables();
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
			consumer.accept(testItem(QuickApiTestItem.TEST_PARAM.testName(), TestItem.RESULT_SUCCESSFULLY));
		}catch (Exception e){
			consumer.accept(testItem(QuickApiTestItem.TEST_PARAM.testName(),TestItem.RESULT_FAILED,e.getMessage()));
		}
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

		List<TestItem> testItem = testQuickApi.testApi();
		Optional.ofNullable(testItem).ifPresent(test->test.forEach(consumer));
		return connectionOptions;
	}

	@Override
	public int tableCount(TapConnectionContext connectionContext) throws Throwable {
		List<String> schema = invoker.tables();
		if (Objects.isNull(schema)) return 0;
		return schema.size();
	}
}
