package io.tapdata.quickapi;

import io.tapdata.base.ConnectorBase;
import io.tapdata.common.APIFactoryImpl;
import io.tapdata.common.support.APIFactory;
import io.tapdata.common.support.APIInvoker;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.CommandResult;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.entity.message.CommandInfo;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.quickapi.common.QuickApiConfig;
import io.tapdata.quickapi.server.QuickAPIResponseInterceptor;
import io.tapdata.quickapi.server.TestQuickApi;
import io.tapdata.quickapi.server.enums.QuickApiTestItem;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

@TapConnectorClass("spec.json")
public class QuickApiConnector extends ConnectorBase {
	private static final String TAG = QuickApiConnector.class.getSimpleName();
	private final Object streamReadLock = new Object();

	private QuickApiConfig config;
	private APIInvoker invoker;
	private APIFactory apiFactory;
	private final Map<String,Object> apiParam = new HashMap<>();

	private final AtomicBoolean task = new AtomicBoolean(true);

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
			connectorFunctions.supportWriteRecord(this::write)
					.supportCommandCallbackFunction(this::command)
					.supportErrorHandleFunction(this::errorHandle);
		} else {
			TapLogger.error(TAG,"ConnectorFunctions must be not empty. ");
		}
	}

	private CommandResult command(TapConnectionContext context, CommandInfo info) {

		return null;
	}

	private void write(TapConnectorContext context, List<TapRecordEvent> events, TapTable table, Consumer<WriteListResult<TapRecordEvent>> consumer) {

	}

	@Override
	public void discoverSchema(TapConnectionContext connectionContext, List<String> tables, int tableSize, Consumer<List<TapTable>> consumer) throws Throwable {
		List<String> schema = invoker.tables();
		List<TapTable> tableList = new ArrayList<>();
		schema.stream().filter(Objects::nonNull).forEach(name -> tableList.add(table(name,name)));
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
		Optional.ofNullable(testItem).ifPresent(test -> test.forEach(consumer));
		return connectionOptions;
	}

	@Override
	public int tableCount(TapConnectionContext connectionContext) throws Throwable {
		List<String> schema = invoker.tables();
		if (Objects.isNull(schema)) return 0;
		return schema.size();
	}
}
