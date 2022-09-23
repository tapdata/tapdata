package io.tapdata.zoho;


import io.tapdata.base.ConnectorBase;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.CommandInfo;
import io.tapdata.pdk.apis.entity.CommandResult;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.zoho.service.zoho.TicketLoader;
import io.tapdata.zoho.utils.Checker;

import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/***
 * 1000.dbf93e07349ca3059401bb35486b2bfb.90a94c1a7f08c71934ab53ef1b9e68a8  14:35
 * 手动-添加client
 * 手动-获得clientId,clientSecret
 * 手动-生成Code可设置3-10分钟内有效
 * api获取-clientId+clientSecret+code 获取 asscessToken(一小时内有效，最多30个，每超过就替换最先生成的)，refresh_token（一直有效，知道重新生成，用户获取asscessToken）
 * api获取-clientId+clientSecret+refresh_token 获取 asscessToken(一小时内有效，最多30个，每超过就替换最先生成的)
 *
 *Desk.tickets.ALL,Desk.contacts.READ,Desk.contacts.WRITE,Desk.contacts.UPDATE,Desk.contacts.CREATE,Desk.tasks.ALL,Desk.basic.READ,Desk.basic.CREATE,Desk.settings.ALL,Desk.events.ALL,Desk.articles.READ,Desk.articles.CREATE,Desk.articles.UPDATE,Desk.articles.DELETE
 * Client ID                     1000.RXERF0BIW3RBP7NOJMK615YT9ATRFB
 * Client Secret                 2d5d8f1518a0232cfa33ff45b8ac9566d9c5344cc5
 * {
 *     "access_token": "1000.99586de7eb697435059f3289adc2b138.236b7f9bd27dc60199d2514c128087a7",
 *     "refresh_token": "1000.a664d08e653ce402c62b609f7ab6051a.bf5b49d68b7fc8428561b304ef1f4874",
 *     "api_domain": "https://www.zohoapis.com.cn",
 *     "token_type": "Bearer",
 *     "expires_in": 3600
 * }
 * */

@TapConnectorClass("spec.json")
public class ZoHoConnector extends ConnectorBase {
	private static final String TAG = ZoHoConnector.class.getSimpleName();

	private final Object streamReadLock = new Object();


	@Override
	public void onStart(TapConnectionContext connectionContext) throws Throwable {
		TicketLoader.create(connectionContext).verifyConnectionConfig();
		DataMap connectionConfig = connectionContext.getConnectionConfig();
		String streamReadType = connectionConfig.getString("streamReadType");
		if (Checker.isEmpty(streamReadType)){
			throw new CoreException("Error in connection parameter [streamReadType], please go to verify");
		}
		switch (streamReadType){
			case "WebHook":this.connectorFunctions.supportStreamRead(null);break;
			case "Polling":this.connectorFunctions.supportRawDataCallbackFilterFunction(null);break;
//			default:
//				throw new CoreException("Error in connection parameters [streamReadType],just be [WebHook] or [Polling], please go to verify");
		}
	}

	@Override
	public void onStop(TapConnectionContext connectionContext) throws Throwable {

	}

	private ConnectorFunctions connectorFunctions;
	@Override
	public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecsRegistry codecRegistry) {
		connectorFunctions.supportBatchRead(this::batchRead)
				.supportBatchCount(this::batchCount)
				.supportTimestampToStreamOffset(this::timestampToStreamOffset)
				.supportStreamRead(this::streamRead)
				.supportRawDataCallbackFilterFunction(this::rawDataCallbackFilterFunction)
				.supportCommandCallbackFunction(this::handleCommand)
		;
		this.connectorFunctions = connectorFunctions;
	}

	private CommandResult handleCommand(TapConnectionContext tapConnectionContext, CommandInfo commandInfo) {

		return null;
	}

	private TapEvent rawDataCallbackFilterFunction(TapConnectorContext connectorContext, Map<String, Object> issueEventData) {
		return null;
	}

	private void streamRead(
			TapConnectorContext nodeContext,
			List<String> tableList,
			Object offsetState,
			int recordSize,
			StreamReadConsumer consumer ) {

	}

	private Object timestampToStreamOffset(TapConnectorContext tapConnectorContext, Long time) {
		return null;
	}

	@Override
	public void discoverSchema(TapConnectionContext connectionContext, List<String> tables, int tableSize, Consumer<List<TapTable>> consumer) throws Throwable {

	}

	@Override
	public ConnectionOptions connectionTest(TapConnectionContext connectionContext, Consumer<TestItem> consumer) throws Throwable {
		return null;
	}


	private void batchRead(
			TapConnectorContext connectorContext,
			TapTable table,
			Object offset,
			int batchCount,
			BiConsumer<List<TapEvent>, Object> consumer) {
	}

	private long batchCount(TapConnectorContext tapConnectorContext, TapTable tapTable) throws Throwable {
		long count = 0;
		return count;
	}

	@Override
	public int tableCount(TapConnectionContext connectionContext) throws Throwable {
		//check how many projects
		return 1;
	}


}
