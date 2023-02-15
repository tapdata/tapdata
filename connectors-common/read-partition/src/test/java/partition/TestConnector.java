package partition;

import io.tapdata.base.ConnectorBase;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.partition.DatabaseReadPartitionSplitter;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.connector.source.GetReadPartitionOptions;
import io.tapdata.pdk.apis.partition.ReadPartition;

import java.util.List;
import java.util.function.Consumer;

/**
 * @author aplomb
 */
public class TestConnector extends ConnectorBase {
	@Override
	public void onStart(TapConnectionContext connectionContext) throws Throwable {

	}

	@Override
	public void onStop(TapConnectionContext connectionContext) throws Throwable {

	}

	@Override
	public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecsRegistry codecRegistry) {

	}

	@Override
	public void discoverSchema(TapConnectionContext connectionContext, List<String> tables, int tableSize, Consumer<List<TapTable>> consumer) throws Throwable {

	}

	@Override
	public ConnectionOptions connectionTest(TapConnectionContext connectionContext, Consumer<TestItem> consumer) throws Throwable {
		return null;
	}

	@Override
	public int tableCount(TapConnectionContext connectionContext) throws Throwable {
		return 0;
	}
	public DatabaseReadPartitionSplitter calculateDatabaseReadPartitions(TapConnectorContext connectorContext, TapTable table, GetReadPartitionOptions getReadPartitionOptions) {
		return DatabaseReadPartitionSplitter.calculateDatabaseReadPartitions(connectorContext, table, getReadPartitionOptions);
	}
}
