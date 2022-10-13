import cn.vika.client.api.VikaApiClient;
import cn.vika.client.api.http.ApiCredential;
import cn.vika.client.api.http.ApiHttpClient;
import cn.vika.client.api.model.Node;
import com.google.common.collect.Lists;
import io.tapdata.base.ConnectorBase;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import space.SpaceApi;
import space.SpaceRespone;
import view.DataSheetView;
import view.DataSheetViewApi;
import view.GetDatasheetViewRespone;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;

@TapConnectorClass("spec_vika.json")
public class VikaConnector extends ConnectorBase {

    private VikaApiClient vikaApiClient;
    private ApiHttpClient apiHttpClient;
    private volatile SpaceApi spaceApi;
    private volatile DataSheetViewApi dataSheetViewApi;

    private String spaceId;

    @Override
    public void onStart(TapConnectionContext connectionContext) throws Throwable {
        DataMap config = connectionContext.getConnectionConfig();
        String hostUrl = config.getString("hostUrl");
        String credential = config.getString("credential");
        spaceId = config.getString("spaceId");

        if (EmptyKit.isNotEmpty(credential) && EmptyKit.isNotEmpty(hostUrl)) {
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

    }

    @Override
    public void discoverSchema(TapConnectionContext connectionContext, List<String> tables, int tableSize, Consumer<List<TapTable>> consumer) throws Throwable {

        List<Node> nodes = vikaApiClient.getNodeApi().getNodes(spaceId);
        if (EmptyKit.isNotEmpty(nodes)) {
            List<String> datasheetIds = nodes.stream().filter(node -> "Datasheet".equals(node.getType())).map(Node::getId).collect(Collectors.toList());
            if (EmptyKit.isNotEmpty(datasheetIds)) {
                for (String datasheetId : datasheetIds) {
                    GetDatasheetViewRespone views = getDataSheetViewApi().getViews(datasheetId);
                    List<DataSheetView> collect = views.getViews().stream().filter(v -> "Grid".equals(v.getType())).collect(Collectors.toList());
                    if (EmptyKit.isNotEmpty(collect)) {
                        List<List<DataSheetView>> partition = Lists.partition(collect, tableSize);
                        for (List<DataSheetView> dataSheetViews : partition) {
                            List<TapTable> tapTableList = list();

                        }
                    }
                }
            }
        }
    }

    @Override
    public ConnectionOptions connectionTest(TapConnectionContext connectionContext, Consumer<TestItem> consumer) throws Throwable {
        TestItem testConnect;

        try {
            getSpaceApi().getSpaces();
            testConnect = testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_SUCCESSFULLY);
        } catch (Exception e) {
            testConnect = testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_FAILED, e.getMessage());
        }

        consumer.accept(testConnect);
        return null;
    }

    @Override
    public int tableCount(TapConnectionContext connectionContext) throws Throwable {
        SpaceRespone spaces = getSpaceApi().getSpaces();
        if (EmptyKit.isNotEmpty(spaces.getSpaces())) {

            AtomicInteger count = new AtomicInteger();

            List<Node> nodes = vikaApiClient.getNodeApi().getNodes(spaceId);
            if (EmptyKit.isNotEmpty(nodes)) {
                List<String> datasheetIds = nodes.stream().filter(node -> "Datasheet".equals(node.getType())).map(Node::getId).collect(Collectors.toList());
                if (EmptyKit.isNotEmpty(datasheetIds)) {
                    for (String datasheetId : datasheetIds) {
                        GetDatasheetViewRespone views = getDataSheetViewApi().getViews(datasheetId);
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

    public SpaceApi getSpaceApi() {
        if (this.spaceApi == null) {
            synchronized (this) {
                if (this.spaceApi == null) {
                    this.spaceApi = new SpaceApi(apiHttpClient);
                }
            }
        }
        return this.spaceApi;
    }

    public DataSheetViewApi getDataSheetViewApi() {
        if (this.dataSheetViewApi == null) {
            synchronized (this) {
                if (this.dataSheetViewApi == null) {
                    this.dataSheetViewApi = new DataSheetViewApi(apiHttpClient);
                }
            }
        }
        return this.dataSheetViewApi;
    }
}
