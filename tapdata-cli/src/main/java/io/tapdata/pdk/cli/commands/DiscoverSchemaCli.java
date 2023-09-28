package io.tapdata.pdk.cli.commands;

import com.alibaba.fastjson.JSON;
import io.tapdata.entity.logger.TapLog;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.pdk.cli.CommonCli;
import io.tapdata.pdk.core.api.ConnectionNode;
import io.tapdata.pdk.core.api.PDKIntegration;
import io.tapdata.pdk.core.utils.CommonUtils;
import picocli.CommandLine;

import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;

@CommandLine.Command(
        description = "Test allTables method",
        subcommands = MainCli.class
)
public class DiscoverSchemaCli extends CommonCli {
    private static final String TAG = DiscoverSchemaCli.class.getSimpleName();

    @CommandLine.Option(names = { "-i", "--id" }, required = true, description = "Provide PDK id")
    private String pdkId;

    @CommandLine.Option(names = { "-g", "--group" }, required = true, description = "Provide PDK group")
    private String pdkGroup;

    @CommandLine.Option(names = { "-v", "--version" }, required = true, description = "Provide PDK buildNumber")
    private String version;

    @CommandLine.Option(names = { "-c", "--connectionConfig" }, required = false, description = "Provide PDK connection config json string")
    private String connectionConfigStr;

    @CommandLine.Option(names = { "-h", "--help" }, usageHelp = true, description = "TapData cli help")
    private boolean helpRequested = false;

    public Integer execute() throws Exception {
        try {
            DataMap dataMap = null;
            if(connectionConfigStr != null) {
                dataMap = JSON.parseObject(connectionConfigStr, DataMap.class);
            }
            ConnectionNode connectionNode = PDKIntegration.createConnectionConnectorBuilder()
                    .withAssociateId(UUID.randomUUID().toString())
                    .withGroup(pdkGroup)
                    .withVersion(version)
                    .withPdkId(pdkId)
                    .withConnectionConfig(dataMap)
                    .withLog(new TapLog())
                    .build();
            connectionNode.discoverSchema(null, 1000, new Consumer<List<TapTable>>() {
                @Override
                public void accept(List<TapTable> tables) {
                    for(TapTable table : tables) {
                        TapLogger.info(TAG, "Table: {}", table);
                    }
                }
            });
        } catch (Throwable throwable) {
            throwable.printStackTrace();
            CommonUtils.logError(TAG, "AllTables failed", throwable);
        }
        return 0;
    }

}
