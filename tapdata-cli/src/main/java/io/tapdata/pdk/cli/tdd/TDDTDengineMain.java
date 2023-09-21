package io.tapdata.pdk.cli.tdd;

import io.tapdata.pdk.cli.Main;
import io.tapdata.pdk.core.utils.CommonUtils;

import java.sql.SQLException;

public class TDDTDengineMain {

    public static void main(String... args) throws ClassNotFoundException, SQLException {
        CommonUtils.setProperty("pdk_external_jar_path", "./connectors/dist");
        args = new String[]{
                "test", "-c", "tapdata-cli/src/main/resources/config/tdengine.json",
                "-t", "io.tapdata.pdk.tdd.tests.basic.BasicTest",
                "-t", "io.tapdata.pdk.tdd.tests.v2.DiscoverSchemaTestV2",
                "-t", "io.tapdata.pdk.tdd.tests.v2.DropTableFunctionTest",
                "-t", "io.tapdata.pdk.tdd.tests.v2.GetTableNamesFunctionTest",
                "-t", "io.tapdata.pdk.tdd.tests.v2.QueryByAdvancedFilterTest",
                "-t", "io.tapdata.pdk.tdd.tests.v2.TimestampToStreamOffsetFunctionTest",
                "-t", "io.tapdata.pdk.tdd.tests.v2.TableCountTest",
                "-t", "io.tapdata.pdk.tdd.tests.v2.ClearTableTest",
                "-t", "io.tapdata.pdk.tdd.tests.v2.ConnectionTest",
                "-t", "io.tapdata.pdk.tdd.tests.v2.CreateTableTest",
                "-t", "io.tapdata.pdk.tdd.tests.v2.WriteRecordTest",
                "-t", "io.tapdata.pdk.tdd.tests.v2.BatchReadTest",
                "connectors/tdengine-connector",};

        Main.registerCommands().execute(args);
    }
}
