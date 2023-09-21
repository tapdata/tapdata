package io.tapdata.pdk.cli.tdd;

import io.tapdata.pdk.cli.Main;
import io.tapdata.pdk.core.utils.CommonUtils;

/**
 * Picocli aims to be the easiest way to create rich command line applications that can run on and off the JVM. Considering picocli? Check what happy users say about picocli.
 * https://picocli.info/
 *
 * @author aplomb
 */
public class TDDBenchmarkNoTableMain2 {
    //
    public static void main(String... args) {
        CommonUtils.setProperty("pdk_external_jar_path", "./connectors/dist");
        args = new String[]{
//                "test", "-c", "B:\\code\\tapdata\\idaas-pdk\\tapdata-pdk-cli\\src\\main\\resources\\config\\aerospike.json",
//                "test", "-c", "tapdata-cli/src/main/resources/config/tddBenchmark.json",
                "test", "-c", "tapdata-cli/src/main/resources/config/mongodb.json",
                "-t", "io.tapdata.pdk.tdd.tests.target.benchmark.BenchmarkNoTableTest",
//                "B:\\code\\tapdata\\idaas-pdk\\dist\\aerospike-connector-v1.0-SNAPSHOT.jar",
//                "B:\\code\\tapdata\\idaas-pdk\\dist\\doris-connector-v1.0-SNAPSHOT.jar",
//                "connectors/tdd-connector",
                "connectors/mongodb-connector",

        };

		Main.registerCommands().execute(args);
    }
}
