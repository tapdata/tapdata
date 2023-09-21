package io.tapdata.pdk.cli.tdd;

import io.tapdata.pdk.cli.Main;

/**
 * Picocli aims to be the easiest way to create rich command line applications that can run on and off the JVM. Considering picocli? Check what happy users say about picocli.
 * https://picocli.info/
 *
 * @author aplomb
 */
public class TDDBenchmarkMain1 {
    //
    public static void main(String... args) {
        args = new String[]{
//                "test", "-c", "B:\\code\\tapdata\\idaas-pdk\\tapdata-pdk-cli\\src\\main\\resources\\config\\aerospike.json",
                "test", "-c", "tapdata-pdk-cli/src/main/resources/config/tddBenchmark.json",
                "-t", "io.tapdata.pdk.tdd.tests.target.benchmark.BenchmarkTest",
//                "B:\\code\\tapdata\\idaas-pdk\\dist\\aerospike-connector-v1.0-SNAPSHOT.jar",
//                "B:\\code\\tapdata\\idaas-pdk\\dist\\doris-connector-v1.0-SNAPSHOT.jar",
                "connectors/tdd-connector",

        };

		Main.registerCommands().execute(args);
    }
}
