package io.tapdata.pdk.cli.tdd;

import io.tapdata.pdk.cli.Main;

/**
 * Picocli aims to be the easiest way to create rich command line applications that can run on and off the JVM. Considering picocli? Check what happy users say about picocli.
 * https://picocli.info/
 *
 * @author aplomb
 */
public class TDDMain {
    //
    public static void main(String... args) {
        args = new String[]{
//                "test", "-c", "B:\\code\\tapdata\\idaas-pdk\\tapdata-pdk-cli\\src\\main\\resources\\config\\aerospike.json",
                "test", "-c", "B:\\code\\tapdata\\idaas-pdk\\tapdata-pdk-cli\\src\\main\\resources\\config\\mongodb.json",
                "-t", "io.tapdata.pdk.tdd.tests.target.DMLTest",
//                "-t", "io.tapdata.pdk.tdd.tests.source.ReadTest",
//                "B:\\code\\tapdata\\idaas-pdk\\dist\\aerospike-connector-v1.0-SNAPSHOT.jar",
//                "B:\\code\\tapdata\\idaas-pdk\\dist\\doris-connector-v1.0-SNAPSHOT.jar",
                "B:\\code\\tapdata\\idaas-pdk\\dist\\mongodb-connector-v1.0-SNAPSHOT.jar",
        };

		Main.registerCommands().execute(args);
    }
}
