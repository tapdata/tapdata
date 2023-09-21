package io.tapdata.pdk.cli.tdd;

import io.tapdata.pdk.cli.Main;

/**
 * Picocli aims to be the easiest way to create rich command line applications that can run on and off the JVM. Considering picocli? Check what happy users say about picocli.
 * https://picocli.info/
 *
 * @author aplomb
 */
public class DiscoverSchemaMain {
    //
    public static void main(String... args) {
        args = new String[] {"discoverSchema",
                "--id", "aerospike",
                "--group", "io.tapdata.connector.aerospike",
                "--version", "1.0-SNAPSHOT",
                "--connectionConfig", "{'token' : 'uskMiSCZAbukcGsqOfRqjZZ', 'spaceId' : 'spcvyGLrtcYgs'}"};

		Main.registerCommands().execute(args);
    }
}
