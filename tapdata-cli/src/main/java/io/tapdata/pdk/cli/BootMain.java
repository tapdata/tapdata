package io.tapdata.pdk.cli;

import io.tapdata.pdk.cli.Main;

/**
 * Picocli aims to be the easiest way to create rich command line applications that can run on and off the JVM. Considering picocli? Check what happy users say about picocli.
 * https://picocli.info/
 *
 * @author aplomb
 */
public class BootMain {
    public static void main(String... args) {
        args = new String[]{"template", "-g", "io.tapdata", "-n", "XDB", "-v", "0.0.1", "-o", "../connectors"};

		Main.registerCommands().execute(args);
    }
}
