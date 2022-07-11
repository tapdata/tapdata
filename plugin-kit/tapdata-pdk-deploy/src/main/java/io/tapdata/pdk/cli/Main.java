package io.tapdata.pdk.cli;

import io.tapdata.pdk.cli.commands.*;
import picocli.CommandLine;

/**
 * Picocli aims to be the easiest way to create rich command line applications that can run on and off the JVM. Considering picocli? Check what happy users say about picocli.
 * https://picocli.info/
 *
 * @author aplomb
 */
public class Main {
    //
    public static void main(String... args) {
        Main.registerCommands().parseWithHandler(new CommandLine.RunLast(), args);
    }

    public static CommandLine registerCommands() {
        CommandLine commandLine = new CommandLine(new MainCli());
        commandLine.addSubcommand("register", new RegisterCli());
        return commandLine;
    }
}
