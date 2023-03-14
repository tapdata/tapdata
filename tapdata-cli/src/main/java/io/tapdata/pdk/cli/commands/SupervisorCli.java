package io.tapdata.pdk.cli.commands;

import io.tapdata.pdk.cli.CommonCli;
import io.tapdata.pdk.core.connector.TapConnectorManager;
import io.tapdata.pdk.core.utils.CommonUtils;
import io.tapdata.supervisor.convert.entity.ClassModifier;
import picocli.CommandLine;

import java.io.File;
import java.util.Arrays;

@CommandLine.Command(
        description = "Insert code to PDK jar class file",
        subcommands = MainCli.class
)
public class SupervisorCli extends CommonCli {
    private static final String TAG = SupervisorCli.class.getSimpleName();
    @CommandLine.Parameters(paramLabel = "FILE", description = "One or more pdk jar files")
    File[] files;


    @CommandLine.Option(names = {"-p", "--path"}, description = "Path to save class")
    private String pathToSave;

    @CommandLine.Option(names = {"-h", "--help"}, usageHelp = true, description = "TapData cli help")
    private boolean helpRequested = false;

    public Integer execute() throws Exception {
        try {
            TapConnectorManager.getInstance().start(Arrays.asList(files));
            for (File file : files) {
                if (!file.isFile()) continue;
                String path = file.getAbsolutePath();
                //根据配置，植入代码
                ClassModifier modifier = ClassModifier.load(path, pathToSave);
                modifier.wave();

                //@todo unzip

                //@todo 把修改的类放入unzip之后的jar目录

                //@todo zip jar

                //@todo 清除pathToSave目录下的class

            }
        } catch (Throwable throwable) {
            throwable.printStackTrace();
            CommonUtils.logError(TAG, "Class modify failed", throwable);
        }
        return 0;
    }
}
