package io.tapdata.pdk.cli.commands;

import io.tapdata.pdk.cli.CommonCli;
import io.tapdata.pdk.cli.utils.ZipUtils;
import io.tapdata.pdk.core.connector.TapConnectorManager;
import io.tapdata.pdk.core.utils.CommonUtils;
import io.tapdata.supervisor.convert.entity.ClassModifier;
import org.apache.commons.io.FileUtils;
import picocli.CommandLine;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.UUID;

/**
 * Class modify by javassist
 * @author 2749984520@qq.com Gavin
 * @time 2023/03/24
 * */
@CommandLine.Command(
        description = "Insert code to PDK jar class file",
        subcommands = MainCli.class
)
public class SupervisorCli extends CommonCli {
    private static final String TAG = SupervisorCli.class.getSimpleName();
    @CommandLine.Parameters(paramLabel = "FILE", description = "One or more pdk jar files")
    File[] files;

    private String pathToSave;

    @CommandLine.Option(names = {"-h", "--help"}, usageHelp = true, description = "TapData cli help")
    private boolean helpRequested = false;

    public Integer execute() throws Exception {
        try {
            TapConnectorManager.getInstance().start(Arrays.asList(files));
            for (File file : files) {
                if (!file.isFile()) continue;
                String path = file.getAbsolutePath();
                System.out.println("Modify class: " + path + ", is starting! ");
                pathToSave = file.getParentFile().getAbsolutePath() + "/" + UUID.randomUUID().toString().replaceAll("-","_") + "/";
                File pathToFile = new File(pathToSave);
                try {
                    // unzip
                    if (!pathToFile.isDirectory()) {
                        boolean mkdir = pathToFile.mkdir();
                    }
                    System.out.println("-> step of unzip jar: ");
                    ZipUtils.unzip(file.getAbsolutePath(), pathToSave);
                    System.out.println("unzip jar successfully. ");

                    //根据配置，植入代码
                    System.out.println("-> step of modify class: ");
                    ClassModifier modifier = ClassModifier.load(path, pathToSave);
                    modifier.wave();
                    System.out.println("modify class successfully. ");

                    System.out.println("-> step of zip jar: ");
                    File atomicFile = new File(file.getAbsolutePath());
                    if (atomicFile.exists())
                        FileUtils.deleteQuietly(atomicFile);
                    try (OutputStream fos = new FileOutputStream(atomicFile)) {
                        ZipUtils.zip(pathToSave, fos);
                        System.out.println("zip jar successfully. ");
                    } catch (Exception e) {
                        System.out.println("zip jar failed. ");
                    }
                } finally {
                    //清除pathToSave目录下的class
                    FileUtils.deleteQuietly(pathToFile);
                    boolean delete = pathToFile.delete();
                }
                System.out.println();
            }
        } catch (Throwable throwable) {
            throwable.printStackTrace();
            CommonUtils.logError(TAG, "Class modify failed", throwable);
        }
        return 0;
    }
}
