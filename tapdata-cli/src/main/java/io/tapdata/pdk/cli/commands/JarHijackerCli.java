package io.tapdata.pdk.cli.commands;

import io.tapdata.pdk.cli.CommonCli;
import io.tapdata.pdk.cli.utils.ZipUtils;
import io.tapdata.pdk.core.utils.CommonUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import picocli.CommandLine;

import java.io.File;
import java.io.OutputStream;
import java.util.*;

@CommandLine.Command(
        description = "Push PDK jar file into Tapdata",
        subcommands = MainCli.class
)
public class JarHijackerCli extends CommonCli {
    private static final String TAG = JarHijackerCli.class.getSimpleName();
    @CommandLine.Option(names = { "-m", "--module" }, required = true, description = "Module path")
    private String module;

    @CommandLine.Parameters(paramLabel = "FILE", description = "One ore more pdk jar files")
    File[] files;


    @CommandLine.Option(names = {"-h", "--help"}, usageHelp = true, description = "TapData cli help")
    private boolean helpRequested = false;

    public Integer execute() throws Exception {
        File moduleDir = new File(module);
        if(!moduleDir.isDirectory())
            throw new IllegalArgumentException("Module " + module + " is not a directory");
        try {
            CommonUtils.setProperty("refresh_local_jars", "true");
            String tempDir = CommonUtils.getProperty("temp_dir", "./temp/");
//            TapConnectorManager.getInstance().start(Arrays.asList(files));

            for (File file : files) {
//                TapConnector connector = TapConnectorManager.getInstance().getTapConnectorByJarName(file.getName());
//                TapOverwriteAnnotationHandler overwriteAnnotationHandler = connector.getTapNodeClassFactory().getTapOverwriteAnnotationHandler();
//                List<Class<?>> overwriteClasses = overwriteAnnotationHandler.getOverwriteClasses();
                String subPath = "src" + File.separator + "main" + File.separator + "overwrite";
                String overwritePath = FilenameUtils.concat(module, subPath);
                File overwriteDir = new File(overwritePath);
                List<String> targetFiles = new ArrayList<>();
                if(overwriteDir.isDirectory()) {
                    Collection<File> files = FileUtils.listFiles(overwriteDir, new String[]{"java"}, true);
                    if(files == null || files.isEmpty()) {
                        continue;
                    }
                    for(File targetFile : files) {
                        String path = targetFile.getPath();
                        int pos = path.indexOf(subPath);
                        if(pos > 0) {
                            String theSubPath = path.substring(pos + subPath.length() + 1, path.length() - 5) + ".class";
                            targetFiles.add(theSubPath);
                        }
                    }
                }
                if(targetFiles.isEmpty())
                    return 1;
                String tempName = file.getName() + UUID.randomUUID().toString().replace("-", "");
                String tempTargetDir = FilenameUtils.concat(tempDir, tempName);
                try {
                    ZipUtils.unzip(file.getAbsolutePath(), tempTargetDir);

                    File overwriteTargetDir = new File(FilenameUtils.concat(module, "target/classes"));
                    if(overwriteTargetDir.isDirectory()) {
                        for(String targetFile : targetFiles) {
                            File theTargetFile = new File(FilenameUtils.concat(overwriteTargetDir.getAbsolutePath(), targetFile));
                            if(theTargetFile.isFile()) {
                                FileUtils.copyFile(theTargetFile, new File(FilenameUtils.concat(tempTargetDir, targetFile)), true);
                            }
                        }
                    }
                    File atomicFile = new File(file.getAbsolutePath() + ".bak");
                    if(atomicFile.exists())
                        FileUtils.deleteQuietly(atomicFile);
                    try (OutputStream fos = FileUtils.openOutputStream(atomicFile)) {
                        ZipUtils.zip(tempTargetDir, fos);
                    }
                    FileUtils.deleteQuietly(file);
                    FileUtils.moveFile(atomicFile, file);
                } finally {
                    FileUtils.forceDelete(new File(tempTargetDir));
                }

            }
        } catch (Throwable throwable) {
            throwable.printStackTrace();
            CommonUtils.logError(TAG, "Start failed", throwable);
        }
        return 0;
    }

}
