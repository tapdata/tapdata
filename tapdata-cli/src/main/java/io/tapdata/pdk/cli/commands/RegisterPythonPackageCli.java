package io.tapdata.pdk.cli.commands;

import io.tapdata.entity.logger.TapLogger;
import io.tapdata.pdk.cli.CommonCli;
import io.tapdata.pdk.cli.utils.ZipUtils;
import io.tapdata.pdk.core.utils.CommonUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import picocli.CommandLine;

import java.io.File;

@CommandLine.Command(
        description = "Insert code to PDK jar class file, java -jar pdk.jar register -a 3324cfdf-7d3e-4792-bd32-571638d4562f -ak '' -sk '' -t http://localhost:3000 ../../connectors/dist/metabas\n" +
                "e-connector-v1.0-SNAPSHOT.jar",
        subcommands = MainCli.class
)
public class RegisterPythonPackageCli extends CommonCli {
    private static final String TAG = PythonInstallCli.class.getSimpleName();
    @CommandLine.Option(names = {"-j", "--jarPath"}, description = "")
    private String jarPath ;
    @CommandLine.Option(names = {"-pd", "--packagesDirectory"}, description = "")
    private String packagesDirectory ;
    @CommandLine.Option(names = {"-p", "--packagesPath"}, description = "")
    private String packagesPath ;
    @CommandLine.Option(names = {"-h", "--help"}, usageHelp = true, description = "Tapdata cli help")
    private boolean helpRequested = false;

    ///usr/local/lib/jython-standalone-2.7.4.jar
    public static final String cmd = "java -jar %s setup.py install";
    public Integer execute() throws Exception {
//        if (null == jarPath) {
//            TapLogger.error(TAG, "Miss jython-standalone-{version}.jar path");
//            return -1;
//        }
//        if (null == packagesDirectory) {
//            TapLogger.error(TAG, "Miss packages directory path");
//            return -1;
//        }
//        if (null == packagesPath) {
//            TapLogger.error(TAG, "Miss packages path");
//            return -1;
//        }
//
////        System.out.println(jarName);
////        System.out.println(pyJarPath);
////        System.out.println(unzipPath);
//        final String jarPath = pyJarPath.endsWith(jarName) ? pyJarPath : (pyJarPath + jarName);
//        File file = new File(jarPath);
//        if (!file.exists() || !file.isFile()){
//            TapLogger.error(TAG, "Miss jython-standalone.jar path: " + jarPath);
//            return -2;
//        }
//        final String libPathName = FilenameUtils.concat(pyJarPath, "temp_engine");
//        try {
//            try {
//                try {
//                    System.out.println("[1]: Start to unzip " + jarPath + ">>>");
//                    ZipUtils.unzip(file.getAbsolutePath(), libPathName);
//                    System.out.println("[2]: Unzip " + jarPath + " succeed >>>");
//                }catch (Exception e){
//                    //TapLogger.error(TAG, "Can not zip from " + jarPath + " to " + libPathName);
//                    System.out.println("[2]: Unzip " + jarPath + " fail.");
//                    return -3;
//                }
//                File libFiles = new File(concat(libPathName, "Lib", "site-packages"));
//                if (!libFiles.exists()) {
//                    System.out.println("[3]: Can not fund Lib path: " + libPathName + ", create file now >>>");
//                    libFiles.createNewFile();
//                    System.out.println("[4]: create file " + jarPath + " succeed >>>");
//                } else {
//                    System.out.println("[3]: Lib path is exists: " + libPathName + " >>>");
//                    System.out.println("[4]: Copy after>>>");
//                }
//                System.out.println("[5]: Start to copy site-packages from Lib: " + libFiles + " to :" + unzipPath + " >>>");
//                FileUtils.copyToDirectory(libFiles, new File(unzipPath));
//                System.out.println("[6]: Copy site-packages to " + unzipPath + ", successfully" );
//            } finally {
//                File temp1 = new File(libPathName);
//                if (temp1.exists()) {
//                    System.out.println("[*]: Start to clean temp files in" + libPathName);
//                    FileUtils.deleteQuietly(temp1);
//                    System.out.println("[*]: Temp file cleaned successfully" );
//                }
//            }
//        } catch (Throwable throwable) {
//            System.out.println("[x]: can not prepare Lib for site-packages, Please manually extract " + jarPath + " and place " + jarPath  + "/Lib/ in the " + (pyJarPath.endsWith(jarName) ? pyJarPath.replace(jarName, "") : pyJarPath ) + " folder." );
//            throwable.printStackTrace();
//            CommonUtils.logError(TAG, "Class modify failed", throwable);
//            return -1;
//        }
        return 0;
    }

    private String concat(String path, String ...paths){
        for (String s : paths) {
            path = FilenameUtils.concat(path, s);
        }
        return path;
    }
}
