package io.tapdata.pdk.cli.commands;

import io.tapdata.entity.logger.TapLogger;
import io.tapdata.pdk.cli.CommonCli;
import io.tapdata.pdk.cli.utils.ZipUtils;
import io.tapdata.pdk.core.utils.CommonUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import picocli.CommandLine;

import java.io.File;

/**
 * @author GavinXiao
 * @description PythonInstallCli create by Gavin
 * @create 2023/6/20 16:33
 **/
@CommandLine.Command(
        description = "Insert code to PDK jar class file",
        subcommands = MainCli.class
)
public class PythonInstallCli extends CommonCli {
    private static final String TAG = PythonInstallCli.class.getSimpleName();
    @CommandLine.Parameters(paramLabel = "FILE", description = "One or more pdk jar files")
    File[] files;

    //@CommandLine.Option(names = {"-s", "--self"}, description = "")
    //private String selfJarPath ;

    @CommandLine.Option(names = {"-j", "--jarName"}, description = "")
    private String jarName ;

    @CommandLine.Option(names = {"-p", "--python"}, description = "")
    private String pyJarPath ;

    //@CommandLine.Option(names = {"-g", "--packages"}, description = "")
    //private String packagesPath = "pip-install";

    @CommandLine.Option(names = {"-h", "--help"}, usageHelp = true, description = "Tapdata cli help")
    private boolean helpRequested = false;
    //    public Integer execute0() throws Exception {
    //        if (null == selfJarPath) {
    //            TapLogger.error(TAG, "Miss tapdata-cli-1.0-SNAPSHOT.jar path");
    //            return -1;
    //        }
    //        if (null == pyJarPath) {
    //            TapLogger.error(TAG, "Miss jython-standalone-2.7.2.jar path");
    //            return -1;
    //        }
    //        if (null == packagesPath){
    //            TapLogger.error(TAG, "Miss package path");
    //            return -1;
    //        }
    //        File self = new File(selfJarPath);
    //        if (null == self || !self.exists() || !self.isFile()){
    //            TapLogger.error(TAG, "Miss script-engine-module.jar path");
    //            return -2;
    //        }
    //
    //        final String jarPath = pyJarPath.endsWith(jarName) ? pyJarPath : pyJarPath + jarName;
    //        final String libPath = (pyJarPath.endsWith(jarName) ? pyJarPath.replace(jarName, "") : pyJarPath ) + "Lib";
    //        File file = new File(jarPath);
    //        if (null == file || !file.exists() || !file.isFile()){
    //            TapLogger.error(TAG, "Miss jython-standalone.jar path: " + jarPath);
    //            return -2;
    //        }
    //        File path = new File(packagesPath);
    //        if (null == path || !path.isDirectory()) {
    //            TapLogger.error(TAG, "Miss package path: " + packagesPath);
    //            return -2;
    //        }
    //
    //        System.out.println(selfJarPath);
    //        System.out.println(pyJarPath);
    //        System.out.println(packagesPath);
    //
    //
    //        final String libJarName = FilenameUtils.concat(pyJarPath, "Lib.jar");
    //        final String libPathName = FilenameUtils.concat(pyJarPath, "temp_engine");
    //
    //        String batPath = pyJarPath + "cmd_bat.bat";
    //        File bat = new File(batPath);
    //        try {
    //            //依次执行命令
    //            File[] files = path.listFiles();
    //            if (null != files && files.length > 0) {
    //                for (File packageItem : files) {
    //                    String absolutePath = packageItem.getAbsolutePath();
    //                    if (!packageItem.isDirectory()) {
    //                       String name = packageItem.getName();
    //                       if (null == name || "".equals(name.trim())) continue;
    //                        String[] split = name.split("\\.");
    //                        if (split.length <= 0 ) continue;
    //                        try {
    //                           ZipUtils.unzip(packageItem.getAbsolutePath(), path.getAbsolutePath()+ "\\" + split[0]);
    //                           if (packageItem.exists()) {
    //                               FileUtils.deleteQuietly(packageItem);
    //                           }
    //                       } catch (Exception e) {
    //                           TapLogger.warn(TAG, "Can not unzip file: {}", packageItem.getAbsolutePath());
    //                           continue;
    //                       }
    //                    }
    //                    String setUpPyPath = packageItem.getAbsolutePath() + (absolutePath.endsWith("\\") ? "" : "\\") + "setup.py";
    //                    File setup = null;
    //                    try {
    //                        setup = new File(setUpPyPath);
    //                    }catch (Exception e){
    //                        TapLogger.error(TAG, "Can not find file {}, error: {}", setUpPyPath, e.getMessage());
    //                    }
    ////C:\Users\Gavin'Xiao\.m2\repository\org\python\jython-standalone\2.7.2
    //                    if (null == setup || !setup.exists() || !setup.isFile()) {
    //                        continue;
    //                    }
    //
    //                    try {
    //                        try(Writer writer = new FileWriter(bat)) {
    //                            writer.write(
    ////                            "cd D: \n " +
    //                                "cd "+ packageItem.getAbsolutePath() + " \r\n "+
    //                                String.format(
    //                                    "java -jar %s %s install"
    //                                    , path(jarPath)
    //                                    , "setup.py")//path(setUpPyPath) )
    //                                );
    //                        } catch (IOException e) {
    //                            TapLogger.warn(TAG, "Can't create bat file {}, error: {}", batPath, e.getMessage());
    //                        }
    //                        // java -jar /usr/local/lib/jython-standalone-2.7.2.jar setup.py install
    //                        cmdRunJar(
    //                                new File(packageItem.getAbsolutePath()),
    //                            "cmd -c start " + path(batPath)
    ////                            "cd "+ packageItem.getAbsolutePath() + " & "+
    ////                            String.format(
    ////                                "java -jar %s %s install"
    ////                                , path(jarPath)
    ////                                , path(setUpPyPath) )
    //                        );
    //                    }catch (Exception e){
    //                        TapLogger.warn(TAG, "Can't import {}, error: {}", setUpPyPath, e.getMessage());
    //                    }
    //                }
    //
    //                try {
    //                    File libFile = new File(libPath);
    //                    if (null != libFile && libFile.isDirectory()) {
    //                        //将Lib文件夹和script-engine-module压缩到一个Jar包
    //                        //解压到
    //                        try {
    //                            ZipUtils.unzip(self.getAbsolutePath(), libPathName);
    //                        }catch (Exception e){
    //                            TapLogger.error(TAG, "Can not zip from " + self.getAbsolutePath() + " to " + libPathName);
    //                            return -3;
    //                        }
    //
    //                        final String pipPath = FilenameUtils.concat(libPathName, path.getName());
    //                        File pipFile = new File(pipPath);
    //                        if (null != pipFile && pipFile.exists()) {
    //                            FileUtils.deleteQuietly(pipFile);
    //                        }
    //
    //                        File libFileItem = new File(libJarName);
    //                        try(OutputStream fos = new FileOutputStream(libFileItem)) {
    //                            ZipUtils.zip(libFile, fos);
    //                            System.out.println("zip jar successfully. " + libJarName);
    //                        } catch (Exception e) {
    //                            System.out.println("zip " + libJarName + " jar failed. msg: " + e.getMessage());
    //                        }
    //
    //                        //写入Lib到压缩的文件夹中
    //                        FileUtils.copyToDirectory(new File(libJarName), new File(libPathName));
    //
    //                        //压缩
    //                        File atomicFile = new File(self.getAbsolutePath());
    //                        try (OutputStream fos = new FileOutputStream(atomicFile)) {
    //                            ZipUtils.zip(libPathName, fos);
    //                            System.out.println("zip jar successfully. " + self.getAbsolutePath());
    //                        } catch (Exception e) {
    //                            System.out.println("zip " + self.getAbsolutePath() + " jar failed, msg: " + e.getMessage());
    //                        }
    //                    }
    //                } catch (Exception e){
    //                    TapLogger.error(TAG, "Failed to compress Lib folder and script-engine-module into a Jar package, msg: " + e.getMessage());
    //                } finally {
    //                    File temp = new File(libJarName);
    //                    if (temp.exists())
    //                        FileUtils.deleteQuietly(temp);
    //                    File temp1 = new File(libPathName);
    //                    if (temp1.exists())
    //                        FileUtils.deleteQuietly(temp1);
    //                    if (bat.exists())
    //                        FileUtils.deleteQuietly(bat);
    //                }
    //
    //            } else {
    //                TapLogger.warn(TAG, "Not fund any import package file");
    //            }
    //        } catch (Throwable throwable) {
    //            throwable.printStackTrace();
    //            CommonUtils.logError(TAG, "Class modify failed", throwable);
    //        }
    //        return 0;
    //    }
    //1. 执行命令java -jar 'path' install setup.py，生成class目录(会自动将class目录写到 ~/${mavenLocalPath}/Lib中)
    //2. 解压jython-standalone-2.7.2.jar -> jython-standalone-2.7.2-item/
    //3. 将class目录写入到jython-standalone-2.7.2-item/Lib/
    //4. 压缩jython-standalone-2.7.2-item/ -> jython-standalone-2.7.2.jar
    //public Integer execute1() throws Exception {
    //    if (null == selfJarPath) {
    //        TapLogger.error(TAG, "Miss tapdata-cli-1.0-SNAPSHOT.jar path");
    //        return -1;
    //    }
    //    if (null == pyJarPath) {
    //        TapLogger.error(TAG, "Miss jython-standalone-2.7.2.jar path");
    //        return -1;
    //    }
    //    if (null == packagesPath){
    //        TapLogger.error(TAG, "Miss package path");
    //        return -1;
    //    }
    //    File self = new File(selfJarPath);
    //    if (null == self || !self.exists() || !self.isFile()){
    //        TapLogger.error(TAG, "Miss script-engine-module.jar path");
    //        return -2;
    //    }
    //    final String jarPath = pyJarPath.endsWith(jarName) ? pyJarPath : pyJarPath + jarName;
    //    final String libPath = (pyJarPath.endsWith(jarName) ? pyJarPath.replace(jarName, "") : pyJarPath ) + "Lib";
    //    File file = new File(jarPath);
    //    if (null == file || !file.exists() || !file.isFile()){
    //        TapLogger.error(TAG, "Miss jython-standalone.jar path: " + jarPath);
    //        return -2;
    //    }
    //    File path = new File(packagesPath);
    //    if (null == path || !path.isDirectory()) {
    //        TapLogger.error(TAG, "Miss package path: " + packagesPath);
    //        return -2;
    //    }
    //    final String libJarName = FilenameUtils.concat(pyJarPath, "Lib.jar");
    //    final String libPathName = FilenameUtils.concat(pyJarPath, "temp_engine");
    //    String batPath = pyJarPath + "cmd_bat.bat";
    //    File bat = new File(batPath);
    //    try {
    //        //依次执行命令
    //        File[] files = path.listFiles();
    //        if (null != files && files.length > 0) {
    //            for (File packageItem : files) {
    //                String absolutePath = packageItem.getAbsolutePath();
    //                if (!packageItem.isDirectory()) {
    //                    String name = packageItem.getName();
    //                    if (null == name || "".equals(name.trim())) continue;
    //                    String[] split = name.split("\\.");
    //                    if (split.length <= 0 ) continue;
    //                    try {
    //                        ZipUtils.unzip(packageItem.getAbsolutePath(), path.getAbsolutePath()+ "\\" + split[0]);
    //                        if (packageItem.exists()) {
    //                            FileUtils.deleteQuietly(packageItem);
    //                        }
    //                    } catch (Exception e) {
    //                        TapLogger.warn(TAG, "Can not unzip file: {}", packageItem.getAbsolutePath());
    //                        continue;
    //                    }
    //                }
    //                String setUpPyPath = packageItem.getAbsolutePath() + (absolutePath.endsWith("\\") ? "" : "\\") + "setup.py";
    //                File setup = null;
    //                try {
    //                    setup = new File(setUpPyPath);
    //                }catch (Exception e){
    //                    TapLogger.error(TAG, "Can not find file {}, error: {}", setUpPyPath, e.getMessage());
    //                }
    //                //C:\Users\Gavin'Xiao\.m2\repository\org\python\jython-standalone\2.7.2
    //                if (null == setup || !setup.exists() || !setup.isFile()) {
    //                    continue;
    //                }
    //                try {
    //                    try(Writer writer = new FileWriter(bat)) {
    //                        writer.write(
    //                            "cd "+ packageItem.getAbsolutePath() + " \r\n"+
    //                                String.format(
    //                                    "java -jar %s %s install"
    //                                    , path(jarPath)
    //                                    , packageItem.getAbsolutePath() + "/setup.py")
    //                        );
    //                    } catch (IOException e) {
    //                        TapLogger.warn(TAG, "Can't create bat file {}, error: {}", batPath, e.getMessage());
    //                    }
    //                    // java -jar /usr/local/lib/jython-standalone-2.7.2.jar setup.py install
    //                    cmdRunJar(
    //                            new File(packageItem.getAbsolutePath()),
    //                            String.format("java -jar %s %s install"
    //                            , path(jarPath)
    //                            , "setup.py")
    //                          "cmd -c start " + path(batPath)
    //                        //"cd "+ packageItem.getAbsolutePath() + " & "+
    //                        //String.format(
    //                        //    "java -jar %s %s install"
    //                        //    , path(jarPath)
    //                        //    , path(setUpPyPath) )
    //                    );
    //                } catch (Exception e){
    //                    TapLogger.warn(TAG, "Can't import {}, error: {}", setUpPyPath, e.getMessage());
    //                }
    //            }
    //            File libFiles = new File(libPath);
    //            try {
    //                try {
    //                    ZipUtils.unzip(file.getAbsolutePath(), libPathName);
    //                }catch (Exception e){
    //                    TapLogger.error(TAG, "Can not zip from " + self.getAbsolutePath() + " to " + libPathName);
    //                    return -3;
    //                }
    //                FileUtils.copyToDirectory(libFiles, new File(libPathName + "\\Lib\\"));
    //                //压缩
    //                if (file.exists())
    //                    FileUtils.deleteQuietly(file);
    //                File atomicFile = new File(file.getAbsolutePath());
    //                try (OutputStream fos = new FileOutputStream(atomicFile)) {
    //                    ZipUtils.zip(libPathName, fos);
    //                    System.out.println("zip jar successfully. " + self.getAbsolutePath());
    //                } catch (Exception e) {
    //                    System.out.println("zip " + self.getAbsolutePath() + " jar failed, msg: " + e.getMessage());
    //                }
    //            } finally {
    //                File temp = new File(libJarName);
    //                if (temp.exists())
    //                    FileUtils.deleteQuietly(temp);
    //                File temp1 = new File(libPathName);
    //                if (temp1.exists())
    //                    FileUtils.deleteQuietly(temp1);
    //                if (bat.exists())
    //                    FileUtils.deleteQuietly(bat);
    //            }
    //        } else {
    //            TapLogger.warn(TAG, "Not fund any import package file");
    //        }
    //    } catch (Throwable throwable) {
    //        throwable.printStackTrace();
    //        CommonUtils.logError(TAG, "Class modify failed", throwable);
    //    }
    //    return 0;
    //}
    public Integer execute() throws Exception {
        if (null == pyJarPath) {
            TapLogger.error(TAG, "Miss jython-standalone-2.7.2.jar path");
            return -1;
        }
        System.out.println(jarName);
        System.out.println(pyJarPath);
        final String jarPath = pyJarPath.endsWith(jarName) ? pyJarPath : (pyJarPath + jarName);
        final String libPath = pyJarPath.endsWith(jarName) ? pyJarPath.replace(jarName, "") : pyJarPath;
        File file = new File(jarPath);
        if (!file.exists() || !file.isFile()){
            TapLogger.error(TAG, "Miss jython-standalone.jar path: " + jarPath);
            return -2;
        }
        final String libPathName = FilenameUtils.concat(pyJarPath, "temp_engine");
        try {
            try {
                try {
                    System.out.println("[1]: Start to unzip " + jarPath + ">>>");
                    ZipUtils.unzip(file.getAbsolutePath(), libPathName);
                    System.out.println("[2]: Unzip " + jarPath + " succeed >>>");
                }catch (Exception e){
                    //TapLogger.error(TAG, "Can not zip from " + jarPath + " to " + libPathName);
                    System.out.println("[2]: Unzip " + jarPath + " fail.");
                    return -3;
                }
                File libFiles = new File(libPathName + "\\Lib\\site-packages\\");
                if (!libFiles.exists()) {
                    System.out.println("[3]: Can not fund Lib path: " + libPathName + ", create file now >>>");
                    libFiles.createNewFile();
                    System.out.println("[4]: create file " + jarPath + " succeed >>>");
                } else {
                    System.out.println("[3]: Lib path is exists: " + libPathName + " >>>");
                    System.out.println("[4]: Copy after>>>");
                }
                System.out.println("[5]: Start to copy site-packages from Lib: " + libFiles + " to :" + libPath + " >>>");
                FileUtils.copyToDirectory(libFiles, new File(libPath + "\\Lib"));
                System.out.println("[6]: Copy site-packages successfully" );
            } finally {
                File temp1 = new File(libPathName);
                if (temp1.exists()) {
                    System.out.println("[*]: Start to clean temp files in" + libPathName);
                    FileUtils.deleteQuietly(temp1);
                    System.out.println("[*]: Temp file cleaned successfully" );
                }
            }
        } catch (Throwable throwable) {
            System.out.println("[x]: can not prepare Lib for site-packages, Please manually extract " + jarPath + " and place " + jarPath  + "/Lib/ in the " + (pyJarPath.endsWith(jarName) ? pyJarPath.replace(jarName, "") : pyJarPath ) + " folder." );
            throwable.printStackTrace();
            CommonUtils.logError(TAG, "Class modify failed", throwable);
            return -1;
        }
        return 0;
    }
}
