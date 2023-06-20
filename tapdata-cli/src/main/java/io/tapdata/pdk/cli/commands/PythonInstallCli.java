package io.tapdata.pdk.cli.commands;

import io.tapdata.entity.logger.TapLogger;
import io.tapdata.pdk.cli.CommonCli;
import io.tapdata.pdk.cli.utils.ZipUtils;
import io.tapdata.pdk.core.connector.TapConnectorManager;
import io.tapdata.pdk.core.utils.CommonUtils;
import io.tapdata.supervisor.convert.entity.ClassModifier;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.springframework.util.ClassUtils;
import picocli.CommandLine;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URI;
import java.util.Arrays;
import java.util.Optional;
import java.util.UUID;

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

    @CommandLine.Option(names = {"-s", "--self"}, description = "")
    private String selfJarPath ;

    @CommandLine.Option(names = {"-j", "--jarName"}, description = "")
    private String jarName ;

    @CommandLine.Option(names = {"-p", "--python"}, description = "")
    private String pyJarPath ;

    @CommandLine.Option(names = {"-g", "--packages"}, description = "")
    private String packagesPath ;

    @CommandLine.Option(names = {"-h", "--help"}, usageHelp = true, description = "Tapdata cli help")
    private boolean helpRequested = false;

    public Integer execute() throws Exception {
        if (null == selfJarPath) {
            TapLogger.error(TAG, "Miss script-engine-module.jar path");
            return -1;
        }
        if (null == pyJarPath) {
            TapLogger.error(TAG, "Miss jython-standalone.jar path");
            return -1;
        }
        if (null == packagesPath){
            TapLogger.error(TAG, "Miss package path");
            return -1;
        }
        File self = new File(selfJarPath);
        if (null == self || !self.exists() || !self.isFile()){
            TapLogger.error(TAG, "Miss script-engine-module.jar path");
            return -2;
        }

        final String jarPath = pyJarPath + jarName;
        final String libPath = pyJarPath + "Lib";
        File file = new File(jarPath);
        if (null == file || !file.exists() || !file.isFile()){
            TapLogger.error(TAG, "Miss jython-standalone.jar path: " + jarPath);
            return -2;
        }
        File path = new File(packagesPath);
        if (null == path || !path.isDirectory()) {
            TapLogger.error(TAG, "Miss package path: " + packagesPath);
            return -2;
        }

        System.out.println(selfJarPath);
        System.out.println(pyJarPath);
        System.out.println(packagesPath);


        final String libJarName = FilenameUtils.concat(pyJarPath, "Lib.jar");
        final String libPathName = FilenameUtils.concat(pyJarPath, "temp_engine");
        try {
            //依次执行命令
            File[] files = path.listFiles();
            if (null != files && files.length > 0) {
                for (File packageItem : files) {
                    String absolutePath = packageItem.getAbsolutePath();
                    if (!packageItem.isDirectory()) {
                       String name = packageItem.getName();
                       if (null == name || "".equals(name.trim())) continue;
                        String[] split = name.split("\\.");
                        if (split.length <= 0 ) continue;
                        try {
                           ZipUtils.unzip(packageItem.getAbsolutePath(), path.getAbsolutePath()+ "/" + split[0]);
                           if (packageItem.exists()) {
                               FileUtils.deleteQuietly(packageItem);
                           }
                       } catch (Exception e) {
                           TapLogger.warn(TAG, "Can not unzip file: {}", packageItem.getAbsolutePath());
                           continue;
                       }
                    }
                    String setUpPyPath = packageItem.getAbsolutePath() + (absolutePath.endsWith("/") ? "" : "/") + "setup.py";
                    File setup = null;
                    try {
                        setup = new File(setUpPyPath);
                    }catch (Exception e){
                        TapLogger.error(TAG, "Can not find file {}, error: {}", setUpPyPath, e.getMessage());
                    }

                    if (null == setup || !setup.exists() || !setup.isFile()) {
                        continue;
                    }
                    try {
                        // java -jar /usr/local/lib/jython-standalone-2.7.2.jar setup.py install
                        cmdRunJar(String.format(
                                "java -jar %s %s install"
                                , jarPath
                                , setUpPyPath
                        ));
                    }catch (Exception e){
                        TapLogger.warn(TAG, "Can't import {}, error: {}", setUpPyPath, e.getMessage());
                    }
                }

                try {
                    File libFile = new File(libPath);
                    if (null != libFile && libFile.isDirectory()) {
                        //将Lib文件夹和script-engine-module压缩到一个Jar包
                        //解压到
                        try {
                            ZipUtils.unzip(self.getAbsolutePath(), libPathName);
                        }catch (Exception e){
                            TapLogger.error(TAG, "Can not zip from " + self.getAbsolutePath() + " to " + libPathName);
                            return -3;
                        }

                        final String pipPath = FilenameUtils.concat(libPathName, path.getName());
                        File pipFile = new File(pipPath);
                        if (null != pipFile && pipFile.exists()) {
                            FileUtils.deleteQuietly(pipFile);
                        }

                        File libFileItem = new File(libJarName);
                        try(OutputStream fos = new FileOutputStream(libFileItem)) {
                            ZipUtils.zip(libFile, fos);
                            System.out.println("zip jar successfully. " + libJarName);
                        } catch (Exception e) {
                            System.out.println("zip " + libJarName + " jar failed. msg: " + e.getMessage());
                        }

                        //写入Lib到压缩的文件夹中
                        FileUtils.copyToDirectory(new File(libJarName), new File(libPathName));

                        //压缩
                        File atomicFile = new File(self.getAbsolutePath());
                        try (OutputStream fos = new FileOutputStream(atomicFile)) {
                            ZipUtils.zip(libPathName, fos);
                            System.out.println("zip jar successfully. " + self.getAbsolutePath());
                        } catch (Exception e) {
                            System.out.println("zip " + self.getAbsolutePath() + " jar failed, msg: " + e.getMessage());
                        }
                    }
                } catch (Exception e){
                    TapLogger.error(TAG, "Failed to compress Lib folder and script-engine-module into a Jar package, msg: " + e.getMessage());
                } finally {
                    File temp = new File(libJarName);
                    if (temp.exists())
                        FileUtils.deleteQuietly(temp);
                    File temp1 = new File(libPathName);
                    if (temp1.exists())
                        FileUtils.deleteQuietly(temp1);
                }

            } else {
                TapLogger.warn(TAG, "Not fund any import package file");
            }
        } catch (Throwable throwable) {
            throwable.printStackTrace();
            CommonUtils.logError(TAG, "Class modify failed", throwable);
        }
        return 0;
    }

    public static void cmdRunJar(String cmd) throws Exception {
        if (null == cmd || "".equals(cmd.trim())) return;
        BufferedReader br = null;
        try {
            Process process = new ProcessBuilder(cmd).start();//Runtime.getRuntime().exec(cmd);
            InputStream is = process.getInputStream();
            br = new BufferedReader(new InputStreamReader(is,"GBK"));
            StringBuffer sb = new StringBuffer();
            String content = br.readLine();
            if (null != content && "".equals(content.trim())) {
                sb.append(br.readLine());
            }
            while ((content = br.readLine()) != null) {
                if ("".equals(content.trim())) {
                    sb.append(content);
                }
            }
            br.close();
            br = null;
            TapLogger.info(TAG, sb.toString());
            //log.info(content);
        } finally {
            if(null!=br) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void cmdTerminateJar(int port) {
        String os = System.getProperties().getProperty("os.name");
        String path = ClassUtils.getDefaultClassLoader().getResource("").getPath();
        String cmd = null;
        if(os.contains("win")) {
            cmd = path+"stopJar.bat "+ port;
        } else {
            cmd = path + "" + port;
        }
        BufferedReader br = null;
        try {
            Process process = Runtime.getRuntime().exec(cmd);
            InputStream is = process.getInputStream();
            br = new BufferedReader(new InputStreamReader(is,"GBK"));
            StringBuffer sb = new StringBuffer();
            sb.append(br.readLine());
            String content;
            while ((content = br.readLine()) != null) {
                content = br.readLine();
                sb.append(content);
            }
            br.close();
            br = null;
            //log.info(content);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if(null!=br) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
