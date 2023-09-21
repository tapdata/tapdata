package io.tapdata.pdk.cli.utils;

import io.tapdata.entity.error.CoreException;
import io.tapdata.pdk.core.error.PDKRunnerErrorCodes;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.zip.ZipUtil;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;

import java.io.*;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;

public class ZipUtils {
    private static final int BUFFER_SIZE = 16 * 1024;

    public static void zip(String srcDir, OutputStream out) {
        zip(srcDir, out, true, true);
    }

    public static void zip(File srcDir, OutputStream out) {
        zip(srcDir, out, true, true);
    }


    public static void zip(String srcDir, OutputStream out, boolean keepDirStructure, boolean zipInsideDirectory)
            throws RuntimeException {

        if (srcDir == null || out == null)
            throw new CoreException(PDKRunnerErrorCodes.COMMON_ILLEGAL_PARAMETERS, "Zip missing srcDir or out");
        File srcDirFile = new File(srcDir);
        if (srcDirFile.isFile())
            throw new CoreException(PDKRunnerErrorCodes.CLI_ZIP_DIR_IS_FILE, "Zip director is a file, expect to be directory or none");
        zip(srcDirFile, out, keepDirStructure, zipInsideDirectory);
    }

    public static void zip(File srcDir, OutputStream out, boolean keepdirstructure, boolean zipInsideDirectory)
            throws RuntimeException {
        long start = System.currentTimeMillis();
        ZipOutputStream zos = null;
        try {
            zos = new ZipOutputStream(out);

            if(zipInsideDirectory && srcDir.isDirectory()) {
                File[] listFiles = srcDir.listFiles();
                for(File file : listFiles) {
                    compress(file, zos, file.getName(), keepdirstructure);
                }
            } else {
                compress(srcDir, zos, srcDir.getName(), keepdirstructure);
            }
            long end = System.currentTimeMillis();
        } catch (Exception e) {
            throw new RuntimeException("zip error from ZipUtils", e);
        } finally {
            if (zos != null) {
                try {
                    zos.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private static void compress(File sourceFile, ZipOutputStream zos, String name,
                                 boolean KeepDirStructure) throws Exception {
        byte[] buf = new byte[BUFFER_SIZE];
        if (sourceFile.isFile()) {
            // 向zip输出流中添加一个zip实体，构造器中name为zip实体的文件的名字
            zos.putNextEntry(new ZipEntry(name));
            // copy文件到zip输出流中
            int len;
            try (FileInputStream in = new FileInputStream(sourceFile)) {
                while ((len = in.read(buf)) != -1) {
                    zos.write(buf, 0, len);
                }
                // Complete the entry
                zos.closeEntry();
            }
        } else {
            File[] listFiles = sourceFile.listFiles();
            if (listFiles == null || listFiles.length == 0) {
                // 需要保留原来的文件结构时,需要对空文件夹进行处理
                if (KeepDirStructure) {
                    // 空文件夹的处理
                    zos.putNextEntry(new ZipEntry(name + "/"));
                    // 没有文件，不需要文件的copy
                    zos.closeEntry();
                }

            } else {
                for (File file : listFiles) {
                    // 判断是否需要保留原来的文件结构
                    if (KeepDirStructure) {
                        // 注意：file.getName()前面需要带上父文件夹的名字加一斜杠,
                        // 不然最后压缩包中就不能保留原来的文件结构,即：所有文件都跑到压缩包根目录下了
                        compress(file, zos, name + "/" + file.getName(), true);
                    } else {
                        compress(file, zos, file.getName(), false);
                    }

                }
            }
        }
    }

    public static void unzip(String zipFile, String outputPath) {
        if (zipFile == null || outputPath == null)
            throw new CoreException(PDKRunnerErrorCodes.COMMON_ILLEGAL_PARAMETERS, "Unzip missing zipFile or outputPath");
        File outputDir = new File(outputPath);
        if (outputDir.isFile())
            throw new CoreException(PDKRunnerErrorCodes.CLI_UNZIP_DIR_IS_FILE, "Unzip director is a file, expect to be directory or none");
        if (zipFile.endsWith(".tar.gz") || zipFile.endsWith(".gz")){
            unTarZip(zipFile, outputPath);
        } else {
            unzip(zipFile, outputDir);
        }
    }

    public static void unTarZip(String tarFilePath, String targetDirectoryPath){
        try (InputStream inputStream = new FileInputStream(tarFilePath)) {
            TarArchiveInputStream tarArchiveInputStream = new TarArchiveInputStream(inputStream);
            TarArchiveEntry entry;
            while ((entry = tarArchiveInputStream.getNextTarEntry()) != null) {
                File outputFile = new File(targetDirectoryPath, entry.getName());
                if (entry.isDirectory()) {
                    if (!outputFile.exists()) {
                        outputFile.mkdirs();
                    }
                    continue;
                }
                outputFile.getParentFile().mkdirs();
                try (OutputStream outputStream = new FileOutputStream(outputFile)) {
                    byte[] buffer = new byte[4096];
                    int len;
                    while ((len = tarArchiveInputStream.read(buffer)) != -1) {
                        outputStream.write(buffer, 0, len);
                    }
                }
            }
            tarArchiveInputStream.close();
        } catch (Exception e){
            throw new CoreException(PDKRunnerErrorCodes.CLI_UNZIP_DIR_IS_FILE, "Unzip director is a file, expect to be directory or none, " + e.getMessage());
        }
    }

    public static void unzip(String zipFile, File outputDir) {
        if (zipFile == null || outputDir == null)
            throw new CoreException(PDKRunnerErrorCodes.COMMON_ILLEGAL_PARAMETERS, "Unzip missing zipFile or outputPath");
        if (outputDir.isFile())
            throw new CoreException(PDKRunnerErrorCodes.CLI_UNZIP_DIR_IS_FILE, "Unzip director is a file, expect to be directory or none");

        try (ZipFile zf = new ZipFile(zipFile)) {

            if (!outputDir.exists())
                FileUtils.forceMkdir(outputDir);

            Enumeration<? extends ZipEntry> zipEntries = zf.entries();
            while (zipEntries.hasMoreElements()) {
                ZipEntry entry = zipEntries.nextElement();

                try {
                    if (entry.isDirectory()) {
                        String entryPath = FilenameUtils.concat(outputDir.getAbsolutePath(), entry.getName());
                        FileUtils.forceMkdir(new File(entryPath));
                    } else {
                        String entryPath = FilenameUtils.concat(outputDir.getAbsolutePath(), entry.getName());
                        try(OutputStream fos = FileUtils.openOutputStream(new File(entryPath))) {
                            IOUtils.copyLarge(zf.getInputStream(entry), fos);
                        }
                    }
                } catch (IOException ei) {
                    ei.printStackTrace();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception {
        /** 测试压缩方法1  */
//        FileOutputStream fos1 = new FileOutputStream(new File("c:/mytest01.zip"));
//        ZipUtils.toZip("D:/log", fos1, true);
//
//        /** 测试压缩方法2  */
//        List<File> fileList = new ArrayList<>();
//        fileList.add(new File("D:/Java/jdk1.7.0_45_64bit/bin/jar.exe"));
//        fileList.add(new File("D:/Java/jdk1.7.0_45_64bit/bin/java.exe"));
//        FileOutputStream fos2 = new FileOutputStream(new File("c:/mytest02.zip"));
//        ZipUtils.toZip(fileList, fos2);
//        unzip("/Users/aplomb/dev/tapdata/GithubProjects/idaas-pdk-new/dist/mysql-connector-v1.0-SNAPSHOT.jar", "/Users/aplomb/dev/tapdata/GithubProjects/idaas-pdk-new/dist/temp/mysql");
        zip("/Users/aplomb/dev/tapdata/GithubProjects/idaas-pdk-new/dist/temp/mysql", FileUtils.openOutputStream(new File("/Users/aplomb/dev/tapdata/GithubProjects/idaas-pdk-new/dist/temp/1.zip")));
    }
}
