package io.tapdata.observable.metric.py;

import io.tapdata.entity.error.CoreException;
import io.tapdata.exception.TapCodeException;
import io.tapdata.observable.metric.py.error.PythonScriptProcessorExCode_31;
import io.tapdata.pdk.core.error.PDKRunnerErrorCodes;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Enumeration;
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
            throws TapCodeException {
        try(ZipOutputStream zos = new ZipOutputStream(out)) {
            if(zipInsideDirectory && srcDir.isDirectory()) {
                File[] listFiles = srcDir.listFiles();
                for(File file : listFiles) {
                    compress(file, zos, file.getName(), keepdirstructure);
                }
            } else {
                compress(srcDir, zos, srcDir.getName(), keepdirstructure);
            }
        } catch (Exception e) {
            throw new TapCodeException(PythonScriptProcessorExCode_31.PYTHON_SCRIPT_ZIP_FAILED,String.format("zip error from ZipUtils failed: %s", e.getMessage()));
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
        File sourceFile = new File(tarFilePath);
        try (TarArchiveInputStream fin = new TarArchiveInputStream(new GzipCompressorInputStream(new FileInputStream(sourceFile)))) {
            // decompressing *.tar.gz files to tar
            File extraceFolder = new File(targetDirectoryPath);
            TarArchiveEntry entry;
            // 将 tar 文件解压到 extractPath 目录下
            while ((entry = fin.getNextTarEntry()) != null) {
                if (entry.isDirectory()) {
                    continue;
                }
                File curfile = new File(extraceFolder, entry.getName());
                File parent = curfile.getParentFile();
                if (!parent.exists()) {
                    parent.mkdirs();
                }
                // 将文件写出到解压的目录
                try (FileOutputStream fileOutputStream = new FileOutputStream(curfile)){
                    IOUtils.copy(fin, fileOutputStream);
                }
            }
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
}
