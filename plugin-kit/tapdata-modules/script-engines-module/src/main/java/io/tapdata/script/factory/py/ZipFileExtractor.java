package io.tapdata.script.factory.py;

import io.tapdata.entity.logger.Log;

import java.io.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class ZipFileExtractor {
    public static int unzip(String resourcesName, String unzipPath, Log logger) {
        // 获取 ZIP 文件的输入流
        InputStream inputStream = ZipFileExtractor.class.getClassLoader().getResourceAsStream(resourcesName);
        if (null == inputStream) {
            logger.warn("Can not get {} from class path", resourcesName);
            return -1;
        }
        // 创建解压目标目录
        File directory = new File(unzipPath);
        if (!directory.exists()) {
            directory.mkdirs();
        }
        
        try(ZipInputStream zipInputStream = new ZipInputStream(inputStream)) {
            ZipEntry entry = zipInputStream.getNextEntry();
            while (entry != null) {
                String filePath = unzipPath + File.separator + entry.getName();
                if (!entry.isDirectory()) {
                    // 如果是文件，则创建文件并写入数据
                    extractFile(zipInputStream, filePath);
                } else {
                    // 如果是目录，则创建目录
                    File dir = new File(filePath);
                    dir.mkdirs();
                }
                zipInputStream.closeEntry();
                entry = zipInputStream.getNextEntry();
            }
            inputStream.close();
            return 1;
        } catch (IOException e) {
           logger.warn(e.getMessage());
           return -1;
        }
    }
    
    private static void extractFile(ZipInputStream zipInputStream, String filePath) throws IOException {
        try (BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(filePath))) {
            byte[] bytesIn = new byte[4096];
            int read;
            while ((read = zipInputStream.read(bytesIn)) != -1) {
                bos.write(bytesIn, 0, read);
            }
        }
    }
}
