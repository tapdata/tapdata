package io.tapdata.modules.api.utils;

import java.io.*;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/** 
 * GZIP工具 
 *  
 * @author <a href="mailto:zlex.dongliang@gmail.com">梁栋</a> 
 * @since 1.0 
 */  
public abstract class GZipUtils {  
  
    public static final int BUFFER = 1024;  
    public static final String EXT = ".gz";  
  
    /** 
     * 数据压缩 
     *  
     * @param data 
     * @return 
     * @throws Exception 
     */  
    public static byte[] compress(byte[] data) throws IOException {  
        try(ByteArrayInputStream bais = new ByteArrayInputStream(data);
            ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            // 压缩
            compress(bais, baos);

            return baos.toByteArray();
        }

    }  
  
    /** 
     * 文件压缩 
     *  
     * @param file 
     * @throws Exception 
     */  
    public static void compress(File file) throws IOException {  
        compress(file, true);  
    }  
  
    /** 
     * 文件压缩 
     *  
     * @param file 
     * @param delete 
     *            是否删除原始文件 
     * @throws Exception 
     */  
    public static void compress(File file, boolean delete) throws IOException {  
        try (FileInputStream fis = new FileInputStream(file);
        FileOutputStream fos = new FileOutputStream(file.getPath() + EXT)) {
            compress(fis, fos);

            fos.flush();
        }
        if (delete) {
            file.delete();
        }
    }
  
    /** 
     * 数据压缩 
     *  
     * @param is 
     * @param os 
     * @throws IOException 
     * @throws Exception 
     */  
    public static void compress(InputStream is, OutputStream os) throws IOException  {
        try(GZIPOutputStream gos = new GZIPOutputStream(os)) {
            int count;
            byte[] data = new byte[BUFFER];
            while ((count = is.read(data, 0, BUFFER)) != -1) {
                gos.write(data, 0, count);
            }
            gos.finish();
            gos.flush();
        }
    }
  
    /** 
     * 文件压缩 
     *  
     * @param path 
     * @throws Exception 
     */  
    public static void compress(String path) throws IOException {  
        compress(path, true);  
    }  
  
    /** 
     * 文件压缩 
     *  
     * @param path 
     * @param delete 
     *            是否删除原始文件 
     * @throws Exception 
     */  
    public static void compress(String path, boolean delete) throws IOException {  
        File file = new File(path);  
        compress(file, delete);  
    }  
  
    /** 
     * 数据解压缩 
     *  
     * @param data 
     * @return 
     * @throws IOException 
     * @throws Exception 
     */  
    public static byte[] decompress(byte[] data) throws IOException  {  
        try(ByteArrayInputStream bais = new ByteArrayInputStream(data);
              ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            // 解压缩
            decompress(bais, baos);
            data = baos.toByteArray();
            baos.flush();
        }

        return data;  
    }  
  
    /** 
     * 文件解压缩 
     *  
     * @param file 
     * @throws Exception 
     */  
    public static void decompress(File file) throws IOException {  
        decompress(file, true);  
    }  
  
    /** 
     * 文件解压缩 
     *  
     * @param file 
     * @param delete 
     *            是否删除原始文件 
     * @throws Exception 
     */  
    public static void decompress(File file, boolean delete) throws IOException {  
        try(FileInputStream fis = new FileInputStream(file);
            FileOutputStream fos = new FileOutputStream(file.getPath().replace(EXT,
                ""))) {
            decompress(fis, fos);
            fos.flush();

            if (delete) {
                file.delete();
            }
        }
    }
  
    /** 
     * 数据解压缩 
     *  
     * @param is 
     * @param os 
     * @throws IOException 
     * @throws Exception 
     */  
    public static void decompress(InputStream is, OutputStream os) throws IOException {
        try(GZIPInputStream gis = new GZIPInputStream(is)) {
            int count;
            byte[] data = new byte[BUFFER];
            while ((count = gis.read(data, 0, BUFFER)) != -1) {
                os.write(data, 0, count);
            }
        }
    }  
  
    /** 
     * 文件解压缩 
     *  
     * @param path 
     * @throws Exception 
     */  
    public static void decompress(String path) throws IOException {  
        decompress(path, true);  
    }  
  
    /** 
     * 文件解压缩 
     *  
     * @param path 
     * @param delete 
     *            是否删除原始文件 
     * @throws Exception 
     */  
    public static void decompress(String path, boolean delete) throws IOException {  
        File file = new File(path);  
        decompress(file, delete);  
    }  
  
}  