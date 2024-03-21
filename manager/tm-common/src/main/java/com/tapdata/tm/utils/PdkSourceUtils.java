package com.tapdata.tm.utils;

import io.tapdata.pdk.core.utils.CommonUtils;
import org.apache.commons.io.FileUtils;
import org.springframework.web.multipart.commons.CommonsMultipartFile;
import java.io.*;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

public class PdkSourceUtils {
    private static final String TAG = PdkSourceUtils.class.getSimpleName();
    public static String getFileMD5(CommonsMultipartFile originFile){
        return calcFileMD5(originFile);
    }

    public static String getFileMD5(File originFile){
        return calcFileMD5(originFile);
    }

    protected static String calcFileMD5(Object originFile){
        AtomicReference<File> file;
        AtomicBoolean needDeleteFile = new AtomicBoolean(false);
        if (originFile instanceof CommonsMultipartFile){
            file = new AtomicReference<>();
            try {
                transformToFile((CommonsMultipartFile) originFile, (k, v)->{
                    file.set((File) k);
                    needDeleteFile.set((Boolean) v);
                });
            } catch (IOException e) {
                CommonUtils.logError(TAG,"get md5 failed",e);
                return null;
            }
        }else if (originFile instanceof File){
            file = new AtomicReference<>();
            file.set((File) originFile);
        }else {
            file = null;
        }
        if(null == file || null == file.get()){
            return null;
        }
        String md5 = calculateFileMD5(file.get());
        if (needDeleteFile.get()){
            FileUtils.deleteQuietly(file.get());
        }
        return md5;
    }
    protected static String calculateFileMD5(File file){
        if(!file.isFile()){
            return null;
        }
        MessageDigest digest = null;
        byte buffer[] = new byte[1024];
        int len;
        try (FileInputStream in = new FileInputStream(file)){
            digest = MessageDigest.getInstance("MD5");
            while(-1 != (len = in.read(buffer,0,1024))){
                digest.update(buffer,0,len);
            }
        }catch(Exception e){
            CommonUtils.logError(TAG,"get md5 failed",e);
            return null;
        }
        BigInteger bigInt = new BigInteger(1, digest.digest());
        return bigInt.toString(16);
    }
    protected static void transformToFile(CommonsMultipartFile originFile, BiConsumer consumer) throws IOException {
        File file;
        Boolean needDeleteFile = false;
        file = new File(originFile.getName());
        try (InputStream ins = originFile.getInputStream()){
            if (!file.exists()){
                inputStreamToFile(ins,file);
                needDeleteFile = true;
            }
        }
        consumer.accept(file, needDeleteFile);
    }
    protected static void inputStreamToFile(InputStream ins, File file) throws IOException{
        try (FileOutputStream os = new FileOutputStream(file)){
            int bytesRead = 0;
            byte[] buffer = new byte[1024];
            while ((bytesRead = ins.read(buffer)) != -1) {
                os.write(buffer, 0, bytesRead);
            }
        }
    }
}
