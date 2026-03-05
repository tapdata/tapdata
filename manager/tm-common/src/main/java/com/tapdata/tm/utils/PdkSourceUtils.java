package com.tapdata.tm.utils;

import io.tapdata.pdk.core.utils.CommonUtils;
import org.apache.commons.io.FileUtils;
import org.springframework.web.multipart.MultipartFile;
import java.io.*;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.util.function.BiConsumer;

public class PdkSourceUtils {
    private static final String TAG = PdkSourceUtils.class.getSimpleName();
    private static final int BUFFER_SIZE = 1024 * 1024;

    public static String getFileMD5(MultipartFile originFile){
        return calcFileMD5(originFile);
    }

    public static String getFileMD5(File originFile){
        return calcFileMD5(originFile);
    }

    protected static String calcFileMD5(Object originFile){
        if (originFile instanceof MultipartFile){
            try (InputStream inputStream = ((MultipartFile) originFile).getInputStream()){
                return calculateStreamMD5(inputStream);
            } catch (IOException e) {
                CommonUtils.logError(TAG,"get md5 failed",e);
                return null;
            }
        }else if (originFile instanceof File){
            return calculateFileMD5((File) originFile);
        }else {
            return null;
        }
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
        return String.format("%032x", bigInt);
    }
    protected static String calculateStreamMD5(InputStream inputStream){
        MessageDigest digest = null;
        byte buffer[] = new byte[BUFFER_SIZE];
        int len;
        try {
            digest = MessageDigest.getInstance("MD5");
            while(-1 != (len = inputStream.read(buffer,0,1024))){
                digest.update(buffer,0,len);
            }
        }catch(Exception e){
            CommonUtils.logError(TAG,"get md5 failed",e);
            return null;
        }
        BigInteger bigInt = new BigInteger(1, digest.digest());
        return String.format("%032x", bigInt);
    }
    protected static void transformToFile(MultipartFile originFile, BiConsumer consumer) throws IOException {
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
