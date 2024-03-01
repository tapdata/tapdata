package com.tapdata.tm.utils;

import io.tapdata.pdk.core.utils.CommonUtils;

import java.io.File;
import java.io.FileInputStream;
import java.math.BigInteger;
import java.security.MessageDigest;

public class PdkSourceUtils {
    private static final String TAG = PdkSourceUtils.class.getSimpleName();
    public static String getFileMD5(File file){
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
}
