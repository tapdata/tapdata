package io.tapdata.js.connector.utli;

public class ObjectUtil {
    public static int hashCode(byte[] bytes){
        int hit = 0;
        for (byte b : bytes){
            hit = 31 * hit + (b & 0xff);
        }
        return hit;
    }
}
