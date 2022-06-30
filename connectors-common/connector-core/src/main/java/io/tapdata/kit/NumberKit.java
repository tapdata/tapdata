package io.tapdata.kit;

public class NumberKit {

    /**
     * convert bytes to long for debezium records
     *
     * @param bs byte array
     * @return long
     */
    public static long bytes2long(byte[] bs) {
        long l = 0;
        int size = bs.length;
        for (int i = 0; i < size; i++) {
            l |= (bs[i] & 0xffL) << (size - i - 1) * 8;
        }
        return l;
    }
}
