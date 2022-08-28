package io.tapdata.wsclient.modules.imclient.impls.data;

import java.io.IOException;
import java.util.Arrays;

public abstract class Data {
    public static final int CODE_SUCCESS = 1;
    public static final int CODE_FAILED = 0;


    protected byte type;
    public static final byte ENCODE_PB = 1;
    public static final byte ENCODE_JSON = 10;
    public static final byte ENCODE_JAVABINARY = 20;
    protected Byte encode = ENCODE_PB;
    protected byte[] data;
    /**
     * content的版本
     */
    protected short encodeVersion;

    public Data(byte type) {
        this.type = type;
    }
    public abstract void resurrect() throws IOException;

    public abstract void persistent() throws IOException;

    public byte getType() {
        return type;
    }

    public void setType(byte type) {
        this.type = type;
    }

    public Byte getEncode() {
        return encode;
    }

    public void setEncode(Byte encode) {
        this.encode = encode;
    }

    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        this.data = data;
    }

    public short getEncodeVersion() {
        return encodeVersion;
    }

    public void setEncodeVersion(short encodeVersion) {
        this.encodeVersion = encodeVersion;
    }

    @Override
    public String toString() {
        return "Data{" +
                "type=" + type +
                ", encode=" + encode +
                ", data=" + Arrays.toString(data) +
                ", encodeVersion=" + encodeVersion +
                '}';
    }
}
