package io.tapdata.wsserver.channels.data;

import io.tapdata.entity.error.CoreException;

public abstract class BinaryCodec {
    public static final byte ENCODE_PB = 1;
    public static final byte ENCODE_JSON = 10;
    public static final byte ENCODE_JAVABINARY = 20;

    protected Byte encode;
    protected byte[] data;
    /**
     * content的版本
     */
    protected short encodeVersion;
    public short getEncodeVersion() {
        return encodeVersion;
    }
    public void setEncodeVersion(short encodeVersion) {
        this.encodeVersion = encodeVersion;
    }

    public abstract void resurrect() throws CoreException;
    public abstract void persistent() throws CoreException;
    public byte[] getData() {
        return data;
    }
    public void setData(byte[] data) {
        this.data = data;
    }
    public Byte getEncode() {
        return encode;
    }
    public void setEncode(Byte encode) {
        this.encode = encode;
    }
}
