package io.tapdata.entity.mapping.type;

import java.util.Map;

import static io.tapdata.entity.utils.TypeUtils.objectToNumber;

public abstract class TapBytesBase extends TapMapping {
    public static final String KEY_FIXED = "fixed";
    public static final String KEY_BYTE = "byte";
    public static final String KEY_BYTE_RATIO = "byteRatio";
    public static final String KEY_BYTE_DEFAULT = "defaultByte";
    public static final String KEY_BYTE_PREFER = "preferByte";

    protected Boolean fixed;
    protected Long bytes;
    protected Long defaultBytes;
    protected Long preferBytes;
    /**
     * some database varchar(8), the 8 means byte or char.
     * if charset is utf8, a char may occupy 3 bytes.
     *
     * That's the reason we define the byteRatio, like above case, the database who using char, it byteRatio is 3.
     * byteRatio is 1 by default.
     */
    protected Integer byteRatio;

    protected Long getFromTapTypeBytes(Long comingBytes, Integer comingByteRatio) {
//        if(this.byteRatio == 1)
//            return bytes;
//        // 14 / 8 = 2, 14 / 7 = 2
//        return (bytes / this.byteRatio + ((bytes % this.byteRatio) > 0 ? 1 : 0));
        if(byteRatio == null && comingByteRatio != null) {
            return comingBytes * comingByteRatio;
        }
        return comingBytes;
    }

    protected Long getToTapTypeBytes(Map<String, String> params) {
        String byteStr = getParam(params, KEY_BYTE);
        Long bytes = null;
        if(byteStr != null) {
            bytes = objectToNumber(byteStr);//Long.parseLong(byteStr);
        }
        if(bytes == null)
            bytes = preferBytes;
        if(bytes == null)
            bytes = defaultBytes;
        if(bytes == null)
            bytes = this.bytes;
        return bytes;
    }

    @Override
    public void from(Map<String, Object> info) {
        Object fixedObj = info.get(KEY_FIXED);
        if(fixedObj instanceof Boolean) {
            fixed = (Boolean) fixedObj;
        }

        Object ratioObj = info.get(KEY_BYTE_RATIO);
        if(ratioObj instanceof Number) {
            byteRatio = ((Number) ratioObj).intValue();
        }

        Object byteDefaultObject = info.get(KEY_BYTE_DEFAULT);
        defaultBytes = objectToNumber(byteDefaultObject);

        Object bytePreferObject = info.get(KEY_BYTE_PREFER);
        preferBytes = objectToNumber(bytePreferObject);

        Object byteObj = info.get(KEY_BYTE);
        bytes = objectToNumber(byteObj);

        if(bytes == null) {
            bytes = Long.MAX_VALUE;
        }
    }

    public Boolean getFixed() {
        return fixed;
    }

    public void setFixed(Boolean fixed) {
        this.fixed = fixed;
    }

    public Long getBytes() {
        return bytes;
    }

    public void setBytes(Long bytes) {
        this.bytes = bytes;
    }

    public Long getDefaultBytes() {
        return defaultBytes;
    }

    public void setDefaultBytes(Long defaultBytes) {
        this.defaultBytes = defaultBytes;
    }

    public Integer getByteRatio() {
        return byteRatio;
    }

    public void setByteRatio(Integer byteRatio) {
        this.byteRatio = byteRatio;
    }

    public Long getPreferBytes() {
        return preferBytes;
    }

    public void setPreferBytes(Long preferBytes) {
        this.preferBytes = preferBytes;
    }
}
