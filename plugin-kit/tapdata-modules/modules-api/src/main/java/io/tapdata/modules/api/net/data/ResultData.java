package io.tapdata.modules.api.net.data;

import io.tapdata.modules.api.net.message.TapEntity;

@Deprecated
public class ResultData {
    public static final byte CONTENT_ENCODE_JSON = 1;
    static final byte CONTENT_ENCODE_JSON_GZIP = 2;

    public static final int CODE_SUCCESS = 1;
    private String forId;
    public ResultData forId(String forId) {
        this.forId = forId;
        return this;
    }
    private Integer code;
    public ResultData code(Integer code) {
        this.code = code;
        return this;
    }
    private TapEntity message;
    public ResultData message(TapEntity message) {
        this.message = message;
        return this;
    }

    private String contentType;
    public ResultData contentType(String contentType) {
        this.contentType = contentType;
        return this;
    }
    private Byte contentEncode;
    public ResultData contentEncode(Byte contentEncode) {
        this.contentEncode = contentEncode;
        return this;
    }

    public ResultData() {
    }

    public ResultData(Integer code) {
        this.code = code;
    }

    public ResultData(Integer code, String forId) {
        this.forId = forId;
        this.code = code;
    }

    public Integer getCode() {
        return code;
    }

    public void setCode(Integer code) {
        this.code = code;
    }

    public Byte getContentEncode() {
        return contentEncode;
    }

    public void setContentEncode(Byte contentEncode) {
        this.contentEncode = contentEncode;
    }

    public String getForId() {
        return forId;
    }

    public void setForId(String forId) {
        this.forId = forId;
    }

    public TapEntity getMessage() {
        return message;
    }

    public void setMessage(TapEntity message) {
        this.message = message;
    }

    public String getContentType() {
        return contentType;
    }

    public void setContentType(String contentType) {
        this.contentType = contentType;
    }
}
