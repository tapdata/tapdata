package io.tapdata.netty.channels.data;

public class ResultData {
    public static final int CONTENT_ENCODE_JSON = 1;
    static final int CONTENT_ENCODE_JSON_GZIP = 2;

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
    private String message;
    public ResultData message(String message) {
        this.message = message;
        return this;
    }
    private String data;
    public ResultData data(String data) {
        this.data = data;
        return this;
    }

    private Integer dataEncode = ResultData.CONTENT_ENCODE_JSON;
    public ResultData dataEncode(Integer dataEncode) {
        this.dataEncode = dataEncode;
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

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }

    public Integer getDataEncode() {
        return dataEncode;
    }

    public void setDataEncode(Integer dataEncode) {
        this.dataEncode = dataEncode;
    }

    public String getForId() {
        return forId;
    }

    public void setForId(String forId) {
        this.forId = forId;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}
