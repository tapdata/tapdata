package io.tapdata.wsserver.channels.data;

import io.tapdata.entity.error.CoreException;
import io.tapdata.wsserver.channels.error.WSErrors;

public class Result extends Data {
    static final byte TYPE = 100;

    public static Result create() {
        return new Result();
    }

    private Integer code;
    public Result code(Integer code) {
        this.code = code;
        return this;
    }
    private String description;
    public Result description(String description) {
        this.description = description;
        return this;
    }
    private String forId;
    public Result forId(String forId) {
        this.forId = forId;
        return this;
    }
    private String serverId;
    public Result serverId(String serverId) {
        this.serverId = serverId;
        return this;
    }
    private Long time;
    public Result time(Long time) {
        this.time = time;
        return this;
    }
    private Integer contentEncode;
    public Result contentEncode(Integer contentEncode) {
        this.contentEncode = contentEncode;
        return this;
    }
    private String content;
    public Result content(String content) {
        this.content = content;
        return this;
    }
    private byte[] binaryContent;
    public Result binaryContent(byte[] binaryContent) {
        this.binaryContent = binaryContent;
        return this;
    }

    public Result(){
        super(TYPE);
        encode = BinaryCodec.ENCODE_PB;
    }

    /**
     * @param code the code to set
     */
    public void setCode(Integer code) {
        this.code = code;
    }
    /**
     * @return the code
     */
    public Integer getCode() {
        return code;
    }
    /**
     * @param description the description to set
     */
    public void setDescription(String description) {
        this.description = description;
    }
    /**
     * @return the description
     */
    public String getDescription() {
        return description;
    }

    public String getForId() {
        return forId;
    }

    public void setForId(String forId) {
        this.forId = forId;
    }

    @Override
    public void resurrect() throws CoreException {
        byte[] bytes = getData();
        Byte encode = getEncode();
        if(bytes != null) {
            if(encode != null) {
                switch(encode) {
                    case BinaryCodec.ENCODE_PB:

                        break;
                    default:
                        throw new CoreException(WSErrors.ERROR_ENCODER_NOT_FOUND, "Encoder type doesn't be found for resurrect");
                }
            }
        }
    }

    @Override
    public void persistent() throws CoreException {
        Byte encode = getEncode();
        if(encode == null)
            encode = BinaryCodec.ENCODE_PB;
//			throw new CoreException(CoreErrorCodes.ERROR_RPC_ENCODER_NULL, "Encoder is null for persistent")
        switch(encode) {
            case BinaryCodec.ENCODE_PB:

                break;
            default:
                throw new CoreException(WSErrors.ERROR_ENCODER_NOT_FOUND, "Encoder type doesn't be found for persistent");
        }
    }

    public String getServerId() {
        return serverId;
    }

    public void setServerId(String serverId) {
        this.serverId = serverId;
    }

    public Long getTime() {
        return time;
    }

    public void setTime(Long time) {
        this.time = time;
    }

    public Integer getContentEncode() {
        return contentEncode;
    }

    public void setContentEncode(Integer contentEncode) {
        this.contentEncode = contentEncode;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public byte[] getBinaryContent() {
        return binaryContent;
    }

    public void setBinaryContent(byte[] binaryContent) {
        this.binaryContent = binaryContent;
    }
}