package io.tapdata.netty.channels.data;

import io.tapdata.entity.error.CoreException;
import io.tapdata.netty.channels.error.WSErrors;

public class IncomingData extends Data{
    public final static byte TYPE = 10;
    private String id;
    private String contentType;
    private Integer contentEncode;
    private String content;

    public IncomingData() {
        super(TYPE);
    }

    public IncomingData(byte[] data, Byte encode) {
        this();

        setData(data);
        setEncode(encode);
        resurrect();
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
            encode = BinaryCodec.ENCODE_PB;//throw new CoreException(CoreErrorCodes.ERROR_RPC_ENCODER_NULL, "Encoder is null for persistent")
        switch(encode) {
            case BinaryCodec.ENCODE_PB:
                break;
            default:
                throw new CoreException(WSErrors.ERROR_ENCODER_NOT_FOUND, "Encoder type doesn't be found for persistent");
        }
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public String getContentType() {
        return contentType;
    }

    public void setContentType(String contentType) {
        this.contentType = contentType;
    }

    @Override
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Integer getContentEncode() {
        return contentEncode;
    }

    public void setContentEncode(Integer contentEncode) {
        this.contentEncode = contentEncode;
    }

}