package io.tapdata.netty.channels.data;

import io.tapdata.entity.error.CoreException;
import io.tapdata.netty.channels.error.WSErrors;

public class IncomingRequest extends Data {
    public final static byte TYPE = 60;
    private String id;
    private String service;
    private String uri; // rest/message/{messageId}?playerId={playerId}
    private String method;
    private String bodyStr;
    private Integer bodyEncode;

    public IncomingRequest() {
        super(TYPE);
    }

    public IncomingRequest(byte[] data, Byte encode) {
        this();
        
        setData(data);
        setEncode(encode);
        resurrect();
    }
    
    @Override
    public void resurrect() throws CoreException {
        byte[] bytes = getData();
        Byte encode = getEncode();
        if (bytes != null) {
            if (encode != null) {
                switch (encode) {
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
        if (encode == null)
            encode = BinaryCodec.ENCODE_PB;
//throw new CoreException(CoreErrorCodes.ERROR_RPC_ENCODER_NULL, "Encoder is null for persistent")
        switch (encode) {
            case BinaryCodec.ENCODE_PB:

                break;
            default:
                throw new CoreException(WSErrors.ERROR_ENCODER_NOT_FOUND, "Encoder type doesn't be found for persistent");
        }
    }
    
    @Override
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getService() {
        return service;
    }

    public void setService(String service) {
        this.service = service;
    }

    public String getUri() {
        return uri;
    }

    public void setUri(String uri) {
        this.uri = uri;
    }

    public String getMethod() {
        return method;
    }

    public void setMethod(String method) {
        this.method = method;
    }

    public String getBodyStr() {
        return bodyStr;
    }

    public void setBodyStr(String bodyStr) {
        this.bodyStr = bodyStr;
    }

    public Integer getBodyEncode() {
        return bodyEncode;
    }

    public void setBodyEncode(Integer bodyEncode) {
        this.bodyEncode = bodyEncode;
    }
}
