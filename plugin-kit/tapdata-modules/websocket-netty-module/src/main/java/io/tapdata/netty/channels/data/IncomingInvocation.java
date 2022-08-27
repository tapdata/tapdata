package io.tapdata.netty.channels.data;

import io.tapdata.entity.error.CoreException;
import io.tapdata.netty.channels.error.WSErrors;

public class IncomingInvocation extends Data {
    public final static byte TYPE = 50;
    private String id;
    private String service;
    private String className;
    private String methodName;
    private String args;
    private Integer contentEncode;

    public IncomingInvocation() {
        super(TYPE);
    }

    public IncomingInvocation(byte[] data, Byte encode) {
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

    public Integer getContentEncode() {
        return contentEncode;
    }

    public void setContentEncode(Integer contentEncode) {
        this.contentEncode = contentEncode;
    }

    public String getService() {
        return service;
    }

    public void setService(String service) {
        this.service = service;
    }

    public String getClassName() {
        return className;
    }

    public void setClassName(String className) {
        this.className = className;
    }

    public String getMethodName() {
        return methodName;
    }

    public void setMethodName(String methodName) {
        this.methodName = methodName;
    }

    public String getArgs() {
        return args;
    }

    public void setArgs(String args) {
        this.args = args;
    }
}
