package io.tapdata.netty.channels.data;


import io.tapdata.entity.error.CoreException;
import io.tapdata.netty.channels.error.WSErrors;

public class Identity extends Data {
    public static final byte TYPE = 1;
    private String id;
    private String token;
    private String reserved;
    private String sign;

    public Identity() {
        super(TYPE);
    }

    public Identity(byte[] data, Byte encode) {
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
            encode = BinaryCodec.ENCODE_PB;//throw new CoreException(CoreErrorCodes.ERROR_RPC_ENCODER_NULL, "Encoder is null for persistent")
        switch (encode) {
            case BinaryCodec.ENCODE_PB:


                break;
            default:
                throw new CoreException(WSErrors.ERROR_ENCODER_NOT_FOUND, "Encoder type doesn't be found for persistent");
        }
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }

    public String getReserved() {
        return reserved;
    }

    public void setReserved(String reserved) {
        this.reserved = reserved;
    }

    public String getSign() {
        return sign;
    }

    public void setSign(String sign) {
        this.sign = sign;
    }
}
