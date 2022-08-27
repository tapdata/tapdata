package io.tapdata.netty.channels.data;

import io.netty.channel.unix.Errors;
import io.tapdata.entity.error.CoreException;
import io.tapdata.netty.channels.error.WSErrors;

public class Ping extends Data {
    public final static byte TYPE = 111;

    public Ping(){
        super(TYPE);
    }

    @Override
    public void resurrect() throws CoreException {
        byte[] bytes = getData();
        Byte encode = getEncode();
        if(bytes != null) {
            if(encode != null) {
                switch(encode) {
                    case BinaryCodec.ENCODE_PB:
//                        try {
//                            MessagePB.Ping request = MessagePB.Ping.parseFrom(bytes)
//                            if(request.hasField(MessagePB.Ping.getDescriptor().findFieldByName("id")))
//                                id = request.getId()
//                        } catch (InvalidProtocolBufferException e) {
//                            e.printStackTrace()
//                            throw new CoreException(IMCoreErrorCodes.ERROR_RPC_ENCODE_PB_PARSE_FAILED, "PB parse data failed, " + e.getMessage())
//                        }
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
//                MessagePB.Ping.Builder builder = MessagePB.Ping.newBuilder()
//                if(id != null)
//                    builder.setId(id)
//                MessagePB.Ping loginRequest = builder.build()
                byte[] bytes = new byte[0];//loginRequest.toByteArray()
                setData(bytes);
                setEncode(BinaryCodec.ENCODE_PB);
                break;
            default:
                throw new CoreException(WSErrors.ERROR_ENCODER_NOT_FOUND, "Encoder type doesn't be found for persistent");
        }
    }

}