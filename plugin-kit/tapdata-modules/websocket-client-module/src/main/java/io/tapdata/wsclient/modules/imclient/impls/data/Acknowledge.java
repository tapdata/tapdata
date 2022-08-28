package io.tapdata.wsclient.modules.imclient.impls.data;

import java.io.IOException;
import java.util.Arrays;
import java.util.Set;

public class Acknowledge extends Data {
    private String id;
    private Set<String> msgIds;
    private String service;

    public Acknowledge(){
        super(HailPack.TYPE_IN_ACKNOWLEDGE);
    }

    public String toString(){
        StringBuffer buffer = new StringBuffer();
        buffer.append(Arrays.toString(msgIds.toArray()));
        return new String(buffer);
    }

    @Override
    public void resurrect() throws IOException {
        byte[] bytes = getData();
        Byte encode = getEncode();
        if(bytes != null) {
            if(encode != null) {
                switch(encode) {
                    case ENCODE_PB:

                        break;
                    default:
                        throw new IOException("Encoder type doesn't be found for resurrect");
                }
            }
        }
    }

    @Override
    public void persistent() throws IOException {
        Byte encode = getEncode();
        if(encode == null)
            encode = ENCODE_PB;//throw new CoreException(CoreErrorCodes.ERROR_RPC_ENCODER_NULL, "Encoder is null for persistent");
        switch(encode) {
            case ENCODE_PB:

                break;
            default:
                throw new IOException("Encoder type doesn't be found for persistent");
        }
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Set<String> getMsgIds() {
        return msgIds;
    }

    public void setMsgIds(Set<String> msgIds) {
        this.msgIds = msgIds;
    }

    public String getService() {
        return service;
    }

    public void setService(String service) {
        this.service = service;
    }

}
