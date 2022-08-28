package io.tapdata.modules.api.net.data;

import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.utils.io.DataInputStreamEx;
import io.tapdata.entity.utils.io.DataOutputStreamEx;
import io.tapdata.modules.api.net.error.NetErrors;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class IncomingRequest extends Data {
    public final static byte TYPE = 60;
    private String id;
    private String service;
    private String uri; // rest/message/{messageId}?playerId={playerId}
    private String method;
    private String bodyStr;
    private Byte bodyEncode;

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
    public void from(InputStream inputStream) throws IOException {
        DataInputStreamEx dis = dataInputStream(inputStream);
        super.from(dis);

        id = dis.readUTF();
        service = dis.readUTF();
        uri = dis.readUTF();
        method = dis.readUTF();
        bodyEncode = dis.readByte();
        bodyStr = dis.readUTF();
    }

    @Override
    public void to(OutputStream outputStream) throws IOException {
        DataOutputStreamEx dos = dataOutputStream(outputStream);
        super.to(outputStream);
        dos.writeUTF(id);
        dos.writeUTF(service);
        dos.writeUTF(uri);
        dos.writeUTF(method);
        dos.writeByte(bodyEncode);
        dos.writeUTF(bodyStr);
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

    public Byte getBodyEncode() {
        return bodyEncode;
    }

    public void setBodyEncode(Byte bodyEncode) {
        this.bodyEncode = bodyEncode;
    }
}
