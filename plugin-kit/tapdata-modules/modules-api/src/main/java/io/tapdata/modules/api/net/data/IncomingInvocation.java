package io.tapdata.modules.api.net.data;

import io.tapdata.entity.utils.io.DataInputStreamEx;
import io.tapdata.entity.utils.io.DataOutputStreamEx;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class IncomingInvocation extends ContentData<IncomingInvocation> {
    public final static byte TYPE = 50;
    private String id;
    private String service;
    private String className;
    private String methodName;

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
    public void from(InputStream inputStream) throws IOException {
        DataInputStreamEx dis = dataInputStream(inputStream);
        super.from(dis);

        id = dis.readUTF();
        service = dis.readUTF();
        className = dis.readUTF();
        methodName = dis.readUTF();
    }

    @Override
    public void to(OutputStream outputStream) throws IOException {
        DataOutputStreamEx dos = dataOutputStream(outputStream);
        super.to(outputStream);
        dos.writeUTF(id);
        dos.writeUTF(service);
        dos.writeUTF(className);
        dos.writeUTF(methodName);
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

}
