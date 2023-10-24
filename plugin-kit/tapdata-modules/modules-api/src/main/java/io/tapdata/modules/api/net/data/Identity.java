package io.tapdata.modules.api.net.data;

import io.tapdata.entity.utils.io.DataInputStreamEx;
import io.tapdata.entity.utils.io.DataOutputStreamEx;

import java.io.*;

public class Identity extends ContentData<Identity> {
    public static final byte TYPE = 1;
    private String id;
    //Engine, User
    private String idType;
    private String token;

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
    public void from(InputStream inputStream) throws IOException {
        DataInputStreamEx dis = dataInputStream(inputStream);
        super.from(dis);

        id = dis.readUTF();
        idType = dis.readUTF();
        token = dis.readUTF();
    }

    @Override
    public void to(OutputStream outputStream) throws IOException {
        DataOutputStreamEx dos = dataOutputStream(outputStream);
        super.to(outputStream);
        dos.writeUTF(id);
        dos.writeUTF(idType);
        dos.writeUTF(token);
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

    public String getIdType() {
        return idType;
    }

    public void setIdType(String idType) {
        this.idType = idType;
    }
}
