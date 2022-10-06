package io.tapdata.modules.api.net.data;

import io.tapdata.entity.utils.io.DataInputStreamEx;
import io.tapdata.entity.utils.io.DataOutputStreamEx;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class IncomingData extends ContentData<IncomingData> {
    public final static byte TYPE = 10;
    private static final String TAG = IncomingData.class.getSimpleName();
    private String id;
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
    public void from(InputStream inputStream) throws IOException {
        DataInputStreamEx dis = dataInputStream(inputStream);
        super.from(dis);

        id = dis.readUTF();
    }

    @Override
    public void to(OutputStream outputStream) throws IOException {
        DataOutputStreamEx dos = dataOutputStream(outputStream);
        super.to(outputStream);
        dos.writeUTF(id);
    }

    @Override
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    @Override
    public String toString() {
        return "IncomingData id " + id + " " + super.toString();
    }

}