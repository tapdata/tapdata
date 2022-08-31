package io.tapdata.modules.api.net.data;

import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.utils.io.DataInputStreamEx;
import io.tapdata.entity.utils.io.DataOutputStreamEx;
import io.tapdata.modules.api.net.error.NetErrors;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class OutgoingData extends ContentData<OutgoingData> {
    public final static byte TYPE = 20;
    public OutgoingData() {
        super(TYPE);
    }
    private String id;
    public OutgoingData id(String id) {
        this.id = id;
        return this;
    }
    private Long time;
    public OutgoingData time(Long time) {
        this.time = time;
        return this;
    }
    @Override
    public void from(InputStream inputStream) throws IOException {
        DataInputStreamEx dis = dataInputStream(inputStream);
        super.from(dis);

        id = dis.readUTF();
        time = dis.readLong();
    }

    @Override
    public void to(OutputStream outputStream) throws IOException {
        DataOutputStreamEx dos = dataOutputStream(outputStream);
        super.to(outputStream);
        dos.writeUTF(id);
        dos.writeLong(time);

    }

    public Long getTime() {
        return time;
    }

    public void setTime(Long time) {
        this.time = time;
    }

    @Override
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

}