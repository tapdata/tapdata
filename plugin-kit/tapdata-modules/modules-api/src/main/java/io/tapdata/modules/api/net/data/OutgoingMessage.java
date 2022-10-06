package io.tapdata.modules.api.net.data;

import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.utils.io.DataInputStreamEx;
import io.tapdata.entity.utils.io.DataOutputStreamEx;
import io.tapdata.modules.api.net.error.NetErrors;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class OutgoingMessage extends ContentData<OutgoingMessage> {
    public final static byte TYPE = 40;

    public OutgoingMessage() {
        super(TYPE);
    }
    private String fromUserId;
    private String fromGroupId;
    private String id;
    private Long time;

    @Override
    public void from(InputStream inputStream) throws IOException {
        DataInputStreamEx dis = dataInputStream(inputStream);
        super.from(dis);

        id = dis.readUTF();
        fromUserId = dis.readUTF();
        fromGroupId = dis.readUTF();
        time = dis.readLong();
    }

    @Override
    public void to(OutputStream outputStream) throws IOException {
        DataOutputStreamEx dos = dataOutputStream(outputStream);
        super.to(outputStream);
        dos.writeUTF(id);
        dos.writeUTF(fromUserId);
        dos.writeUTF(fromGroupId);
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

    public String getFromUserId() {
        return fromUserId;
    }

    public void setFromUserId(String fromUserId) {
        this.fromUserId = fromUserId;
    }

    public String getFromGroupId() {
        return fromGroupId;
    }

    public void setFromGroupId(String fromGroupId) {
        this.fromGroupId = fromGroupId;
    }

}