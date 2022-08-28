package io.tapdata.modules.api.net.data;

import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.utils.io.DataInputStreamEx;
import io.tapdata.entity.utils.io.DataOutputStreamEx;
import io.tapdata.modules.api.net.error.NetErrors;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class OutgoingData extends Data {
    public final static byte TYPE = 20;
    public OutgoingData() {
        super(TYPE);
    }
    private String id;
    private Long time;
    private String contentType;
    private Byte contentEncode;
    private String content;
    private byte[] binaryContent;
    @Override
    public void from(InputStream inputStream) throws IOException {
        DataInputStreamEx dis = dataInputStream(inputStream);
        super.from(dis);

        id = dis.readUTF();
        time = dis.readLong();
        contentType = dis.readUTF();
        contentEncode = dis.readByte();
        content = dis.readUTF();
        binaryContent = dis.readBytes();
    }

    @Override
    public void to(OutputStream outputStream) throws IOException {
        DataOutputStreamEx dos = dataOutputStream(outputStream);
        super.to(outputStream);
        dos.writeUTF(id);
        dos.writeLong(time);
        dos.writeUTF(contentType);
        dos.writeByte(contentEncode);
        dos.writeUTF(content);
        dos.writeBytes(binaryContent);

    }
    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public String getContentType() {
        return contentType;
    }

    public void setContentType(String contentType) {
        this.contentType = contentType;
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

    public Byte getContentEncode() {
        return contentEncode;
    }

    public void setContentEncode(Byte contentEncode) {
        this.contentEncode = contentEncode;
    }

    public byte[] getBinaryContent() {
        return binaryContent;
    }

    public void setBinaryContent(byte[] binaryContent) {
        this.binaryContent = binaryContent;
    }
}