package io.tapdata.modules.api.net.data;

import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.utils.io.DataInputStreamEx;
import io.tapdata.entity.utils.io.DataOutputStreamEx;
import io.tapdata.modules.api.net.message.TapEntity;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public abstract class ContentData<T extends ContentData> extends Data {
    private static final String TAG = ContentData.class.getSimpleName();
    private String contentType;
    public T contentType(String contentType) {
        this.contentType = contentType;
        //noinspection unchecked
        return (T) this;
    }
    private Byte contentEncode;
    public T contentEncode(Byte contentEncode) {
        this.contentEncode = contentEncode;
        //noinspection unchecked
        return (T) this;
    }
    private byte[] content;
    public T content(byte[] content) {
        this.content = content;
        //noinspection unchecked
        return (T) this;
    }
    private TapEntity message;
    public T message(TapEntity message) {
        this.message = message;
        if(message != null) {
            contentType = message.getClass().getSimpleName();
            contentEncode = ENCODE_JAVA_CUSTOM_SERIALIZER;
        }
        //noinspection unchecked
        return (T) this;
    }

    public ContentData(byte type) {
        super(type);
    }

    @Override
    public void from(InputStream inputStream) throws IOException {
        DataInputStreamEx dis = dataInputStream(inputStream);
        super.from(dis);

        contentType = dis.readUTF();
        contentEncode = dis.readByte();
        if(contentEncode != null) {
            content = dis.readBytes();
            //TODO debug
//            TapLogger.info(TAG, "readBytes {} length {}", content, content != null ? content.length : "null");
            message = toTapMessage(content, contentType, contentEncode);
            content = null;
        }
    }

    @Override
    public void to(OutputStream outputStream) throws IOException {
        DataOutputStreamEx dos = dataOutputStream(outputStream);
        super.to(outputStream);
        dos.writeUTF(contentType);
        dos.writeByte(contentEncode);
        if(contentEncode != null) {
            if(message != null) {
                content = fromTapMessage(message, contentType, contentEncode);
            }
            dos.writeBytes(content);
        }
    }

    public byte[] getContent() {
        return content;
    }

    public void setContent(byte[] content) {
        this.content = content;
    }

    public String getContentType() {
        return contentType;
    }

    public void setContentType(String contentType) {
        this.contentType = contentType;
    }

    public Byte getContentEncode() {
        return contentEncode;
    }

    public void setContentEncode(Byte contentEncode) {
        this.contentEncode = contentEncode;
    }

    public TapEntity getMessage() {
        return message;
    }

    public void setMessage(TapEntity message) {
        this.message = message;
    }
    @Override
    public String toString() {
        return "ContentData ContentType " + contentType + " ContentEncode " + contentEncode + " message " + message;
    }
}
