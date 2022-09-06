package io.tapdata.modules.api.net.data;

import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.utils.io.DataInputStreamEx;
import io.tapdata.entity.utils.io.DataOutputStreamEx;
import io.tapdata.modules.api.net.error.NetErrors;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class Result extends ContentData<Result> {
    public static final byte TYPE = 100;

    public static Result create() {
        return new Result();
    }

    private Integer code;
    public Result code(Integer code) {
        this.code = code;
        return this;
    }
    private String description;
    public Result description(String description) {
        this.description = description;
        return this;
    }
    private String forId;
    public Result forId(String forId) {
        this.forId = forId;
        return this;
    }
    private String serverId;
    public Result serverId(String serverId) {
        this.serverId = serverId;
        return this;
    }
    private Long time;
    public Result time(Long time) {
        this.time = time;
        return this;
    }

    public Result(){
        super(TYPE);
    }

    @Override
    public void from(InputStream inputStream) throws IOException {
        DataInputStreamEx dis = dataInputStream(inputStream);
        super.from(dis);

        forId = dis.readUTF();
        code = dis.readInt();
        description = dis.readUTF();
        serverId = dis.readUTF();
        time = dis.readLong();

    }

    @Override
    public void to(OutputStream outputStream) throws IOException {
        DataOutputStreamEx dos = dataOutputStream(outputStream);
        super.to(outputStream);
        dos.writeUTF(forId);
        dos.writeInt(code);
        dos.writeUTF(description);
        dos.writeUTF(serverId);
        dos.writeLong(time);
    }

    /**
     * @param code the code to set
     */
    public void setCode(Integer code) {
        this.code = code;
    }
    /**
     * @return the code
     */
    public Integer getCode() {
        return code;
    }
    /**
     * @param description the description to set
     */
    public void setDescription(String description) {
        this.description = description;
    }
    /**
     * @return the description
     */
    public String getDescription() {
        return description;
    }

    public String getForId() {
        return forId;
    }

    public void setForId(String forId) {
        this.forId = forId;
    }


    public String getServerId() {
        return serverId;
    }

    public void setServerId(String serverId) {
        this.serverId = serverId;
    }

    public Long getTime() {
        return time;
    }

    public void setTime(Long time) {
        this.time = time;
    }

    @Override
    public String toString() {
        return "Result code " + code + " forId " + forId + " description " + description + " time " + time + super.toString();
    }
}