package io.tapdata.modules.api.net.data;


import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.utils.ClassFactory;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.JsonParser;
import io.tapdata.entity.serializer.JavaCustomSerializer;
import io.tapdata.entity.utils.TapUtils;
import io.tapdata.modules.api.net.error.NetErrors;
import io.tapdata.modules.api.net.message.TapEntity;
import io.tapdata.modules.api.net.message.TapEntityEx;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

import static io.tapdata.entity.simplify.TapSimplify.toJson;

public abstract class Data extends BinaryCodec implements JavaCustomSerializer {

    public static final int CODE_SUCCESS = 1;
    public static final int CODE_FAILED = 0;
    private static final String TAG = Data.class.getSimpleName();

    private byte type;

    public Data(byte type) {
        this.type = type;
    }
    protected TapEntity toTapMessage(byte[] content, String contentType, Byte contentEncode) throws IOException {
        if(content == null || contentType == null || contentEncode == null) {
            TapLogger.error(TAG, "Some parameters are null, content {}, contentType {} contentEncode {}", content, contentType, contentEncode);
            return null;
        }
        TapEntityEx message = null;
        switch (contentEncode) {
            case ENCODE_JAVA_CUSTOM_SERIALIZER:
//                TapLogger.info(TAG, "toTapMessage contentType {}", contentType);
                message = (TapEntityEx) ClassFactory.create(TapEntity.class, contentType);
//                TapLogger.info(TAG, "toTapMessage message {}", message);
                if(message != null) {
                    try(ByteArrayInputStream bais = new ByteArrayInputStream(content)) {
                        message.from(bais);
                    } catch (Throwable throwable) {
                        TapLogger.error(TAG, "message {} from failed, {}", message, throwable.getMessage());
                        message.setParseError(throwable);
                    }
                } else {
                    TapLogger.warn(TAG, "(toTapMessage OBJECT_SERIALIZABLE) Content type {} doesn't match any TapMessage for {}, contentEncode {}", contentType, this.getClass().getSimpleName(), contentEncode);
                }
                break;
            case ENCODE_JSON:
                Class<? extends TapEntity> messageClass = ClassFactory.getImplementationClass(TapEntity.class, contentType);
                if(messageClass != null) {
                    String contentStr = new String(content, StandardCharsets.UTF_8);
                    JsonParser jsonParser = InstanceFactory.instance(JsonParser.class);
                    message = (TapEntityEx) Objects.requireNonNull(jsonParser).fromJson(contentStr, messageClass);
                } else {
                    TapLogger.warn(TAG, "(toTapMessage JSON) Content type {} doesn't match any TapMessage for {}, contentEncode {}", contentType, this.getClass().getSimpleName(), contentEncode);
                }
                break;
            default:
                TapLogger.warn(TAG, "(toTapMessage) ContentEncode {} not found for Content Type {}, {}", contentEncode, contentType, this.getClass().getSimpleName());
                break;
        }
        return message;
    }

    protected byte[] fromTapMessage(TapEntity message, String contentType, Byte contentEncode) throws IOException {
        byte[] data = null;
        switch (contentEncode) {
            case ENCODE_JAVA_CUSTOM_SERIALIZER:
//                TapLogger.info(TAG, "fromTapMessage message {}", toJson(message));
                try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
                    message.to(baos);
                    data = baos.toByteArray();
                } catch (Throwable throwable) {
                    TapLogger.debug(TAG, "message {} to failed, {}", message, Objects.requireNonNull(InstanceFactory.instance(TapUtils.class)).getStackTrace(throwable));
                    ((TapEntityEx)message).setParseError(throwable);
                }
                break;
            case ENCODE_JSON:
                JsonParser jsonParser = InstanceFactory.instance(JsonParser.class);
                String jsonStr = Objects.requireNonNull(jsonParser).toJson(message);
                data = jsonStr.getBytes(StandardCharsets.UTF_8);
                break;
            default:
                TapLogger.warn(TAG, "(fromTapMessage) ContentEncode {} not found for Content Type {}, {}", contentEncode, contentType, this.getClass().getSimpleName());
                break;
        }
        return data;
    }
    public void from(InputStream inputStream) throws IOException {
//        DataInputStreamEx dis = dataInputStream(inputStream);
//        type = dis.readByte();
    }

    public void to(OutputStream outputStream) throws IOException {
//        DataOutputStreamEx dos = dataOutputStream(outputStream);
//        dos.writeByte(type);
    }

    @Override
    public void resurrect() throws CoreException {
        if(encode == null)
            encode = ENCODE_JAVA_CUSTOM_SERIALIZER;
        if(data == null)
            throw new CoreException(NetErrors.RESURRECT_DATA_NULL, "data is null while resurrect for {}", this.getClass().getSimpleName());

        switch (encode) {
            case ENCODE_JAVA_CUSTOM_SERIALIZER:
                try(ByteArrayInputStream input = new ByteArrayInputStream(data)) {
                    from(input);
                } catch (IOException e) {
                    throw new CoreException(NetErrors.JAVA_CUSTOM_DESERIALIZE_FAILED, "Deserialize {} failed, {}", this.getClass().getSimpleName(), e.getMessage());
                }
                break;
            default:
                throw new CoreException(NetErrors.ENCODE_NOT_SUPPORTED, "Encode {} not supported for identity", encode);
        }
    }

    @Override
    public void persistent() throws CoreException {
        if(encode == null)
            encode = ENCODE_JAVA_CUSTOM_SERIALIZER;

        switch (encode) {
            case BinaryCodec.ENCODE_JAVA_CUSTOM_SERIALIZER:
                ByteArrayOutputStream output = new ByteArrayOutputStream();
                try {
                    to(output);
                    data = output.toByteArray();
                } catch (IOException e) {
                    throw new CoreException(NetErrors.JAVA_CUSTOM_DESERIALIZE_FAILED, "Serialize {} failed, {}", this.getClass().getSimpleName(), e.getMessage());
                }

                break;
            default:
                throw new CoreException(NetErrors.ERROR_ENCODER_NOT_FOUND, "Encode type {} not found to persistent for {}", encode, this.getClass().getSimpleName());
        }
    }

    public String getId(){
        return null;
    }
    public String getContentType() {
        return null;
    }
//	
//	void fromHailPack(HailPack pack) throws CoreException {
//		if(type != pack.getType())
//			throw new IllegalArgumentException("Incompatible Data type, expected " + type + ", but " + pack.getType());
//		short encode = pack.getEncode();
//		byte[] data = pack.getContent();
//		if(data == null)
//			throw new NullPointerException("HailPack's content is null, " + this);
//		try {
//			switch(encode) {
//			case HailPack.ENCODE_JSON:
//				String str = new String(data, "utf8");
////				LoggerEx.info(TAG, "Data received: " + str);
//				Document dbObj = Document.parse(str);
//				fromDBObjectPrivate(dbObj);
//				break;
//			case HailPack.ENCODE_JSON_GZIP:
//				GZIPInputStream gis = new GZIPInputStream(new ByteArrayInputStream(data));
//				ByteArrayOutputStream baos = new ByteArrayOutputStream();
//				byte[] buffer = new byte[8192];
//				int size;
//				while((size = gis.read(buffer)) != -1){
//					if(size > 0){
//						baos.write(buffer, 0, size);
//					}
//				}
//				String gzipStr = new String(baos.toByteArray(), "utf8");
//				LoggerEx.info(TAG, "Data received(gzip): " + gzipStr);
//				Document gzipDbObj = Document.parse(gzipStr);
//				fromDBObjectPrivate(gzipDbObj);
//				break;
//			default:
//				throw new CoreException(CoreErrorCodes.ERROR_TCPCHANNEL_ENCODE_ILLEGAL, "Unsupported HailPack decode, " + encode + " data " + this);
//			}
//		} catch(IOException e) {
//			LoggerEx.error(TAG, "Resurrect from Hail Pack " + pack + " failed, " + e.getMessage());
//			throw new CoreException(CoreErrorCodes.ERROR_HAILPACK_IO_ERROR, "Resurrect from Hail Pack failed, " + e.getMessage());
//		} catch(Throwable t) {
//			LoggerEx.error(TAG, "Resurrect from Hail Pack " + pack + " failed (Unknown), " + t.getMessage());
//			throw new CoreException(CoreErrorCodes.ERROR_HAILPACK_UNKNOWNERROR, "Resurrect from Hail Pack failed (Unknown), " + t.getMessage());
//		}
//	}
//	
//	HailPack toHailPack(short encode) throws CoreException {
//		try {
//			HailPack pack = new HailPack();
//			byte[] data;
//			switch(encode) {
//			case HailPack.ENCODE_JSON:
//				Document dbObj = toDocument();
//				data = dbObj.toJson(CommonUtils.MONGODB_JSONSETTINGS).getBytes("utf8");
//				break;
//			case HailPack.ENCODE_JSON_GZIP:
//				Document gzipDbObj = toDocument();
//				String str = gzipDbObj.toJson(CommonUtils.MONGODB_JSONSETTINGS);
//				ByteArrayOutputStream gzipBaos = new ByteArrayOutputStream(str.length());
//				GZIPOutputStream gos = new GZIPOutputStream(gzipBaos);
//				byte[] d = str.getBytes("utf8");
//				gos.write(d);
//				gos.close();
//				data = gzipBaos.toByteArray();
//				break;
//			default:
//				throw new IllegalArgumentException("Unsupported HailPack encode, " + encode);
//			}
//			pack.setContent(data);
//			pack.setLength(data.length);
//			pack.setEncode(encode);
//			pack.setType(type);
//			return pack;
//		} catch(IOException e) {
//			LoggerEx.error(TAG, "Persistent to Hail Pack " + this + " failed, " + e.getMessage());
//			throw new CoreException(CoreErrorCodes.ERROR_HAILPACK_IO_ERROR, "Persistent to Hail Pack failed, " + e.getMessage());
//		} catch(Throwable t) {
//			LoggerEx.error(TAG, "Persistent to Hail Pack " + this + " failed (Unknown), " + t.getMessage());
//			throw new CoreException(CoreErrorCodes.ERROR_HAILPACK_UNKNOWNERROR, "Persistent to Hail Pack failed (Unknown), " + t.getMessage());
//		}
//	}
//	
    /**
     * @param type the type to set
     */
    public void setType(byte type) {
        this.type = type;
    }

    /**
     * @return the type
     */
    public byte getType() {
        return type;
    }
}
