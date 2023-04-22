package io.tapdata.service.skeleton;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.parser.DefaultJSONParser;
import com.alibaba.fastjson.parser.Feature;
import com.alibaba.fastjson.parser.ParserConfig;
import com.alibaba.fastjson.serializer.SerializerFeature;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.serializer.JavaCustomSerializer;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.ReflectionUtil;
import io.tapdata.entity.utils.io.BinarySerializable;
import io.tapdata.entity.utils.io.DataInputStreamEx;
import io.tapdata.entity.utils.io.DataOutputStreamEx;
import io.tapdata.modules.api.net.error.NetErrors;
import io.tapdata.modules.api.utils.GZipUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.io.*;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.List;

public class MethodRequest extends RPCRequest {

    public static final String RPCTYPE = "mthd";
    private static final String TAG = MethodRequest.class.getSimpleName();
    private byte version = 1;

    public static MethodRequest create() {
        return new MethodRequest();
    }
    /**
     * generated from classname and method name and parameters
     */
    private Long crc;
    public MethodRequest crc(Long crc) {
        this.crc = crc;
        return this;
    }

    private String service;

    private String fromServerName;

    private Object[] args;
    public MethodRequest args(Object[] args) {
        this.args = args;
        return this;
    }

    private Integer argCount;

    private String trackId;
    //客户端的server的ip和port
    private String sourceIp;

    private Integer sourcePort;
    /**
     * 只用于内存, 不错传输序列化
     */
    private String fromService;
    private String argsTmpStr; //Only use for logging

    private String callbackFutureId;

    private Class<?> specifiedReturnClass;
    public MethodRequest specifiedReturnClass(Class<?> specifiedReturnClass) {
        this.specifiedReturnClass = specifiedReturnClass;
        return this;
    }

    @Override
    public String toString() {
        return "MethodRequest crc: " + crc + " service: " + service + " fromServerName: " + fromServerName + " argsSize: " + (args != null ? args.length : 0)
                + " trackId: " + trackId + " sourceIp: " + sourceIp + " sourcePort: " + sourcePort + " fromService: " + fromService + " specifiedReturnClass: " + specifiedReturnClass;
    }

    public MethodRequest() {
        super(RPCTYPE);
    }
//    public Object invoke() {
//        return methodMapping.invoke(args);
//    }

//    @Override
//    public void resurrect() throws CoreException {
//        byte[] bytes = getData();
//        Byte encode = getEncode();
//        if (bytes != null) {
//            if (encode != null) {
//                switch (encode) {
//                    case ENCODE_JAVABINARY:
//                        ByteArrayInputStream bais = null;
//                        DataInputStreamEx dis = null;
//                        try {
//                            bais = new ByteArrayInputStream(GZipUtils.decompress(bytes));
//                            dis = new DataInputStreamEx(bais);
//                            version = dis.getDataInputStream().readByte();
//                            resurrectVersionOne(dis);
////                            switch (version) {
////                                case 1:
////                                    resurrectVersionOne(dis);
////                                    break;
////                                case 2:
////                                    resurrectVersionTwo(dis);
////                                    break;
////                                default:
////                                    throw new CoreException(ChatErrorCodes.ERROR_ILLEGAL_METHOD_REQUEST_VERSION, "Unsupported version for method request");
////                            }
//                        } catch (Throwable e) {
//                            e.printStackTrace();
//                            if (e instanceof CoreException) {
//                                throw (CoreException) e;
//                            }
//                            throw new CoreException(NetErrors.ERROR_RPC_DECODE_FAILED, "PB parse data failed, " + e.getMessage() + ",service_class_method: " + RpcCacheManager.getInstance().getMethodByCrc(crc));
//                        } finally {
//                            if(bais != null)
//                                IOUtils.closeQuietly(bais);
//                            if(dis != null && dis.original() != null)
//                                IOUtils.closeQuietly(dis.original());
//                        }
//                        break;
//                    default:
//                        throw new CoreException(NetErrors.ERROR_RPC_ENCODER_NOTFOUND, "Encoder type doesn't be found for resurrect,service_class_method: " + RpcCacheManager.getInstance().getMethodByCrc(crc));
//                }
//            }
//        }
//    }

    private MethodMapping resurrectCommon(DataInputStreamEx dis) throws IOException {
        crc = dis.readLong();
        service = dis.readUTF();
        fromServerName = dis.readUTF();
        fromService = dis.readUTF();
        sourceIp = dis.readUTF();
        sourcePort = dis.readInt();
        if (crc == null || crc == 0 || crc == -1)
            throw new CoreException(NetErrors.ERROR_METHODREQUEST_CRC_ILLEGAL, "CRC is illegal for MethodRequest,crc: " + crc);

        if (service == null)
            throw new CoreException(NetErrors.ERROR_METHODREQUEST_SERVICE_NULL, "Service is null for service_class_method: " + RpcCacheManager.getInstance().getMethodByCrc(crc));

        ServiceSkeletonAnnotationHandler serviceSkeletonAnnotationHandler = InstanceFactory.bean(ServiceSkeletonAnnotationHandler.class);
        if (serviceSkeletonAnnotationHandler == null)
            throw new CoreException(NetErrors.ERROR_METHODREQUEST_SKELETON_NULL, "Skeleton handler is not for service " + service + " on method service_class_method: " + RpcCacheManager.getInstance().getMethodByCrc(crc));
        MethodMapping methodMapping = serviceSkeletonAnnotationHandler.getMethodMapping(crc);
        if (methodMapping == null) {
            TapLogger.error(TAG, "All methodMappings: " + JSON.toJSONString(serviceSkeletonAnnotationHandler.getMethodMap().keySet()));
            throw new CoreException(NetErrors.ERROR_METHODREQUEST_METHODNOTFOUND, "Method not found by service_class_method " + RpcCacheManager.getInstance().getMethodByCrc(crc) + ",crc: " + crc);
        }

        argCount = dis.getDataInputStream().readInt();
        return methodMapping;
    }
//    private void resurrectVersionOne(DataInputStreamEx dis) throws IOException {
//        MethodMapping methodMapping = resurrectVersionForOneAndTwo(dis);
//        if (argCount > 0) {
//            Integer length = dis.readInt();
//            byte[] argsData = new byte[length];
//            dis.readFully(argsData);
//
//            Type[] parameterTypes = getParameterTypes(methodMapping);
//            if (parameterTypes != null && parameterTypes.length > 0) {
//                try {
//                    if (argsData.length > 0) {
//                        byte[] data = GZipUtils.decompress(argsData);
//                        String json = new String(data, "utf8");
//                        argsTmpStr = json;
//                        List<Object> array = parseArray(json, parameterTypes, ParserConfig.global);
//                        if (array != null)
//                            args = array.toArray();
//                    }
//                } catch (IOException e) {
//                    e.printStackTrace();
//                    TapLogger.error(TAG, "Parse bytes failed, " + ExceptionUtils.getFullStackTrace(e) + ",service_class_method: " + RpcCacheManager.getInstance().getMethodByCrc(crc));
//                }
//            }
//        }
//
//        trackId = dis.readUTF();
//    }

    private Type[] getParameterTypes(MethodMapping methodMapping) {
        Type[] parameterTypes = methodMapping.getGenericParameterTypes();
        if (parameterTypes != null && parameterTypes.length > 0) {
            if (parameterTypes.length > argCount) {
                TapLogger.debug(TAG, "Parameter types not equal actual is " + parameterTypes.length + " but expected " + argCount + ". Cut off,service_class_method: " + RpcCacheManager.getInstance().getMethodByCrc(crc));
                Type[] newParameterTypes = new Type[argCount];
                System.arraycopy(parameterTypes, 0, newParameterTypes, 0, argCount);
                parameterTypes = newParameterTypes;
            } else if (parameterTypes.length < argCount) {
                TapLogger.debug(TAG, "Parameter types not equal actual is " + parameterTypes.length + " but expected " + argCount + ". Fill with Object.class,service_class_method: " + RpcCacheManager.getInstance().getMethodByCrc(crc));
                Type[] newParameterTypes = new Type[argCount];
                System.arraycopy(parameterTypes, 0, newParameterTypes, 0, parameterTypes.length);
                for (int i = parameterTypes.length; i < argCount; i++) {
                    newParameterTypes[i] = Object.class;
                }
                parameterTypes = newParameterTypes;
            }
        }
        return parameterTypes;
    }

    private void resurrectVersionOne(DataInputStreamEx dis) throws IOException {
        MethodMapping methodMapping = resurrectCommon(dis);
        if (argCount > 0) {
            Type[] parameterTypes = getParameterTypes(methodMapping);
            if (parameterTypes != null && parameterTypes.length > 0) {
                args = new Object[argCount];
                for(int i = 0; i < argCount; i++) {
                    byte argumentType = dis.getDataInputStream().readByte();
                    switch (argumentType) {
                        case ARGUMENT_TYPE_BYTES:
                            int length = dis.getDataInputStream().readInt();
                            byte[] bytes = new byte[length];
                            dis.getDataInputStream().readFully(bytes);
                            args[i] = bytes;
                            break;
                        case ARGUMENT_TYPE_JAVA_BINARY:
                            int length1 = dis.getDataInputStream().readInt();
                            byte[] bytes1 = new byte[length1];
                            dis.getDataInputStream().readFully(bytes1);
                            Class<?> theClass = null;
                            if(parameterTypes[i] instanceof Class<?>) {
                                theClass = (Class<?>) parameterTypes[i];
                                if(!ReflectionUtil.canBeInitiated(theClass)) {
                                    theClass = null;
                                }
                            } else if (parameterTypes[i] instanceof ParameterizedType) {
                                Type rawType = ((ParameterizedType) parameterTypes[i]).getRawType();
                                if (rawType instanceof Class<?>) {
                                    theClass = (Class<?>) rawType;
                                    if(!ReflectionUtil.canBeInitiated(theClass)) {
                                        theClass = null;
                                    }
                                }
                            }
                            if(theClass != null) {
                                BinarySerializable binarySerializable = null;
                                try {
                                    binarySerializable = (BinarySerializable) theClass.getConstructor().newInstance();
                                    try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes1)) {
                                        binarySerializable.resurrect(bais);
                                    }
                                } catch (Throwable e) {
                                    e.printStackTrace();
                                    TapLogger.error(TAG, "binarySerializable resurrect failed, " + e.getMessage() + " argument " + parameterTypes[i] + " from method " + methodMapping.getMethod() + " stack " + ExceptionUtils.getStackTrace(e) + " binarySerializable " + JSON.toJSONString(binarySerializable));
                                }
                                args[i] = binarySerializable;
                            }
                            break;
                        case ARGUMENT_TYPE_JSON:
                            String jsonString = dis.getDataInputStream().readUTF();
                            if(parameterTypes[i] != null) {
                                Object value = parseObject(jsonString, parameterTypes[i], ParserConfig.global);
                                args[i] = value;
                            }
                            break;
                        case ARGUMENT_TYPE_NONE:
                            break;
                    }
                }
            }
        }

        trackId = dis.readUTF();
    }
    @Override
    public void from(InputStream inputStream) throws IOException {
        DataInputStreamEx dataInputStreamEx = dataInputStream(inputStream);
        version = dataInputStreamEx.getDataInputStream().readByte();
        resurrectVersionOne(dataInputStreamEx);
    }

    @Override
    public void to(OutputStream outputStream) throws IOException {
        DataOutputStreamEx dataOutputStreamEx = dataOutputStream(outputStream);
        persistentVersionOne(dataOutputStreamEx);
    }
//    @Override
//    public void persistent() throws CoreException {
//        Byte encode = getEncode();
//        if (encode == null)
//            throw new CoreException(NetErrors.ERROR_RPC_ENCODER_NULL, "Encoder is null for persistent,service_class_method: " + RpcCacheManager.getInstance().getMethodByCrc(crc));
//        switch (encode) {
//            case ENCODE_JAVABINARY:
//                ByteArrayOutputStream baos = null;
//                DataOutputStreamEx dis = null;
//                try {
//                    baos = new ByteArrayOutputStream();
//                    dis = new DataOutputStreamEx(baos);
//
//                    persistentVersionOne(dis);
//                    byte[] bytes = GZipUtils.compress(baos.toByteArray());
////                    switch (version) {
////                        case 1:
////                            persistentVersionOne(dis);
////                            bytes = baos.toByteArray();
////                            break;
////                        case 2:
////                            persistentVersionTwo(dis);
////                            bytes = GZipUtils.compress(baos.toByteArray());
////                            break;
////                        default:
////                            throw new CoreException(ChatErrorCodes.ERROR_ILLEGAL_METHOD_REQUEST_VERSION, "Unsupported version for method request");
////                    }
//
//                    setData(bytes);
//                    setEncode(ENCODE_JAVABINARY);
//                    setType(RPCTYPE);
//                } catch (Throwable t) {
//                    t.printStackTrace();
//                    throw new CoreException(NetErrors.ERROR_RPC_ENCODE_FAILED, "PB parse data failed, " + t.getMessage() + ",service_class_method: " + RpcCacheManager.getInstance().getMethodByCrc(crc));
//                } finally {
//                    IOUtils.closeQuietly(baos);
//                    IOUtils.closeQuietly(dis.original());
//                }
//                break;
//            default:
//                throw new CoreException(NetErrors.ERROR_RPC_ENCODER_NOTFOUND, "Encoder type doesn't be found for persistent,service_class_method: " + RpcCacheManager.getInstance().getMethodByCrc(crc));
//        }
//    }

    private void persistentCommon(DataOutputStreamEx dis) throws IOException {
        dis.getDataOutputStream().writeByte(version);
        dis.writeLong(crc);
        dis.writeUTF(service);
        dis.writeUTF(fromServerName);
        dis.writeUTF(fromService);
        dis.writeUTF(sourceIp);
        dis.writeInt(sourcePort);

        MethodMapping methodMapping = null;
        if (methodMapping != null) {
            Class<?>[] parameterTypes = methodMapping.getParameterTypes();
            if (parameterTypes != null) {
                argCount = parameterTypes.length;
            } else {
                argCount = 0;
            }
        } else {
            if (args != null)
                argCount = args.length;
            else
                argCount = 0;
        }
        dis.getDataOutputStream().writeInt(argCount);
    }
    static final byte ARGUMENT_TYPE_BYTES = 1;
    static final byte ARGUMENT_TYPE_JSON = 2;
    static final byte ARGUMENT_TYPE_JAVA_BINARY = 3;

    static final byte ARGUMENT_TYPE_JAVA_CUSTOM = 4;
    static final byte ARGUMENT_TYPE_NONE = 10;
    private void persistentVersionOne(DataOutputStreamEx dos) throws IOException {
        persistentCommon(dos);
        if (argCount > 0 && args != null) {
            for(int i = 0; i < argCount; i++) {
                Object arg = null;
                if(i < args.length) {
                    arg = args[i];
                }
                if(arg == null) {
                    dos.getDataOutputStream().writeByte(ARGUMENT_TYPE_NONE);
                    continue;
                }
                if(arg instanceof byte[]) {
                    dos.getDataOutputStream().writeByte(ARGUMENT_TYPE_BYTES);
                    byte[] bytes = (byte[]) arg;
                    dos.getDataOutputStream().writeInt(bytes.length);
                    dos.getDataOutputStream().write(bytes);
                } else if(arg instanceof JavaCustomSerializer) {
                    dos.getDataOutputStream().writeByte(ARGUMENT_TYPE_JAVA_CUSTOM);
                    JavaCustomSerializer customSerializer = (JavaCustomSerializer) arg;
                    try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
                        customSerializer.to(baos);
                        byte[] data = baos.toByteArray();
                        dos.getDataOutputStream().writeInt(data.length);
                        dos.getDataOutputStream().write(data);
                    }
                } if(arg instanceof BinarySerializable) {
                    dos.getDataOutputStream().writeByte(ARGUMENT_TYPE_JAVA_BINARY);
                    BinarySerializable binarySerializable = (BinarySerializable) arg;
                    try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream()) {
                        binarySerializable.persistent(byteArrayOutputStream);

                        byte[] finalBytes = byteArrayOutputStream.toByteArray();
                        dos.getDataOutputStream().writeInt(finalBytes.length);
                        dos.getDataOutputStream().write(finalBytes);
                    } catch(Throwable throwable) {
                        TapLogger.error(TAG, "binarySerializable persistent failed, " + throwable.getMessage() + " stack " + ExceptionUtils.getStackTrace(throwable));
                        throw throwable;
                    }
                } else {
                    dos.getDataOutputStream().writeByte(ARGUMENT_TYPE_JSON);
                    dos.getDataOutputStream().writeUTF(JSON.toJSONString(arg, SerializerFeature.DisableCircularReferenceDetect));
                }
            }
        }
        dos.writeUTF(trackId);
    }

//    private void persistentVersionOne(DataOutputStreamEx dis) throws IOException {
//        persistentVersionOneAndTwo(dis);
//        if (argCount > 0 && args != null) {
//            String json = null;
//            if (argsTmpStr == null)
//                json = JSON.toJSONString(args, SerializerFeature.DisableCircularReferenceDetect);
//            else
//                json = argsTmpStr;
//            try {
//                byte[] data = GZipUtils.compress(json.getBytes("utf8"));
//                dis.writeInt(data.length);
//                dis.write(data);
//            } catch (IOException e) {
//                e.printStackTrace();
//                TapLogger.error(TAG, "Generate " + json + " to bytes failed, " + ExceptionUtils.getFullStackTrace(e) + ",service_class_method: " + RpcCacheManager.getInstance().getMethodByCrc(crc));
//            }
//        }
//        dis.writeUTF(trackId);
//    }

    public Long getCrc() {
        return crc;
    }

    public void setCrc(Long crc) {
        this.crc = crc;
    }

    public Object[] getArgs() {
        return args;
    }

    public void setArgs(Object[] args) {
        this.args = args;
    }

    public Integer getArgCount() {
        return argCount;
    }

    public void setArgCount(Integer argCount) {
        this.argCount = argCount;
    }

    public byte getVersion() {
        return version;
    }

    public void setVersion(byte version) {
        this.version = version;
    }

    public String getService() {
        return service;
    }

    public void setService(String service) {
        this.service = service;
    }

    public String getFromServerName() {
        return fromServerName;
    }

    public void setFromServerName(String fromServerName) {
        this.fromServerName = fromServerName;
    }

    public String getFromService() {
        return fromService;
    }

    public void setFromService(String fromService) {
        this.fromService = fromService;
    }

    public String getTrackId() {
        return trackId;
    }

    public void setTrackId(String trackId) {
        this.trackId = trackId;
    }

    public String getSourceIp() {
        return sourceIp;
    }

    public void setSourceIp(String sourceIp) {
        this.sourceIp = sourceIp;
    }

    public Integer getSourcePort() {
        return sourcePort;
    }

    public void setSourcePort(Integer sourcePort) {
        this.sourcePort = sourcePort;
    }

    public String getArgsTmpStr() {
        return argsTmpStr;
    }

    public void setArgsTmpStr(String argsTmpStr) {
        this.argsTmpStr = argsTmpStr;
    }

    public Class<?> getSpecifiedReturnClass() {
        return specifiedReturnClass;
    }

    public void setSpecifiedReturnClass(Class<?> specifiedReturnClass) {
        this.specifiedReturnClass = specifiedReturnClass;
    }

    public String getCallbackFutureId() {
        return callbackFutureId;
    }

    public void setCallbackFutureId(String callbackFutureId) {
        this.callbackFutureId = callbackFutureId;
    }

    private List<Object> parseArray(String text, Type[] types, ParserConfig config) {
        if (text == null) {
            return null;
        }

        List<Object> list;

        DefaultJSONParser parser = new DefaultJSONParser(text, config);
        parser.lexer.setFeatures(JSON.DEFAULT_PARSER_FEATURE & ~Feature.UseBigDecimal.getMask());
        Object[] objectArray = parser.parseArray(types);
        if (objectArray == null) {
            list = null;
        } else {
            list = Arrays.asList(objectArray);
        }

        parser.handleResovleTask(list);

        parser.close();

        return list;
    }

    private Object parseObject(String text, Type type, ParserConfig config) {
        if (text == null) {
            return null;
        }

        DefaultJSONParser parser = new DefaultJSONParser(text, config);
        parser.lexer.setFeatures(JSON.DEFAULT_PARSER_FEATURE & ~Feature.UseBigDecimal.getMask());
        Object object = parser.parseObject(type);

        parser.handleResovleTask(object);

        parser.close();

        return object;
    }


}
