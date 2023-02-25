package io.tapdata.service.skeleton;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.utils.ReflectionUtil;
import io.tapdata.entity.utils.io.BinarySerializable;
import io.tapdata.entity.utils.io.DataInputStreamEx;
import io.tapdata.entity.utils.io.DataOutputStreamEx;
import io.tapdata.modules.api.net.error.NetErrors;
import io.tapdata.modules.api.utils.GZipUtils;
import org.apache.commons.io.IOUtils;

import java.io.*;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

public class MethodResponse extends RPCResponse {
    private static final String TAG = MethodResponse.class.getSimpleName();
    private byte version = 1;
    private Long crc;
    private Object returnObject;
    private CoreException exception;

    private String returnTmpStr;

    public static final String FIELD_RETURN = "return";
    public static final String FIELD_ERROR = "error";

    public MethodResponse() {
        super(MethodRequest.RPCTYPE);
    }

    public MethodResponse(Object returnObj, CoreException exception) {
        super(MethodRequest.RPCTYPE);
        this.returnObject = returnObj;
        this.exception = exception;
    }

    public String toString() {
        StringBuilder builder = new StringBuilder(MethodResponse.class.getSimpleName());
//		builder.append(": ").append(server);
        return builder.toString();
    }

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
////                            switch (version) {
////                                case 1:
////                                    resurrectVersionOne(dis);
////                                    break;
////                                case 2:
////                                    break;
////                                default:
////                                    throw new CoreException(ChatErrorCodes.ERROR_ILLEGAL_METHOD_RESPONSE_VERSION, "Illegal method response version " + version);
////                            }
//                            crc = dis.getDataInputStream().readLong();
//                            if (crc == 0 || crc == -1)
//                                throw new CoreException(NetErrors.ERROR_METHODREQUEST_CRC_ILLEGAL, "CRC is illegal for MethodRequest,service_class_method: " + RpcCacheManager.getInstance().getMethodByCrc(crc));
//
//                            ServiceStubManager serviceStubManager = null;
//                            MethodRequest methodRequest = (MethodRequest) request;
//                            if (request != null) {
//                                serviceStubManager = methodRequest.getServiceStubManager();
//                            }
//                            if (serviceStubManager == null) {
//                                BaseRuntimeContext runtimeContext = (BaseRuntimeContext) baseConfiguration.getRuntimeContext(methodRequest.getFromService());
//                                if (runtimeContext == null)
//                                    throw new CoreException(ChatErrorCodes.ERROR_METHODREQUEST_SERVICE_NOTFOUND, "Service " + methodRequest.getFromService() + " not found for service_class_method: " + RpcCacheManager.getInstance().getMethodByCrc(crc));
//                                serviceStubManager = runtimeContext.getServiceStubManagerFactory().get();
//                            }
//
//                            MethodMapping methodMapping = serviceStubManager.getMethodMapping(crc);
//
//                            Class<?> returnClass = ((MethodRequest)request).getSpecifiedReturnClass();
//                            if(returnClass == null || returnClass.equals(Object.class)) {
//                                if (methodMapping == null || methodMapping.getReturnClass().equals(Object.class)) {
//                                    returnClass = JSONObject.class;
//                                } else {
//                                    Type type = methodMapping.getGenericReturnClass();
//                                    if(type instanceof Class<?>) {
//                                        returnClass = (Class<?>) type;
//                                    } else if (type instanceof ParameterizedType) {
//                                        Type rawType = ((ParameterizedType) type).getRawType();
//                                        if (rawType instanceof Class<?>) {
//                                            returnClass = (Class<?>) rawType;
//                                        }
//                                    }
//                                }
//                            }
//
//                            byte argumentType = dis.getDataInputStream().readByte();
//                            switch (argumentType) {
//                                case MethodRequest.ARGUMENT_TYPE_BYTES:
//                                    int length = dis.getDataInputStream().readInt();
//                                    byte[] bytes2 = new byte[length];
//                                    dis.getDataInputStream().readFully(bytes2);
//                                    if(returnClass != null && returnClass.equals(byte[].class)) {
//                                        returnObject = bytes2;
//                                    }
//                                    break;
//                                case MethodRequest.ARGUMENT_TYPE_JAVA_BINARY:
//                                    int length1 = dis.getDataInputStream().readInt();
//                                    byte[] bytes1 = new byte[length1];
//                                    dis.getDataInputStream().readFully(bytes1);
//                                    if (returnClass != null && BinarySerializable.class.isAssignableFrom(returnClass)) {
//                                        if(ReflectionUtil.canBeInitiated(returnClass)) {
//                                            try {
//                                                BinarySerializable binarySerializable = (BinarySerializable) returnClass.getConstructor().newInstance();
//                                                try (ByteArrayInputStream bais1 = new ByteArrayInputStream(bytes1)) {
//                                                    binarySerializable.resurrect(bais1);
//                                                    returnObject = binarySerializable;
//                                                }
//                                            } catch (Throwable e) {
//                                                e.printStackTrace();
//                                                TapLogger.error(TAG, "Deserialize return object " + returnClass + " from method " + (methodMapping != null ? methodMapping.getMethod() : "") + " failed, " + e.getMessage());
//                                            }
//                                        }
//                                    }
//                                    break;
//                                case MethodRequest.ARGUMENT_TYPE_JSON:
//                                    String jsonString = dis.getDataInputStream().readUTF();
//                                    if(returnClass != null) {
//                                        if(returnClass.equals(JSONObject.class)) {
//                                            returnObject = JSON.parse(jsonString);
//                                        } else {
//                                            returnObject = JSON.parseObject(jsonString, returnClass);
//                                        }
//                                    }
//                                    break;
//                                case MethodRequest.ARGUMENT_TYPE_NONE:
//                                    break;
//                            }
//
//                            int execeptionLength = dis.getDataInputStream().readInt();
//                            if (execeptionLength > 0) {
//                                byte[] exceptionBytes = new byte[execeptionLength];
//                                dis.getDataInputStream().readFully(exceptionBytes);
//                                String json = new String(exceptionBytes, StandardCharsets.UTF_8);
//                                JSONObject jsonObj = (JSONObject) JSON.parse(json);
//                                if (jsonObj != null) {
//                                    Integer code = jsonObj.getInteger("code");
//                                    String message = jsonObj.getString("message");
//                                    String logLevel = jsonObj.getString("logLevel");
//                                    if (code != null) {
//                                        exception = new CoreException(code, message, logLevel);
//                                    }
//                                }
//                            }
//                        } catch (Throwable e) {
//                            e.printStackTrace();
//                            throw new CoreException(NetErrors.ERROR_RPC_ENCODE_FAILED, "PB parse data failed, " + e.getMessage() + ",service_class_method: " + RpcCacheManager.getInstance().getMethodByCrc(crc));
//                        } finally {
//                            IOUtils.closeQuietly(bais);
//                            IOUtils.closeQuietly(dis.original());
//                        }
//                        break;
//                    default:
//                        throw new CoreException(NetErrors.ERROR_RPC_ENCODER_NOTFOUND, "Encoder type doesn't be found for resurrect,service_class_method: " + RpcCacheManager.getInstance().getMethodByCrc(crc));
//                }
//            }
//        }
//    }
    @Override
    public void from(InputStream inputStream) throws IOException {
        DataInputStreamEx dis = dataInputStream(inputStream);
        version = dis.getDataInputStream().readByte();
//                            switch (version) {
//                                case 1:
//                                    resurrectVersionOne(dis);
//                                    break;
//                                case 2:
//                                    break;
//                                default:
//                                    throw new CoreException(ChatErrorCodes.ERROR_ILLEGAL_METHOD_RESPONSE_VERSION, "Illegal method response version " + version);
//                            }
        crc = dis.getDataInputStream().readLong();
        if (crc == 0 || crc == -1)
            throw new CoreException(NetErrors.ERROR_METHODREQUEST_CRC_ILLEGAL, "CRC is illegal for MethodRequest,service_class_method: " + RpcCacheManager.getInstance().getMethodByCrc(crc));

//        ServiceStubManager serviceStubManager = null;
//        MethodRequest methodRequest = (MethodRequest) request;
//        if (request != null) {
//            serviceStubManager = methodRequest.getServiceStubManager();
//        }
//        if (serviceStubManager == null) {
//            BaseRuntimeContext runtimeContext = (BaseRuntimeContext) baseConfiguration.getRuntimeContext(methodRequest.getFromService());
//            if (runtimeContext == null)
//                throw new CoreException(ChatErrorCodes.ERROR_METHODREQUEST_SERVICE_NOTFOUND, "Service " + methodRequest.getFromService() + " not found for service_class_method: " + RpcCacheManager.getInstance().getMethodByCrc(crc));
//            serviceStubManager = runtimeContext.getServiceStubManagerFactory().get();
//        }

        MethodMapping methodMapping = null;//serviceStubManager.getMethodMapping(crc);

        Class<?> returnClass = ((MethodRequest)request).getSpecifiedReturnClass();
        if(returnClass == null || returnClass.equals(Object.class)) {
            if (methodMapping == null || methodMapping.getReturnClass().equals(Object.class)) {
                returnClass = JSONObject.class;
            } else {
                Type type = methodMapping.getGenericReturnClass();
                if(type instanceof Class<?>) {
                    returnClass = (Class<?>) type;
                } else if (type instanceof ParameterizedType) {
                    Type rawType = ((ParameterizedType) type).getRawType();
                    if (rawType instanceof Class<?>) {
                        returnClass = (Class<?>) rawType;
                    }
                }
            }
        }

        byte argumentType = dis.getDataInputStream().readByte();
        switch (argumentType) {
            case MethodRequest.ARGUMENT_TYPE_BYTES:
                int length = dis.getDataInputStream().readInt();
                byte[] bytes2 = new byte[length];
                dis.getDataInputStream().readFully(bytes2);
                if(returnClass != null && returnClass.equals(byte[].class)) {
                    returnObject = bytes2;
                }
                break;
            case MethodRequest.ARGUMENT_TYPE_JAVA_BINARY:
                int length1 = dis.getDataInputStream().readInt();
                byte[] bytes1 = new byte[length1];
                dis.getDataInputStream().readFully(bytes1);
                if (returnClass != null && BinarySerializable.class.isAssignableFrom(returnClass)) {
                    if(ReflectionUtil.canBeInitiated(returnClass)) {
                        try {
                            BinarySerializable binarySerializable = (BinarySerializable) returnClass.getConstructor().newInstance();
                            try (ByteArrayInputStream bais1 = new ByteArrayInputStream(bytes1)) {
                                binarySerializable.resurrect(bais1);
                                returnObject = binarySerializable;
                            }
                        } catch (Throwable e) {
                            e.printStackTrace();
                            TapLogger.error(TAG, "Deserialize return object " + returnClass + " from method " + (methodMapping != null ? methodMapping.getMethod() : "") + " failed, " + e.getMessage());
                        }
                    }
                }
                break;
            case MethodRequest.ARGUMENT_TYPE_JSON:
                String jsonString = dis.getDataInputStream().readUTF();
                if(returnClass != null) {
                    if(returnClass.equals(JSONObject.class)) {
                        returnObject = JSON.parse(jsonString);
                    } else {
                        returnObject = JSON.parseObject(jsonString, returnClass);
                    }
                }
                break;
            case MethodRequest.ARGUMENT_TYPE_NONE:
                break;
        }

        int execeptionLength = dis.getDataInputStream().readInt();
        if (execeptionLength > 0) {
            byte[] exceptionBytes = new byte[execeptionLength];
            dis.getDataInputStream().readFully(exceptionBytes);
            String json = new String(exceptionBytes, StandardCharsets.UTF_8);
            JSONObject jsonObj = (JSONObject) JSON.parse(json);
            if (jsonObj != null) {
                Integer code = jsonObj.getInteger("code");
                String message = jsonObj.getString("message");
//                String logLevel = jsonObj.getString("logLevel");
                if (code != null) {
                    exception = new CoreException(code, message);
                }
            }
        }
    }

    @Override
    public void to(OutputStream outputStream) throws IOException {
        DataOutputStreamEx dos = dataOutputStream(outputStream);
        dos.getDataOutputStream().writeByte(version);
        dos.getDataOutputStream().writeLong(crc);

        byte[] returnBytes = null;
        if (returnObject != null) {
            if(returnObject instanceof byte[]) {
                dos.getDataOutputStream().writeByte(MethodRequest.ARGUMENT_TYPE_BYTES);
                returnBytes = (byte[]) returnObject;
                dos.getDataOutputStream().writeInt(returnBytes.length);
                dos.getDataOutputStream().write(returnBytes);
            } else if(returnObject instanceof BinarySerializable) {
                dos.getDataOutputStream().writeByte(MethodRequest.ARGUMENT_TYPE_JAVA_BINARY);
                BinarySerializable binarySerializable = (BinarySerializable) returnObject;
                try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream()) {
                    binarySerializable.persistent(byteArrayOutputStream);

                    byte[] finalBytes = byteArrayOutputStream.toByteArray();
                    dos.getDataOutputStream().writeInt(finalBytes.length);
                    dos.getDataOutputStream().write(finalBytes);
                }
            } else {
                dos.getDataOutputStream().writeByte(MethodRequest.ARGUMENT_TYPE_JSON);

                String returnStr = null;
                if(returnTmpStr == null) {
                    returnStr = JSON.toJSONString(returnObject, SerializerFeature.DisableCircularReferenceDetect);
                } else {
                    returnStr = returnTmpStr;
                }
//                            String returnStr = Objects.requireNonNullElseGet(returnTmpStr, () -> JSON.toJSONString(returnObject, SerializerFeature.DisableCircularReferenceDetect));
                dos.getDataOutputStream().writeUTF(returnStr);
            }
        } else {
            dos.getDataOutputStream().writeByte(MethodRequest.ARGUMENT_TYPE_NONE);
        }

        byte[] exceptionBytes = null;
        if (exception != null) {
            JSONObject json = new JSONObject();
            json.put("code", exception.getCode());
            json.put("message", exception.getMessage());
//                        json.put("logLevel", exception.getLogLevel());
            String errorStr = json.toJSONString();//JSON.toJSONString(exception);
            exceptionBytes = errorStr.getBytes(StandardCharsets.UTF_8);
        }
        if (exceptionBytes != null) {
            dos.getDataOutputStream().writeInt(exceptionBytes.length);
            dos.getDataOutputStream().write(exceptionBytes);
        } else {
            dos.getDataOutputStream().writeInt(0);
        }
    }
//    @Override
//    public void persistent() throws CoreException {
//        Byte encode = getEncode();
//        if (encode == null)
//            throw new CoreException(NetErrors.ERROR_RPC_ENCODER_NULL, "Encoder is null for persistent,service_class_method: " + RpcCacheManager.getInstance().getMethodByCrc(crc));
//        switch (encode) {
//            case ENCODE_JAVABINARY:
//                ByteArrayOutputStream baos = null;
//                DataOutputStreamEx dos = null;
//                try {
//                    baos = new ByteArrayOutputStream();
//                    dos = new DataOutputStreamEx(baos);
//                    dos.getDataOutputStream().writeByte(version);
//                    dos.getDataOutputStream().writeLong(crc);
//
//                    byte[] returnBytes = null;
//                    if (returnObject != null) {
//                        if(returnObject instanceof byte[]) {
//                            dos.getDataOutputStream().writeByte(MethodRequest.ARGUMENT_TYPE_BYTES);
//                            returnBytes = (byte[]) returnObject;
//                            dos.getDataOutputStream().writeInt(returnBytes.length);
//                            dos.getDataOutputStream().write(returnBytes);
//                        } else if(returnObject instanceof BinarySerializable) {
//                            dos.getDataOutputStream().writeByte(MethodRequest.ARGUMENT_TYPE_JAVA_BINARY);
//                            BinarySerializable binarySerializable = (BinarySerializable) returnObject;
//                            try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream()) {
//                                binarySerializable.persistent(byteArrayOutputStream);
//
//                                byte[] finalBytes = byteArrayOutputStream.toByteArray();
//                                dos.getDataOutputStream().writeInt(finalBytes.length);
//                                dos.getDataOutputStream().write(finalBytes);
//                            }
//                        } else {
//                            dos.getDataOutputStream().writeByte(MethodRequest.ARGUMENT_TYPE_JSON);
//
//                            String returnStr = null;
//                            if(returnTmpStr == null) {
//                                returnStr = JSON.toJSONString(returnObject, SerializerFeature.DisableCircularReferenceDetect);
//                            } else {
//                                returnStr = returnTmpStr;
//                            }
////                            String returnStr = Objects.requireNonNullElseGet(returnTmpStr, () -> JSON.toJSONString(returnObject, SerializerFeature.DisableCircularReferenceDetect));
//                            dos.getDataOutputStream().writeUTF(returnStr);
//                        }
//                    } else {
//                        dos.getDataOutputStream().writeByte(MethodRequest.ARGUMENT_TYPE_NONE);
//                    }
//
//                    byte[] exceptionBytes = null;
//                    if (exception != null) {
//                        JSONObject json = new JSONObject();
//                        json.put("code", exception.getCode());
//                        json.put("message", exception.getMessage());
////                        json.put("logLevel", exception.getLogLevel());
//                        String errorStr = json.toJSONString();//JSON.toJSONString(exception);
//                        exceptionBytes = errorStr.getBytes(StandardCharsets.UTF_8);
//                    }
//                    if (exceptionBytes != null) {
//                        dos.getDataOutputStream().writeInt(exceptionBytes.length);
//                        dos.getDataOutputStream().write(exceptionBytes);
//                    } else {
//                        dos.getDataOutputStream().writeInt(0);
//                    }
//
//                    byte[] bytes = baos.toByteArray();
//                    setData(GZipUtils.compress(bytes));
//                    setEncode(ENCODE_JAVABINARY);
//                    setType(MethodRequest.RPCTYPE);
//                } catch (Throwable t) {
//                    t.printStackTrace();
//                    throw new CoreException(NetErrors.ERROR_RPC_ENCODE_FAILED, "PB parse data failed, " + t.getMessage() + ",service_class_method: " + RpcCacheManager.getInstance().getMethodByCrc(crc));
//                } finally {
//                    if(baos != null)
//                        IOUtils.closeQuietly(baos);
//                    if(dos != null)
//                        IOUtils.closeQuietly(dos.original());
//                }
//                break;
//            default:
//                throw new CoreException(NetErrors.ERROR_RPC_ENCODER_NOTFOUND, "Encoder type doesn't be found for persistent,service_class_method: " + RpcCacheManager.getInstance().getMethodByCrc(crc));
//        }
//    }

    public Long getCrc() {
        return crc;
    }

    public void setCrc(Long crc) {
        this.crc = crc;
    }

    public Object getReturnObject() {
        return returnObject;
    }

    public void setReturnObject(Object returnObject) {
        this.returnObject = returnObject;
    }

    public CoreException getException() {
        return exception;
    }

    public void setException(CoreException exception) {
        this.exception = exception;
    }

    public byte getVersion() {
        return version;
    }

    public void setVersion(byte version) {
        this.version = version;
    }

    public String getReturnTmpStr() {
        return returnTmpStr;
    }

    public void setReturnTmpStr(String returnTmpStr) {
        this.returnTmpStr = returnTmpStr;
    }


}
