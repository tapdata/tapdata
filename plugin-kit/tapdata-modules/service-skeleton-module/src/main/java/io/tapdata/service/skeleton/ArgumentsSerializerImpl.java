package io.tapdata.service.skeleton;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.parser.DefaultJSONParser;
import com.alibaba.fastjson.parser.Feature;
import com.alibaba.fastjson.parser.ParserConfig;
import com.alibaba.fastjson.serializer.SerializerFeature;
import io.tapdata.entity.annotations.Implementation;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.serializer.JavaCustomSerializer;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.ReflectionUtil;
import io.tapdata.entity.utils.io.BinarySerializable;
import io.tapdata.entity.utils.io.DataInputStreamEx;
import io.tapdata.entity.utils.io.DataOutputStreamEx;
import io.tapdata.modules.api.net.error.NetErrors;
import io.tapdata.modules.api.service.ArgumentsSerializer;
import io.tapdata.modules.api.service.SkeletonService;
import io.tapdata.entity.tracker.MessageTracker;
import io.tapdata.pdk.apis.entity.message.ServiceCaller;
import io.tapdata.pdk.core.utils.TapConstants;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

@Implementation(ArgumentsSerializer.class)
public class ArgumentsSerializerImpl implements ArgumentsSerializer {

	private static final String TAG = ArgumentsSerializerImpl.class.getSimpleName();

	@Override
	public Object[] argumentsFrom(DataInputStreamEx dis, ServiceCaller serviceCaller) throws IOException {
		String service = SkeletonService.SERVICE_ENGINE;

//		TapLogger.info(TAG, "argumentsFrom {}", toJson(serviceCaller));
		long crc = ReflectionUtil.getCrc(serviceCaller.getClassName(), serviceCaller.getMethod(), SkeletonService.SERVICE_ENGINE);
//		TapLogger.info(TAG, "argumentsFrom crc {}", crc);
		ServiceSkeletonAnnotationHandler serviceSkeletonAnnotationHandler = InstanceFactory.bean(ServiceSkeletonAnnotationHandler.class);
		if (serviceSkeletonAnnotationHandler == null)
			throw new CoreException(NetErrors.ERROR_METHODREQUEST_SKELETON_NULL, "Skeleton handler is not for service " + service + " on method service_class_method: " + RpcCacheManager.getInstance().getMethodByCrc(crc));
		MethodMapping methodMapping = serviceSkeletonAnnotationHandler.getMethodMapping(crc);
		if (methodMapping == null) {
			TapLogger.error(TAG, "All methodMappings: " + JSON.toJSONString(serviceSkeletonAnnotationHandler.getMethodMap().keySet()));
			throw new CoreException(NetErrors.ERROR_METHODREQUEST_METHODNOTFOUND, "Method not found by service_class_method " + RpcCacheManager.getInstance().getMethodByCrc(crc) + ",crc: " + crc);
		}
//		TapLogger.info(TAG, "argumentsFrom methodMapping {}", methodMapping);
		int argCount = dis.getDataInputStream().readInt();
		Object[] args = null;

		if (argCount > 0) {
			Type[] parameterTypes = getParameterTypes(methodMapping, argCount, crc);
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
						case ARGUMENT_TYPE_JAVA_CUSTOM:
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
								if(argumentType == ARGUMENT_TYPE_JAVA_BINARY) {
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
								} else {
									JavaCustomSerializer javaCustomSerializer = null;
									try {
										javaCustomSerializer = (JavaCustomSerializer) theClass.getConstructor().newInstance();
										try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes1)) {
											javaCustomSerializer.from(bais);
										}
									} catch (Throwable e) {
										e.printStackTrace();
										TapLogger.error(TAG, "javaCustomSerializer resurrect failed, " + e.getMessage() + " argument " + parameterTypes[i] + " from method " + methodMapping.getMethod() + " stack " + ExceptionUtils.getStackTrace(e) + " binarySerializable " + JSON.toJSONString(javaCustomSerializer));
									}
									args[i] = javaCustomSerializer;
								}
							}
							break;
						case ARGUMENT_TYPE_JSON:
							String jsonString = dis.readLongString();
							if(parameterTypes[i] != null) {
								Object value = JSON.parseObject(jsonString, parameterTypes[i], TapConstants.tapdataParserConfig, Feature.DisableCircularReferenceDetect);//parseObject(jsonString, parameterTypes[i], TapConstants.tapdataParserConfig);
								args[i] = value;
							}
							break;
						case ARGUMENT_TYPE_NONE:
							break;
					}
				}
			}
		}
		return args;
	}

	@Override
	public void argumentsTo(DataOutputStreamEx dos, ServiceCaller serviceCaller) throws IOException {
		Object[] args = serviceCaller.getArgs();

//		TapLogger.info(TAG, "argumentsTo {}", toJson(serviceCaller));
		long crc = ReflectionUtil.getCrc(serviceCaller.getClassName(), serviceCaller.getMethod(), SkeletonService.SERVICE_ENGINE);
//		TapLogger.info(TAG, "argumentsTo crc {}", crc);
		ServiceSkeletonAnnotationHandler serviceSkeletonAnnotationHandler = InstanceFactory.bean(ServiceSkeletonAnnotationHandler.class);
//		TapLogger.info(TAG, "argumentsTo serviceSkeletonAnnotationHandler {}", serviceSkeletonAnnotationHandler);
		MethodMapping methodMapping = null;
		if (serviceSkeletonAnnotationHandler != null) {
			methodMapping = serviceSkeletonAnnotationHandler.getMethodMapping(crc);
//			TapLogger.info(TAG, "argumentsTo methodMapping {}", methodMapping);
		}
		if (methodMapping != null) {
			Class<?>[] parameterTypes = methodMapping.getParameterTypes();
			if (parameterTypes != null) {
				serviceCaller.setArgCount(parameterTypes.length);
			} else {
				serviceCaller.setArgCount(0);
			}
		} else {
			if (args != null)
				serviceCaller.setArgCount(args.length);
			else
				serviceCaller.setArgCount(0);
		}
		dos.getDataOutputStream().writeInt(serviceCaller.getArgCount());

		int argCount = serviceCaller.getArgCount();
		
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
					dos.writeLongString(JSON.toJSONString(arg, SerializerFeature.DisableCircularReferenceDetect));
				}
			}
		}
	}

	@Override
	public void returnObjectTo(DataOutputStreamEx dos, Object content, String contentClass) throws IOException {
		byte[] returnBytes = null;
		if (content != null) {
			if(content instanceof byte[]) {
				dos.writeUTF(contentClass);
				dos.getDataOutputStream().writeByte(ArgumentsSerializer.ARGUMENT_TYPE_BYTES);
				returnBytes = (byte[]) content;
				dos.getDataOutputStream().writeInt(returnBytes.length);
				dos.getDataOutputStream().write(returnBytes);
			} else if(content instanceof BinarySerializable) {
				if(contentClass != null)
					dos.writeUTF(contentClass);
				else
					dos.writeUTF(content.getClass().getName());

				dos.getDataOutputStream().writeByte(ArgumentsSerializer.ARGUMENT_TYPE_JAVA_BINARY);
				BinarySerializable binarySerializable = (BinarySerializable) content;
				try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream()) {
					binarySerializable.persistent(byteArrayOutputStream);

					byte[] finalBytes = byteArrayOutputStream.toByteArray();
					dos.getDataOutputStream().writeInt(finalBytes.length);
					dos.getDataOutputStream().write(finalBytes);
				}
			} else if(content instanceof JavaCustomSerializer) {
				if(contentClass != null)
					dos.writeUTF(contentClass);
				else
					dos.writeUTF(content.getClass().getName());

				dos.getDataOutputStream().writeByte(ArgumentsSerializer.ARGUMENT_TYPE_JAVA_CUSTOM);
				JavaCustomSerializer binarySerializable = (JavaCustomSerializer) content;
				try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream()) {
					binarySerializable.to(byteArrayOutputStream);

					byte[] finalBytes = byteArrayOutputStream.toByteArray();
					dos.getDataOutputStream().writeInt(finalBytes.length);
					dos.getDataOutputStream().write(finalBytes);
				}
			} else {
				if(contentClass != null)
					dos.writeUTF(contentClass);
				else
					dos.writeUTF(content.getClass().getName());

				dos.getDataOutputStream().writeByte(ArgumentsSerializer.ARGUMENT_TYPE_JSON);

				String returnStr = JSON.toJSONString(content, SerializerFeature.DisableCircularReferenceDetect);

				dos.writeLongString(returnStr);
			}
		} else {
			dos.writeUTF(contentClass);
			dos.getDataOutputStream().writeByte(MethodRequest.ARGUMENT_TYPE_NONE);
		}
	}

	@Override
	public Object returnObjectFrom(DataInputStreamEx dis, String contentClass) throws IOException {
		return returnObjectFrom(dis, contentClass, null);
	}
	@Override
	public Object returnObjectFrom(DataInputStreamEx dis, String contentClass, MessageTracker messageTracker) throws IOException {
		Class<?> clazz = null;
		if(contentClass != null && !contentClass.equals("java.lang.Object")) {
			try {
				clazz = Class.forName(contentClass);
			} catch (ClassNotFoundException e) {
				TapLogger.debug(TAG, "contentClass {} not found", contentClass);
			}
		}
//		if(clazz == null) {
//			clazz = DataMap.class;
//		}
		Class<?> returnClass = clazz;

		Object content = null;
		byte argumentType = dis.getDataInputStream().readByte();
		switch (argumentType) {
			case MethodRequest.ARGUMENT_TYPE_BYTES:
				int length = dis.getDataInputStream().readInt();
				if(messageTracker != null) messageTracker.responseBytes(length);
				byte[] bytes2 = new byte[length];
				dis.getDataInputStream().readFully(bytes2);
				content = bytes2;
				break;
			case MethodRequest.ARGUMENT_TYPE_JAVA_BINARY:
				int length1 = dis.getDataInputStream().readInt();
				if(messageTracker != null) messageTracker.responseBytes(length1);
				byte[] bytes1 = new byte[length1];
				dis.getDataInputStream().readFully(bytes1);
//				if(returnClass == null) {
//					TapLogger.debug(TAG, "");
//					break;
//				}
				if(ReflectionUtil.canBeInitiated(returnClass)) {
					try {
						BinarySerializable binarySerializable = (BinarySerializable) returnClass.getConstructor().newInstance();
						try (ByteArrayInputStream bais1 = new ByteArrayInputStream(bytes1)) {
							binarySerializable.resurrect(bais1);
							content = binarySerializable;
						}
					} catch (Throwable e) {
						e.printStackTrace();
						TapLogger.error(TAG, "Deserialize return object(BinarySerializable) " + returnClass + " failed, " + e.getMessage());
					}
				}
				break;
			case MethodRequest.ARGUMENT_TYPE_JAVA_CUSTOM:
				int length3 = dis.getDataInputStream().readInt();
				if(messageTracker != null) messageTracker.responseBytes(length3);
				byte[] bytes3 = new byte[length3];
				dis.getDataInputStream().readFully(bytes3);
				if (returnClass != null && JavaCustomSerializer.class.isAssignableFrom(returnClass)) {
					if(ReflectionUtil.canBeInitiated(returnClass)) {
						try {
							JavaCustomSerializer binarySerializable = (JavaCustomSerializer) returnClass.getConstructor().newInstance();
							try (ByteArrayInputStream bais1 = new ByteArrayInputStream(bytes3)) {
								binarySerializable.from(bais1);
								content = binarySerializable;
							}
						} catch (Throwable e) {
							e.printStackTrace();
							TapLogger.error(TAG, "Deserialize(JavaCustomSerializer) return object " + returnClass + " failed, " + e.getMessage());
						}
					}
				}
				break;
			case MethodRequest.ARGUMENT_TYPE_JSON:
				String jsonString = dis.readLongString(messageTracker);
				if(returnClass == null || returnClass.equals(JSONObject.class)) {
					content = JSON.parse(jsonString, TapConstants.tapdataParserConfig, Feature.DisableCircularReferenceDetect);
				} else {
					content = JSON.parseObject(jsonString, returnClass, TapConstants.tapdataParserConfig, Feature.DisableCircularReferenceDetect);
				}
				break;
			case MethodRequest.ARGUMENT_TYPE_NONE:
				break;
		}

		return content;
	}

	private Type[] getParameterTypes(MethodMapping methodMapping, int argCount, long crc) {
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
