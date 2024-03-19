package io.tapdata.inspect;

import com.tapdata.mongo.ClientMongoOperator;
import io.tapdata.common.SettingService;
import io.tapdata.factory.InspectType;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

public class InspectFactory {

//    private enum InspectServiceInstance {
//        INSTANCE();
//
//        private final InspectService inspectService;
//
//        InspectServiceInstance() {
//            inspectService = new InspectServiceImpl();
//        }
//
//        public InspectService getInstance() {
//            return inspectService;
//        }
//    }
//    public static InspectService getInstance(ClientMongoOperator clientMongoOperator, SettingService settingService) {
//        return InspectFactory.InspectServiceInstance.INSTANCE.getInstance()
//                .setClientMongoOperator(clientMongoOperator)
//                .setSettingService(settingService);
//    }
    private static InspectService inspectService;


    public static InspectService getInstance(ClientMongoOperator clientMongoOperator, SettingService settingService) throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        if (inspectService == null) {
            synchronized (InspectFactory.class) {
                if (inspectService == null) {
                    io.tapdata.factory.InspectType inspectType = InspectType.INSPECT_SERVICE_IMPL;
                    String clazz = inspectType.getClazz();
                    Constructor<?> declaredConstructor = Class.forName(clazz).getDeclaredConstructor(inspectType.getClasses());
                    declaredConstructor.setAccessible(true);
                    Object instance = declaredConstructor.newInstance();
                    if (instance instanceof InspectService) {
                        inspectService = (InspectService) instance;
                    } else {
                        throw new IllegalArgumentException("Implementation class must be InspectImpl: " + clazz);
                    }
                    inspectService.init(clientMongoOperator,settingService);
                    return inspectService;
                }
            }
        }
        return inspectService;
    }
}
