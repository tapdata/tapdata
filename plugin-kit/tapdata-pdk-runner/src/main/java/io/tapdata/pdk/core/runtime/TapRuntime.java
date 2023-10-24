package io.tapdata.pdk.core.runtime;

import io.tapdata.pdk.core.implementation.ImplementationClassFactory;
import io.tapdata.pdk.core.utils.CommonUtils;

/**
 *
 */
public class TapRuntime {
    private static volatile TapRuntime instance;
    private ImplementationClassFactory implementationClassFactory;

    private static final Object lock = new int[0];

    public static TapRuntime getInstance() {
        if(instance == null) {
            synchronized (lock) {
                if(instance == null) {
                    instance = new TapRuntime();
                    instance.init();
                }
            }
        }
        return instance;
    }

    private TapRuntime() {
    }
    private void init() {
        String scanPackage = CommonUtils.getProperty("pdk_implementation_scan_package", "io.tapdata,com.tapdata");
        String[] packages = scanPackage.split(",");
        implementationClassFactory = new ImplementationClassFactory();
        implementationClassFactory.init(packages, this.getClass().getClassLoader());
    }

    public ImplementationClassFactory getImplementationClassFactory() {
        return implementationClassFactory;
    }


}
