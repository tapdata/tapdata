package io.tapdata.service.skeleton;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by lick on 2019/8/22.
 * Description：Used for this rpc service globally
 */
public class RpcCacheManager {
    private static RpcCacheManager instance;
    private Map<Long, String> crcMethodMap = new ConcurrentHashMap<>();

    public void putCrcMethodMap(Long crc, String value){
        crcMethodMap.put(crc, value);
    }
    public String getMethodByCrc(Long crc){
        if(crc != null){
            return crcMethodMap.get(crc);
        }
        return null;
    }
    public synchronized static RpcCacheManager getInstance() {
        if(instance == null){
            instance = new RpcCacheManager();
        }
        return instance;
    }
}
