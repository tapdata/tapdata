package com.tapdata.tm.externalStorage.vo;

import com.tapdata.tm.ds.entity.DataSourceEntity;
import com.tapdata.tm.utils.MessageUtil;
import lombok.Data;

import java.util.List;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2026/6/4 10:19 Create
 * @description
 */
@Data
public class ConnectionSharedCDCEnable {
    String msg;
    List<ConnectionInfo> connections;

    public static ConnectionSharedCDCEnable empty() {
        return instance("Empty");
    }

    public static ConnectionSharedCDCEnable succeed() {
        return instance(MessageUtil.getMessage("connection.share.cdc.enable.succeed"));
    }

    public static ConnectionSharedCDCEnable tip(List<DataSourceEntity> all) {
       String msg = MessageUtil.getMessage("connection.share.cdc.enable.tip");
       ConnectionSharedCDCEnable info = instance(msg);
        List<ConnectionInfo> list = all.stream().map(e -> {
            ConnectionInfo connectionInfo = new ConnectionInfo();
            connectionInfo.setName(e.getName());
            connectionInfo.setId(e.getId().toHexString());
            return connectionInfo;
        }).toList();
        info.setConnections(list);
       return info;
    }

    static ConnectionSharedCDCEnable instance(String msg) {
        ConnectionSharedCDCEnable connectionSharedCDCEnable = new ConnectionSharedCDCEnable();
        connectionSharedCDCEnable.setMsg(msg);
        return connectionSharedCDCEnable;
    }

    @Data
    public static class ConnectionInfo {
        String name;
        String id;
    }
}
