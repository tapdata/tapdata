package com.tapdata.tm.report.util;

import lombok.extern.slf4j.Slf4j;

import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
@Slf4j
public class UserDataReportUtils {

    public static String PARAM_MAX_LENGTH = System.getenv().getOrDefault("PRELOAD_SCHEMA_WAIT_TIME","100");
    private UserDataReportUtils(){
    }

    public static String generateMachineId() {
        List<String> machineIdList = new ArrayList<>();
        try {
            Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();
            while (networkInterfaces.hasMoreElements()) {
                NetworkInterface networkInterface = networkInterfaces.nextElement();
                byte[] mac = networkInterface.getHardwareAddress();
                if (mac != null) {
                    StringBuilder macAddress = new StringBuilder();
                    for (int i = 0; i < mac.length; i++) {
                        macAddress.append(String.format("%02X%s", mac[i], (i < mac.length - 1) ? "-" : ""));
                    }
                    machineIdList.add(macAddress.toString());
                }
            }
        } catch (SocketException e) {
            log.error("get mac address failed", e);
        }
        String machineId = machineIdList.toString();
        int lengthLimit = Integer.parseInt(PARAM_MAX_LENGTH);
        if (null != machineId && machineId.length() > lengthLimit){
            machineId = machineId.substring(0,lengthLimit);
        }
        return machineId;
    }
}