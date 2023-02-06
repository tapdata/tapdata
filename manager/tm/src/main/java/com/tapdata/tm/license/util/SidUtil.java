package com.tapdata.tm.license.util;

import com.tapdata.manager.common.utils.JsonUtil;
import com.tapdata.tm.utils.RC4Util;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.Configuration;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1Node;
import io.kubernetes.client.openapi.models.V1NodeList;
import io.kubernetes.client.util.Config;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import oshi.SystemInfo;
import oshi.hardware.Firmware;
import oshi.hardware.HardwareAbstractionLayer;
import oshi.hardware.NetworkIF;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class SidUtil {

    private SidUtil() {
    }

    private static final Logger logger = LoggerFactory.getLogger(SidUtil.class);

    private static final String RUN_ENV_TYPE = System.getenv("tapdata_running_env");

    public static String generatorSID() throws Exception {
        String sid;
        if (StringUtils.equalsAnyIgnoreCase(RUN_ENV_TYPE, "kubernetes")) {
            ApiClient apiClient = Config.defaultClient();
            Configuration.setDefaultApiClient(apiClient);
            CoreV1Api api = new CoreV1Api();
            V1NodeList v1NodeList = api.listNode(null, null, null, null,
                    null, null, null, null, null, null);
            List<V1Node> items = v1NodeList.getItems();
            List<String> ids = items.stream()
                    .map(item -> Objects.requireNonNull(Objects.requireNonNull(item.getStatus()).getNodeInfo()).getMachineID())
                    .collect(Collectors.toList());
            if (logger.isDebugEnabled()) {
                logger.debug("k8s machine id {}", JsonUtil.toJson(ids));
            }
            sid = RC4Util.encrypt("Gotapd8", JsonUtil.toJson(ids));
        } else {
            Map<String, String> systemFeatureMap = getSystemFeature();
            if (logger.isDebugEnabled()) {
                logger.debug("sid systemFeatureMap {}", JsonUtil.toJson(systemFeatureMap));
            }
            String queryString = createQueryString(systemFeatureMap);
            sid = DigestUtils.sha256Hex(queryString);
        }
        if (logger.isDebugEnabled()) {
            logger.debug("sid {}", sid);
        }
        return sid;
    }

    private static String createQueryString(Map<String, String> dataMap) {
        String queryStr = dataMap.entrySet().stream().sorted(Map.Entry.comparingByKey())
                .map(e -> e.getKey() + "=" + e.getValue())
                .collect(Collectors.joining("&"));
        if (logger.isDebugEnabled()) {
            logger.debug("query str {}", queryStr);
        }
        return queryStr;
    }

    /**
     * {
     *     "machine_id": "67BCB41B-BAA4-3634-8E51-B0210457E324",
     *     "manufacturer": "Apple Inc.",
     *     "product_name": "MacBookPro18,4",
     *     "version": "",
     *     "bios_vendor": "Apple Inc.",
     *     "bios_version": "",
     *     "bios_releaseDate": "",
     *     "network_macs[1]": "36:33:04:8e:83:80",
     *     "network_macs[2]": "36:33:04:8e:83:80",
     *     "network_macs[3]": "36:33:04:8e:83:84",
     *     "network_macs[4]": "36:33:04:8e:83:88",
     *     "network_macs[5]": "52:8b:36:98:db:1f",
     *     "network_macs[6]": "52:8b:36:98:db:1f",
     *     "network_macs[7]": "aa:04:6f:f9:ec:0b",
     *     "network_macs[8]": "aa:04:6f:f9:ec:0c",
     *     "network_macs[9]": "aa:04:6f:f9:ec:0d",
     *     "network_macs[10]": "aa:04:6f:f9:ec:2b",
     *     "network_macs[11]": "aa:04:6f:f9:ec:2c",
     *     "network_macs[12]": "aa:04:6f:f9:ec:2d",
     *     "network_macs[13]": "f8:4d:89:6f:8e:a3",
     *     "network_macs[14]": "fa:4d:89:6f:8e:a3"
     * }
     * @return
     */
    private static Map<String, String> getSystemFeature() {
        Map<String, String> data = new HashMap<>();

        SystemInfo systemInfo = new SystemInfo();
        HardwareAbstractionLayer hardware = systemInfo.getHardware();
        data.put("machine_id", getMachineId(hardware));
        data.put("manufacturer", getManufacturer(hardware));
        data.put("product_name", getModel(hardware));
        data.put("version", getVersion(hardware));

        Firmware firmware = hardware.getComputerSystem().getFirmware();
        data.put("bios_vendor", getBiosVendor(firmware));
        data.put("bios_version", getBiosVersion(firmware));
        data.put("bios_releaseDate", getBiosReleaseDate(firmware));

        List<String> networkMacs = getNetworkMacs(hardware);
        for (int i = 0; i < networkMacs.size(); i++) {
            data.put("network_macs[" + (i + 1) + "]", networkMacs.get(i));
        }

        return data;
    }


    private static String getMachineId(HardwareAbstractionLayer hardware) {
        return convert(hardware.getComputerSystem().getSerialNumber());
//        String machineId = "";
//
//        switch (platformEnum) {
//            case MACOS:
//                machineId = SysctlUtil.sysctl("kern.uuid", "");
//                break;
//            case WINDOWS:
//                // todo WINDIR + '\\system32\\wbem\\wmic.exe' 执行 'os get /value'， 截取 'SerialNumber = '后的信息
//                break;
//            case LINUX:
//                //
//                break;
//            default:
//                break;
//        }
//
//        return convert(machineId);
    }

    private static String getManufacturer(HardwareAbstractionLayer hardware) {
        return convert(hardware.getComputerSystem().getManufacturer());
    }

    private static String getModel(HardwareAbstractionLayer hardware) {
        return convert(hardware.getComputerSystem().getModel());
    }

    private static String getVersion(HardwareAbstractionLayer hardware) {
        String version = hardware.getComputerSystem().getBaseboard().getVersion();
        return convert(version);
    }

    private static String getBiosVendor(Firmware firmware) {
        return convert(firmware.getManufacturer());
    }

    private static String getBiosVersion(Firmware firmware) {
        return convert(firmware.getVersion());
    }

    private static String getBiosReleaseDate(Firmware firmware) {
        return convert(firmware.getReleaseDate());
    }

    private static List<String> getNetworkMacs(HardwareAbstractionLayer hardware) {
        List<NetworkIF> networkIFs = hardware.getNetworkIFs();

        return networkIFs.stream().map(NetworkIF::getMacaddr).sorted().collect(Collectors.toList());
//        List<String> macs = new ArrayList<>();
//        switch (platformEnum) {
//            case MACOS:
//                List<String> strList = ExecutingCommand.runNative("/sbin/ifconfig");
//                macs = strList.stream().filter(s -> s.contains("ether")).map(s -> s.split("\tether ")[1])
//                        .sorted().collect(Collectors.toList());
//                break;
//            case LINUX:
//            case WINDOWS:
//                break;
//            default:
//                break;
//        }
//        return macs;
    }

    private static String convert(String value) {
        return StringUtils.equalsAnyIgnoreCase(value, "unknown") ? "" : value;
    }



}
