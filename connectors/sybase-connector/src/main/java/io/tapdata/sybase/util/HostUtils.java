package io.tapdata.sybase.util;

import io.tapdata.entity.error.CoreException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;

/**
 * 修改hosts文件
 */
public abstract class HostUtils {

    public synchronized static boolean updateHostName(String hostName, String ip) throws Exception {
        if (StringUtils.isEmpty(hostName) || StringUtils.isEmpty(ip)) {
            throw new CoreException("HostName or Ip can't be null.");
        }

        if (StringUtils.isEmpty(hostName.trim()) || StringUtils.isEmpty(ip.trim())) {
            throw new CoreException("HostName or Ip can't be null.");
        }

        String splitter = " ";
        String fileName = null;

        // 判断系统
        if ("linux".equalsIgnoreCase(System.getProperty("os.name"))) {
            fileName = "/etc/hosts";
        } else {
            fileName = "C://WINDOWS//system32//drivers//etc//hosts";
        }

        // 更新设定文件
        List<?> lines = FileUtils.readLines(new File(fileName));
        List<String> newLines = new ArrayList<String>();
        boolean findFlag = false;
        boolean updateFlag = false;
        for (Object line : lines) {
            String strLine = (String) line;
            if (StringUtils.isNotEmpty(strLine) && !strLine.startsWith("#")) {
                strLine = strLine.replaceAll("/t", splitter);
                int index = strLine.toLowerCase().indexOf(hostName.toLowerCase());
                if (index != -1) {
                    String[] array = strLine.trim().split(splitter);
                    for (String name : array) {
                        if (hostName.equalsIgnoreCase(name)) {
                            findFlag = true;
                            if (array[0].equals(ip)) {
                                // 如果IP相同，则不更新
                                newLines.add(strLine);
                                break;
                            }
                            // 更新成设定好的IP地址
                            StringBuilder sb = new StringBuilder();
                            sb.append(ip);
                            for (int i = 1; i < array.length; i++) {
                                sb.append(splitter).append(array[i]);
                            }
                            newLines.add(sb.toString());
                            updateFlag = true;
                            break;
                        }
                    }

                    if (findFlag) {
                        break;
                    }
                }
            }
            newLines.add(strLine);
        }
        // 如果没有Host名，则追加
        if (!findFlag) {
            newLines.add(ip + splitter + hostName);
        }

        if (updateFlag || !findFlag) {
            // 写设定文件
            FileUtils.writeLines(new File(fileName), newLines);

            // 确认设定结果
            String formatIp = formatIpv6IP(ip);
            for (int i = 0; i < 20; i++) {
                try {
                    boolean breakFlg = false;
                    InetAddress[] addressArr = InetAddress.getAllByName(hostName);

                    for (InetAddress address : addressArr) {
                        if (formatIp.equals(address.getHostAddress())) {
                            breakFlg = true;
                            break;
                        }
                    }

                    if (breakFlg) {
                        break;
                    }
                } catch (Exception e) {
                    throw new CoreException(e.getMessage());
                }

                Thread.sleep(3000);
            }
        }

        return updateFlag;
    }

    private static String formatIpv6IP(String ipV6Addr) {
        String strRet = ipV6Addr;
        StringBuffer replaceStr;
        int iCount = 0;
        char ch = ':';

        if (StringUtils.isNotEmpty(ipV6Addr) && ipV6Addr.contains("::")) {
            for (int i = 0; i < ipV6Addr.length(); i++) {
                if (ch == ipV6Addr.charAt(i)) {
                    iCount++;
                }
            }

            if (ipV6Addr.startsWith("::")) {
                replaceStr = new StringBuffer("0:0:");
                for (int i = iCount; i < 7; i++) {
                    replaceStr.append("0:");
                }
            } else if (ipV6Addr.endsWith("::")) {
                replaceStr = new StringBuffer(":0:0");
                for (int i = iCount; i < 7; i++) {
                    replaceStr.append(":0");
                }
            } else {
                replaceStr = new StringBuffer(":0:");
                for (int i = iCount; i < 7; i++) {
                    replaceStr.append("0:");
                }
            }
            strRet = ipV6Addr.trim().replaceAll("\\:\\:", replaceStr.toString());
        }

        return strRet;
    }
}