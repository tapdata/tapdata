package io.tapdata.connector.dameng.cdc.logminer.constant;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JNIUtil {
    private static final Map<String, String> LOAD_LIB_MAP = new HashMap<String, String>();

    public static final String DM_LIBRARY_PATH = "dm.library.path";

    public static final String JNI_AUDIT_DLL = "AuditDll";

    public static final String JNI_CONNECT_DLL = "ConnectDLL";

    public static final String JNI_BACKUP_DLL = "BackupDLL";

    public static final String JNI_DRMAN_DLL = "DrmanDLL";

    public static final String JNI_CRYPT_DLL = "CryptDll";

    public static final String JNI_DMMON_DLL = "DmMonDLL";

    public static final String JNI_DMRWM_DLL = "DmRwmDLL";

    public static final String JNI_DW_DLL = "DwDLL";

    public static final String JNI_DWMON_DLL = "DwMonDLL";

    public static final String JNI_LICREAD_DLL = "LicReadDll";

    public static final String JNI_MPPMON_DLL = "MppMonDLL";

    public static final String JNI_PARA_DLL = "ParaDLL";

    public static final String JNI_RACMON_DLL = "RacMonDLL";

    public static final String JNI_CSSM_DLL = "CssMonDLL";

    public static final String JNI_ASM_DLL = "DmASMDLL";

    public static final String JNI_INSTANCE_DLL = "Instance";

    public static final String JNI_IMPEXP_DLL = "ImpExpDLL";

    public static final String JNI_LOGMNR_DLL = "LogmnrDll";

    public static final int OS_UNKNOW = -1;

    public static final int OS_WINDOWS = 1;

    public static final int OS_LINUX = 2;

    public static final int OS_SOLARIS = 3;

    public static final int OS_AIX = 4;

    public static final int OS_HP_UX = 5;

    public static final int OS_FREE_BSD = 6;

    public static final int OS_MAC = 7;

    public static final int CURRENT_OS = getOS();

    public static final String EMPTY = "";

    public static int getOS() {
        String osname = getOSName();
        if (osname.startsWith("Win"))
            return 1;
        if (osname.startsWith("Linux"))
            return 2;
        if (osname.startsWith("AIX"))
            return 4;
        if (osname.startsWith("Solaris") || osname.startsWith("Sun"))
            return 3;
        if (osname.startsWith("HP-UX"))
            return 5;
        if (osname.startsWith("FreeBSD"))
            return 6;
        if (osname.startsWith("Mac OS"))
            return 7;
        return -1;
    }

    public static String getOSName(int osType) {
        String value = "";
        switch (osType) {
            case 1:
                value = "Windows";
                break;
            case 2:
                value = "Linux";
                break;
            case 4:
                value = "AIX";
                break;
            case 3:
                value = "Solaris|Sun";
                break;
            case 5:
                value = "HP-UX";
                break;
        }
        return value;
    }

    public static boolean isWindows() {
        return (getOS() == 1);
    }

    public static String getOSName() {
        return System.getProperty("os.name");
    }

    public static String trimToEmpty(String str) {
        return (str == null) ? "" : str.trim();
    }

    public static boolean isEmpty(String str) {
        return !(str != null && str.length() != 0);
    }

    public static boolean isNotEmpty(String str) {
        return (str != null && str.length() > 0);
    }

    public static void loadSSLLibrary() {
        List<String> sslLibNameList = new ArrayList<String>();
        if (getOS() == 1) {
            sslLibNameList.add("libeay32");
            sslLibNameList.add("ssleay32");
        } else {
            sslLibNameList.add("crypto");
            sslLibNameList.add("ssl");
        }
        for (int i = 0; i < sslLibNameList.size(); i++)
            loadLibrary(sslLibNameList.get(i));
    }

    public static void loadLibrary(String libname) {
        if (LOAD_LIB_MAP.containsKey(libname))
            return;
        String libraryPath = trimToEmpty(System.getProperty("dm.library.path"));
        if (isEmpty(libraryPath)) {
            System.loadLibrary(libname);
            LOAD_LIB_MAP.put(libname, libname);
        } else {
            File libFle = new File(libraryPath, isWindows() ? (String.valueOf(libname) + ".dll") : ("lib" + libname + ".so"));
            System.load(libFle.getPath());
            LOAD_LIB_MAP.put(libname, libFle.getPath());
        }
    }

    public static void loadDmLibrarys(String libname) {
        loadDmLibrarys(libname, null);
    }

    public static void loadDmLibrarys(String libname, String[] excludeLibNames) {
        String libraryPath = trimToEmpty(System.getProperty("dm.library.path"));
        if (isEmpty(libraryPath)) {
            libraryPath = trimToEmpty(System.getProperty("java.library.path"));
            if (isEmpty(libraryPath)) {
                loadLibrary(libname);
                return;
            }
        }
        File rootDirFile = new File(libraryPath);
        if (!rootDirFile.exists()) {
            loadLibrary(libname);
            return;
        }
        List<String> libNameList = new ArrayList<String>();
        byte b;
        int i;
        File[] arrayOfFile;
        for (i = (arrayOfFile = rootDirFile.listFiles()).length, b = 0; b < i; ) {
            File tmpFile = arrayOfFile[b];
            String fileName = tmpFile.getName();
            if (isWindows()) {
                if (fileName.startsWith("d") && fileName.endsWith(".dll"))
                    libNameList.add(fileName.substring(0, fileName.length() - 4));
            } else if (fileName.startsWith("libd") && fileName.endsWith(".so")) {
                libNameList.add(fileName.substring(3, fileName.length() - 3));
            }
            b++;
        }
        if (libNameList.size() == 0) {
            loadLibrary(libname);
            return;
        }
        libNameList = sortLibNameList(libNameList);
        if (excludeLibNames != null)
            for (int j = 0; j < excludeLibNames.length; j++)
                libNameList.remove(excludeLibNames[j]);
        while (true) {
            boolean finish = false;
            List<String> loadedLibNameList = new ArrayList<String>();
            for (String tmpLibName : libNameList) {
                try {
                    loadLibrary(tmpLibName);
                    if (tmpLibName.equals(libname)) {
                        finish = true;
                        break;
                    }
                    loadedLibNameList.add(tmpLibName);
                } catch (Throwable throwable) {}
            }
            if (finish)
                break;
            try {
                loadLibrary(libname);
                break;
            } catch (Throwable e) {
                if (loadedLibNameList.size() == 0)
                    throw new RuntimeException(e);
                libNameList.removeAll(loadedLibNameList);
            }
        }
    }

    public static List<String> sortLibNameList(List<String> libNameList) {
        List<String> newList = new ArrayList<String>();
        List<String> tmpList = getLibNamesForFix(isWindows());
        for (String tmpStr : tmpList) {
            boolean flag = libNameList.remove(tmpStr);
            if (flag)
                if (!newList.contains(tmpStr))
                    newList.add(tmpStr);
        }
        for (int i = 0; i < libNameList.size(); i++) {
            if (!newList.contains(libNameList.get(i)))
                newList.add(libNameList.get(i));
        }
        removeDuplicateData(newList);
        newList.removeAll(getLibNamesForNotLoad(isWindows()));
        return newList;
    }

    public static List<String> getLibNamesForFix(boolean isWin) {
        List<String> list = new ArrayList<String>();
        list.add("dmcvt");
        list.add("dmmc");
        list.add("dmutl");
        list.add("dmelog");
        list.add("dmmsg");
        list.add("dmos");
        list.add("dmreadline");
        list.add("dmvtdsk");
        list.add("dmcpr");
        list.add("dmmem");
        list.add("dmsbtree");
        list.add("dmstrt");
        list.add("dmasmparse");
        list.add("dmcalc");
        list.add("dmclientlex");
        list.add("dmcyt");
        list.add("dmdcr");
        list.add("dmde");
        list.add("dmmout");
        list.add("dmmsg_parse");
        list.add("dmnlssort");
        list.add("dmsbt");
        list.add("dmsbtex");
        list.add("dmshm");
        list.add(isWin ? "dmshpldr_dll" : "dmshpldr");
        list.add("dmwseg");
        list.add("dwctl");
        list.add("dmcomm");
        list.add("dmcrypt");
        list.add("dmcfg");
        list.add("dmcss");
        list.add("dmcssmon");
        list.add("dmdta");
        list.add("dmfldr_comm");
        list.add(isWin ? "dmimon_dll" : "dmimon");
        list.add("dmimon_dll_java");
        list.add("dmjson");
        list.add("dmlic");
        list.add("dmregex");
        list.add("dmuthr");
        list.add(isWin ? "dcp_dll" : "dmdcp");
        list.add("dmbcast");
        list.add("dmfil");
        list.add(isWin ? "dmfldr_dll" : "dmfldr");
        list.add("dmfldr_dll_java");
        list.add("dmmal");
        list.add("dmsys");
        list.add("dmtimer");
        list.add(isWin ? "dmwmon_dll" : "dmwmon");
        list.add("dwatcher");
        list.add("dwmon");
        list.add("dmdci");
        list.add("dmdpc");
        list.add(isWin ? "dmdw_dll" : "dmdw");
        list.add("dmlnk");
        list.add(isWin ? "dmmppmon_dll" : "dmmppmon");
        list.add("dmnci");
        list.add("dmrarch");
        list.add("dmrlog");
        list.add(isWin ? "dmrwm_dll" : "dmrwm");
        list.add(isWin ? "dmrww_dll" : "dmrww");
        list.add("dmasm");
        list.add("dmasmapi");
        list.add("dmknl");
        list.add("dmstg");
        list.add("dmasvr");
        list.add("dmbtr");
        list.add("dmnsort");
        list.add("dmpara");
        list.add("dmblb");
        list.add("dmckpt");
        list.add("dmdct");
        list.add("dmllog");
        list.add("dmpif");
        list.add(isWin ? "dmrtr" : "dmrtree");
        list.add("dmscp");
        list.add("dmtbl");
        list.add("dmtrv");
        list.add("dmbcast2");
        list.add("dmbifun");
        list.add("dmhfs");
        list.add("dmredo");
        list.add("dmrps");
        list.add("dmrs");
        list.add("dmtrx");
        list.add("dmbak");
        list.add("dmcti");
        list.add(isWin ? "dmdbg_dll" : "dmdbg");
        list.add("dmlogmnr");
        list.add("dmrac");
        list.add("dmsess");
        list.add("dmaud");
        list.add("dmbrtsk");
        list.add("dmapx");
        list.add("dmbak2");
        list.add("dmopt");
        list.add("dmrep");
        list.add("dmtrc");
        list.add("dmapsvc");
        list.add("dmjob");
        list.add("dmjschdl");
        list.add("dmexe");
        list.add(isWin ? "dmrman_dll" : "dmrman");
        list.add(isWin ? "dmbackup_dll" : "dmbackup");
        list.add("dmbldr");
        return list;
    }

    public static List<String> getLibNamesForNotLoad(boolean isWin) {
        List<String> list = new ArrayList<String>();
        list.add("svc_ctl_dll");
        list.add("dmdpi");
        list.add("dmsvr");
        list.add(isWin ? "dmamon_dll" : "dmamon");
        list.add("dmlogmnr_client");
        list.add("dmp_disql_dll_java");
        list.add(isWin ? "dmcpt_dll" : "dmcpt");
        list.add("disql_dll");
        list.add(isWin ? "dmfldr_dll" : "dmfldr");
        list.add("dmfldr_dll_java");
        list.add(isWin ? "dmjmon_dll" : "dmjmon");
        list.add("dmoci");
        list.add("dmoo4o");
        list.add("dmoopi");
        list.add("dmp_dll");
        list.add("dodbc");
        list.add("doledb");
        list.add("dmocci");
        return list;
    }

    private static void removeDuplicateData(List<String> libNameList) {
        List<String> tmpList = new ArrayList<String>();
        for (int i = 0; i < libNameList.size(); i++) {
            if (!tmpList.contains(libNameList.get(i)))
                tmpList.add(libNameList.get(i));
        }
        libNameList.clear();
        libNameList.addAll(tmpList);
    }

    public static void main(String[] args) {}
}
