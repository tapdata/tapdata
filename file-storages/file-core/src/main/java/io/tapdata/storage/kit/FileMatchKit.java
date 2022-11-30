package io.tapdata.storage.kit;

import java.util.Collection;
import java.util.regex.Pattern;

public class FileMatchKit {

    public static boolean matchRegs(String str, Collection<String> includeRegs, Collection<String> excludeRegs) {
        return (includeRegs == null || includeRegs.stream().anyMatch(reg -> matchReg(str, reg)))
                && (excludeRegs == null || excludeRegs.stream().noneMatch(reg -> matchReg(str, reg)));
    }

    public static boolean matchReg(String str, String reg) {
        String newReg = reg.replaceAll("\\*", ".*");
        Pattern pattern = Pattern.compile(newReg);
        return pattern.matcher(str).matches();
    }

}
