package tests;

import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.JsonParser;
import io.tapdata.file.TapFileStorage;
import io.tapdata.file.TapFileStorageBuilder;
import io.tapdata.storage.local.LocalFileStorage;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.regex.Pattern;

public class Main {

    @Test
    public void test() throws IOException {
//        TapFileStorage storage = new TapFileStorageBuilder()
//                .withClassLoader(LocalFileStorage.class.getClassLoader()) //PDK's classloader
////				.withParams(map(entry("rootPath", "/root/")))
//                .withStorageClassName("io.tapdata.storage.local.LocalFileStorage")
//                .build();
//        String path = "/users/jarad/gj.txt";
////        InputStream is = storage.readFile(path);
////        storage.saveFile("/users/jarad/gj.txt", is, true);
////        is.close();
//        storage.delete(path);

        Set<String> set = new HashSet<>(Arrays.asList("12.doc", "34.doc", "56.xls","3412.docx", "pdpd.xls"));
        Set<String> in = new HashSet<>(Arrays.asList("*.doc", "*.docx"));
        Set<String> out = new HashSet<>(Collections.singletonList("34*"));
        matchRegs(set, in, out);
    }

    public static List<String> matchRegs(Collection<String> collection, Collection<String> includeRegs, Collection<String> excludeRegs) {
        List<String> res = new ArrayList<>();
        collection.forEach(str -> {
            if ((includeRegs == null || includeRegs.stream().anyMatch(reg -> matchReg(str, reg)))
                    && (excludeRegs == null || excludeRegs.stream().noneMatch(reg -> matchReg(str, reg)))) {
                res.add(str);
            }
        });
        res.sort(Comparator.naturalOrder());
        return res;
    }

    public static boolean matchReg(String str, String reg) {
        String newReg = reg.replaceAll("\\*", ".*");
        Pattern pattern = Pattern.compile(newReg);
        return pattern.matcher(str).matches();
    }
}
