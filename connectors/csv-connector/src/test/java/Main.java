import io.tapdata.common.FileProtocolEnum;
import io.tapdata.connector.csv.CsvSchema;
import io.tapdata.connector.csv.config.CsvConfig;
import io.tapdata.entity.utils.BeanUtils;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.file.TapFile;
import io.tapdata.file.TapFileStorage;
import io.tapdata.file.TapFileStorageBuilder;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.stream.Collectors;

public class Main {
    public static void main(String[] args) throws Exception {
        BeanUtils beanUtils = InstanceFactory.instance(BeanUtils.class); //bean util
        CsvConfig csvConfig = new CsvConfig();
        csvConfig.setFileType("csv");
        csvConfig.setProtocol("local");
        csvConfig.setFilePathString("/Users/jarad/Desktop");
        csvConfig.setIncludeRegString("*.csv");
        csvConfig.setExcludeRegString(null);
        csvConfig.setRecursive(true);
        csvConfig.setHeaderLine(1);
        csvConfig.setDataStartLine(1);
        csvConfig.setModelName("GG");
//        csvConfig.setHeader("name,value,age,date");
        Map<String, Object> dataMap = beanUtils.beanToMap(csvConfig);
        csvConfig = (CsvConfig) new CsvConfig().load(dataMap);
        String clazz = FileProtocolEnum.fromValue(csvConfig.getProtocol()).getStorage();
        TapFileStorage storage = new TapFileStorageBuilder()
                .withClassLoader(Class.forName(clazz).getClassLoader())
                .withParams(dataMap)
                .withStorageClassName(clazz)
                .build();
        Set<TapFile> csvFiles = new HashSet<>();
        for (String path : csvConfig.getFilePathSet()) {
            storage.getFilesInDirectory(path, csvConfig.getIncludeRegs(), csvConfig.getExcludeRegs(), csvConfig.getRecursive(), 10, csvFiles::addAll);
        }
        ConcurrentMap<String, TapFile> csvFileMap = csvFiles.stream().collect(Collectors.toConcurrentMap(TapFile::getPath, Function.identity()));
        CsvSchema csvSchema = new CsvSchema(csvConfig, storage);
        csvSchema.sampleFixedFileData(csvFileMap);
        storage.destroy();
    }
}
