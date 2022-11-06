package io.tapdata.connector.csv;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import io.tapdata.common.FileSchema;
import io.tapdata.connector.csv.config.CsvConfig;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.file.TapFile;
import io.tapdata.file.TapFileStorage;
import io.tapdata.kit.EmptyKit;

import java.io.InputStreamReader;
import java.io.Reader;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class CsvSchema extends FileSchema {

    private final static String TAG = CsvSchema.class.getSimpleName();

    public CsvSchema(CsvConfig csvConfig, TapFileStorage storage) {
        super(csvConfig, storage);
    }

    public Map<String, Object> sampleFixedFileData(Map<String, TapFile> csvFileMap) throws Exception {
        CsvConfig csvConfig = (CsvConfig) fileConfig;
        Map<String, Object> sampleResult = new LinkedHashMap<>();
        String[] headers = csvConfig.getHeader().split(csvConfig.getDelimiter());
        if (EmptyKit.isEmpty(csvFileMap)) {
            putIntoMap(headers, null, sampleResult);
        } else {
            for (String path : csvFileMap.keySet().stream().sorted().collect(Collectors.toList())) {
                try (
                        Reader reader = new InputStreamReader(storage.readFile(path));
                        CSVReader csvReader = new CSVReaderBuilder(reader).build()
                ) {
                    if (csvConfig.getIncludeHeader()) {
                        csvReader.skip(csvConfig.getDataStartLine());
                    } else {
                        csvReader.skip(csvConfig.getDataStartLine() - 1);
                    }
                    String[] data = csvReader.readNext();
                    if (EmptyKit.isNotNull(data)) {
                        putIntoMap(headers, data, sampleResult);
                        break;
                    }
                }
            }
        }
        return sampleResult;
    }

    @Override
    protected void sampleOneFile(Map<String, Object> sampleResult, TapFile tapFile) {
        try (
                Reader reader = new InputStreamReader(storage.readFile(tapFile.getPath()));
                CSVReader csvReader = new CSVReaderBuilder(reader).build()
        ) {
            csvReader.skip(((CsvConfig) fileConfig).getDataStartLine() - 1);
            if (((CsvConfig) fileConfig).getIncludeHeader()) {
                String[] headers = csvReader.readNext();
                if (EmptyKit.isNull(headers)) {
                    return;
                }
                String[] data = csvReader.readNext();
                putIntoMap(headers, data, sampleResult);
            } else {
                String[] data = csvReader.readNext();
                if (EmptyKit.isNull(data)) {
                    return;
                }
                String[] headers = new String[data.length];
                for (int j = 0; j < headers.length; j++) {
                    headers[j] = "column" + (j + 1);
                }
                putIntoMap(headers, data, sampleResult);
            }
        } catch (Exception e) {
            TapLogger.error(TAG, "read csv file error!", e);
        }
    }
}
