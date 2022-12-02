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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class CsvSchema extends FileSchema {

    private final static String TAG = CsvSchema.class.getSimpleName();

    public CsvSchema(CsvConfig csvConfig, TapFileStorage storage) {
        super(csvConfig, storage);
    }

    public Map<String, Object> sampleFixedFileData(Map<String, TapFile> csvFileMap) throws Exception {
        Map<String, Object> sampleResult = new LinkedHashMap<>();
        String[] headers = fileConfig.getHeader().split(",");
        if (EmptyKit.isEmpty(csvFileMap)) {
            putIntoMap(headers, null, sampleResult);
        } else {
            for (String path : csvFileMap.keySet().stream().sorted().collect(Collectors.toList())) {
                AtomicBoolean needStop = new AtomicBoolean(false);
                storage.readFile(path, is -> {
                    try (
                            Reader reader = new InputStreamReader(is, fileConfig.getFileEncoding());
                            CSVReader csvReader = new CSVReaderBuilder(reader).build()
                    ) {
                        csvReader.skip(fileConfig.getDataStartLine() - 1);
                        String[] data = csvReader.readNext();
                        if (EmptyKit.isNotNull(data)) {
                            putIntoMap(headers, data, sampleResult);
                            needStop.set(true);
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
                if (needStop.get()) {
                    break;
                }
            }
        }
        return sampleResult;
    }

    @Override
    protected void sampleOneFile(Map<String, Object> sampleResult, TapFile tapFile) throws Exception {
        storage.readFile(tapFile.getPath(), is -> {
            try (
                    Reader reader = new InputStreamReader(is, fileConfig.getFileEncoding());
                    CSVReader csvReader = new CSVReaderBuilder(reader).build()
            ) {
                if (fileConfig.getHeaderLine() > 0) {
                    csvReader.skip(fileConfig.getHeaderLine() - 1);
                    String[] headers = csvReader.readNext();
                    if (EmptyKit.isNull(headers)) {
                        return;
                    }
                    csvReader.skip(fileConfig.getDataStartLine() - fileConfig.getHeaderLine() - 1);
                    String[] data = csvReader.readNext();
                    putIntoMap(headers, data, sampleResult);
                } else {
                    csvReader.skip(fileConfig.getDataStartLine() - 1);
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
        });
    }
}
