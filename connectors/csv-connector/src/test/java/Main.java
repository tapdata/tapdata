import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.CSVWriter;
import com.opencsv.bean.CsvToBean;
import com.opencsv.bean.CsvToBeanBuilder;

import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

public class Main {
    public static void main(String[] args) throws Exception {
        Reader reader = new InputStreamReader(Files.newInputStream(Paths.get("/Users/jarad/Desktop/file1.csv")), "GBK");
        CSVReader csvReader = new CSVReaderBuilder(reader).build();
        CsvToBean<Map<String, Object>> csvToBean = new CsvToBeanBuilder(csvReader)
                .withSeparator(CSVWriter.DEFAULT_SEPARATOR)
                .withQuoteChar(CSVWriter.NO_QUOTE_CHARACTER)
                .build();
        List<Map<String, Object>> list = csvToBean.parse();
        list.forEach(o -> o.forEach((k,v) -> System.out.println(k + "=" + v)));
        csvReader.close();
        reader.close();
    }
}
