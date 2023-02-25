import com.alibaba.fastjson.JSON;
import io.tapdata.connector.excel.util.ExcelUtil;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;

import java.io.File;
import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException {
        Workbook wb = WorkbookFactory.create(new File("/Users/jarad/Desktop/11.xlsx"), null);
        Sheet sheet = wb.getSheetAt(0);
        sheet.getMergedRegions();
        System.out.println(sheet.getFirstRowNum());
        System.out.println(sheet.getLastRowNum());
        Row row = sheet.getRow(20);
        System.out.println(row);
//        row.forEach(cell -> {
//            System.out.println(ExcelUtil.getCellValue(cell, null));
//        });
        wb.close();
    }
}
