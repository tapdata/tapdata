import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;

import java.io.File;
import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException {
        Workbook wb = WorkbookFactory.create(new File("/Users/jarad/Desktop/22.xlsx"), "gj");
        wb.getSheetAt(0);
        wb.close();
    }
}
