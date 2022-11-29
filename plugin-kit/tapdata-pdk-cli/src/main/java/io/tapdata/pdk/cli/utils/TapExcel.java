package io.tapdata.pdk.cli.utils;

import jxl.CellView;
import jxl.Workbook;
import jxl.format.Alignment;
import jxl.format.VerticalAlignment;
import jxl.write.*;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 导出Excel
 */
@SuppressWarnings("ALL")
public class TapExcel {
    List<TapCell> cells;
    /**
     * Excel文件路径
     */
    private static String filePath = "";

    List<String> columnId = new ArrayList<>();
    List<String> columnName = new ArrayList<>();
    /**
     * Excel文件名
     */
    private static String fileName = "excle.xlsx";
    private TapExcel(String savePath){
        this.fileName = savePath;
    }
    public TapExcel path(String savePath){
        this.filePath = savePath;
        return this;
    }
    public TapExcel cells(List<TapCell> cells){
        this.cells = cells;
        return this;
    }

    // 列、行、数据、格式
    public TapExcel cells(List<Map<String,Object>> cloum,Map<String,Map<String,String>> data){
        int row = 0;
        columnId = new ArrayList<>();
        columnName = new ArrayList<>();
        if (null != cloum && ! cloum.isEmpty()){
            this.cells = new ArrayList<>();
            for (int cell = 0; cell < cloum.size(); cell++) {
                Map<String, Object> objectMap = cloum.get(cell);
                Object label = objectMap.get("label");
                cells.add(TapCell.cell(row,cell,label,title()));
                columnId.add(String.valueOf(objectMap.get("id")));
                columnName.add(String.valueOf(label));
            }
            row++;
        }
        if (null!=data && !data.isEmpty()) {
            if (row == 0) {
                this.cells = new ArrayList<>();
                for (Map.Entry<String, Map<String, String>> rowData : data.entrySet()) {
                    AtomicInteger cell = new AtomicInteger(0);
                    int finalRow = row;
                    //cells.add(TapCell.cell(row,0,rowData.getKey(),title()));
                    Map<String, String> value = rowData.getValue();
                    if ( null != value){
                        value.forEach((c, d)->{
                            cells.add(TapCell.cell(finalRow,cell.get() ,d,context()));
                            cell.getAndIncrement();
                        });
                    }
                    row++;
                }
            } else {
                for (Map.Entry<String, Map<String, String>> rowData : data.entrySet()) {
                    //cells.add(TapCell.cell(row,0,rowData.getKey(),title()));
                    Map<String, String> value = rowData.getValue();
                    for (int cell = 0; cell < columnId.size(); cell++) {
                        cells.add(TapCell.cell(row,cell,null == value?"":value.get(columnId.get(cell)),context()));
                    }
                    row++;
                }
            }
        }
        return this;
    }

    public static TapExcel create(String savePath){
        return new TapExcel(savePath);
    }

    public static void export(String fileName,String savePath) {
        try {
            if (null == fileName || "".equals(fileName)){
                fileName = "excle_"+System.currentTimeMillis()+".xlsx";
            }
            TapExcel.create(savePath).export(fileName);
        } catch (Exception e) {
            //Export error
        }
    }

    private WritableCellFormat context(){
        try {
            // 加边框及居中对齐-内容（常规、居左）
            WritableFont fontBorder = new WritableFont(WritableFont.createFont("宋体"), 12, WritableFont.NO_BOLD);
            WritableCellFormat wcfBorder = new WritableCellFormat(fontBorder);
            wcfBorder.setVerticalAlignment(VerticalAlignment.CENTRE);
            wcfBorder.setAlignment(Alignment.CENTRE);
            wcfBorder.setBorder(jxl.format.Border.ALL, jxl.format.BorderLineStyle.MEDIUM, jxl.format.Colour.GRAY_50);
            return wcfBorder;
        }catch (Exception e){
            return null;
        }
    }
    private WritableCellFormat title() {
        try {
            // 加边框及居中对齐-表头（加粗、居中）
            WritableFont fontBorderBt = new WritableFont(WritableFont.createFont("宋体"), 10, WritableFont.NO_BOLD);
            WritableCellFormat wcfBorderBt = new WritableCellFormat(fontBorderBt);
            wcfBorderBt.setVerticalAlignment(VerticalAlignment.CENTRE);
            wcfBorderBt.setAlignment(Alignment.CENTRE);
            wcfBorderBt.setVerticalAlignment(VerticalAlignment.CENTRE);
            wcfBorderBt.setBorder(jxl.format.Border.ALL, jxl.format.BorderLineStyle.MEDIUM, jxl.format.Colour.GRAY_50);
            return wcfBorderBt;
        }catch (Exception e){
            return null;
        }
    }
    /**
     * 导出Excel
     * @param filePath 文件路径
     * @param fileName 文件名
     */
    public void export(String fileName) throws Exception {
        File file = new File(filePath, fileName);
        OutputStream outputStream = new FileOutputStream(file);
        WritableSheet sheet = null;
        WritableWorkbook workbook = Workbook.createWorkbook(outputStream);
        CellView cellView = new CellView();
        cellView.setAutosize(true);
        if (workbook != null) {
            sheet = workbook.createSheet("sheet1", 0);
            // 设置每列宽度
            for (int nameIndex = 0; nameIndex < this.columnName.size(); nameIndex++) {
                String name = this.columnName.get(nameIndex);
                sheet.setColumnView(nameIndex, null == name? 5: name.getBytes().length + name.getBytes().length/3);
            }
            if (null != this.cells && !this.cells.isEmpty()){
                for (TapCell tapCell : this.cells) {
                    sheet.addCell(tapCell.build());
                }
            }
        }
        workbook.write();
        workbook.close();
        System.out.println(file.getAbsolutePath());
    }

}

class TapCell{
    int row;
    int cell;
    Object data;
    WritableCellFormat style;
    public TapCell(int x,int y,Object data,WritableCellFormat style){
        this.row = x;
        this.cell = y;
        this.data = data;
        this.style = style;
    }
    public static TapCell cell(int x,int y,Object data,WritableCellFormat style){
        return new TapCell(x, y, data,style);
    }
    public Label build(){
        return null != style ? new Label(cell,row,null == data?"":String.valueOf(data),style)
                :new Label(cell,row,null == data?"":String.valueOf(data));
    }
}

