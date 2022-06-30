/**
 * @title: FileUtil
 * @description:
 * @author lk
 * @date 2020/10/11
 */
package com.tapdata.tm.utils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.FillPatternType;
import org.apache.poi.ss.usermodel.Font;
import org.apache.poi.ss.usermodel.HorizontalAlignment;
import org.apache.poi.ss.usermodel.IndexedColors;
import org.apache.poi.ss.usermodel.VerticalAlignment;
import org.apache.poi.ss.util.CellRangeAddress;
import org.apache.poi.xssf.streaming.SXSSFCell;
import org.apache.poi.xssf.streaming.SXSSFRow;
import org.apache.poi.xssf.streaming.SXSSFSheet;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;

public class FileUtil {

	public static SXSSFWorkbook exportDataToExcel(List<String> titleList,List<List<Object>> dataList) {

		/* 1.创建一个Excel文件 */
		SXSSFWorkbook workbook = new SXSSFWorkbook();
		/* 2.创建Excel的一个Sheet */
		SXSSFSheet sheet = workbook.createSheet();
		/* 3.创建表头冻结 */
		sheet.createFreezePane(0, 1);
		/* 4.设置列宽 */
		for (int i = 0; i < titleList.size(); i++) {
			sheet.setColumnWidth((short) i, (short) 5000);
		}

		/* 5.表头字体 */
		Font headfont = workbook.createFont();
		headfont.setFontName("宋体");
		headfont.setFontHeightInPoints((short) 12);// 字体大小
		headfont.setBold(true);// 加粗
		/* 6.表头样式 */
		CellStyle cellStyle = workbook.createCellStyle();
		cellStyle.setFont(headfont);
		cellStyle.setAlignment(HorizontalAlignment.CENTER);// 左右居中
		cellStyle.setFillForegroundColor(IndexedColors.LIGHT_BLUE.getIndex());
		cellStyle.setFillPattern(FillPatternType.SOLID_FOREGROUND);

		/* 7.普通单元格字体 */
		Font font = workbook.createFont();
		font.setFontName("宋体");
		font.setFontHeightInPoints((short) 12);
		/* 8.普通单元格样式 */
		CellStyle cellStyle1 = workbook.createCellStyle();
		cellStyle1.setFont(font);
		cellStyle1.setAlignment(HorizontalAlignment.CENTER);// 左右居中
		cellStyle1.setVerticalAlignment(VerticalAlignment.CENTER);
		cellStyle1.setWrapText(true);
		/* 9. 拼装表头 */
		Iterator<String> titleRowIterator = titleList.iterator();
		int columnIndex = 0;
		SXSSFRow row = sheet.createRow(0);
		while (titleRowIterator.hasNext()) {

			String cellValue = titleRowIterator.next();
			SXSSFCell cell = row.createCell(columnIndex);
			cell.setCellType(CellType.STRING);
			cell.setCellValue(cellValue);
			cell.setCellStyle(cellStyle);
			columnIndex++;

		}
		/* 10.组织表数据 */
		Iterator<List<Object>> rowIterator = dataList.iterator();
		int rowIndex = 1;
		while (rowIterator.hasNext()) {
			List<Object> columnList = rowIterator.next();
			row = sheet.createRow(rowIndex);
			Iterator<Object> columnIterator = columnList.iterator();
			columnIndex = 0;

			int maxRowIndex = 1;
			List<Integer> mergeIndex = new ArrayList<>();
			while (columnIterator.hasNext()) {

				Object cellValue = columnIterator.next();
				if (cellValue instanceof List){
					maxRowIndex = Math.max(maxRowIndex, ((List)cellValue).size());
					for (int i = 0; i < ((List)cellValue).size(); i++) {
						Object object = ((List) cellValue).get(i);
						String value = "";
						if (object != null){
							value = object.toString();
						}
						if (i > 0){
							if (sheet.getRow(rowIndex + i) == null){
								sheet.createRow(rowIndex + i);
							}
							SXSSFRow row1 = sheet.getRow(rowIndex + i);
							SXSSFCell cell = row1.createCell(columnIndex);
							cell.setCellType(CellType.STRING);
							cell.setCellValue(value);
							cell.setCellStyle(cellStyle1);
						}else {
							SXSSFCell cell = row.createCell(columnIndex);
							cell.setCellType(CellType.STRING);
							cell.setCellValue(value);
							cell.setCellStyle(cellStyle1);
						}
					}
				}else {
					SXSSFCell cell = row.createCell(columnIndex);
					cell.setCellType(CellType.STRING);
					cell.setCellValue(cellValue.toString());
					cell.setCellStyle(cellStyle1);
					mergeIndex.add(columnIndex);
				}

				columnIndex++;

			}
			if (maxRowIndex > 1){
				for (Integer index : mergeIndex) {
					CellRangeAddress region = new CellRangeAddress(rowIndex, rowIndex + maxRowIndex - 1, index, index);
					sheet.addMergedRegion(region);
				}
			}

			sheet.createRow(rowIndex + maxRowIndex);
			CellRangeAddress region = new CellRangeAddress(rowIndex + maxRowIndex, rowIndex + maxRowIndex, 0, columnIndex);
			sheet.addMergedRegion(region);
			rowIndex += maxRowIndex + 1;
		}
		return workbook;
	}

}
