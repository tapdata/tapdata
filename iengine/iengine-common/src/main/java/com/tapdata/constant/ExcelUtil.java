package com.tapdata.constant;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.poi.EncryptedDocumentException;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.ss.util.CellRangeAddress;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ExcelUtil {

	private final static Logger logger = LogManager.getLogger(ExcelUtil.class);

	/**
	 * merge map in a sheet
	 * key: concat merge cell index, like 00 is A1 in excel
	 * value: CellRangeAddress
	 */
	private Map<String, CellRangeAddress> mergedMap;

	public Map<String, CellRangeAddress> getMergedMap() {
		return mergedMap;
	}

	private static final String INVALIDFORMATEXCEPTION_MESSAGE_REGEX = ".*Your InputStream was neither an OLE2 stream, nor an OOXML stream.*";
	private static final int EXCEL_BUFFER_SIZE = 1024;

	public static int toNumber(String name) {
		int number = 0;
		for (int i = 0; i < name.length(); i++) {
			number = number * 26 + (name.charAt(i) - ('A' - 1));
		}
		return number;
	}

	public static String toName(int number) {
		StringBuilder sb = new StringBuilder();
		while (number-- > 0) {
			sb.append((char) ('A' + (number % 26)));
			number /= 26;
		}
		return sb.reverse().toString();
	}

	public CellRangeAddress getMergedRegion(int rowIndex, int colIndex) {
		if (MapUtils.isEmpty(mergedMap)) return null;

		String key = colIndex + "-" + rowIndex;
		if (mergedMap.containsKey(key)) {
			return mergedMap.get(key);
		} else {
			return null;
		}
	}

	public int initMergedMap(Sheet sheet) {
		mergedMap = new HashMap<>();
		List<CellRangeAddress> mergedRegions = sheet.getMergedRegions();
		if (CollectionUtils.isNotEmpty(mergedRegions)) {
			for (CellRangeAddress mergedRegion : mergedRegions) {
				int firstRow = mergedRegion.getFirstRow();
				int firstCol = mergedRegion.getFirstColumn();
				int lastRow = mergedRegion.getLastRow();
				int lastCol = mergedRegion.getLastColumn();
				for (int row = firstRow; row <= lastRow; row++) {
					for (int col = firstCol; col <= lastCol; col++) {
						mergedMap.put(col + "-" + row, mergedRegion);
					}
				}
			}
		}
		return mergedRegions.size();
	}

	public Object getMergedCellValue(Sheet sheet, int rowIndex, int colIndex, FormulaEvaluator formulaEvaluator) {
		Cell cell;
		CellRangeAddress mergedRegion = getMergedRegion(rowIndex, colIndex);
		if (mergedRegion != null) {
			cell = sheet.getRow(mergedRegion.getFirstRow()).getCell(mergedRegion.getFirstColumn());
		} else {
			cell = sheet.getRow(rowIndex).getCell(colIndex, Row.MissingCellPolicy.CREATE_NULL_AS_BLANK);
		}

		Object cellValue = null;
		try {
			cellValue = getCellValue(cell, formulaEvaluator);
		} catch (Exception e) {
			logger.warn("Read cell value failed, sheet: {}, row index: {}, col index: {}, cell string: {}, will skip this cell, err: {}, stacks: {}",
					sheet.getSheetName(), rowIndex, colIndex, cell.toString(), e.getMessage(), Log4jUtil.getStackString(e));
		}

		return cellValue;
	}

	public static boolean isCellMerge(CellRangeAddress cellRangeAddress) {
		if (cellRangeAddress == null) return false;

		return cellRangeAddress.getFirstColumn() != cellRangeAddress.getLastColumn();
	}

	public static Object getCellValue(Cell cell, FormulaEvaluator formulaEvaluator) {
		switch (cell.getCellTypeEnum()) {
			case BOOLEAN:
				return cell.getBooleanCellValue();

			case STRING:
				return cell.getRichStringCellValue().getString();

			case NUMERIC:
				if (DateUtil.isCellDateFormatted(cell) || cell.getCellStyle().getDataFormat() == 58) {
					return cell.getDateCellValue();
				} else {
					return cell.getNumericCellValue();
				}
			case FORMULA:
				if (formulaEvaluator != null) {
					CellType cellType = null;
					// 当以等号开头，excel中会认为是一个函数单元格
					// 当函数无法执行、语法出错、本身值以等号开头，都会导致下面这一行报错
					// 当前处理方法，用try避免报错，如果异常，代表函数无法执行，则直接取该单元格的字符串值
					try {
						cellType = formulaEvaluator.evaluateFormulaCellEnum(cell);
					} catch (Exception e) {
					}
					if (cellType == null) {
						return "=" + cell.getCellFormula();
					}
					switch (cellType) {
						case BOOLEAN:
							return cell.getBooleanCellValue();

						case STRING:
							return cell.getRichStringCellValue().getString();

						case NUMERIC:
							if (DateUtil.isCellDateFormatted(cell)) {
								return cell.getDateCellValue();
							} else {
								return cell.getNumericCellValue();
							}
						case FORMULA:
							return cell.getCellFormula();
						case BLANK:
						case ERROR:
						case _NONE:
						default:
							return "";
					}
				}
				return cell.getCellFormula();
			case BLANK:
			case ERROR:
			case _NONE:
			default:
				return "";
		}
	}

	public static Workbook createWorkbook(InputStream inputStream, String password) throws IOException, InvalidFormatException, EncryptedDocumentException {
		if (inputStream == null) return null;

		if (StringUtils.isNotBlank(password)) {
			return WorkbookFactory.create(inputStream, password);
		} else {
			return WorkbookFactory.create(inputStream);
		}
	}
}
