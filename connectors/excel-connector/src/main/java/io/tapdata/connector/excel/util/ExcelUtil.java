package io.tapdata.connector.excel.util;

import io.tapdata.kit.EmptyKit;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.ss.util.CellRangeAddress;

import java.util.*;
import java.util.stream.Collectors;

public class ExcelUtil {

    //3,5~9,12
    public static List<Integer> getSheetNumber(String reg) {
        if (EmptyKit.isBlank(reg)) {
            return Collections.emptyList();
        }
        Set<Integer> set = new HashSet<>();
        String[] arr = reg.split(",");
        Arrays.stream(arr).forEach(v -> {
            if (v.contains("~")) {
                for (int i = Integer.parseInt(v.substring(0, v.indexOf("~"))); i <= Integer.parseInt(v.substring(v.indexOf("~") + 1)); i++) {
                    set.add(i);
                }
            } else {
                set.add(Integer.parseInt(v));
            }
        });
        return set.stream().sorted(Comparator.naturalOrder()).collect(Collectors.toList());
    }

    public static List<Integer> getAllSheetNumber(int sheetNumber) {
        List<Integer> list = new ArrayList<>();
        for (int i = 1; i <= sheetNumber; i++) {
            list.add(i);
        }
        return list;
    }

    public static int getColumnNumber(String letter) {
        int res = 0;
        char[] arr = letter.toCharArray();
        for (char c : arr) {
            res = 26 * res + (int) c + 1 - (int) 'A';
        }
        return res;
    }

    public static Map<CellRangeAddress, Cell> getMergedDataMap(Sheet sheet) {
        return sheet.getMergedRegions().stream().collect(Collectors.toMap(v -> v, v -> sheet.getRow(v.getFirstRow()).getCell(v.getFirstColumn())));
    }

    public static Object getMergedCellValue(List<CellRangeAddress> mergedList, Map<CellRangeAddress, Cell> mergedDataMap, Cell cell, FormulaEvaluator formulaEvaluator) {
        if (EmptyKit.isNull(cell)) {
            return null;
        }
        if (EmptyKit.isEmpty(mergedList)) {
            return getCellValue(cell, formulaEvaluator);
        }
        for (CellRangeAddress merge : mergedList) {
            if (merge.isInRange(cell)) {
                return getCellValue(mergedDataMap.get(merge), formulaEvaluator);
            }
        }
        return getCellValue(cell, formulaEvaluator);
    }

    public static Object getCellValue(Cell cell, FormulaEvaluator formulaEvaluator) {
        if (EmptyKit.isNull(cell)) {
            return null;
        }
        switch (cell.getCellType()) {
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
                        cellType = formulaEvaluator.evaluateFormulaCell(cell);
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

    public static void main(String[] args) {
        System.out.println(getColumnNumber("BB"));
        System.out.println(getSheetNumber("7~10,11~13,17,2,5"));
    }
}
