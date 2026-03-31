package org.genom.reportservice.interfaces;

import com.gnm.interfaces.RowXlsCreator;
import lombok.SneakyThrows;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.Row;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.gnm.utils.NumberUtils.NUMERIC_PATTERN;

public interface RowXlsCreatorEditorInt extends RowXlsCreator {
    Pattern NUMBER_PATTERN = Pattern.compile(NUMERIC_PATTERN);
    String REGEX = "(#\\{[^}]+})";
    Pattern PATTERN_EL = Pattern.compile(REGEX);

    @Override
    default void editVal(Row row, int cellIdx, String value) {
        /*Cell cell = row.getCell(cellIdx);
        if (cell == null)
            return;
        if (value != null) {
            cell.setCellValue(value);
        } else
            cell.setCellValue("");*/
        Cell cell = row.getCell(cellIdx);
        if (NUMBER_PATTERN.matcher(value).find()) {
            editVal(row, cellIdx, Double.valueOf(value));
        } else if (value.isEmpty()) {
            cell.setCellValue("");
            cell.setCellType(CellType.BLANK);
        } else {
            cell.setCellValue(value);
        }
    }

    @Override
    default void editVal(Row row, int cellIdx, Boolean value) {
        Cell hCell = row.getCell(cellIdx);
        if (hCell == null || value == null)
            return;
        hCell.setCellValue(value);
    }

    @Override
    default void editVal(Row row, int cellIdx, Double value) {
        Cell hCell = row.getCell(cellIdx);
        if (hCell == null)
            return;
        if (value != null) {
            if (value != 0.d) {
                if (value.isNaN()) {
                    hCell.setCellValue(0.0);
                } else {
                    hCell.setCellValue(value);
                }
            } else
                hCell.setCellValue(0.0);
        } else
            hCell.setCellValue(0.0);
    }

    default void evalRow(Row row, String nameBean) {
        for (Cell cell : row) {
            evalCell(cell, nameBean);
        }
    }

    default void evalHeaderRow(Row row, String nameBean) {
        for (Cell cell : row) {
            if (CellType.STRING.equals(cell.getCellType())) {

                Matcher matcher = PATTERN_EL.matcher(cell.getStringCellValue());
                StringBuilder stringBuilder = new StringBuilder();
                while (matcher.find()) {
                    String result = (String) evalString(matcher.group(1), nameBean);
                    matcher.appendReplacement(stringBuilder, result);
                }
                matcher.appendTail(stringBuilder);
                cell.setCellValue(stringBuilder.toString());
            }

        }
    }


    default void evalCell(Cell cell, String nameBean) {
        Object eval = eval(cell, nameBean);

        if (eval != null) {
            switch (cell.getCellType()) {
                case NUMERIC -> {
                    editVal(cell.getRow(), cell.getColumnIndex(), (Double) eval);
                }
                case STRING -> {
                    editVal(cell.getRow(), cell.getColumnIndex(), String.valueOf(eval));
                }
            }
        } else {
            evalNullFillAction(cell);
        }
    }

    default void evalNullFillAction(Cell cell) {
        if (CellType.STRING.equals(cell.getCellType()))
            cell.setCellValue("");
    }


    private Object evalString(String value, String nameBean) {
        String s = value.replaceAll("#\\{" + nameBean + ".", "")
                .replaceAll("}", "")
                .replaceAll("\\(\\)", "").toLowerCase();
        return Arrays.stream(this.getClass().getMethods())
                .filter(method -> method.getName().replaceAll("get", "").toLowerCase().equals(s) || method.getName().toLowerCase().equals(s))
                .findFirst()
                .map(this::invokeMethod)
                .orElse(null);
    }

    default Object eval(Cell cell, String nameBean) {
        if (CellType.STRING.equals(cell.getCellType())) {
            String stringCellValue = cell.getStringCellValue();
            return evalString(cell.getStringCellValue(), nameBean);

        }
        return null;
    }

    @SneakyThrows
    default Object invokeMethod(Method method) {
        return method.invoke(this, null);
    }
}
