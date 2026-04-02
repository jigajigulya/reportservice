package org.genom.reportservice.reporter;

import com.gnm.enums.DocTypeEnum;
import com.gnm.enums.EntityStatusEnum;
import com.gnm.enums.FormatDocEnum;
import com.gnm.interfaces.FormulaXLSConstructorInt;
import com.gnm.interfaces.RowXlsChecker;
import com.gnm.interfaces.RowXlsCreator;
import com.gnm.utils.CollectionUtils;
import com.gnm.utils.CommonUtils;
import com.gnm.utils.ExcelFormula;
import com.gnm.utils.NumberUtils;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import one.util.streamex.StreamEx;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbookFactory;
import org.genom.reportservice.criteria.PhytoExpertizeCriteria;
import org.genom.reportservice.interfaces.RowXlsCreatorEditorInt;
import org.genom.reportservice.model.PhytoExpertizeQualReport;
import org.genom.reportservice.model.TuberQualReport;
import org.genom.reportservice.repository.PhytoExpertizeRepository;
import org.genom.reportservice.utils.DocUtilsLite;
import org.springframework.context.annotation.Scope;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.stereotype.Component;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.util.*;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.gnm.utils.NumberUtils.isNullReturnZero;
import static java.util.Comparator.comparing;
import static java.util.Comparator.naturalOrder;
import static java.util.stream.Collectors.toList;


@Component
@Scope("prototype")
@Slf4j
public class PhytoExpQualReporter implements FormulaXLSConstructorInt, RowXlsCreatorEditorInt, RowXlsChecker {


    private final PhytoExpertizeRepository phytoExpertizeRepository;
    private final ResourceLoader resourceLoader;

    protected List<TuberQualReport> workList = new ArrayList<>();
    protected Map<Long, List<TuberQualReport>> mapQuals = new LinkedHashMap<>();
    private Iterator<Map.Entry<Long, List<TuberQualReport>>> iteratorMap;
    private Map.Entry<Long, List<TuberQualReport>> currEntry;
    protected Row lastRow;
    protected double sumTotalChecked = 0d;
    protected Map<Cell, Double> cellAvgResult = new HashMap<>();
    protected Cell currentCell;
    private List<Integer> twnRowNumRows = new ArrayList<>();
    private List<Integer> depRowNumRows = new ArrayList<>();
    private List<Integer> conRowNumRows = new ArrayList<>();
    private List<Integer> cultureNumRows = new ArrayList<>();
    private List<Integer> allNotFormulasRows = new ArrayList<>();
    private String depName = null;
    private String twnName = null;
    private String conName = null;
    private String culName = null;
    private Workbook workbook;
    private PhytoExpertizeCriteria criteria;
    protected List<TuberQualReport> useEarly = new ArrayList<>();
    protected Predicate<TuberQualReport> predicateCell;
    private FormulaEvaluator formulaEvaluator;

    @Override
    public String convertNumToColChar(int i) {
        return DocUtilsLite.colNumToColChar(i);
    }

    @Override
    public FormulaEvaluator getEvaluator(Workbook workbook) {
        return formulaEvaluator;
    }


    public PhytoExpQualReporter(PhytoExpertizeRepository phytoExpertizeRepository, ResourceLoader resourceLoader) {
        this.phytoExpertizeRepository = phytoExpertizeRepository;
        this.resourceLoader = resourceLoader;
    }


    protected boolean clauseWorkingRow(Row row) {
        return row.getRowNum() >= ROW.BEGIN_WRITE.idx;
    }


    public Row cloneExpRow(int curRow) {
        Row newRow = createNewRow(
                workbook.getSheetAt(SHEET.ORIG.idx),
                curRow,
                workbook.getSheetAt(SHEET.TEMPLATE.idx).getRow(ROW.SAMPLE_ROW.idx)
        );
        int idxReplaceFormulaCol = curRow + 1;
        Cell checkFormula = newRow.getCell(CELL.FORMULA_CHECK.idx);
        if (CellType.FORMULA.equals(checkFormula.getCellType())) {
            checkFormula.setCellFormula(checkFormula.getCellFormula().replaceAll(String.valueOf(ROW.BEGIN_WRITE.idx + 1), String.valueOf(idxReplaceFormulaCol)));
        }
        return newRow;
    }


    public Row cloneTotalRow(int curRow) {
        return createNewRow(
                workbook.getSheetAt(SHEET.ORIG.idx),
                curRow,
                workbook.getSheetAt(SHEET.TEMPLATE.idx).getRow(ROW.SAMPLE_ROW_TOTAL.idx)
        );
    }

    public Row cloneMaxTotalRow(int curRow) {
        return createNewRow(
                workbook.getSheetAt(SHEET.ORIG.idx),
                curRow,
                workbook.getSheetAt(SHEET.TEMPLATE.idx).getRow(ROW.SAMPLE_ROW_TOTAL_MAX.idx)
        );
    }


    public ByteArrayOutputStream write(InputStream stream) {
        try (XSSFWorkbook xssfWorkbook = XSSFWorkbookFactory.createWorkbook(OPCPackage.open(stream))) {
            this.workbook = xssfWorkbook;
            writeData(workbook);
            ByteArrayOutputStream b = new ByteArrayOutputStream();
            workbook.write(b);
            return b;
        } catch (IOException | InvalidFormatException e) {
            e.printStackTrace();
            return new ByteArrayOutputStream();
        }
    }

    public Workbook writeData(Workbook workbook) {
        formulaEvaluator = workbook.getCreationHelper().createFormulaEvaluator();
        mainCycle();
        formulaEvaluator.evaluateAll();
        return workbook;
    }




    @AllArgsConstructor
    @Getter
    enum ROW {
        SAMPLE_ROW(0),
        SAMPLE_ROW_TOTAL(1),
        SAMPLE_ROW_TOTAL_MAX(2),
        BEGIN_WRITE(10),
        GROUP(0),
        CODE(1),
        TOTAL(2), FIELD_ID(3);

        private int idx;
    }

    @AllArgsConstructor
    @Getter
    enum CELL {
        CONTRACTOR(8),
        REGION(9),
        TWN(10),
        DEPARTMENT(11),
        CULTURE(13),
        SAMPLES(19),
        FILL(20),
        CHECKED(21),
        BEGIN_FRESH_CELL(21),
        FORMULA_CHECK(118);

        private int idx;
    }


    @AllArgsConstructor
    @Getter
    enum SHEET {
        ORIG(0),
        TEMPLATE(1);

        private int idx;
    }


    protected void mainCycle() {
        IntStream.iterate(0, idx -> idx - ROW.BEGIN_WRITE.idx < mapQuals.size(), i -> i + 1)
                .forEach(idx -> {
                    Row row = getOrigSheet().getRow(idx);
                    if (row == null) {
                        row = cloneExpRow(idx);
                    }

                    if (clauseWorkingRow(row)) {
                        useEarly = new ArrayList<>();
                        setCurrentRowAndStream();
                        if (idx != ROW.BEGIN_WRITE.idx)
                            row = checkChangeParameters(row, false);
                        addToAllList(row);
                        setValueToTempValuesForGroupping();
                        fillRow(row);
                        calculateSumQuals();

                    } else {

                        evalHeaderRow(row,"phytoExpQualReporter");
                    }
                });
        if (!mapQuals.isEmpty()) {
            checkChangeParameters(lastRow, true);
            addFormulaTotalRow();
            addFormulaTotalMaxRow();
        }
    }



    private void fillRow(Row row) {
        for (Cell cell : row) {
            if (needFresh(cell)) freshPredicateByCell(cell);
            currentCell = cell;
            //evalmethod
            //evaluateCellFaces(currentInstance, cell);
            evalCell(cell,"phytoExpQualReporter");
            if (predicateCell != null) useEarly.addAll(getCurrentStreamExpertize()
                    .filter(this::earlyPredicate)
                    .collect(Collectors.toList()));
            lastRow = row;
        }
    }

    @Override
    public void evalNullFillAction(Cell cell) {
        RowXlsCreatorEditorInt.super.evalNullFillAction(cell);
        if (isPercentInfectedCell(cell) || isCheckedCell(cell)) {
            cell.setCellValue(0D);
        }
    }

    private boolean isCheckedCell(Cell cell) {
        return cell.getSheet()
                .getWorkbook()
                .getSheetAt(SHEET.TEMPLATE.idx)
                .getRow(ROW.SAMPLE_ROW.idx)
                .getCell(cell.getColumnIndex()).getStringCellValue().contains("checked");
    }

    private boolean earlyPredicate(TuberQualReport exp) {
        return (exp.getFieldSubjectCode() != null || exp.getFieldGroupId() != null || exp.isTotalResult())
                && notMatchLastGroupId(exp);
    }

    protected void freshPredicateByCell(Cell cell) {
        predicateCell = buildPredicateByCell(cell);
    }


    private Row checkChangeParameters(Row row, boolean isLastRow) {
        Optional<TuberQualReport> optTub = Optional.ofNullable(currEntry).map(Map.Entry::getValue).flatMap(CollectionUtils::getOrNull);
        if (optTub.isPresent()) {
            PhytoExpertizeQualReport item = (PhytoExpertizeQualReport) optTub.get();
            groupCulture(isLastRow, item);
            groupContractor(isLastRow, item);
            groupTwn(isLastRow, item);
            groupDepartment(isLastRow, item);

        }
        if (row.getRowNum() < lastRow.getRowNum()) row = cloneExpRow(lastRow.getRowNum() + 1);
        return row;
    }

    private void groupDepartment(boolean isLastRow, PhytoExpertizeQualReport item) {
        if ((!Objects.equals(depName, item.getDepartmentName()) || isLastRow) && criteria.isOnDepSums()) {
            addFormulaSumAvgRow(depRowNumRows);
            lastRow.getCell(CELL.DEPARTMENT.idx).setCellValue(depName);
            addFormulaMaxRow(depRowNumRows);
            lastRow.getCell(CELL.DEPARTMENT.idx).setCellValue(depName);
            depRowNumRows.clear();
            twnRowNumRows.clear();
            conRowNumRows.clear();
            cultureNumRows.clear();
            depName = item.getDepartmentName();
            twnName = item.getTown();
            culName = item.getCultureName();
            conName = item.getContractorName();
        }
    }

    private void groupTwn(boolean isLastRow, PhytoExpertizeQualReport item) {
        if ((!Objects.equals(twnName, item.getTown()) || isLastRow) && criteria.isOnTownSums()) {
            addFormulaSumAvgRow(twnRowNumRows);
            lastRow.getCell(CELL.TWN.idx).setCellValue(twnName);
            addFormulaMaxRow(twnRowNumRows);
            lastRow.getCell(CELL.TWN.idx).setCellValue(twnName);
            twnRowNumRows.clear();
            conRowNumRows.clear();
            cultureNumRows.clear();
            twnName = item.getTown();
            culName = item.getCultureName();
            conName = item.getContractorName();
        }
    }

    private void groupContractor(boolean isLastRow, PhytoExpertizeQualReport item) {
        if ((!Objects.equals(conName, item.getContractorName()) || isLastRow) && criteria.isOnConSums()) {
            addFormulaSumAvgRow(conRowNumRows);
            lastRow.getCell(CELL.CONTRACTOR.idx).setCellValue(conName);
            addFormulaMaxRow(conRowNumRows);
            lastRow.getCell(CELL.CONTRACTOR.idx).setCellValue(conName);
            cultureNumRows.clear();
            conRowNumRows.clear();
            culName = item.getCultureName();
            conName = item.getContractorName();
        }
    }

    private void groupCulture(boolean isLastRow, PhytoExpertizeQualReport item) {
        if (((!Objects.equals(culName, item.getCultureName()) || isLastRow) || cultureNeedGroupRow(item)) && criteria.isOnCultureSums()) {
            addFormulaSumAvgRow(cultureNumRows);
            if (!onCulGroupAndOffConGroups())
                lastRow.getCell(CELL.CONTRACTOR.idx).setCellValue(conName);
            lastRow.getCell(CELL.CULTURE.idx).setCellValue(culName);
            addFormulaMaxRow(cultureNumRows);
            lastRow.getCell(CELL.CULTURE.idx).setCellValue(culName);
            if (!onCulGroupAndOffConGroups())
                lastRow.getCell(CELL.CONTRACTOR.idx).setCellValue(conName);
            cultureNumRows.clear();
            culName = item.getCultureName();
        }
    }

    private boolean cultureNeedGroupRow(PhytoExpertizeQualReport item) {
        boolean changeContrAndOnGroupContr = (!Objects.equals(conName, item.getContractorName())) && criteria.isOnConSums();
        boolean changeDepartmentAndOnGroupDep = (!Objects.equals(depName, item.getDepartmentName())) && criteria.isOnDepSums();
        boolean changeTwnAndOnGroupTwn = (!Objects.equals(twnName, item.getTown())) && criteria.isOnTownSums();
        return changeContrAndOnGroupContr || changeDepartmentAndOnGroupDep || changeTwnAndOnGroupTwn;
    }

    private void addToAllList(Row row) {
        depRowNumRows.add(row.getRowNum() + 1);
        twnRowNumRows.add(row.getRowNum() + 1);
        conRowNumRows.add(row.getRowNum() + 1);
        cultureNumRows.add(row.getRowNum() + 1);
        allNotFormulasRows.add(row.getRowNum() + 1);
    }

    private void setValueToTempValuesForGroupping() {
        Optional<TuberQualReport> optTub = CollectionUtils.getOrNull(currEntry.getValue());
        if (optTub.isPresent()) {
            PhytoExpertizeQualReport item = (PhytoExpertizeQualReport) optTub.get();
            depName = item.getDepartmentName();
            twnName = item.getTown();
            conName = item.getContractorName();
            culName = item.getCultureName();
        }
    }


    private void addFormulaMaxRow(List<Integer> rows) {
        if (lastRow != null) {
            int idxTotalRow = lastRow.getRowNum() + 1;
            Row row = cloneMaxTotalRow(idxTotalRow);
            for (Cell cell : row) {
                if (cell.getColumnIndex() > CELL.CHECKED.idx) {
                    if (sumTotalChecked != 0D) {
                        cell.setCellFormula(constructFormulaCombine(cell, rows, ExcelFormula.MAX));
                    }
                }
            }
            lastRow = lastRow.getSheet().getRow(idxTotalRow);
        }
    }

    private void addFormulaTotalMaxRow() {
        addFormulaMaxRow(allNotFormulasRows);
    }

    protected void calculateSumQuals() {
        sumTotalChecked += isNullReturnZero(getChecked());
    }

    protected void addFormulaTotalRow() {
        addFormulaSumAvgRow(allNotFormulasRows);
    }

    protected void addFormulaSumAvgRow(List<Integer> rows) {
        if (lastRow != null) {
            int idxTotalRow = lastRow.getRowNum() + 1;
            Row row = cloneTotalRow(idxTotalRow);
            for (Cell cell : row) {
                if (CELL.FILL.getIdx() == cell.getColumnIndex() || CELL.SAMPLES.getIdx() == cell.getColumnIndex() || CELL.CHECKED.getIdx() == cell.getColumnIndex()) {
                    cell.setCellFormula(constructFormulaCombine(cell, rows, ExcelFormula.SUM));
                } else if (isPercentInfectedCell(cell)) {
                    cell.setCellFormula(getAvgMassFormulaBy(rows, cell, CELL.CHECKED.idx, true));

                }
            }
            lastRow = lastRow.getSheet().getRow(idxTotalRow);
        }
    }

    private boolean isPercentInfectedCell(Cell cell) {
        return cell.getSheet()
                .getWorkbook()
                .getSheetAt(SHEET.TEMPLATE.idx)
                .getRow(ROW.SAMPLE_ROW.idx)
                .getCell(cell.getColumnIndex()).getStringCellValue().contains("percentInfected");
    }

    protected void setAvgPercInfoToCell(Cell cell) {
        Supplier<Stream<Map.Entry<Cell, Double>>> entryStream = () -> cellAvgResult.entrySet()
                .stream()
                .filter(entry -> Objects.equals(entry.getKey().getColumnIndex(), cell.getColumnIndex()));
        if (entryStream.get().count() > 1)
            cell.setCellValue(entryStream
                    .get()
                    .mapToDouble(Map.Entry::getValue)
                    .sum() / sumTotalChecked);
    }

    protected boolean needFresh(Cell cell) {
        return cell.getColumnIndex() >= CELL.BEGIN_FRESH_CELL.getIdx();
    }

    private void setCurrentRowAndStream() {
        if (iteratorMap == null) {
            iteratorMap = mapQuals.entrySet().iterator();
        }
        if (!iteratorMap.hasNext()) {
            currEntry = null;
        }
        currEntry = iteratorMap.next();
    }

    private Sheet getOrigSheet() {
        return workbook.getSheetAt(SHEET.ORIG.idx);
    }


    public byte[] download(PhytoExpertizeCriteria criteria) {
        this.criteria = criteria;
        workList.addAll(phytoExpertizeRepository.findForQualReport(criteria));
        sortWorkListByGrouping();
        mapQuals.putAll(workList.stream()
                .collect(Collectors.groupingBy(TuberQualReport::getCropQualityId, LinkedHashMap::new, toList())));
        return work();

    }

    @SneakyThrows
    private byte[] work() {

        Resource resource = resourceLoader.getResource("classpath:/excel/" + DocTypeEnum.PHYTO_EXP_REPORT.name() + "." + FormatDocEnum.XLSX.name().toLowerCase());
        try (InputStream stream = resource.getInputStream()) {
            return write(stream).toByteArray();
        } finally {
            clearTempVars();
        }
    }

    private void clearTempVars() {
        iteratorMap = null;
    }

    public String getDate() {
        return CommonUtils.getDateFormatter()
                .format(criteria.getDateFinish());
    }

    private void sortWorkListByGrouping() {

        Comparator<? super TuberQualReport> comparator = null;
        if (criteria.isOnDepSums()) {
            comparator = comparing(val -> ((PhytoExpertizeQualReport) val).getDepartmentName(), Comparator.nullsLast(naturalOrder()));
        }
        if (criteria.isOnTownSums()) {
            Comparator comparatorTwn = comparing(val -> ((PhytoExpertizeQualReport) val).getTown(), Comparator.nullsLast(naturalOrder()));
            if (comparator == null) {
                comparator = comparatorTwn;
            } else
                comparator = comparator.thenComparing(comparatorTwn);
        }

        if (criteria.isOnConSums()) {
            Comparator compCon = comparing(val -> ((PhytoExpertizeQualReport) val).getContractorName(), Comparator.nullsLast(naturalOrder()));
            if (comparator == null) {
                comparator = compCon;
            } else
                comparator = comparator.thenComparing(compCon);
        }

        if (criteria.isOnCultureSums()) {
            Comparator comCul = comparing(val -> ((PhytoExpertizeQualReport) val).getCultureName(), Comparator.nullsLast(naturalOrder()));
            if (comparator == null) {
                comparator = comCul;
            } else
                comparator = comparator.thenComparing(comCul);
        }

        if (comparator != null)
            workList.sort(comparator);

    }

    private boolean onlyConSums() {
        return criteria.isOnConSums() && !criteria.isOnTownSums() && !criteria.isOnDepSums() && !criteria.isOnCultureSums();
    }

    private boolean onCulGroupAndOffConGroups() {
        return criteria.isOnCultureSums() && !criteria.isOnConSums();
    }


    private Cell getCellGroupId(Cell cell) {
        return cell.getRow().getSheet().getRow(getRowIdxGroupId()).getCell(cell.getColumnIndex());
    }

    private Cell getCellCode(Cell cell) {
        return cell.getRow().getSheet().getRow(getRowIdxCode()).getCell(cell.getColumnIndex());
    }

    private Cell getCellTotalMarker(Cell cell) {
        return cell.getRow().getSheet().getRow(getTotalMarkerRowIdx()).getCell(cell.getColumnIndex());
    }

    protected Predicate<TuberQualReport> buildPredicateByCell(Cell cell) {
        Cell cellGroupId = getCellGroupId(cell);
        Cell cellCode = getCellCode(cell);
        Cell cellTotalMarker = getCellTotalMarker(cell);
        Cell cellFieldId = getCellFieldId(cell);
        if (!clauseWorking(cellGroupId) && !clauseWorking(cellCode) && !clauseWorking(cellFieldId)) {
            if (clauseWorking(cellTotalMarker)) {
                return TuberQualReport::isTotalResult;
            }
            return phytoExpertizeReport -> !useEarly.contains(phytoExpertizeReport);
        } else if (clauseWorking(cellGroupId)) {
            return phytoExpertizeReport -> Objects.equals(((long) cellGroupId.getNumericCellValue()), phytoExpertizeReport.getFieldGroupId());
        } else if (clauseWorking(cellCode)) {
            return phytoExpertizeReport -> Objects.equals(((long) cellCode.getNumericCellValue()), phytoExpertizeReport.getFieldSubjectCode());
        } else
            return phytoExpertizeReport -> Objects.equals(((long) cellFieldId.getNumericCellValue()), ((PhytoExpertizeQualReport) phytoExpertizeReport).getTemplateFieldId());
    }


    public int getRowIdxGroupId() {
        return ROW.GROUP.idx;
    }

    public int getRowIdxCode() {
        return ROW.CODE.idx;
    }

    public int getTotalMarkerRowIdx() {
        return ROW.TOTAL.idx;
    }

    public DocTypeEnum getDocTypeEnum() {
        return DocTypeEnum.PHYTO_EXP_REPORT;
    }


    public int getRowFieldId() {
        return ROW.FIELD_ID.idx;
    }

    private Cell getCellFieldId(Cell cell) {
        return cell.getRow()
                .getSheet()
                .getRow(getRowFieldId())
                .getCell(cell.getColumnIndex());
    }


    protected Stream<TuberQualReport> getCurrentStreamExpertize() {
        return getCurrentStreamRow().filter(predicateCell);
    }

    protected Stream<TuberQualReport> getCurrentStreamRow() {
        return currEntry == null ? Stream.empty() : currEntry.getValue().stream();
    }

    public Long getBackFillId() {
        return getCurrentStreamRow().findFirst()
                .map(TuberQualReport::getBackFillId)
                .orElse(null);
    }

    public String getBatchNumber() {
        return getCurrentStreamRow().findFirst()
                .map(TuberQualReport::getBatchNumber)
                .orElse(null);
    }

    public String transferedFundName() {
        return getCurrentStreamRow().findFirst()
                .map(TuberQualReport::transferedFundName)
                .orElse(null);
    }


    public String prevAnalyzeName() {
        return getCurrentStreamRow().findFirst()
                .map(TuberQualReport::prevAnalyzeName)
                .orElse(null);
    }

    public String getIndicatorTemplateName() {
        return getCurrentStreamRow().findFirst()
                .map(TuberQualReport::getIndicatorTemplateName)
                .orElse(null);
    }


    public Long getCropQualityId() {
        return getCurrentStreamRow().findFirst()
                .map(TuberQualReport::getCropQualityId)
                .orElse(null);
    }

    public String getMassOrProductionReproduction() {
        return getCurrentStreamRow().findFirst()
                .map(TuberQualReport::isMassOrProductionReproduction)
                .map(val -> val ? "Да" : "Нет")
                .orElse("Нет");
    }


    public String formatDateQuality() {
        return getCurrentStreamRow().findFirst()
                .map(TuberQualReport::formatDateQuality)
                .orElse(null);
    }


    public String getContractorName() {
        return StreamEx.of(getCurrentStreamRow())
                .select(PhytoExpertizeQualReport.class)
                .findFirst()
                .map(PhytoExpertizeQualReport::getContractorName)
                .orElse(null);
    }

    public String getDepartmentName() {
        return StreamEx.of(getCurrentStreamRow())
                .select(PhytoExpertizeQualReport.class)
                .findFirst()
                .map(PhytoExpertizeQualReport::getDepartmentName)
                .orElse(null);
    }

    public String getTown() {
        return StreamEx.of(getCurrentStreamRow())
                .select(PhytoExpertizeQualReport.class)
                .findFirst()
                .map(PhytoExpertizeQualReport::getTown)
                .orElse(null);
    }


    public String getRegionItem() {
        return StreamEx.of(getCurrentStreamRow())
                .select(PhytoExpertizeQualReport.class)
                .findFirst()
                .map(PhytoExpertizeQualReport::getRegionName)
                .orElse(null);
    }

    public String getCultureName() {
        return StreamEx.of(getCurrentStreamRow())
                .select(PhytoExpertizeQualReport.class)
                .findFirst()
                .map(PhytoExpertizeQualReport::getCultureName)
                .orElse(null);
    }

    public String getSortName() {
        return StreamEx.of(getCurrentStreamRow())
                .select(PhytoExpertizeQualReport.class)
                .findFirst()
                .map(PhytoExpertizeQualReport::getSortName)
                .orElse(null);
    }

    public Integer getCropYear() {
        return StreamEx.of(getCurrentStreamRow())
                .select(PhytoExpertizeQualReport.class)
                .findFirst()
                .map(PhytoExpertizeQualReport::getCropYear)
                .orElse(null);
    }

    public Integer getHarvestYear() {
        return StreamEx.of(getCurrentStreamRow())
                .select(PhytoExpertizeQualReport.class)
                .findFirst()
                .map(PhytoExpertizeQualReport::getSeedProdForHarvestYear)
                .orElse(null);
    }

    public String getReproduction() {
        return StreamEx.of(getCurrentStreamRow())
                .select(PhytoExpertizeQualReport.class)
                .findFirst()
                .map(PhytoExpertizeQualReport::getReproduction)
                .orElse(null);
    }


    public Double getFill() {
        return getCurrentStreamRow().findFirst()
                .map(TuberQualReport::getFill)
                .orElse(null);
    }

    public Integer getSamplesCount() {
        return getCurrentStreamRow().findFirst()
                .map(TuberQualReport::getSamplesCount)
                .orElse(null);
    }


    public Double getChecked() {
        return getCurrentStreamRow().findFirst()
                .map(TuberQualReport::getChecked)
                .orElse(null);
    }


    public Double getPercentInfected() {
        Supplier<Stream<TuberQualReport>> currentStreamExpertize = this::getCurrentStreamExpertize;
        double sum = currentStreamExpertize
                .get()
                .map(TuberQualReport::getPercentInfected)
                .mapToDouble(NumberUtils::isNullReturnZero)
                .sum();
        boolean notFoundMarker = currentStreamExpertize.get().anyMatch(item -> ((PhytoExpertizeQualReport) item).getNotFound() != null && ((PhytoExpertizeQualReport) item).getNotFound());
        cellAvgResult.put(currentCell, sum * isNullReturnZero(getChecked()));
        return sum == 0D ? (notFoundMarker ? 0D : null) : Double.valueOf(sum);
    }


    public String getMeasureError() {
        return StreamEx.of(getCurrentStreamExpertize())
                .select(PhytoExpertizeQualReport.class)
                .findFirst()
                .map(PhytoExpertizeQualReport::getMeasureError)
                .orElse(null);
    }

    public String getNormsForNd() {
        return StreamEx.of(getCurrentStreamExpertize())
                .select(PhytoExpertizeQualReport.class)
                .findFirst()
                .map(PhytoExpertizeQualReport::getNormsForNd)
                .orElse(null);
    }

    public String getGost() {
        return StreamEx.of(getCurrentStreamExpertize())
                .select(PhytoExpertizeQualReport.class)
                .findFirst()
                .map(PhytoExpertizeQualReport::getGost)
                .orElse(null);
    }


    public String getStatus() {
        return StreamEx.of(getCurrentStreamRow())
                .select(PhytoExpertizeQualReport.class)
                .findFirst()
                .map(PhytoExpertizeQualReport::getStatusEnum)
                .map(EntityStatusEnum::getName)
                .orElse(null);
    }


    public boolean notMatchLastGroupId(TuberQualReport expertizeReport) {
        return true;
    }
}
