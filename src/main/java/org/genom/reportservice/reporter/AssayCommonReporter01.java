package org.genom.reportservice.reporter;

import com.gnm.enums.*;
import com.gnm.model.common.Contractor;
import com.gnm.model.common.DepartmentStructure;
import com.gnm.model.common.MeasureUnit;
import com.gnm.model.common.geo.ClimaticZone;
import com.gnm.model.common.geo.TerTownship;
import com.gnm.model.pmon.AddDataField;
import com.gnm.model.pmon.CommonAssayReport;
import com.gnm.model.pmon.FieldByContractor;
import com.gnm.model.pmon.calc.AssayCommonReport;
import com.gnm.model.pmon.calc.CropTypeAndKindCulture;
import com.gnm.model.pmon.calc.PhytoSubjectState;
import com.gnm.model.pmon.calc.SubjectCommonReport;
import com.gnm.service.AssayService;
import com.gnm.service.ContractorService;
import com.gnm.utils.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.ss.util.CellRangeAddress;
import org.apache.poi.xssf.streaming.SXSSFCell;
import org.apache.poi.xssf.streaming.SXSSFRow;
import org.apache.poi.xssf.streaming.SXSSFSheet;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;
import org.apache.poi.xssf.usermodel.*;
import org.genom.reportservice.comparator.PhytoSubjectStateComparator;
import org.genom.reportservice.model.ContractorLite;
import org.genom.reportservice.repository.ContractorRepo;
import org.genom.reportservice.repository.ReportRep;
import org.genom.reportservice.service.ReporterService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ResourceLoader;
import org.springframework.stereotype.Component;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.format.FormatStyle;
import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.gnm.utils.NumberUtils.isNullReturnZero;
import static com.gnm.utils.NumberUtils.parseDouble;
import static org.genom.reportservice.utils.DocUtilsLite.colNumToColChar;


@Component
@RequiredArgsConstructor
public class AssayCommonReporter01 {

    public static final Logger log = LoggerFactory.getLogger(AssayCommonReporter01.class);


    private final ReporterService reporterService;
    private final ContractorRepo contractorRepo;
    private final ResourceLoader resourceLoader;




    void shiftLeftColumns(XSSFRow row, int startingIndex, int shiftCount) {

        int row_cell_count = row.getPhysicalNumberOfCells();

        for (int i = startingIndex; i <= row_cell_count; i++) {
            Cell oldCell = row.getCell(i);
            Cell newCell = row.createCell(i - shiftCount, oldCell.getCellType());
            cloneCellValue(oldCell, newCell);
        }


        for (int i = row_cell_count; i > row_cell_count - shiftCount; i--) {
            row.removeCell(row.getCell(i));
        }
    }

    void cloneCellValue(Cell oldCell, Cell newCell) { //TODO test it
        switch (oldCell.getCellType()) {
            case STRING:
                newCell.setCellValue(oldCell.getStringCellValue());
                break;
            case NUMERIC:
                newCell.setCellValue(oldCell.getNumericCellValue());
                break;
            case BOOLEAN:
                newCell.setCellValue(oldCell.getBooleanCellValue());
                break;
            case FORMULA:
                newCell.setCellFormula(oldCell.getCellFormula());
                break;
            case ERROR:
                newCell.setCellErrorValue(oldCell.getErrorCellValue());
            case BLANK:
            case _NONE:
                break;
        }

        newCell.setCellStyle(oldCell.getCellStyle());
    }

    // AgroTreatmentTypeEnum.AGROTECHNICAL("Агротехнический"),
    // AgroTreatmentTypeEnum.AVIATION("Авиационный"),
    // AgroTreatmentTypeEnum.GROUND("Наземный"),
    // AgroTreatmentTypeEnum.TOXIC("Обработка складов"),
    // AgroTreatmentTypeEnum.SEED_TOXIC("Обработка семян");

    // AgroTreatmentTypeDetailEnum.BIOLOGICAL("Биологический")
    // AgroTreatmentTypeDetailEnum.CHEMICAL("Химический");

    private double calculateProtectionPhysicalArea(AssayCommonReport assayCommonReport, List<PhytoSubjectState> subjectStates, List<AgroTreatmentTypeEnum> agroTreatmentType, List<AgroTreatmentTypeDetailEnum> agroTreatmentTypeDetail) {
        return helpCalculatePhysicalProtectionArea(
                assayCommonReport,
                subjectStates,
                agroTreatmentType,
                agroTreatmentTypeDetail,
                sub -> sub.getPhysicalProtectedArea(assayCommonReport)
        );
    }

    private double calculateProtectionPhysicalAreaWoTreatmentPerc(AssayCommonReport assayCommonReport, List<PhytoSubjectState> subjectStates, List<AgroTreatmentTypeEnum> agroTreatmentType, List<AgroTreatmentTypeDetailEnum> agroTreatmentTypeDetail) {
        return helpCalculatePhysicalProtectionArea(
                assayCommonReport,
                subjectStates,
                agroTreatmentType,
                agroTreatmentTypeDetail,
                sub -> isNullReturnZero(assayCommonReport.getCropFieldArea())
        );
    }

    private double helpCalculatePhysicalProtectionArea(AssayCommonReport assayCommonReport,
                                                       List<PhytoSubjectState> subjectStates,
                                                       List<AgroTreatmentTypeEnum> agroTreatmentType,
                                                       List<AgroTreatmentTypeDetailEnum> agroTreatmentTypeDetail,
                                                       Function<SubjectCommonReport, Double> functionAreaConverter) {
        return assayCommonReport.getSubjectsProtectionByType(agroTreatmentType, agroTreatmentTypeDetail)
                .stream()
                .filter(isProtectedPredicate(subjectStates))
                .map(functionAreaConverter)
                .max(Double::compareTo)
                .orElse(0D);
    }

    private Predicate<SubjectCommonReport> isProtectedPredicate(List<PhytoSubjectState> subjectStates) {
        return sub -> Objects.equals(PhytoProtectionTypeEnum.PROCESSED.ordinal(), sub.getProtectionType())
                && Stream.ofNullable(sub.getProtectionProcessedTypes())
                .flatMap(Collection::stream)
                .map(SubjectCommonReport::getCode)
                .anyMatch(code -> subjectStates.stream().map(PhytoSubjectState::getPhytosubjectCode).filter(Objects::nonNull).anyMatch(codeState -> Objects.equals(codeState, code)));
    }

    public Map<String, String> makeSheetReportCommonAssayState(XSSFWorkbook workbook, SXSSFWorkbook sxs_workbook, XSSFSheet sheet, XSSFSheet sheet_template, List<AssayCommonReport> assays, PhytoTypeEnum phytoType, CommonAssayReport report, List<PhytoSubjectState> weedsIn, List<PhytoSubjectState> diseasesIn, List<PhytoSubjectState> pestsIn) throws IOException {
        Chronograph.start(4);

        try {
            log.info("TRACE makeSheetReportCommonAssayState BEGIN [assays " + assays.size() + " weedsIn " + weedsIn.size() + " diseasesIn " + diseasesIn.size() + " pestsIn " + pestsIn.size() + "]");
        } catch (Exception ex) {
            log.info("TRACE makeSheetReportCommonAssayState BEGIN EXCEPTION " + ex.getMessage());
        }

        if (CollectionUtils.isNullOrEmpty(assays)) {
            log.info("cancel , assays is empty");
            return Map.of("Не найдено данных удовлетворяющим критериям фильтрации", "Попробуйте еще раз указав другие параметры");
        } else {


            Supplier<Stream<AssayCommonReport>> streamSupplier = assays::stream;
            LocalDateTime dateBegin = report.getDateBegin();
            LocalDateTime dateEnd = report.getDateEnd();
            List<TerTownship> townships = report.getSelectedTownships();
            Collection<ClimaticZone> zones = report.getSelectedZones();

            PhytoSubjectStateComparator pss_comp = new PhytoSubjectStateComparator();

            List<PhytoSubjectState> weeds = report.getSplitSubjectTypes() ? (PhytoTypeEnum.WEEDS.equals(phytoType) ? weedsIn : new ArrayList<>()) : weedsIn;
            List<PhytoSubjectState> diseases = report.getSplitSubjectTypes() ? (PhytoTypeEnum.DISEASES.equals(phytoType) ? diseasesIn : new ArrayList<>()) : diseasesIn;
            List<PhytoSubjectState> pests = report.getSplitSubjectTypes() ? (PhytoTypeEnum.PESTS.equals(phytoType) ? pestsIn : new ArrayList<>()) : pestsIn;
            Map<Long, SubjectCommonReport> subjectsMaxValues = new HashMap<>();
            Map<Long, SubjectCommonReport> subjectsMaxDamageValues = new HashMap<>();
            List<String> templateFields = new ArrayList<>(report.getSelectedTemplateFields());


            weeds.sort(pss_comp);
            diseases.sort(pss_comp);
            pests.sort(pss_comp);

            sheet.getRow(1).getCell(1).setCellValue("Отчет по обследованиям");
            sheet.getRow(2).getCell(1).setCellValue("На период - " +
                    DateTimeFormatter.ofLocalizedDate(FormatStyle.MEDIUM).withLocale(new Locale("ru")).format(dateBegin) + " по " +
                    DateTimeFormatter.ofLocalizedDate(FormatStyle.MEDIUM).withLocale(new Locale("ru")).format(dateEnd));

            XSSFRow row_assay = sheet_template.getRow(6);
            XSSFRow row_township = sheet_template.getRow(7);
            XSSFRow row_zone = sheet_template.getRow(9);
            XSSFRow row_total = sheet_template.getRow(10);
            XSSFRow row_total_max = sheet_template.getRow(11);


            CreationHelper createHelper = workbook.getCreationHelper();

            Font def_font = row_assay.getCell(2).getCellStyle().getFont();
//            def_font.setFontName("Arial");
//            def_font.setFontHeight((short)9);

            CellStyle cellStyle_date = workbook.createCellStyle();
            cellStyle_date.setDataFormat(createHelper.createDataFormat().getFormat("dd.MM.yyyy"));
            cellStyle_date.setBorderTop(BorderStyle.THIN);
            cellStyle_date.setBorderBottom(BorderStyle.THIN);
            cellStyle_date.setBorderLeft(BorderStyle.THIN);
            cellStyle_date.setBorderRight(BorderStyle.THIN);
            cellStyle_date.setFont(def_font);


            CellStyle cellStyle_double = workbook.createCellStyle();
            cellStyle_double.setDataFormat(createHelper.createDataFormat().getFormat("[>=0.01]0.00;[<0.01]0.00####"));
            cellStyle_double.setBorderTop(BorderStyle.THIN);
            cellStyle_double.setBorderBottom(BorderStyle.THIN);
            cellStyle_double.setBorderLeft(BorderStyle.THIN);
            cellStyle_double.setBorderRight(BorderStyle.THIN);
            cellStyle_date.setFont(def_font);


            List<Integer> township_rows = new ArrayList<Integer>();
            List<Integer> zones_rows = new ArrayList<Integer>();
            List<Integer> all_rows_writen = new ArrayList<Integer>();

            CellCopyPolicy ccp = new CellCopyPolicy();

            boolean byZones = report.getByZones();


            int cur_row = 7;
            Long zone_id = null;
            Long twn_id = null;
            int twn_row_begin = 0;
            int zone_row_begin = 0;
            int twn_row_end = 0;
            boolean group_twn_row = false;
            boolean group_zone_row = false;
            boolean group_zone_row_begin = true;
            boolean last_row = false;

//            XSSFRow xs_newRow = null;
//            XSSFRow xs_newRowZone = null;
//            XSSFRow xs_newRowTownship = null;

            SXSSFRow sxs_newRow = null;
            SXSSFRow sxs_newRowZone = null;
            SXSSFRow sxs_newRowTownship = null;

            int col_solve_ins = 12;
            int col_main_ins = 63;


            int col_templateFields_begin = 12;

            int col_total_weeds_begin = 13;
            int col_total_ins_weeds_cnt = 13;

            int col_total_pests_begin = 26;
            int col_total_ins_pests_cnt = 12;

            int col_total_diseases_begin = 38;
            int col_total_ins_diseases_cnt = 12;


            int col_weeds_begin = 50;
            int col_ins_weed_cnt = 3;

            int col_pests_begin = 53;
            int col_ins_pest_cnt = 6;

            int col_diseases_begin = 59;
            int col_ins_disease_cnt = 5;

            int col_ins = col_main_ins;

            int col_num_total_area = 10;
            int col_num_total_plantscount = 11;

            int cnt_shift_columns = col_main_ins - col_solve_ins + 1;

            List<FieldByContractor> fields = new ArrayList<>();
            List<FieldByContractor> fieldsInfectedProtected = new ArrayList<>();


            FormulaEvaluator evaluator = sxs_workbook.getCreationHelper().createFormulaEvaluator();


            ///     Копирование заголовков из шаблонного листа

            for (int tf = 0; tf < templateFields.size(); tf++) {
                for (int cp = 0; cp < 8; cp++) {
                    XSSFRow row_tpl_header = sheet_template.getRow(3 + cp);
                    XSSFCell new_cell_tpl_01 = row_tpl_header.createCell(col_ins + 1);
                    new_cell_tpl_01.copyCellFrom(row_tpl_header.getCell(col_templateFields_begin), ccp);

                    if (cp == 1 || cp < 3) {
                        XSSFRow row_header = sheet.getRow(4 + cp);
                        XSSFCell new_cell_01 = row_header.createCell(col_ins + 1);

                        new_cell_01.copyCellFrom(row_tpl_header.getCell(col_templateFields_begin), ccp);


                        if (cp == 1) {
                            new_cell_01.setCellValue(templateFields.get(tf));
                            new_cell_tpl_01.setCellValue(templateFields.get(tf));
                        } else if (cp == 0 && tf == 0) {
                            new_cell_01.setCellValue("Доп. параметры");
                            new_cell_tpl_01.setCellValue("Доп. параметры");
                        }
                    }
                }

                col_ins += 1;
            }


            if (weeds.size() > 0) {
                for (int cp = 0; cp < 8; cp++) {
                    XSSFRow row_tpl_header = sheet_template.getRow(3 + cp);
                    XSSFRow row_header = sheet.getRow(4 + cp);

                    for (int ti = 0; ti < col_total_ins_weeds_cnt; ti++) {
                        XSSFCell new_cell_tpl = row_tpl_header.createCell(col_ins + ti + 1);
                        new_cell_tpl.copyCellFrom(row_tpl_header.getCell(col_total_weeds_begin + ti), ccp);

                        if (cp == 1 || cp < 3) {
                            XSSFCell new_cell = row_header.createCell(col_ins + ti + 1);
                            new_cell.copyCellFrom(row_tpl_header.getCell(col_total_weeds_begin + ti), ccp);
                        }
                    }

                }

                col_ins += col_total_ins_weeds_cnt;
            }


            if (pests.size() > 0) {
                for (int cp = 0; cp < 8; cp++) {
                    XSSFRow row_tpl_header = sheet_template.getRow(3 + cp);
                    XSSFRow row_header = sheet.getRow(4 + cp);

                    for (int ti = 0; ti < col_total_ins_pests_cnt; ti++) {
                        XSSFCell new_cell_tpl = row_tpl_header.createCell(col_ins + ti + 1);
                        new_cell_tpl.copyCellFrom(row_tpl_header.getCell(col_total_pests_begin + ti), ccp);

                        if (cp == 1 || cp < 3) {
                            XSSFCell new_cell = row_header.createCell(col_ins + ti + 1);
                            new_cell.copyCellFrom(row_tpl_header.getCell(col_total_pests_begin + ti), ccp);
                        }
                    }
                }

                col_ins += col_total_ins_pests_cnt;
            }


            if (diseases.size() > 0) {
                for (int cp = 0; cp < 8; cp++) {
                    XSSFRow row_tpl_header = sheet_template.getRow(3 + cp);
                    XSSFRow row_header = sheet.getRow(4 + cp);

                    for (int ti = 0; ti < col_total_ins_diseases_cnt; ti++) {
                        XSSFCell new_cell_tpl = row_tpl_header.createCell(col_ins + ti + 1);
                        new_cell_tpl.copyCellFrom(row_tpl_header.getCell(col_total_diseases_begin + ti), ccp);

                        if (cp == 1 || cp < 3) {
                            XSSFCell new_cell = row_header.createCell(col_ins + ti + 1);
                            new_cell.copyCellFrom(row_tpl_header.getCell(col_total_diseases_begin + ti), ccp);
                        }
                    }
                }

                col_ins += col_total_ins_diseases_cnt;
            }

            ///     Конец Копирование заголовков из шаблонного листа


            for (int wi = 0; wi < weeds.size(); wi++) {
                PhytoSubjectState weed = weeds.get(wi);

                for (int cp = 0; cp < 8; cp++) {
                    XSSFRow row_tpl_header = sheet_template.getRow(3 + cp);
                    XSSFCell new_cell_tpl_01 = row_tpl_header.createCell(col_ins + 1);
                    XSSFCell new_cell_tpl_02 = row_tpl_header.createCell(col_ins + 2);
                    XSSFCell new_cell_tpl_03 = row_tpl_header.createCell(col_ins + 3);

                    new_cell_tpl_01.copyCellFrom(row_tpl_header.getCell(col_weeds_begin), ccp);
                    new_cell_tpl_02.copyCellFrom(row_tpl_header.getCell(col_weeds_begin + 1), ccp);
                    new_cell_tpl_03.copyCellFrom(row_tpl_header.getCell(col_weeds_begin + 2), ccp);

                    if (cp == 1 || cp < 3) {
                        XSSFRow row_header = sheet.getRow(4 + cp);
                        XSSFCell new_cell_01 = row_header.createCell(col_ins + 1);
                        XSSFCell new_cell_02 = row_header.createCell(col_ins + 2);
                        XSSFCell new_cell_03 = row_header.createCell(col_ins + 3);


                        new_cell_01.copyCellFrom(row_tpl_header.getCell(col_weeds_begin), ccp);
                        new_cell_02.copyCellFrom(row_tpl_header.getCell(col_weeds_begin + 1), ccp);
                        new_cell_03.copyCellFrom(row_tpl_header.getCell(col_weeds_begin + 2), ccp);

                        if (cp == 1) {
                            String name_weed = "";
                            String name_weed_lat = "";
                            String mu_weed = "";

                            if (weed.getPhytoSubjectName() != null) {
                                if (StringUtils.isNotNullOrNotTrimEmpty(weed.getPhytoSubjectName())) {
                                    name_weed = weed.getPhytoSubjectName();
                                } else {
                                    name_weed = "Сорняк";
                                }

                                if (StringUtils.isNotNullOrNotTrimEmpty(weed.getPhytosubjectNameLatin())) {
                                    name_weed_lat = weed.getPhytosubjectNameLatin();
                                } else {
                                    name_weed_lat = "";
                                }

                            } else {
                                name_weed = "Сорняк";
                                name_weed_lat = "";
                            }

                            if (weed.getMeasureunitId() != null) {
                                if (StringUtils.isNotNullOrNotTrimEmpty(weed.getMeasureUnitNameShort())) {
                                    mu_weed = weed.getMeasureUnitNameShort();
                                }
                            }

                            new_cell_01.setCellValue(name_weed + (StringUtils.isNotNullOrNotTrimEmpty(name_weed_lat) ? " (" + name_weed_lat + ")" : "") + (StringUtils.isNotNullOrNotTrimEmpty(mu_weed) ? ", " + mu_weed : ""));
                            new_cell_tpl_01.setCellValue(name_weed + (StringUtils.isNotNullOrNotTrimEmpty(mu_weed) ? ", " + mu_weed : ""));
                        }
                    }
                }

                col_ins += col_ins_weed_cnt;
            }


            for (int pi = 0; pi < pests.size(); pi++) {
                PhytoSubjectState pest = pests.get(pi);

                for (int cp = 0; cp < 8; cp++) {
                    XSSFRow row_tpl_header = sheet_template.getRow(3 + cp);
                    XSSFCell new_cell_tpl_01 = row_tpl_header.createCell(col_ins + 1);
                    XSSFCell new_cell_tpl_02 = row_tpl_header.createCell(col_ins + 2);
                    XSSFCell new_cell_tpl_03 = row_tpl_header.createCell(col_ins + 3);
                    XSSFCell new_cell_tpl_04 = row_tpl_header.createCell(col_ins + 4);
                    XSSFCell new_cell_tpl_05 = row_tpl_header.createCell(col_ins + 5);
                    XSSFCell new_cell_tpl_06 = row_tpl_header.createCell(col_ins + 6);

                    new_cell_tpl_01.copyCellFrom(row_tpl_header.getCell(col_pests_begin), ccp);
                    new_cell_tpl_02.copyCellFrom(row_tpl_header.getCell(col_pests_begin + 1), ccp);
                    new_cell_tpl_03.copyCellFrom(row_tpl_header.getCell(col_pests_begin + 2), ccp);
                    new_cell_tpl_04.copyCellFrom(row_tpl_header.getCell(col_pests_begin + 3), ccp);
                    new_cell_tpl_05.copyCellFrom(row_tpl_header.getCell(col_pests_begin + 4), ccp);
                    new_cell_tpl_06.copyCellFrom(row_tpl_header.getCell(col_pests_begin + 5), ccp);

                    if (cp < 3) {
                        XSSFRow row_header = sheet.getRow(4 + cp);
                        XSSFCell new_cell_01 = row_header.createCell(col_ins + 1);
                        XSSFCell new_cell_02 = row_header.createCell(col_ins + 2);
                        XSSFCell new_cell_03 = row_header.createCell(col_ins + 3);
                        XSSFCell new_cell_04 = row_header.createCell(col_ins + 4);
                        XSSFCell new_cell_05 = row_header.createCell(col_ins + 5);
                        XSSFCell new_cell_06 = row_header.createCell(col_ins + 6);

                        new_cell_01.copyCellFrom(row_tpl_header.getCell(col_pests_begin), ccp);
                        new_cell_02.copyCellFrom(row_tpl_header.getCell(col_pests_begin + 1), ccp);
                        new_cell_03.copyCellFrom(row_tpl_header.getCell(col_pests_begin + 2), ccp);
                        new_cell_04.copyCellFrom(row_tpl_header.getCell(col_pests_begin + 3), ccp);
                        new_cell_05.copyCellFrom(row_tpl_header.getCell(col_pests_begin + 4), ccp);
                        new_cell_06.copyCellFrom(row_tpl_header.getCell(col_pests_begin + 5), ccp);

                        if (cp == 1) {
                            String name_pest = "";
                            String name_pest_lat = "";
                            String mu_weed = "";

                            if (pest.getPhytosubjectCode() != null) {
                                if (StringUtils.isNotNullOrNotTrimEmpty(pest.getPhytoSubjectName())) {
                                    name_pest = pest.getPhytoSubjectName();
                                } else {
                                    name_pest = "Вредитель";
                                }

                                if (StringUtils.isNotNullOrNotTrimEmpty(pest.getPhytosubjectNameLatin())) {
                                    name_pest_lat = pest.getPhytosubjectNameLatin();
                                } else {
                                    name_pest_lat = "";
                                }

                            } else {
                                name_pest = "Вредитель";
                                name_pest_lat = "";
                            }


                            String pest_full_name = name_pest + (pest.getSubjectPhaseevolutionId() != null ? ", " + pest.getSubjectPhaseEvolutionName() + "" : "") + (StringUtils.isNotNullOrNotTrimEmpty(name_pest_lat) ? " (" + name_pest_lat + ")" : "");

                            new_cell_tpl_01.setCellValue(pest_full_name);
                            new_cell_01.setCellValue(pest_full_name);
                        }

                        if (cp == 2) {
                            String pest_count = "численность" + (pest.getMeasureunitId() != null ? ", " + pest.getMeasureUnitNameShort() : "");

                            new_cell_tpl_03.setCellValue(pest_count);
                            new_cell_03.setCellValue(pest_count);
                        }
                    }

                }

                col_ins += col_ins_pest_cnt;
            }

            for (int di = 0; di < diseases.size(); di++) {
                PhytoSubjectState dis = diseases.get(di);

                for (int cp = 0; cp < 8; cp++) {
                    XSSFRow row_tpl_header = sheet_template.getRow(3 + cp);
                    XSSFCell new_cell_tpl_01 = row_tpl_header.createCell(col_ins + 1);
                    XSSFCell new_cell_tpl_02 = row_tpl_header.createCell(col_ins + 2);
                    XSSFCell new_cell_tpl_03 = row_tpl_header.createCell(col_ins + 3);
                    XSSFCell new_cell_tpl_04 = row_tpl_header.createCell(col_ins + 4);
                    XSSFCell new_cell_tpl_05 = row_tpl_header.createCell(col_ins + 5);

                    new_cell_tpl_01.copyCellFrom(row_tpl_header.getCell(col_diseases_begin), ccp);
                    new_cell_tpl_02.copyCellFrom(row_tpl_header.getCell(col_diseases_begin + 1), ccp);
                    new_cell_tpl_03.copyCellFrom(row_tpl_header.getCell(col_diseases_begin + 2), ccp);
                    new_cell_tpl_04.copyCellFrom(row_tpl_header.getCell(col_diseases_begin + 3), ccp);
                    new_cell_tpl_05.copyCellFrom(row_tpl_header.getCell(col_diseases_begin + 4), ccp);

                    if (cp == 1 || cp < 3) {
                        XSSFRow row_header = sheet.getRow(4 + cp);

                        XSSFCell new_cell_01 = row_header.createCell(col_ins + 1);
                        XSSFCell new_cell_02 = row_header.createCell(col_ins + 2);
                        XSSFCell new_cell_03 = row_header.createCell(col_ins + 3);
                        XSSFCell new_cell_04 = row_header.createCell(col_ins + 4);
                        XSSFCell new_cell_05 = row_header.createCell(col_ins + 5);

                        new_cell_01.copyCellFrom(row_tpl_header.getCell(col_diseases_begin), ccp);
                        new_cell_02.copyCellFrom(row_tpl_header.getCell(col_diseases_begin + 1), ccp);
                        new_cell_03.copyCellFrom(row_tpl_header.getCell(col_diseases_begin + 2), ccp);
                        new_cell_04.copyCellFrom(row_tpl_header.getCell(col_diseases_begin + 3), ccp);
                        new_cell_05.copyCellFrom(row_tpl_header.getCell(col_diseases_begin + 4), ccp);

                        if (cp == 1) {
                            String name_disease = "";
                            String name_disease_lat = "";

                            if (dis.getPhytosubjectCode() != null) {
                                if (StringUtils.isNotNullOrNotTrimEmpty(dis.getPhytoSubjectName())) {
                                    name_disease = dis.getPhytoSubjectName();
                                } else {
                                    name_disease = "Болезнь";
                                }
                            } else {
                                name_disease = "Болезнь";
                            }

                            if (dis.getPhytosubjectCode() != null) {
                                if (StringUtils.isNotNullOrNotTrimEmpty(dis.getPhytosubjectNameLatin())) {
                                    name_disease_lat = dis.getPhytosubjectNameLatin();
                                }
                            }

                            new_cell_01.setCellValue(name_disease + (StringUtils.isNotNullOrNotTrimEmpty(name_disease_lat) ? " (" + name_disease_lat + ")" : ""));
                        }
                    }
                }

                col_ins += col_ins_disease_cnt;
            }


            for (int scw = 4; scw <= 6; scw++) {
                shiftLeftColumns(sheet.getRow(scw), col_main_ins + 1, cnt_shift_columns);
            }

            for (int sct = 3; sct <= 10; sct++) {
                shiftLeftColumns(sheet_template.getRow(sct), col_main_ins + 1, cnt_shift_columns);
            }


            SXSSFSheet sxs_sheet = sxs_workbook.getSheet(sheet.getSheetName());

            for (int i = 0; i < assays.size(); i++) {
                AssayCommonReport assay = assays.get(i);
                if (Objects.equals(assay.getId(), 1148358L)) {
                    log.info("check");
                }
                boolean field_area_is_not_null = false;
                boolean is_protection = assay.isProtection();
                Double field_area = null;
                if (assay.getCropFieldArea() == null) {
                    if (assay.getPolygonArea() == null) {
                        field_area_is_not_null = false;
                    } else {
                        field_area = assay.getPolygonArea();
                        field_area_is_not_null = true;
                    }
                } else {
                    field_area = assay.getCropFieldArea();
                    field_area_is_not_null = true;
                }


                boolean is_new_field = true;
                boolean is_new_field_protected = true;
                boolean is_new_field_weeded = true;
                boolean is_new_field_diseased = true;
                boolean is_new_field_pested = true;

                if (FieldByContractor.isCheckFieldClause(assay)) {
                    FieldByContractor field_assay = new FieldByContractor();
                    field_assay.setContractorId(assay.getContractorId());
                    field_assay.setFieldNumber(assay.getCropFieldNumber().trim().toLowerCase());
                    field_assay.setTownId(assay.getTwnId());
                    if (report.getSplitSubjectTypes() == null || !report.getSplitSubjectTypes()) {
                        field_assay.setProtected(findProtections(assay));
                    } else {
                        field_assay.setProtected(findProtectionsByType(assay, phytoType));
                    }
                    if (!assay.isProtection()) {
                        field_assay.setWeeded(findFoundSubjectByType(assay, PhytoTypeEnum.WEEDS, weeds));
                        field_assay.setDiseased(findFoundSubjectByType(assay, PhytoTypeEnum.DISEASES, diseases));
                        field_assay.setPested(findFoundSubjectByType(assay, PhytoTypeEnum.PESTS, pests));
                    }


                    is_new_field = !fields.contains(field_assay);
                    is_new_field_protected = fieldsInfectedProtected.stream().filter(field_assay::equals).noneMatch(FieldByContractor::isProtected);
                    if (!is_new_field) {
                        if (!assay.isProtection()) {
                            is_new_field_weeded = fieldsInfectedProtected.stream().filter(field_assay::equals).noneMatch(FieldByContractor::isWeeded);
                            is_new_field_diseased = fieldsInfectedProtected.stream().filter(field_assay::equals).noneMatch(FieldByContractor::isDiseased);
                            is_new_field_pested = fieldsInfectedProtected.stream().filter(field_assay::equals).noneMatch(FieldByContractor::isPested);
                        } else {
                            is_new_field_weeded = false;
                            is_new_field_diseased = false;
                            is_new_field_pested = false;
                        }

                    }
                    if (is_new_field) {
                        field_assay.setMaxArea(field_area);
                        field_assay.setMinArea(field_area);
                        if (!assay.isProtection())
                            fields.add(field_assay);
                        fieldsInfectedProtected.add(field_assay);
                    } else {
                        FieldByContractor old_field = fields.get(fields.indexOf(field_assay));

                        if (field_area != null) {
                            if (old_field.getMaxArea() == null) {
                                old_field.setMaxArea(field_area);
                            } else {
                                old_field.setMaxArea(Math.max(old_field.getMaxArea(), field_area));
                            }

                            if (old_field.getMinArea() == null) {
                                old_field.setMinArea(field_area);
                            } else {
                                old_field.setMinArea(Math.min(old_field.getMinArea(), field_area));
                            }
                        }
                        addFieldToInfectedProtectedIfFound(fieldsInfectedProtected, field_assay);
                    }
                }


                if (byZones) {
                    if (group_zone_row_begin) {
                        sxs_newRowZone = sxs_sheet.createRow(cur_row);
                        sxs_newRowZone.setHeight(row_zone.getHeight());
//                        sxs_newRowZone.copyRowFrom(row_zone, ccp);

                        for (int cc = 0; cc <= col_ins; cc++) {
                            SXSSFCell sxs_cell = sxs_newRowZone.createCell(cc);
                            XSSFCell xs_cell = row_zone.getCell(cc);

                            if (xs_cell != null) {
                                sxs_cell.setCellStyle(xs_cell.getCellStyle());
                                sxs_cell.getCellStyle().setWrapText(false);
                            }
                        }


                        zones_rows.add(cur_row + 1);

                        sxs_newRowZone.getCell(3).setCellValue(assay.getClimaticZoneName() != null ? assay.getClimaticZoneName() : "");

                        zone_row_begin = cur_row + 1 + 1; // +1 для ячеек в эксель т.к. у них нумерация с 1 начинается
                        group_zone_row_begin = false;

                        cur_row++;
                    }
                }


                sxs_newRow = sxs_sheet.createRow(cur_row);
                sxs_newRow.setHeight(row_assay.getHeight());

                for (int cc = 0; cc <= col_ins; cc++) {
                    SXSSFCell sxs_cell = sxs_newRow.createCell(cc);
                    XSSFCell xs_cell = row_assay.getCell(cc);

                    if (xs_cell != null) {
                        sxs_cell.setCellStyle(xs_cell.getCellStyle());
                        sxs_cell.getCellStyle().setWrapText(false);
                    }
                }


                if (i == 0) {
                    if (byZones) {
//                        zone_row_begin = cur_row + 1;
                    } else {
                        twn_row_begin = cur_row + 1;
                    }
                }

                if (byZones) {
                    if (assay.getClimaticZoneId() != null) {
                        zone_id = assay.getClimaticZoneId();
                    } else {
                        zone_id = null;
                    }
                } else {
                    if (assay.getTwnId() != null) {
                        twn_id = assay.getTwnId();
                    } else {
                        twn_id = null;
                    }
                }

                all_rows_writen.add(cur_row);
                if (assay.getId() != null) {
                    sxs_newRow.getCell(1).setCellValue(assay.getId());
                }


                if (assay.getDate() != null) {
                    sxs_newRow.getCell(2).setCellValue(new Date(CommonUtils.getMillisecondFromLDT(assay.getDate())));
                }


                sxs_newRow.getCell(3).setCellValue(assay.getTwnName() != null ? assay.getTwnName() : "");

                try {
                    sxs_newRow.getCell(4).setCellValue(assay.getContractorName() != null ? assay.getContractorName() : "");
                } catch (org.hibernate.LazyInitializationException ex) {
                    sxs_newRow.getCell(4).setCellValue("<Контрагент не найден>");
                }


                if (Objects.equals(assay.getCultureCropType(), CropTypeEnum.SOWING)) {
                    if (assay.getCulture() != null) {
                        sxs_newRow.getCell(5).setCellValue(assay.getCultureCropKind() != null ? assay.getCultureCropKind() : "");
                        sxs_newRow.getCell(6).setCellValue(assay.getCulture() != null ? assay.getCulture() : "");
                    }
                } else {
                    sxs_newRow.getCell(5).setCellValue(assay.getCultureCropType().getName());
                }


                if (assay.getCulturePhase() != null) {
                    sxs_newRow.getCell(7).setCellValue(assay.getCulturePhase());
                }

                if (assay.getSourceCustomerEnum() != null) {
                    sxs_newRow.getCell(8).setCellValue(assay.getSourceCustomerEnum().getName());
                }

                if (assay.getCropFieldNumber() != null) {
                    sxs_newRow.getCell(9).setCellValue(assay.getCropFieldNumber());
                }

                if (field_area_is_not_null) {
                    sxs_newRow.getCell(col_num_total_area).setCellValue(field_area);
                }


                if (assay.getPlantsCount() != null) {
                    sxs_newRow.getCell(col_num_total_plantscount).setCellValue(assay.getPlantsCount());
                }


                col_ins = col_main_ins - cnt_shift_columns;

                for (int tf = 0; tf < templateFields.size(); tf++) {
                    String field_value = assay.getFieldValue(templateFields.get(tf));
                    AddDataField field = assay.getField(templateFields.get(tf));


                    col_ins += 1;

                    if (field_value != null && field != null) {

//                        "String"
//                        "LocalDateTime"
//                        "Integer"
//                        "Double"
//                        "Boolean"
//                        "ReproductionType"
//                        "SeedsBackFillPurpose"
//                        "Contractor"
//                        "group"


                        switch (field.getField().getAClass()) {
                            case "String":
                                sxs_newRow.getCell(col_ins).setCellValue(field_value);
                                break;
                            case "LocalDateTime":
                                try {
                                    sxs_newRow.getCell(col_ins).setCellStyle(cellStyle_date);

                                    sxs_newRow.getCell(col_ins).setCellValue(LocalDate.parse(field_value, CommonUtils.getDateFormatter()));
                                } catch (DateTimeParseException nfe) {
                                    sxs_newRow.getCell(col_ins).setCellValue(field_value);
                                }
                                break;
                            case "Integer":
                            case "Double":
                                try {
                                    double double_value = Double.parseDouble(field_value);
                                    sxs_newRow.getCell(col_ins).setCellType(CellType.NUMERIC);

                                    sxs_newRow.getCell(col_ins).setCellStyle(cellStyle_double);

                                    sxs_newRow.getCell(col_ins).setCellValue(double_value);
                                } catch (NumberFormatException nfe) {
                                    sxs_newRow.getCell(col_ins).setCellValue(field_value);
                                }
                                break;
                            case "Boolean":
                                if (StringUtils.equalsStrings("true", field_value)) {
                                    sxs_newRow.getCell(col_ins).setCellType(CellType.BOOLEAN);
                                    sxs_newRow.getCell(col_ins).setCellValue(true);
                                } else {
                                    sxs_newRow.getCell(col_ins).setCellValue(false);
                                }
                                break;
                            case "ReproductionType":
                                if (field.getReproductionTypeValue() != null) {
                                    sxs_newRow.getCell(col_ins).setCellValue(field.getReproductionTypeValue().getNameShort());
                                } else {
                                    sxs_newRow.getCell(col_ins).setCellValue(false);
                                }

                                break;
                            case "SeedsBackFillPurpose":
                                if (field.getSeedsBackFillPurposeValue() != null) {
                                    sxs_newRow.getCell(col_ins).setCellValue(field.getSeedsBackFillPurposeValue().getName());
                                } else {
                                    sxs_newRow.getCell(col_ins).setCellValue(false);
                                }

                                break;
                            case "Contractor":
                                try {
                                    Long contractor_id = Long.parseLong(field_value);
                                    Optional<ContractorLite> contractor_value = contractorRepo.findById(contractor_id);
                                    if (contractor_value.isPresent()) {
                                        sxs_newRow.getCell(col_ins).setCellValue(contractor_value.get().getNameShort());
                                    } else {
                                        sxs_newRow.getCell(col_ins).setCellValue(field_value);
                                    }
                                } catch (NumberFormatException nfe) {
                                    sxs_newRow.getCell(col_ins).setCellValue(field_value);
                                }

                                break;
                            case "group":
                                break;
                        }
                    }
                }


                int weeds_total_begin = col_ins;

                if (weeds.size() > 0) {
                    col_ins += col_total_ins_weeds_cnt;
                }

                int pests_total_begin = col_ins;

                if (pests.size() > 0) {
                    col_ins += col_total_ins_pests_cnt;
                }

                int diseases_total_begin = col_ins;

                if (diseases.size() > 0) {
                    col_ins += col_total_ins_diseases_cnt;
                }


                boolean is_weeds_protected = false;
                boolean is_pests_protected = false;
                boolean is_diseases_protected = false;


                // AgroTreatmentTypeDetailEnum.CHEMICAL("Химический");
                // AgroTreatmentTypeEnum.GROUND("Наземный"),
                // AgroTreatmentTypeEnum.AVIATION("Авиационный"),

                // AgroTreatmentTypeDetailEnum.BIOLOGICAL("Биологический")
                // AgroTreatmentTypeEnum.GROUND("Наземный"),
                // AgroTreatmentTypeEnum.AVIATION("Авиационный"),

                // AgroTreatmentTypeEnum.AGROTECHNICAL("Агротехнический"),

                // AgroTreatmentTypeEnum.TOXIC("Обработка складов"),
                // AgroTreatmentTypeEnum.SEED_TOXIC("Обработка семян");


//                Map<String, List<AgroTreatmentTypeEnum>> lists_treatmentTypes_types = new HashMap<String, List<AgroTreatmentTypeEnum>>();
//                Map<String, List<AgroTreatmentTypeDetailEnum>> lists_TreatmentTypeDetails_types = new HashMap<String, List<AgroTreatmentTypeDetailEnum>>();
//
//                List<String> prot_types = Arrays.asList("CHEM", "CHEM_GROUND", "CHEM_AVIA", "BIO", "AGROTECH");
//
//
//
//                lists_treatmentTypes_types.put("CHEM", Arrays.asList(AgroTreatmentTypeEnum.GROUND, AgroTreatmentTypeEnum.AVIATION));
//                lists_treatmentTypes_types.put("CHEM_GROUND", Arrays.asList(AgroTreatmentTypeEnum.GROUND));
//                lists_treatmentTypes_types.put("CHEM_AVIA", Arrays.asList(AgroTreatmentTypeEnum.AVIATION));
//                lists_treatmentTypes_types.put("BIO", Arrays.asList(AgroTreatmentTypeEnum.GROUND, AgroTreatmentTypeEnum.AVIATION));
//                lists_treatmentTypes_types.put("AGROTECH", Arrays.asList(AgroTreatmentTypeEnum.AGROTECHNICAL));
//
//                lists_TreatmentTypeDetails_types.put("CHEM", Arrays.asList(AgroTreatmentTypeDetailEnum.CHEMICAL));
//                lists_TreatmentTypeDetails_types.put("CHEM_GROUND", Arrays.asList(AgroTreatmentTypeDetailEnum.CHEMICAL));
//                lists_TreatmentTypeDetails_types.put("CHEM_AVIA", Arrays.asList(AgroTreatmentTypeDetailEnum.CHEMICAL));
//                lists_TreatmentTypeDetails_types.put("BIO", Arrays.asList(AgroTreatmentTypeDetailEnum.BIOLOGICAL));
//                lists_TreatmentTypeDetails_types.put("AGROTECH", Arrays.asList(AgroTreatmentTypeDetailEnum.CHEMICAL, AgroTreatmentTypeDetailEnum.BIOLOGICAL));


                Map<String, Double> set_is_weeds_protected_count = new HashMap<>();
                Map<String, Double> set_is_pests_protected_count = new HashMap<>();
                Map<String, Double> set_is_diseases_protected_count = new HashMap<>();

                Map<String, HashSet> set_weeds_prot_methods = new HashMap<String, HashSet>();
                Map<String, HashSet> set_pests_prot_methods = new HashMap<String, HashSet>();
                Map<String, HashSet> set_diseases_prot_methods = new HashMap<String, HashSet>();

                for (ProtectionGroupReportEnum protectionGroup : ProtectionGroupReportEnum.values()) {
                    set_is_weeds_protected_count.put(protectionGroup.name(), 0D);
                    set_is_pests_protected_count.put(protectionGroup.name(), 0D);
                    set_is_diseases_protected_count.put(protectionGroup.name(), 0D);

                    set_weeds_prot_methods.put(protectionGroup.name(), new HashSet<>());
                    set_pests_prot_methods.put(protectionGroup.name(), new HashSet<>());
                    set_diseases_prot_methods.put(protectionGroup.name(), new HashSet<>());
                }

//                set_is_weeds_protected_count.put("CHEM", 0);
//                set_is_weeds_protected_count.put("CHEM_GROUND", 0);
//                set_is_weeds_protected_count.put("CHEM_AVIA", 0);
//                set_is_weeds_protected_count.put("BIO", 0);
//                set_is_weeds_protected_count.put("AGROTECH", 0);
//
//                set_is_pests_protected_count.put("CHEM", 0);
//                set_is_pests_protected_count.put("CHEM_GROUND", 0);
//                set_is_pests_protected_count.put("CHEM_AVIA", 0);
//                set_is_pests_protected_count.put("BIO", 0);
//                set_is_pests_protected_count.put("AGROTECH", 0);
//
//                set_is_diseases_protected_count.put("CHEM", 0);
//                set_is_diseases_protected_count.put("CHEM_GROUND", 0);
//                set_is_diseases_protected_count.put("CHEM_AVIA", 0);
//                set_is_diseases_protected_count.put("BIO", 0);
//                set_is_diseases_protected_count.put("AGROTECH", 0);

//                int is_weeds_protected_count = 0;
//                int is_pests_protected_count = 0;
//                int is_diseases_protected_count = 0;

//                set_weeds_prot_methods.put("CHEM", new HashSet<>());
//                set_weeds_prot_methods.put("CHEM_GROUND", new HashSet<>());
//                set_weeds_prot_methods.put("CHEM_AVIA", new HashSet<>());
//                set_weeds_prot_methods.put("BIO", new HashSet<>());
//                set_weeds_prot_methods.put("AGROTECH", new HashSet<>());
//
//                set_pests_prot_methods.put("CHEM", new HashSet<>());
//                set_pests_prot_methods.put("CHEM_GROUND", new HashSet<>());
//                set_pests_prot_methods.put("CHEM_AVIA", new HashSet<>());
//                set_pests_prot_methods.put("BIO", new HashSet<>());
//                set_pests_prot_methods.put("AGROTECH", new HashSet<>());
//
//                set_diseases_prot_methods.put("CHEM", new HashSet<>());
//                set_diseases_prot_methods.put("CHEM_GROUND", new HashSet<>());
//                set_diseases_prot_methods.put("CHEM_AVIA", new HashSet<>());
//                set_diseases_prot_methods.put("BIO", new HashSet<>());
//                set_diseases_prot_methods.put("AGROTECH", new HashSet<>());


//                Set<PhytotypeMethod> phytoTypeMethodSet = new HashSet<>();
                List<SubjectCommonReport> protections = assay.getSubjectsByType(PhytoTypeEnum.PROTECTION);

                if (protections != null) {
                    for (int pi = 0; pi < protections.size(); pi++) {
                        SubjectCommonReport prot = protections.get(pi);

                        if (prot.clauseCalculateProtection()) {
                            if (prot.getProtectionProcessedTypes() != null) {
                                Set<PhytoTypeEnum> protectionTypes = getTypesProtectedStream(prot).collect(Collectors.toSet());

                                for (PhytoTypeEnum phytoTypeItem : PhytoTypeEnum.values()) {
                                    if (protectionTypes.contains(phytoTypeItem)) {
                                        switch (phytoTypeItem) {
                                            case WEEDS -> {
                                                if (isProtectedPredicate(weeds).test(prot)) {
                                                    is_weeds_protected = true;

                                                    for (ProtectionGroupReportEnum protectionGroup : ProtectionGroupReportEnum.values()) {
                                                        if (assay.compareTreatments(prot, protectionGroup.getTreatmentTypes(), protectionGroup.getTreatmentTypeDetails())) {
                                                            if (Objects.isNull(prot.getMethod()) || set_weeds_prot_methods.get(protectionGroup.name()).add(new PhytotypeMethod(phytoTypeItem, prot.getMethodForCheck()))) {
                                                                set_is_weeds_protected_count.put(protectionGroup.name(), set_is_weeds_protected_count.get(protectionGroup.name()) + getTreatmentVolume(prot));
                                                            }
                                                        }
                                                    }
                                                }

//                                                if (Objects.isNull(prot.getMethod()) || phytoTypeMethodSet.add(new PhytotypeMethod(phytoTypeItem, prot.getMethod()))) {
//                                                    is_weeds_protected_count++;
//                                                }
                                            }
                                            case DISEASES -> {
                                                if (isProtectedPredicate(diseases).test(prot)) {
                                                    is_diseases_protected = true;
                                                    for (ProtectionGroupReportEnum protectionGroup : ProtectionGroupReportEnum.values()) {
                                                        if (assay.compareTreatments(prot, protectionGroup.getTreatmentTypes(), protectionGroup.getTreatmentTypeDetails())) {
                                                            if (Objects.isNull(prot.getMethod()) || set_diseases_prot_methods.get(protectionGroup.name()).add(new PhytotypeMethod(phytoTypeItem, prot.getMethodForCheck()))) {
                                                                set_is_diseases_protected_count.put(protectionGroup.name(), set_is_diseases_protected_count.get(protectionGroup.name()) + getTreatmentVolume(prot));
                                                            }
                                                        }
                                                    }
                                                }


//                                                if (Objects.isNull(prot.getMethod()) || phytoTypeMethodSet.add(new PhytotypeMethod(phytoTypeItem, prot.getMethod()))) {
//                                                    is_diseases_protected_count++;
//                                                }
                                            }
                                            case PESTS -> {
                                                if (assay.getId() == 710317L) {
                                                    log.info("check");
                                                }
                                                if (isProtectedPredicate(pests).test(prot)) {
                                                    is_pests_protected = true;

                                                    for (ProtectionGroupReportEnum protectionGroup : ProtectionGroupReportEnum.values()) {
                                                        if (assay.compareTreatments(prot, protectionGroup.getTreatmentTypes(), protectionGroup.getTreatmentTypeDetails())) {
                                                            if (Objects.isNull(prot.getMethod()) || set_pests_prot_methods.get(protectionGroup.name()).add(new PhytotypeMethod(phytoTypeItem, prot.getMethodForCheck()))) {
                                                                set_is_pests_protected_count.put(protectionGroup.name(), set_is_pests_protected_count.get(protectionGroup.name()) + getTreatmentVolume(prot));
                                                            }
                                                        }
                                                    }
                                                }


//                                                if (Objects.isNull(prot.getMethod()) || phytoTypeMethodSet.add(new PhytotypeMethod(phytoTypeItem, prot.getMethod()))) {
//                                                    is_pests_protected_count++;
//                                                }
                                            }
                                        }
                                    }
                                }

                            }
                        }

//                        prot.getSubProtectionType();    // PROCESSED("Обработано", 0), RECOMMENDED("Рекомендовано", 1);
//                        prot.getSubProtectionDate();
//                        prot.getSubProtectionDoze();
//                        prot.getSubjectsOfPesticide(); // Обрабатываемые объекты
//                        prot.getAgroTreatmentType();   //  AGROTECHNICAL("Агротехнический"), AVIATION("Авиационный"), BIOLOGICAL("Биологический"), CHEMICAL("Химическая"), GROUND("Наземный");
                    }
                }


                boolean is_weeded = false;
                boolean is_pested = false;
                boolean is_diseased = false;

                List<String> columns_weeds_counts = new ArrayList<>();

                for (int wi = 0; wi < weeds.size(); wi++) {
                    PhytoSubjectState weed = weeds.get(wi);
                    SubjectCommonReport asj = AssayService.getSubjectBySubjectState(assay, weed);

                    columns_weeds_counts.add("" + colNumToColChar(col_ins + 3) + "");

                    if (asj != null && !is_protection) {
                        subjectsMaxValues.putIfAbsent(asj.getCode(), asj);
                        subjectsMaxValues.compute(asj.getCode(), (key, val) -> isNullReturnZero(asj.getSubWeedCount()) > isNullReturnZero(val.getSubWeedCount()) ? asj : val);
                        if (field_area_is_not_null) {
                            sxs_newRow.getCell(col_ins + 1).setCellValue(field_area);
                        }

                        if (asj.getSubWeedCount() != null && !asj.getNotFound()) {
                            if (field_area_is_not_null) {
                                sxs_newRow.getCell(col_ins + 2).setCellValue(field_area);
                            }
                            if (asj.getSubWeedCount() != null)
                                sxs_newRow.getCell(col_ins + 3).setCellValue(asj.getSubWeedCount());
                            is_weeded = true;
                        }
                    }
                    col_ins += col_ins_weed_cnt;
                }

                int weeds_end = col_ins;

                if (weeds.size() > 0 && clauseTypeChecked(assay.getSubjects(), PhytoTypeEnum.WEEDS.ordinal())) {
                    if (field_area_is_not_null) {
                        if (!is_protection) {
                            if (is_new_field) {
                                sxs_newRow.getCell(weeds_total_begin + 1).setCellValue(field_area);
                            }

                            sxs_newRow.getCell(weeds_total_begin + 2).setCellValue(field_area);


                            if (is_weeded) {
                                if (is_new_field_weeded) {
                                    sxs_newRow.getCell(weeds_total_begin + 3).setCellValue(field_area);
                                }

                                sxs_newRow.getCell(weeds_total_begin + 4).setCellValue(field_area);
                            }
                        }

                        if (is_weeds_protected) {
                            if (Objects.equals(assay.getId(), 653768L)) {
                                log.info("check");
                            }
                            double protected_area_ALL = calculateProtectionPhysicalArea(assay, weeds, ProtectionGroupReportEnum.ALL_FIELDS.getTreatmentTypes(), ProtectionGroupReportEnum.ALL_FIELDS.getTreatmentTypeDetails());

                            double protected_area_CHEM = calculateProtectionPhysicalAreaWoTreatmentPerc(assay, weeds, ProtectionGroupReportEnum.CHEM.getTreatmentTypes(), ProtectionGroupReportEnum.CHEM.getTreatmentTypeDetails());
                            double protected_area_CHEM_GROUND = calculateProtectionPhysicalAreaWoTreatmentPerc(assay, weeds, ProtectionGroupReportEnum.CHEM_GROUND.getTreatmentTypes(), ProtectionGroupReportEnum.CHEM_GROUND.getTreatmentTypeDetails());
                            double protected_area_CHEM_AVIA = calculateProtectionPhysicalAreaWoTreatmentPerc(assay, weeds, ProtectionGroupReportEnum.CHEM_AVIA.getTreatmentTypes(), ProtectionGroupReportEnum.CHEM_AVIA.getTreatmentTypeDetails());
                            double protected_area_BIO = calculateProtectionPhysicalAreaWoTreatmentPerc(assay, weeds, ProtectionGroupReportEnum.BIO.getTreatmentTypes(), ProtectionGroupReportEnum.BIO.getTreatmentTypeDetails());
                            double protected_area_AGROTECH = calculateProtectionPhysicalAreaWoTreatmentPerc(assay, weeds, ProtectionGroupReportEnum.AGROTECH.getTreatmentTypes(), ProtectionGroupReportEnum.AGROTECH.getTreatmentTypeDetails());

                            if (is_new_field_protected) {
                                sxs_newRow.getCell(weeds_total_begin + 5).setCellValue(protected_area_ALL);
                            }


                            sxs_newRow.getCell(weeds_total_begin + 6).setCellValue(protected_area_CHEM * set_is_weeds_protected_count.get(ProtectionGroupReportEnum.CHEM.name()));
                            sxs_newRow.getCell(weeds_total_begin + 7).setCellValue(protected_area_CHEM_GROUND * set_is_weeds_protected_count.get(ProtectionGroupReportEnum.CHEM_GROUND.name()));
                            sxs_newRow.getCell(weeds_total_begin + 8).setCellValue(protected_area_CHEM_AVIA * set_is_weeds_protected_count.get(ProtectionGroupReportEnum.CHEM_AVIA.name()));
                            sxs_newRow.getCell(weeds_total_begin + 9).setCellValue(protected_area_BIO * set_is_weeds_protected_count.get(ProtectionGroupReportEnum.BIO.name()));
                            sxs_newRow.getCell(weeds_total_begin + 10).setCellValue(protected_area_AGROTECH * set_is_weeds_protected_count.get(ProtectionGroupReportEnum.AGROTECH.name()));

//                            sxs_newRow.getCell(weeds_total_begin + 6).setCellValue(field_protected_area * ((double) is_weeds_protected_count));

                        }
                    }

                    sxs_newRow.getCell(weeds_total_begin + col_total_ins_weeds_cnt).setCellFormula("SUM(" + genArrayRowColumns((cur_row + 1), columns_weeds_counts) + ")");
                    evaluator.evaluateFormulaCell(sxs_newRow.getCell(weeds_total_begin + col_total_ins_weeds_cnt));
                }

                for (int pi = 0; pi < pests.size(); pi++) {
                    PhytoSubjectState pest = pests.get(pi);
                    SubjectCommonReport asj = AssayService.getSubjectBySubjectState(assay, pest);

                    if (asj != null && !is_protection) {
                        subjectsMaxValues.merge(asj.getCode(), asj, (val1, val2) -> isNullReturnZero(val1.getSubPestCount()) > isNullReturnZero(val2.getSubPestCount()) ? val1 : val2);
                        subjectsMaxDamageValues.merge(asj.getCode(), asj, (val1, val2) -> isNullReturnZero(val1.getDamage()) > isNullReturnZero(val2.getDamage()) ? val1 : val2);
                        if (field_area_is_not_null) {
                            sxs_newRow.getCell(col_ins + 1).setCellValue(field_area);
                        }

                        if (asj.getSubPestCount() != null && !asj.getNotFound()) {
                            if (field_area_is_not_null) {
                                sxs_newRow.getCell(col_ins + 2).setCellValue(field_area);
                            }
                            is_pested = true;
                            if (asj.getSubPestCount() != null)
                                sxs_newRow.getCell(col_ins + 3).setCellValue(asj.getSubPestCount());
                            if (asj.getDamage() != null)
                                sxs_newRow.getCell(col_ins + 4).setCellValue(asj.getDamage());

                        }
                    }
                    sxs_newRow.getCell(col_ins + 5).setCellFormula("IFERROR(" + colNumToColChar(col_ins + 1) + (cur_row + 1) + "*" + colNumToColChar(col_ins + 3) + (cur_row + 1) + ",\"\")");
                    sxs_newRow.getCell(col_ins + 6).setCellFormula("IFERROR(" + colNumToColChar(col_ins + 1) + (cur_row + 1) + "*" + colNumToColChar(col_ins + 4) + (cur_row + 1) + ",\"\")");
                    evaluator.evaluateFormulaCell(sxs_newRow.getCell(col_ins + 5));
                    evaluator.evaluateFormulaCell(sxs_newRow.getCell(col_ins + 6));
                    col_ins += col_ins_pest_cnt;
                }


                if (pests.size() > 0 && clauseTypeChecked(assay.getSubjects(), PhytoTypeEnum.PESTS.ordinal())) {
                    if (field_area_is_not_null) {
                        if (!is_protection) {
                            if (is_new_field) {
                                sxs_newRow.getCell(pests_total_begin + 1).setCellValue(field_area);
                            }

                            sxs_newRow.getCell(pests_total_begin + 2).setCellValue(field_area);

                            if (is_pested) {
                                if (is_new_field_pested) {
                                    sxs_newRow.getCell(pests_total_begin + 3).setCellValue(field_area);
                                }

                                sxs_newRow.getCell(pests_total_begin + 4).setCellValue(field_area);
                            }
                        }

                        if (is_pests_protected) {
//                            double field_protected_area = calculateProtectionPhysicalArea(assay, pests);

                            double protected_area_ALL = calculateProtectionPhysicalArea(assay, pests, ProtectionGroupReportEnum.ALL_FIELDS.getTreatmentTypes(), ProtectionGroupReportEnum.ALL_FIELDS.getTreatmentTypeDetails());

                            double protected_area_CHEM = calculateProtectionPhysicalAreaWoTreatmentPerc(assay, pests, ProtectionGroupReportEnum.CHEM.getTreatmentTypes(), ProtectionGroupReportEnum.CHEM.getTreatmentTypeDetails());
                            double protected_area_CHEM_GROUND = calculateProtectionPhysicalAreaWoTreatmentPerc(assay, pests, ProtectionGroupReportEnum.CHEM_GROUND.getTreatmentTypes(), ProtectionGroupReportEnum.CHEM_GROUND.getTreatmentTypeDetails());
                            double protected_area_CHEM_AVIA = calculateProtectionPhysicalAreaWoTreatmentPerc(assay, pests, ProtectionGroupReportEnum.CHEM_AVIA.getTreatmentTypes(), ProtectionGroupReportEnum.CHEM_AVIA.getTreatmentTypeDetails());
                            double protected_area_BIO = calculateProtectionPhysicalAreaWoTreatmentPerc(assay, pests, ProtectionGroupReportEnum.BIO.getTreatmentTypes(), ProtectionGroupReportEnum.BIO.getTreatmentTypeDetails());
                            double protected_area_AGROTECH = calculateProtectionPhysicalAreaWoTreatmentPerc(assay, pests, ProtectionGroupReportEnum.AGROTECH.getTreatmentTypes(), ProtectionGroupReportEnum.AGROTECH.getTreatmentTypeDetails());

                            if (is_new_field_protected) {
                                sxs_newRow.getCell(pests_total_begin + 5).setCellValue(protected_area_ALL);
                            }

                            sxs_newRow.getCell(pests_total_begin + 6).setCellValue(protected_area_CHEM * set_is_pests_protected_count.get(ProtectionGroupReportEnum.CHEM.name()));
                            sxs_newRow.getCell(pests_total_begin + 7).setCellValue(protected_area_CHEM_GROUND * set_is_pests_protected_count.get(ProtectionGroupReportEnum.CHEM_GROUND.name()));
                            sxs_newRow.getCell(pests_total_begin + 8).setCellValue(protected_area_CHEM_AVIA * set_is_pests_protected_count.get(ProtectionGroupReportEnum.CHEM_AVIA.name()));
                            sxs_newRow.getCell(pests_total_begin + 9).setCellValue(protected_area_BIO * set_is_pests_protected_count.get(ProtectionGroupReportEnum.BIO.name()));
                            sxs_newRow.getCell(pests_total_begin + 10).setCellValue(protected_area_AGROTECH * set_is_pests_protected_count.get(ProtectionGroupReportEnum.AGROTECH.name()));

//                            sxs_newRow.getCell(pests_total_begin + 6).setCellValue(field_protected_area * ((double) is_pests_protected_count));
                        }
                    }
                }


                for (int di = 0; di < diseases.size(); di++) {
                    PhytoSubjectState dis = diseases.get(di);
                    SubjectCommonReport asj = AssayService.getSubjectBySubjectState(assay, dis);

                    if (asj != null && !is_protection) {
                        subjectsMaxValues.putIfAbsent(asj.getCode(), asj);
                        subjectsMaxValues.compute(asj.getCode(), (key, val) -> isNullReturnZero(asj.getSubDiseaseR()) > isNullReturnZero(val.getSubDiseaseR()) ? asj : val);
                        if (field_area_is_not_null) {
                            sxs_newRow.getCell(col_ins + 1).setCellValue(field_area);
                        }

                        if (asj.getSubDiseaseP() != null && !asj.getNotFound()) {
                            if (field_area_is_not_null) {
                                sxs_newRow.getCell(col_ins + 2).setCellValue(field_area);
                            }

                            is_diseased = true;
                            if (asj.getSubDiseaseP() != null) {
                                sxs_newRow.getCell(col_ins + 3).setCellValue(asj.getSubDiseaseP());
                            }


                            if (asj.getSubDiseaseR() != null) {
                                sxs_newRow.getCell(col_ins + 4).setCellValue(asj.getSubDiseaseR());
                            }
                        }
                    }

                    sxs_newRow.getCell(col_ins + 5).setCellFormula("IFERROR(" + colNumToColChar(col_ins + 1) + (cur_row + 1) + "*" + colNumToColChar(col_ins + 3) + (cur_row + 1) + ",\"\")");
                    evaluator.evaluateFormulaCell(sxs_newRow.getCell(col_ins + 5));

                    col_ins += col_ins_disease_cnt;
                }

                if (diseases.size() > 0 && clauseTypeChecked(assay.getSubjects(), PhytoTypeEnum.DISEASES.ordinal())) {
                    if (field_area_is_not_null) {
                        if (!is_protection) {
                            if (is_new_field) {
                                sxs_newRow.getCell(diseases_total_begin + 1).setCellValue(field_area);
                            }

                            sxs_newRow.getCell(diseases_total_begin + 2).setCellValue(field_area);


                            if (is_diseased) {
                                if (is_new_field_diseased) {
                                    sxs_newRow.getCell(diseases_total_begin + 3).setCellValue(field_area);
                                }

                                sxs_newRow.getCell(diseases_total_begin + 4).setCellValue(field_area);
                            }
                        }

                        if (is_diseases_protected) {
//                            double field_protected_area = calculateProtectionPhysicalArea(assay, diseases);

                            double protected_area_ALL = calculateProtectionPhysicalArea(assay, diseases, ProtectionGroupReportEnum.ALL_FIELDS.getTreatmentTypes(), ProtectionGroupReportEnum.ALL_FIELDS.getTreatmentTypeDetails());

                            double protected_area_CHEM = calculateProtectionPhysicalAreaWoTreatmentPerc(assay, diseases, ProtectionGroupReportEnum.CHEM.getTreatmentTypes(), ProtectionGroupReportEnum.CHEM.getTreatmentTypeDetails());
                            double protected_area_CHEM_GROUND = calculateProtectionPhysicalAreaWoTreatmentPerc(assay, diseases, ProtectionGroupReportEnum.CHEM_GROUND.getTreatmentTypes(), ProtectionGroupReportEnum.CHEM_GROUND.getTreatmentTypeDetails());
                            double protected_area_CHEM_AVIA = calculateProtectionPhysicalAreaWoTreatmentPerc(assay, diseases, ProtectionGroupReportEnum.CHEM_AVIA.getTreatmentTypes(), ProtectionGroupReportEnum.CHEM_AVIA.getTreatmentTypeDetails());
                            double protected_area_BIO = calculateProtectionPhysicalAreaWoTreatmentPerc(assay, diseases, ProtectionGroupReportEnum.BIO.getTreatmentTypes(), ProtectionGroupReportEnum.BIO.getTreatmentTypeDetails());
                            double protected_area_AGROTECH = calculateProtectionPhysicalAreaWoTreatmentPerc(assay, diseases, ProtectionGroupReportEnum.AGROTECH.getTreatmentTypes(), ProtectionGroupReportEnum.AGROTECH.getTreatmentTypeDetails());

                            if (is_new_field_protected) {
                                sxs_newRow.getCell(diseases_total_begin + 5).setCellValue(protected_area_ALL);
                            }

                            sxs_newRow.getCell(diseases_total_begin + 6).setCellValue(protected_area_CHEM * set_is_diseases_protected_count.get(ProtectionGroupReportEnum.CHEM.name()));
                            sxs_newRow.getCell(diseases_total_begin + 7).setCellValue(protected_area_CHEM_GROUND * set_is_diseases_protected_count.get(ProtectionGroupReportEnum.CHEM_GROUND.name()));
                            sxs_newRow.getCell(diseases_total_begin + 8).setCellValue(protected_area_CHEM_AVIA * set_is_diseases_protected_count.get(ProtectionGroupReportEnum.CHEM_AVIA.name()));
                            sxs_newRow.getCell(diseases_total_begin + 9).setCellValue(protected_area_BIO * set_is_diseases_protected_count.get(ProtectionGroupReportEnum.BIO.name()));
                            sxs_newRow.getCell(diseases_total_begin + 10).setCellValue(protected_area_AGROTECH * set_is_diseases_protected_count.get(ProtectionGroupReportEnum.AGROTECH.name()));

                            //                            sxs_newRow.getCell(diseases_total_begin + 6).setCellValue(field_protected_area * ((double) is_diseases_protected_count));
                        }
                    }
                }


                cur_row++;

                if (i == (assays.size() - 1)) {
                    group_twn_row = true;
                    group_zone_row = true;
                    last_row = true;
                } else {
                    AssayCommonReport assay_next = assays.get(i + 1);

                    if (byZones) {
                        if (assay_next.getClimaticZoneId() != null) {
                            if (!assay_next.getClimaticZoneId().equals(zone_id)) {
                                group_zone_row = true;
                            } else if (zone_id == null) {
                                group_zone_row = true;
                            }
                        } else if (zone_id == null) {
                            group_zone_row = true;
                        }
                    } else {

                        if (assay_next.getTwnId() != null) {
                            if (!assay_next.getTwnId().equals(twn_id)) {
                                group_twn_row = true;
                            }
                        } else if (twn_id == null) {
                            group_twn_row = true;
                        } else if (twn_id == null) {
                            group_twn_row = true;
                        }
                    }
                }


                if (byZones) {
                    if (group_zone_row) {

                        sxs_newRowZone.getCell(col_num_total_area).setCellFormula("SUM(" + colNumToColChar(col_num_total_area) + zone_row_begin + ":" + colNumToColChar(col_num_total_area) + cur_row + ")");
                        sxs_newRowZone.getCell(col_num_total_plantscount).setCellFormula("IFERROR(AVERAGE(" + colNumToColChar(col_num_total_plantscount) + zone_row_begin + ":" + colNumToColChar(col_num_total_plantscount) + cur_row + "),\"\")");

                        evaluator.evaluateFormulaCell(sxs_newRowZone.getCell(col_num_total_area));
                        evaluator.evaluateFormulaCell(sxs_newRowZone.getCell(col_num_total_plantscount));

                        col_ins = col_main_ins - cnt_shift_columns;

                        for (int tf = 0; tf < templateFields.size(); tf++) {
                            int col_count = col_ins + 1;

                            sxs_newRowZone.getCell(col_count).setCellFormula("IFERROR(AVERAGE(" + colNumToColChar(col_count) + zone_row_begin + ":" + colNumToColChar(col_count) + cur_row + "),\"\")");
                            evaluator.evaluateFormulaCell(sxs_newRowZone.getCell(col_count));

                            col_ins += 1;
                        }


                        if (weeds.size() > 0) {
                            for (int wcc = 1; wcc <= 12; wcc++) {
                                sxs_newRowZone.getCell(col_ins + wcc).setCellFormula("SUM(" + colNumToColChar(col_ins + wcc) + zone_row_begin + ":" + colNumToColChar(col_ins + wcc) + cur_row + ")");
                                evaluator.evaluateFormulaCell(sxs_newRowZone.getCell(col_ins + wcc));
                            }

                            sxs_newRowZone.getCell(col_ins + col_total_ins_weeds_cnt).setCellFormula("IFERROR(AVERAGE(" + colNumToColChar(col_ins + col_total_ins_weeds_cnt) + zone_row_begin + ":" + colNumToColChar(col_ins + col_total_ins_weeds_cnt) + cur_row + "),\"\")");
                            evaluator.evaluateFormulaCell(sxs_newRowZone.getCell(col_ins + col_total_ins_weeds_cnt));

                            col_ins += col_total_ins_weeds_cnt;
                        }

                        if (pests.size() > 0) {
                            for (int pcc = 1; pcc <= 12; pcc++) {
                                sxs_newRowZone.getCell(col_ins + pcc).setCellFormula("SUM(" + colNumToColChar(col_ins + pcc) + zone_row_begin + ":" + colNumToColChar(col_ins + pcc) + cur_row + ")");
                                evaluator.evaluateFormulaCell(sxs_newRowZone.getCell(col_ins + pcc));
                            }

                            col_ins += col_total_ins_pests_cnt;
                        }

                        if (diseases.size() > 0) {
                            for (int dcc = 1; dcc <= 12; dcc++) {
                                sxs_newRowZone.getCell(col_ins + dcc).setCellFormula("SUM(" + colNumToColChar(col_ins + dcc) + zone_row_begin + ":" + colNumToColChar(col_ins + dcc) + cur_row + ")");
                                evaluator.evaluateFormulaCell(sxs_newRowZone.getCell(col_ins + dcc));
                            }

                            col_ins += col_total_ins_diseases_cnt;
                        }


                        for (int wi = 0; wi < weeds.size(); wi++) {
                            int col_area_check = col_ins + 1;
                            int col_area_weeded = col_ins + 2;
                            int col_count = col_ins + 3;


                            sxs_newRowZone.getCell(col_area_check).setCellFormula("SUM(" + colNumToColChar(col_area_check) + zone_row_begin + ":" + colNumToColChar(col_area_check) + cur_row + ")");
                            sxs_newRowZone.getCell(col_area_weeded).setCellFormula("SUM(" + colNumToColChar(col_area_weeded) + zone_row_begin + ":" + colNumToColChar(col_area_weeded) + cur_row + ")");
                            sxs_newRowZone.getCell(col_count).setCellFormula("IFERROR(AVERAGE(" + colNumToColChar(col_count) + zone_row_begin + ":" + colNumToColChar(col_count) + cur_row + "),\"\")");

                            evaluator.evaluateFormulaCell(sxs_newRowZone.getCell(col_area_check));
                            evaluator.evaluateFormulaCell(sxs_newRowZone.getCell(col_area_weeded));
                            evaluator.evaluateFormulaCell(sxs_newRowZone.getCell(col_count));

                            col_ins += col_ins_weed_cnt;
                        }


                        for (int pi = 0; pi < pests.size(); pi++) {
                            int col_area_check = col_ins + 1;
                            int col_area_pest = col_ins + 2;
                            int col_count = col_ins + 3;
                            int col_damage = col_ins + 4;
                            int col_avg = col_ins + 5;
                            int col_avg_damage = col_ins + 6;

                            sxs_newRowZone.getCell(col_area_check).setCellFormula("SUM(" + colNumToColChar(col_area_check) + zone_row_begin + ":" + colNumToColChar(col_area_check) + cur_row + ")");
                            sxs_newRowZone.getCell(col_area_pest).setCellFormula("SUM(" + colNumToColChar(col_area_pest) + zone_row_begin + ":" + colNumToColChar(col_area_pest) + cur_row + ")");
                            sxs_newRowZone.getCell(col_count).setCellFormula("IFERROR(AVERAGE(" + colNumToColChar(col_count) + zone_row_begin + ":" + colNumToColChar(col_count) + cur_row + "),\"\")");
                            sxs_newRowZone.getCell(col_damage).setCellFormula("IFERROR(AVERAGE(" + colNumToColChar(col_damage) + zone_row_begin + ":" + colNumToColChar(col_damage) + cur_row + "),\"\")");
                            sxs_newRowZone.getCell(col_avg).setCellFormula("IFERROR(SUM(" + colNumToColChar(col_avg) + zone_row_begin + ":" + colNumToColChar(col_avg) + cur_row + ")/" + colNumToColChar(col_area_pest) + (zone_row_begin - 1) + ",\"\")");
                            sxs_newRowZone.getCell(col_avg_damage).setCellFormula("IFERROR(SUM(" + colNumToColChar(col_avg_damage) + zone_row_begin + ":" + colNumToColChar(col_avg_damage) + cur_row + ")/" + colNumToColChar(col_area_pest) + (zone_row_begin - 1) + ",\"\")");

                            evaluator.evaluateFormulaCell(sxs_newRowZone.getCell(col_area_check));
                            evaluator.evaluateFormulaCell(sxs_newRowZone.getCell(col_area_pest));
                            evaluator.evaluateFormulaCell(sxs_newRowZone.getCell(col_count));
                            evaluator.evaluateFormulaCell(sxs_newRowZone.getCell(col_damage));
                            evaluator.evaluateFormulaCell(sxs_newRowZone.getCell(col_avg));
                            evaluator.evaluateFormulaCell(sxs_newRowZone.getCell(col_avg_damage));

                            col_ins += col_ins_pest_cnt;
                        }

                        for (int di = 0; di < diseases.size(); di++) {
                            int col_area_check = col_ins + 1;
                            int col_area_disease = col_ins + 2;
                            int col_dis_p = col_ins + 3;
                            int col_dis_r = col_ins + 4;
                            int col_avg = col_ins + 5;

                            sxs_newRowZone.getCell(col_area_check).setCellFormula("SUM(" + colNumToColChar(col_area_check) + zone_row_begin + ":" + colNumToColChar(col_area_check) + cur_row + ")");
                            sxs_newRowZone.getCell(col_area_disease).setCellFormula("SUM(" + colNumToColChar(col_area_disease) + zone_row_begin + ":" + colNumToColChar(col_area_disease) + cur_row + ")");
                            sxs_newRowZone.getCell(col_dis_p).setCellFormula("IFERROR(AVERAGE(" + colNumToColChar(col_dis_p) + zone_row_begin + ":" + colNumToColChar(col_dis_p) + cur_row + "),\"\")");
                            sxs_newRowZone.getCell(col_dis_r).setCellFormula("IFERROR(AVERAGE(" + colNumToColChar(col_dis_r) + zone_row_begin + ":" + colNumToColChar(col_dis_r) + cur_row + "),\"\")");
                            sxs_newRowZone.getCell(col_avg).setCellFormula("IFERROR(SUM(" + colNumToColChar(col_avg) + zone_row_begin + ":" + colNumToColChar(col_avg) + cur_row + ")/" + colNumToColChar(col_area_check) + (zone_row_begin - 1) + ",\"\")");

                            evaluator.evaluateFormulaCell(sxs_newRowZone.getCell(col_area_check));
                            evaluator.evaluateFormulaCell(sxs_newRowZone.getCell(col_area_disease));
                            evaluator.evaluateFormulaCell(sxs_newRowZone.getCell(col_dis_p));
                            evaluator.evaluateFormulaCell(sxs_newRowZone.getCell(col_dis_r));
                            evaluator.evaluateFormulaCell(sxs_newRowZone.getCell(col_avg));

                            col_ins += col_ins_disease_cnt;
                        }

                        sxs_sheet.groupRow(zone_row_begin - 1, cur_row - 1);

                        sxs_sheet.setRowSumsBelow(false);

                        group_zone_row = false;
                        group_zone_row_begin = true;
                    }
                } else {
                    if (group_twn_row) {
                        sxs_newRow = sxs_sheet.createRow(cur_row);
                        sxs_newRow.setHeight(row_township.getHeight());

                        for (int cc = 0; cc <= col_ins; cc++) {
                            SXSSFCell sxs_cell = sxs_newRow.createCell(cc);
                            XSSFCell xs_cell = row_township.getCell(cc);

                            if (xs_cell != null) {
                                sxs_cell.setCellStyle(xs_cell.getCellStyle());
                                sxs_cell.getCellStyle().setWrapText(false);
                            }
                        }

                        township_rows.add(cur_row + 1);
                        sxs_newRow.getCell(3).setCellValue(assay.getTwnName() != null ? assay.getTwnName() : "");
                        sxs_newRow.getCell(col_num_total_area).setCellFormula("SUM(" + colNumToColChar(col_num_total_area) + twn_row_begin + ":" + colNumToColChar(col_num_total_area) + cur_row + ")");
                        sxs_newRow.getCell(col_num_total_plantscount).setCellFormula("IFERROR(AVERAGE(" + colNumToColChar(col_num_total_plantscount) + twn_row_begin + ":" + colNumToColChar(col_num_total_plantscount) + cur_row + "),\"\")");

                        evaluator.evaluateFormulaCell(sxs_newRow.getCell(col_num_total_area));
                        evaluator.evaluateFormulaCell(sxs_newRow.getCell(col_num_total_plantscount));

                        col_ins = col_main_ins - cnt_shift_columns;

                        for (int tf = 0; tf < templateFields.size(); tf++) {
                            int col_count = col_ins + 1;

                            sxs_newRow.getCell(col_count).setCellFormula("IFERROR(AVERAGE(" + colNumToColChar(col_count) + twn_row_begin + ":" + colNumToColChar(col_count) + cur_row + "),\"\")");
                            evaluator.evaluateFormulaCell(sxs_newRow.getCell(col_count));

                            col_ins += 1;
                        }

                        if (weeds.size() > 0) {
                            for (int wcc = 1; wcc <= 12; wcc++) {
                                sxs_newRow.getCell(col_ins + wcc).setCellFormula("SUM(" + colNumToColChar(col_ins + wcc) + twn_row_begin + ":" + colNumToColChar(col_ins + wcc) + cur_row + ")");
                                evaluator.evaluateFormulaCell(sxs_newRow.getCell(col_ins + wcc));
                            }

                            sxs_newRow.getCell(col_ins + col_total_ins_weeds_cnt).setCellFormula("IFERROR(AVERAGE(" + colNumToColChar(col_ins + col_total_ins_weeds_cnt) + twn_row_begin + ":" + colNumToColChar(col_ins + col_total_ins_weeds_cnt) + cur_row + "),\"\")");
                            evaluator.evaluateFormulaCell(sxs_newRow.getCell(col_ins + col_total_ins_weeds_cnt));

                            col_ins += col_total_ins_weeds_cnt;
                        }

                        if (pests.size() > 0) {
                            for (int pcc = 1; pcc <= 12; pcc++) {
                                sxs_newRow.getCell(col_ins + pcc).setCellFormula("SUM(" + colNumToColChar(col_ins + pcc) + twn_row_begin + ":" + colNumToColChar(col_ins + pcc) + cur_row + ")");
                                evaluator.evaluateFormulaCell(sxs_newRow.getCell(col_ins + pcc));
                            }

                            col_ins += col_total_ins_pests_cnt;
                        }

                        if (diseases.size() > 0) {
                            for (int dcc = 1; dcc <= 12; dcc++) {
                                sxs_newRow.getCell(col_ins + dcc).setCellFormula("SUM(" + colNumToColChar(col_ins + dcc) + twn_row_begin + ":" + colNumToColChar(col_ins + dcc) + cur_row + ")");
                                evaluator.evaluateFormulaCell(sxs_newRow.getCell(col_ins + dcc));
                            }

                            col_ins += col_total_ins_diseases_cnt;
                        }


                        for (int wi = 0; wi < weeds.size(); wi++) {
                            int col_area_check = col_ins + 1;
                            int col_area_weeded = col_ins + 2;
                            int col_count = col_ins + 3;

                            sxs_newRow.getCell(col_area_check).setCellFormula("SUM(" + colNumToColChar(col_area_check) + twn_row_begin + ":" + colNumToColChar(col_area_check) + cur_row + ")");
                            sxs_newRow.getCell(col_area_weeded).setCellFormula("SUM(" + colNumToColChar(col_area_weeded) + twn_row_begin + ":" + colNumToColChar(col_area_weeded) + cur_row + ")");
                            sxs_newRow.getCell(col_count).setCellFormula("IFERROR(AVERAGE(" + colNumToColChar(col_count) + twn_row_begin + ":" + colNumToColChar(col_count) + cur_row + "),\"\")");

                            evaluator.evaluateFormulaCell(sxs_newRow.getCell(col_area_check));
                            evaluator.evaluateFormulaCell(sxs_newRow.getCell(col_area_weeded));
                            evaluator.evaluateFormulaCell(sxs_newRow.getCell(col_count));

                            col_ins += col_ins_weed_cnt;
                        }

                        for (int pi = 0; pi < pests.size(); pi++) {
                            int col_area_check = col_ins + 1;
                            int col_area_pest = col_ins + 2;
                            int col_count = col_ins + 3;
                            int col_damage = col_ins + 4;
                            int col_avg = col_ins + 5;
                            int col_avg_damage = col_ins + 6;

                            sxs_newRow.getCell(col_area_check).setCellFormula("SUM(" + colNumToColChar(col_area_check) + twn_row_begin + ":" + colNumToColChar(col_area_check) + cur_row + ")");
                            sxs_newRow.getCell(col_area_pest).setCellFormula("SUM(" + colNumToColChar(col_area_pest) + twn_row_begin + ":" + colNumToColChar(col_area_pest) + cur_row + ")");
                            sxs_newRow.getCell(col_count).setCellFormula("IFERROR(AVERAGE(" + colNumToColChar(col_count) + twn_row_begin + ":" + colNumToColChar(col_count) + cur_row + "),\"\")");
                            sxs_newRow.getCell(col_damage).setCellFormula("IFERROR(AVERAGE(" + colNumToColChar(col_damage) + twn_row_begin + ":" + colNumToColChar(col_damage) + cur_row + "),\"\")");
                            sxs_newRow.getCell(col_avg).setCellFormula("IFERROR(SUM(" + colNumToColChar(col_avg) + twn_row_begin + ":" + colNumToColChar(col_avg) + cur_row + ")/" + colNumToColChar(col_area_pest) + (cur_row + 1) + ",\"\")");
                            sxs_newRow.getCell(col_avg_damage).setCellFormula("IFERROR(SUM(" + colNumToColChar(col_avg_damage) + twn_row_begin + ":" + colNumToColChar(col_avg_damage) + cur_row + ")/" + colNumToColChar(col_area_pest) + (cur_row + 1) + ",\"\")");

                            evaluator.evaluateFormulaCell(sxs_newRow.getCell(col_area_check));
                            evaluator.evaluateFormulaCell(sxs_newRow.getCell(col_area_pest));
                            evaluator.evaluateFormulaCell(sxs_newRow.getCell(col_count));
                            evaluator.evaluateFormulaCell(sxs_newRow.getCell(col_damage));
                            evaluator.evaluateFormulaCell(sxs_newRow.getCell(col_avg));
                            evaluator.evaluateFormulaCell(sxs_newRow.getCell(col_avg_damage));

                            col_ins += col_ins_pest_cnt;
                        }

                        for (int di = 0; di < diseases.size(); di++) {
                            int col_area_check = col_ins + 1;
                            int col_area_disease = col_ins + 2;
                            int col_dis_p = col_ins + 3;
                            int col_dis_r = col_ins + 4;
                            int col_avg = col_ins + 5;

                            sxs_newRow.getCell(col_area_check).setCellFormula("SUM(" + colNumToColChar(col_area_check) + twn_row_begin + ":" + colNumToColChar(col_area_check) + cur_row + ")");
                            sxs_newRow.getCell(col_area_disease).setCellFormula("SUM(" + colNumToColChar(col_area_disease) + twn_row_begin + ":" + colNumToColChar(col_area_disease) + cur_row + ")");
                            sxs_newRow.getCell(col_dis_p).setCellFormula("IFERROR(AVERAGE(" + colNumToColChar(col_dis_p) + twn_row_begin + ":" + colNumToColChar(col_dis_p) + cur_row + "),\"\")");
                            sxs_newRow.getCell(col_dis_r).setCellFormula("IFERROR(AVERAGE(" + colNumToColChar(col_dis_r) + twn_row_begin + ":" + colNumToColChar(col_dis_r) + cur_row + "),\"\")");
                            sxs_newRow.getCell(col_avg).setCellFormula("IFERROR(SUM(" + colNumToColChar(col_avg) + twn_row_begin + ":" + colNumToColChar(col_avg) + cur_row + ")/" + colNumToColChar(col_area_check) + (cur_row + 1) + ",\"\")");

                            evaluator.evaluateFormulaCell(sxs_newRow.getCell(col_area_check));
                            evaluator.evaluateFormulaCell(sxs_newRow.getCell(col_area_disease));
                            evaluator.evaluateFormulaCell(sxs_newRow.getCell(col_dis_p));
                            evaluator.evaluateFormulaCell(sxs_newRow.getCell(col_dis_r));
                            evaluator.evaluateFormulaCell(sxs_newRow.getCell(col_avg));

                            col_ins += col_ins_disease_cnt;
                        }

                        sxs_sheet.groupRow(twn_row_begin - 1, cur_row - 1);

                        twn_row_begin = cur_row + 2;
                        group_twn_row = false;

                        cur_row++;
                    }
                }

                if (byZones) {
                    if (last_row) {
                        sxs_newRow = sxs_sheet.createRow(cur_row);
                        sxs_newRow.setHeight(row_total.getHeight());

                        for (int cc = 0; cc <= col_ins; cc++) {
                            SXSSFCell sxs_cell = sxs_newRow.createCell(cc);
                            XSSFCell xs_cell = row_total.getCell(cc);

                            if (xs_cell != null) {
                                sxs_cell.setCellStyle(xs_cell.getCellStyle());
                                sxs_cell.getCellStyle().setWrapText(false);
                            }
                        }

//                        sxs_newRow.copyRowFrom(row_total, ccp);

                        sxs_newRow.getCell(3).setCellValue("Итого:");

                        sxs_newRow.getCell(col_num_total_area).setCellFormula("SUM(" + genArrayColumnRows(colNumToColChar(col_num_total_area), zones_rows) + ")");
                        sxs_newRow.getCell(col_num_total_plantscount).setCellFormula("IFERROR(AVERAGE(" + genArrayColumnRows(colNumToColChar(col_num_total_plantscount), zones_rows) + "),\"\")");

                        evaluator.evaluateFormulaCell(sxs_newRow.getCell(col_num_total_area));
                        evaluator.evaluateFormulaCell(sxs_newRow.getCell(col_num_total_plantscount));

                        col_ins = col_main_ins - cnt_shift_columns;

                        for (int tf = 0; tf < templateFields.size(); tf++) {
                            int col_count = col_ins + 1;

                            sxs_newRow.getCell(col_count).setCellFormula("IFERROR(AVERAGE(" + genArrayColumnRows(colNumToColChar(col_count), zones_rows) + "),\"\")");
                            evaluator.evaluateFormulaCell(sxs_newRow.getCell(col_count));

                            col_ins += 1;
                        }

                        if (weeds.size() > 0) {
                            for (int wcc = 1; wcc <= 12; wcc++) {
                                sxs_newRow.getCell(col_ins + wcc).setCellFormula("SUM(" + genArrayColumnRows(colNumToColChar(col_ins + wcc), zones_rows) + ")");
                                evaluator.evaluateFormulaCell(sxs_newRow.getCell(col_ins + wcc));
                            }

                            sxs_newRow.getCell(col_ins + col_total_ins_weeds_cnt).setCellFormula("IFERROR(AVERAGE(" + genArrayColumnRows(colNumToColChar(col_ins + col_total_ins_weeds_cnt), zones_rows) + "),\"\")");
                            evaluator.evaluateFormulaCell(sxs_newRow.getCell(col_ins + col_total_ins_weeds_cnt));

                            col_ins += col_total_ins_weeds_cnt;
                        }

                        if (pests.size() > 0) {
                            for (int pcc = 1; pcc <= 12; pcc++) {
                                sxs_newRow.getCell(col_ins + pcc).setCellFormula("SUM(" + genArrayColumnRows(colNumToColChar(col_ins + pcc), zones_rows) + ")");
                                evaluator.evaluateFormulaCell(sxs_newRow.getCell(col_ins + pcc));
                            }

                            col_ins += col_total_ins_pests_cnt;
                        }

                        if (diseases.size() > 0) {
                            for (int dcc = 1; dcc <= 12; dcc++) {
                                sxs_newRow.getCell(col_ins + dcc).setCellFormula("SUM(" + genArrayColumnRows(colNumToColChar(col_ins + dcc), zones_rows) + ")");
                                evaluator.evaluateFormulaCell(sxs_newRow.getCell(col_ins + dcc));
                            }

                            col_ins += col_total_ins_diseases_cnt;
                        }


                        for (int wi = 0; wi < weeds.size(); wi++) {
                            int col_area_check = col_ins + 1;
                            int col_area_weeded = col_ins + 2;
                            int col_count = col_ins + 3;

                            sxs_newRow.getCell(col_area_check).setCellFormula("SUM(" + genArrayColumnRows(colNumToColChar(col_area_check), zones_rows) + ")");
                            sxs_newRow.getCell(col_area_weeded).setCellFormula("SUM(" + genArrayColumnRows(colNumToColChar(col_area_weeded), zones_rows) + ")");
                            sxs_newRow.getCell(col_count).setCellFormula("IFERROR(AVERAGE(" + genArrayColumnRows(colNumToColChar(col_count), zones_rows) + "),\"\")");

                            evaluator.evaluateFormulaCell(sxs_newRow.getCell(col_area_check));
                            evaluator.evaluateFormulaCell(sxs_newRow.getCell(col_area_weeded));
                            evaluator.evaluateFormulaCell(sxs_newRow.getCell(col_count));

                            col_ins += col_ins_weed_cnt;
                        }

                        for (int pi = 0; pi < pests.size(); pi++) {
                            int col_area_check = col_ins + 1;
                            int col_area_pest = col_ins + 2;
                            int col_count = col_ins + 3;
                            int col_damage = col_ins + 4;
                            int col_avg = col_ins + 5;
                            int col_avg_damage = col_ins + 6;

                            StringBuilder zones_avg = new StringBuilder();
                            StringBuilder zones_avg_damage = new StringBuilder();

                            int row_z_sum_begin = 8;

                            for (int z = 0; z < zones_rows.size() + 1; z++) {
                                if (zones_rows.size() > z) {
                                    if (row_z_sum_begin < zones_rows.get(z)) {

                                        zones_avg.append((zones_avg.length() == 0) ? "" : ",").append(colNumToColChar(col_avg))
                                                .append(row_z_sum_begin + 1)
                                                .append(":").append(colNumToColChar(col_avg)).append(zones_rows.get(z) - 1);
                                        zones_avg_damage.append((zones_avg_damage.length() == 0) ? "" : ",").append(colNumToColChar(col_avg_damage)).append(row_z_sum_begin + 1).append(":").append(colNumToColChar(col_avg_damage)).append(zones_rows.get(z) - 1);
                                        row_z_sum_begin = zones_rows.get(z);
                                    }
                                } else {
                                    zones_avg.append((zones_avg.length() == 0) ? "" : ",").append(colNumToColChar(col_avg)).append(row_z_sum_begin + 1).append(":").append(colNumToColChar(col_avg)).append(cur_row);
                                    zones_avg_damage.append((zones_avg_damage.length() == 0) ? "" : ",").append(colNumToColChar(col_avg_damage)).append(row_z_sum_begin + 1).append(":").append(colNumToColChar(col_avg_damage)).append(cur_row);
                                }
                            }


                            sxs_newRow.getCell(col_area_check).setCellFormula("SUM(" + genArrayColumnRows(colNumToColChar(col_area_check), zones_rows) + ")");
                            sxs_newRow.getCell(col_area_pest).setCellFormula("SUM(" + genArrayColumnRows(colNumToColChar(col_area_pest), zones_rows) + ")");
                            sxs_newRow.getCell(col_count).setCellFormula("IFERROR(AVERAGE(" + genArrayColumnRows(colNumToColChar(col_count), zones_rows) + "),\"\")");
                            sxs_newRow.getCell(col_damage).setCellFormula("IFERROR(AVERAGE(" + genArrayColumnRows(colNumToColChar(col_damage), zones_rows) + "),\"\")");

                            sxs_newRow.getCell(col_avg).setCellFormula("IFERROR(SUM(" + zones_avg + ")/" + colNumToColChar(col_area_pest) + (cur_row + 1) + ",\"\")");
                            sxs_newRow.getCell(col_avg_damage).setCellFormula("IFERROR(SUM(" + zones_avg_damage + ")/" + colNumToColChar(col_area_pest) + (cur_row + 1) + ",\"\")");

                            evaluator.evaluateFormulaCell(sxs_newRow.getCell(col_area_check));
                            evaluator.evaluateFormulaCell(sxs_newRow.getCell(col_area_pest));
                            evaluator.evaluateFormulaCell(sxs_newRow.getCell(col_count));
                            evaluator.evaluateFormulaCell(sxs_newRow.getCell(col_damage));
                            evaluator.evaluateFormulaCell(sxs_newRow.getCell(col_avg));
                            evaluator.evaluateFormulaCell(sxs_newRow.getCell(col_avg_damage));

                            col_ins += col_ins_pest_cnt;
                        }

                        for (int di = 0; di < diseases.size(); di++) {
                            int col_area_check = col_ins + 1;
                            int col_area_disease = col_ins + 2;
                            int col_dis_p = col_ins + 3;
                            int col_dis_r = col_ins + 4;
                            int col_avg = col_ins + 5;

                            StringBuilder zones_avg = new StringBuilder();

                            int row_z_sum_begin = 8;

                            for (int z = 0; z < zones_rows.size() + 1; z++) {
                                if (zones_rows.size() > z) {
                                    if (row_z_sum_begin < zones_rows.get(z)) {
                                        zones_avg.append((zones_avg.length() == 0) ? "" : ",").append(colNumToColChar(col_avg)).append(row_z_sum_begin + 1).append(":").append(colNumToColChar(col_avg)).append(zones_rows.get(z) - 1);
                                        row_z_sum_begin = zones_rows.get(z);
                                    }
                                } else {
                                    zones_avg.append((zones_avg.length() == 0) ? "" : ",").append(colNumToColChar(col_avg)).append(row_z_sum_begin + 1).append(":").append(colNumToColChar(col_avg)).append(cur_row);
                                }
                            }

                            sxs_newRow.getCell(col_area_check).setCellFormula("SUM(" + genArrayColumnRows(colNumToColChar(col_area_check), zones_rows) + ")");
                            sxs_newRow.getCell(col_area_disease).setCellFormula("SUM(" + genArrayColumnRows(colNumToColChar(col_area_disease), zones_rows) + ")");
                            sxs_newRow.getCell(col_dis_p).setCellFormula("IFERROR(AVERAGE(" + genArrayColumnRows(colNumToColChar(col_dis_p), zones_rows) + "),\"\")");
                            sxs_newRow.getCell(col_dis_r).setCellFormula("IFERROR(AVERAGE(" + genArrayColumnRows(colNumToColChar(col_dis_r), zones_rows) + "),\"\")");
                            sxs_newRow.getCell(col_avg).setCellFormula("IFERROR(SUM(" + zones_avg + ")/" + colNumToColChar(col_area_check) + (cur_row + 1) + ",\"\")");

                            evaluator.evaluateFormulaCell(sxs_newRow.getCell(col_area_check));
                            evaluator.evaluateFormulaCell(sxs_newRow.getCell(col_area_disease));
                            evaluator.evaluateFormulaCell(sxs_newRow.getCell(col_dis_p));
                            evaluator.evaluateFormulaCell(sxs_newRow.getCell(col_dis_r));
                            evaluator.evaluateFormulaCell(sxs_newRow.getCell(col_avg));

                            col_ins += col_ins_disease_cnt;
                        }
                    }
                } else {
                    if (last_row) {
                        sxs_newRow = sxs_sheet.createRow(cur_row);
                        sxs_newRow.setHeight(row_total.getHeight());
                        SXSSFRow sxs_newRowMax = sxs_sheet.createRow(cur_row + 1);
                        sxs_newRowMax.setHeight(row_total_max.getHeight());
                        XSSFCell lastCellMaxNotNull = null;
                        for (int cc = 0; cc <= col_ins; cc++) {
                            SXSSFCell sxs_cell = sxs_newRow.createCell(cc);
                            XSSFCell xs_cell = row_total.getCell(cc);
                            SXSSFCell sxs_cell_max = sxs_newRowMax.createCell(cc);
                            XSSFCell xs_cell_max = row_total_max.getCell(cc);
                            if (xs_cell_max != null && xs_cell_max.getCellStyle() != null) {
                                lastCellMaxNotNull = xs_cell_max;
                            }
                            if (xs_cell != null) {
                                sxs_cell.setCellStyle(xs_cell.getCellStyle());
                                sxs_cell.getCellStyle().setWrapText(false);
                            }
                            if (xs_cell_max != null) {
                                sxs_cell_max.setCellStyle(xs_cell_max.getCellStyle());
                                sxs_cell_max.getCellStyle().setWrapText(false);
                            } else if (lastCellMaxNotNull != null) {
                                sxs_cell_max.setCellStyle(lastCellMaxNotNull.getCellStyle());
                                sxs_cell_max.getCellStyle().setWrapText(false);
                            }
                        }
//                        sxs_newRow.copyRowFrom(row_total, ccp);


                        sxs_newRow.getCell(3).setCellValue("Итого:");

                        sxs_newRowMax.getCell(3).setCellValue("Максимальные значения:");

                        sxs_newRow.getCell(col_num_total_area).setCellFormula("SUM(" + genArrayColumnRows(colNumToColChar(col_num_total_area), township_rows) + ")");
                        //sxs_newRowMax.getCell(col_num_total_area).setCellFormula("MAX(" + genArrayColumnRows(colNumToColChar(col_num_total_area), all_rows_writen) + ")");
                        sxs_newRowMax.getCell(col_num_total_area).setCellValue(getMaxValueByDouble(streamSupplier, AssayCommonReport::getCropFieldArea));

                        sxs_newRow.getCell(col_num_total_plantscount).setCellFormula("IFERROR(AVERAGE(" + genArrayColumnRows(colNumToColChar(col_num_total_plantscount), township_rows) + "),\"\")");
                        sxs_newRowMax.getCell(col_num_total_plantscount).setCellValue(getMaxValueByDouble(streamSupplier, AssayCommonReport::getPlantsCount));


                        evaluator.evaluateFormulaCell(sxs_newRow.getCell(col_num_total_area));
                        evaluator.evaluateFormulaCell(sxs_newRow.getCell(col_num_total_plantscount));

                        //evaluator.evaluateFormulaCell(sxs_newRowMax.getCell(col_num_total_area));
                        //evaluator.evaluateFormulaCell(sxs_newRowMax.getCell(col_num_total_plantscount));

                        col_ins = col_main_ins - cnt_shift_columns;


                        for (int tf = 0; tf < templateFields.size(); tf++) {
                            int col_count = col_ins + 1;
                            String key = templateFields.get(tf);
                            sxs_newRow.getCell(col_count).setCellFormula("IFERROR(AVERAGE(" + genArrayColumnRows(colNumToColChar(col_count), township_rows) + "),\"\")");
                            evaluator.evaluateFormulaCell(sxs_newRow.getCell(col_count));


                            sxs_newRowMax.getCell(col_count).setCellValue(getMaxValueByString(streamSupplier, commonReport -> commonReport.getFieldValue(key)));
                            //evaluator.evaluateFormulaCell(sxs_newRowMax.getCell(col_count));
                            col_ins += 1;
                        }

                        if (weeds.size() > 0) {

                            for (int wcc = 1; wcc <= 12; wcc++) {
                                sxs_newRow.getCell(col_ins + wcc).setCellFormula("SUM(" + genArrayColumnRows(colNumToColChar(col_ins + wcc), township_rows) + ")");
                                evaluator.evaluateFormulaCell(sxs_newRow.getCell(col_ins + wcc));
                            }
                            sxs_newRow.getCell(col_ins + col_total_ins_weeds_cnt).setCellFormula("IFERROR(AVERAGE(" + genArrayColumnRows(colNumToColChar(col_ins + col_total_ins_weeds_cnt), township_rows) + "),\"\")");
                            evaluator.evaluateFormulaCell(sxs_newRow.getCell(col_ins + col_total_ins_weeds_cnt));

                            sxs_newRowMax.getCell(col_ins + col_total_ins_weeds_cnt).setCellValue(subjectsMaxValues.entrySet()
                                    .stream()
                                    .filter(entry -> entry.getValue() != null && PhytoTypeEnum.WEEDS.ordinal() == entry.getValue().getPhytoType())
                                    .mapToDouble(entry -> isNullReturnZero(entry.getValue().getSubWeedCount()))
                                    .max().orElse(0D));
                            //evaluator.evaluateFormulaCell(sxs_newRowMax.getCell(col_ins + 7));


                            col_ins += col_total_ins_weeds_cnt;
                        }

                        if (pests.size() > 0) {
                            for (int pcc = 1; pcc <= 12; pcc++) {
                                sxs_newRow.getCell(col_ins + pcc).setCellFormula("SUM(" + genArrayColumnRows(colNumToColChar(col_ins + pcc), township_rows) + ")");
                                evaluator.evaluateFormulaCell(sxs_newRow.getCell(col_ins + pcc));
                            }

                            col_ins += col_total_ins_pests_cnt;
                        }

                        if (diseases.size() > 0) {
                            for (int dcc = 1; dcc <= 12; dcc++) {
                                sxs_newRow.getCell(col_ins + dcc).setCellFormula("SUM(" + genArrayColumnRows(colNumToColChar(col_ins + dcc), township_rows) + ")");
                                evaluator.evaluateFormulaCell(sxs_newRow.getCell(col_ins + dcc));
                            }

                            col_ins += col_total_ins_diseases_cnt;
                        }


                        for (int wi = 0; wi < weeds.size(); wi++) {
                            int col_area_check = col_ins + 1;
                            int col_area_weeded = col_ins + 2;
                            int col_count = col_ins + 3;
                            Long code = Optional.ofNullable(weeds.get(wi).getPhytosubjectCode()).orElse(-1L);
                            sxs_newRow.getCell(col_area_check).setCellFormula("SUM(" + genArrayColumnRows(colNumToColChar(col_area_check), township_rows) + ")");
                            sxs_newRow.getCell(col_area_weeded).setCellFormula("SUM(" + genArrayColumnRows(colNumToColChar(col_area_weeded), township_rows) + ")");
                            sxs_newRow.getCell(col_count).setCellFormula("IFERROR(AVERAGE(" + genArrayColumnRows(colNumToColChar(col_count), township_rows) + "),\"\")");
                            sxs_newRowMax.getCell(col_count).setCellValue(Optional.ofNullable(subjectsMaxValues.getOrDefault(code, new SubjectCommonReport())).map(SubjectCommonReport::getSubWeedCount).orElse(0D));

                            evaluator.evaluateFormulaCell(sxs_newRow.getCell(col_area_check));
                            evaluator.evaluateFormulaCell(sxs_newRow.getCell(col_area_weeded));
                            evaluator.evaluateFormulaCell(sxs_newRow.getCell(col_count));
                            //evaluator.evaluateFormulaCell(sxs_newRowMax.getCell(col_count));

                            col_ins += col_ins_weed_cnt;
                        }

                        for (int pi = 0; pi < pests.size(); pi++) {
                            int col_area_check = col_ins + 1;
                            int col_area_pest = col_ins + 2;
                            int col_count = col_ins + 3;
                            int col_damge = col_ins + 4;
                            int col_avg = col_ins + 5;
                            int col_avgDamage = col_ins + 6;
                            Long code = Optional.ofNullable(pests.get(pi).getPhytosubjectCode()).orElse(0L);
                            int row_t_sum_begin = 7;
                            StringBuilder twns_avg = new StringBuilder();
                            StringBuilder twns_avg_damage = new StringBuilder();

                            for (int t = 0; t < township_rows.size(); t++) {
                                if (row_t_sum_begin < township_rows.get(t)) {
                                    twns_avg.append((twns_avg.length() == 0) ? "" : ",").append(colNumToColChar(col_avg)).append(row_t_sum_begin + 1).append(":").append(colNumToColChar(col_avg)).append(township_rows.get(t) - 1);
                                    twns_avg_damage.append((twns_avg_damage.length() == 0) ? "" : ",").append(colNumToColChar(col_avgDamage)).append(row_t_sum_begin + 1).append(":").append(colNumToColChar(col_avgDamage)).append(township_rows.get(t) - 1);
                                    row_t_sum_begin = township_rows.get(t);
                                }
                            }


                            sxs_newRow.getCell(col_area_check).setCellFormula("SUM(" + genArrayColumnRows(colNumToColChar(col_area_check), township_rows) + ")");
                            sxs_newRow.getCell(col_area_pest).setCellFormula("SUM(" + genArrayColumnRows(colNumToColChar(col_area_pest), township_rows) + ")");
                            sxs_newRow.getCell(col_count).setCellFormula("IFERROR(AVERAGE(" + genArrayColumnRows(colNumToColChar(col_count), township_rows) + "),\"\")");
                            sxs_newRow.getCell(col_damge).setCellFormula("IFERROR(AVERAGE(" + genArrayColumnRows(colNumToColChar(col_damge), township_rows) + "),\"\")");
                            sxs_newRow.getCell(col_avg).setCellFormula("IFERROR(SUM(" + twns_avg + ")/" + colNumToColChar(col_area_pest) + (cur_row + 1) + ",\"\")");
                            sxs_newRow.getCell(col_avgDamage).setCellFormula("IFERROR(SUM(" + twns_avg_damage + ")/" + colNumToColChar(col_area_pest) + (cur_row + 1) + ",\"\")");


                            sxs_newRowMax.getCell(col_count).setCellValue(Optional.ofNullable(subjectsMaxValues.getOrDefault(code, new SubjectCommonReport())).map(SubjectCommonReport::getSubPestCount).orElse(0D));
                            sxs_newRowMax.getCell(col_damge).setCellValue(Optional.ofNullable(subjectsMaxDamageValues.getOrDefault(code, new SubjectCommonReport())).map(SubjectCommonReport::getDamage).orElse(0D));

                            evaluator.evaluateFormulaCell(sxs_newRow.getCell(col_area_check));
                            evaluator.evaluateFormulaCell(sxs_newRow.getCell(col_area_pest));
                            evaluator.evaluateFormulaCell(sxs_newRow.getCell(col_count));
                            evaluator.evaluateFormulaCell(sxs_newRow.getCell(col_damge));
                            evaluator.evaluateFormulaCell(sxs_newRow.getCell(col_avg));
                            evaluator.evaluateFormulaCell(sxs_newRow.getCell(col_avgDamage));

                            //evaluator.evaluateFormulaCell(sxs_newRowMax.getCell(col_count));

                            col_ins += col_ins_pest_cnt;
                        }
                        for (int di = 0; di < diseases.size(); di++) {
                            int col_area_check = col_ins + 1;
                            int col_area_disease = col_ins + 2;
                            int col_dis_p = col_ins + 3;
                            int col_dis_r = col_ins + 4;
                            int col_avg = col_ins + 5;
                            Long code = Optional.ofNullable(diseases.get(di).getPhytosubjectCode()).orElse(0L);
                            int row_t_sum_begin = 7;
                            StringBuilder twns_avg = new StringBuilder();

                            for (int t = 0; t < township_rows.size(); t++) {
                                if (row_t_sum_begin < township_rows.get(t)) {
                                    twns_avg.append((twns_avg.length() == 0) ? "" : ",").append(colNumToColChar(col_avg)).append(row_t_sum_begin + 1).append(":").append(colNumToColChar(col_avg)).append(township_rows.get(t) - 1);

                                    row_t_sum_begin = township_rows.get(t);
                                }
                            }

                            sxs_newRow.getCell(col_area_check).setCellFormula("SUM(" + genArrayColumnRows(colNumToColChar(col_area_check), township_rows) + ")");
                            sxs_newRow.getCell(col_area_disease).setCellFormula("SUM(" + genArrayColumnRows(colNumToColChar(col_area_disease), township_rows) + ")");
                            sxs_newRow.getCell(col_dis_p).setCellFormula("IFERROR(AVERAGE(" + genArrayColumnRows(colNumToColChar(col_dis_p), township_rows) + "),\"\")");
                            sxs_newRow.getCell(col_dis_r).setCellFormula("IFERROR(AVERAGE(" + genArrayColumnRows(colNumToColChar(col_dis_r), township_rows) + "),\"\")");
                            sxs_newRow.getCell(col_avg).setCellFormula("IFERROR(SUM(" + twns_avg + ")/" + colNumToColChar(col_area_check) + (cur_row + 1) + ",\"\")");

                            sxs_newRowMax.getCell(col_dis_r).setCellValue(Optional.ofNullable(subjectsMaxValues.getOrDefault(code, new SubjectCommonReport())).map(SubjectCommonReport::getSubDiseaseR).orElse(0D));

                            evaluator.evaluateFormulaCell(sxs_newRow.getCell(col_area_check));
                            evaluator.evaluateFormulaCell(sxs_newRow.getCell(col_area_disease));
                            evaluator.evaluateFormulaCell(sxs_newRow.getCell(col_dis_p));
                            evaluator.evaluateFormulaCell(sxs_newRow.getCell(col_dis_r));
                            evaluator.evaluateFormulaCell(sxs_newRow.getCell(col_avg));
                            //evaluator.evaluateFormulaCell(sxs_newRowMax.getCell(col_dis_r));

                            col_ins += col_ins_disease_cnt;
                        }
                    }
                }
            }

            // headers merge

            col_ins = col_solve_ins - 1;
            int cp = 1;


            if (!templateFields.isEmpty()) {
                int sol_col_addfields_begin = col_ins + 1;

                for (int tf = 0; tf < templateFields.size(); tf++) {
                    sxs_sheet.addMergedRegion(new CellRangeAddress(4 + cp, 5 + cp, col_ins + 1, col_ins + 1));
                    col_ins += 1;
                }

                int sol_col_addfields_end = col_ins;
                if (sol_col_addfields_begin != sol_col_addfields_end) {
                    sxs_sheet.addMergedRegion(new CellRangeAddress(3 + cp, 3 + cp, sol_col_addfields_begin, sol_col_addfields_end));
                }

            }


            if (weeds.size() > 0) {
                int sol_col_total_weeds_begin = col_ins;

                col_ins += 1;

                for (int wi = 0; wi < col_total_ins_weeds_cnt; wi++) {
                    if (wi <= 4 || (wi + 1) >= col_total_ins_weeds_cnt - 2) {
                        sxs_sheet.addMergedRegion(new CellRangeAddress(4 + cp, 5 + cp, col_ins + wi, col_ins + wi));
                    }
                    if (wi == 5) {
                        sxs_sheet.addMergedRegion(new CellRangeAddress(4 + cp, 4 + cp, col_ins + wi, col_ins + col_total_ins_weeds_cnt - 4));
                    }
                }

                col_ins = sol_col_total_weeds_begin + col_total_ins_weeds_cnt;
                sxs_sheet.addMergedRegion(new CellRangeAddress(3 + cp, 3 + cp, sol_col_total_weeds_begin + 1, col_ins));
            }

            if (pests.size() > 0) {
                int sol_col_total_pests_begin = col_ins;
                col_ins += 1;

                for (int pi = 0; pi < col_total_ins_pests_cnt; pi++) {
                    if (pi <= 4 || pi >= col_total_ins_pests_cnt - 2) {
                        sxs_sheet.addMergedRegion(new CellRangeAddress(4 + cp, 5 + cp, col_ins + pi, col_ins + pi));
                    }
                    if (pi == 5) {
                        sxs_sheet.addMergedRegion(new CellRangeAddress(4 + cp, 4 + cp, col_ins + pi, col_ins + col_total_ins_pests_cnt - 3));
                    }
                }

                col_ins = sol_col_total_pests_begin + col_total_ins_pests_cnt;
                sxs_sheet.addMergedRegion(new CellRangeAddress(3 + cp, 3 + cp, sol_col_total_pests_begin + 1, col_ins));
            }

            if (diseases.size() > 0) {
                int sol_col_total_diseases_begin = col_ins;
                col_ins += 1;

                for (int di = 0; di < col_total_ins_diseases_cnt; di++) {
                    if (di <= 4 || di >= col_total_ins_pests_cnt - 2) {
                        sxs_sheet.addMergedRegion(new CellRangeAddress(4 + cp, 5 + cp, col_ins + di, col_ins + di));
                    }
                    if (di == 5) {
                        sxs_sheet.addMergedRegion(new CellRangeAddress(4 + cp, 4 + cp, col_ins + di, col_ins + col_total_ins_pests_cnt - 3));
                    }
                }

                col_ins = sol_col_total_diseases_begin + col_total_ins_diseases_cnt;
                sxs_sheet.addMergedRegion(new CellRangeAddress(3 + cp, 3 + cp, sol_col_total_diseases_begin + 1, col_ins));
            }


            cp = 0;

            for (int wi = 0; wi < weeds.size(); wi++) {
                sxs_sheet.addMergedRegion(new CellRangeAddress(3 + cp + 2, 3 + cp + 2, col_ins + 1, col_ins + col_ins_weed_cnt));
                sxs_sheet.addMergedRegion(new CellRangeAddress(3 + cp + 1, 3 + cp + 1, col_ins + 1, col_ins + col_ins_weed_cnt));
                col_ins += col_ins_weed_cnt;
            }

            for (int pi = 0; pi < pests.size(); pi++) {
                sxs_sheet.addMergedRegion(new CellRangeAddress(3 + cp + 2, 3 + cp + 2, col_ins + 1, col_ins + col_ins_pest_cnt));
                sxs_sheet.addMergedRegion(new CellRangeAddress(3 + cp + 1, 3 + cp + 1, col_ins + 1, col_ins + col_ins_pest_cnt));
                col_ins += col_ins_pest_cnt;
            }

            for (int di = 0; di < diseases.size(); di++) {
                sxs_sheet.addMergedRegion(new CellRangeAddress(3 + cp + 2, 3 + cp + 2, col_ins + 1, col_ins + col_ins_disease_cnt));
                sxs_sheet.addMergedRegion(new CellRangeAddress(3 + cp + 1, 3 + cp + 1, col_ins + 1, col_ins + col_ins_disease_cnt));
                col_ins += col_ins_disease_cnt;
            }

            sxs_sheet.flushRows();

            log.info("TRACE makeSheetReportCommonAssayState END " + Chronograph.getTime(4));
        }
        return new HashMap<>();
    }

    private Double getTreatmentVolume(SubjectCommonReport prot) {
        if (Objects.isNull(prot.getTreatmentPow()) || Objects.equals(AgroTreatmentPowEnum.FULL.name(), prot.getTreatmentPow())) {
            return 1D;
        }
        return isNullReturnZero(prot.getTreatmentPercentage()) / 100;
    }

    private void addFieldToInfectedProtectedIfFound(List<FieldByContractor> infectedProtected, FieldByContractor field_assay) {
        if (field_assay.isPested() || field_assay.isDiseased() || field_assay.isWeeded() || field_assay.isProtected()) {
            infectedProtected.add(field_assay);
        }
    }

    private Stream<PhytoTypeEnum> getTypesProtectedStream(SubjectCommonReport protection) {
        return Stream.ofNullable(protection.getProtectionProcessedTypes())
                .flatMap(Collection::stream)
                .filter(Objects::nonNull)
                .map(SubjectCommonReport::getPhytoType)
                .map(PhytoTypeEnum::findByOrdinal);
    }

    private boolean clauseTypeChecked(Collection<SubjectCommonReport> subjects, int phytoType) {
        return subjects.stream().map(SubjectCommonReport::getPhytoType).anyMatch(type -> phytoType == type);
    }

    private boolean findFoundSubjectByType(AssayCommonReport assay, PhytoTypeEnum type, List<PhytoSubjectState> states) {
        if (CollectionUtils.isNullOrEmpty(states))
            return false;
        List<SubjectCommonReport> collectFound = assay.getSubjectsByType(type)
                .stream()
                .filter(SubjectCommonReport::isFound)
                .collect(Collectors.toList());
        for (PhytoSubjectState state : states) {
            SubjectCommonReport subject = AssayService.getSubjectBySubjectState(assay, state);
            if (collectFound.contains(subject))
                return true;
        }
        return false;
    }

    private boolean findProtectionsByType(AssayCommonReport assay, PhytoTypeEnum phytoType) {
        Supplier<Stream<SubjectCommonReport>> subjSupplier = () -> assay.getSubjectsByType(PhytoTypeEnum.PROTECTION)
                .stream()
                .filter(prot -> Objects.equals(PhytoProtectionTypeEnum.PROCESSED.ordinal(), prot.getProtectionType()) && prot.getProtectionProcessedTypes() != null);
        if (phytoType == null)
            return subjSupplier.get().count() > 0;

        return subjSupplier
                .get()
                .flatMap(this::getTypesProtectedStream)
                .anyMatch(phytoType::equals);
    }


    private boolean findProtections(AssayCommonReport assay) {
        return findProtectionsByType(assay, null);
    }


    public List<PhytoSubjectState> cleanEmptySubjectsStates(List<PhytoSubjectState> inSubjects, List<AssayCommonReport> assays) {
        List<PhytoSubjectState> retSubjects = new ArrayList<>();

        for (AssayCommonReport assay : assays) {
            for (PhytoSubjectState ph_sub_state : inSubjects) {
                if (AssayService.getSubjectBySubjectState(assay, ph_sub_state) != null) {
                    if (!retSubjects.contains(ph_sub_state)) {
                        retSubjects.add(ph_sub_state);
                    }
                }
            }
        }

        return retSubjects;
    }

    public String genArrayRowColumns(Integer row, List<String> columns) {
        StringBuilder sb = new StringBuilder();
        for (String column : columns) {
            if (sb.length() > 0) {
                sb.append(",");
            }
            sb.append(column).append(row);
        }

        return sb.toString();
    }


    private double getMaxValueByString(Supplier<Stream<AssayCommonReport>> streamSupplier, Function<AssayCommonReport, String> function) {
        return streamSupplier.get().
                map(function)
                .mapToDouble(str -> isNullReturnZero(parseDouble(str)))
                .max().orElse(0D);
    }

    private double getMaxValueByDouble(Supplier<Stream<AssayCommonReport>> streamSupplier, Function<AssayCommonReport, Double> function) {
        return streamSupplier.get().
                map(function)
                .mapToDouble(NumberUtils::isNullReturnZero)
                .max().orElse(0D);
    }


    public byte[] makeReportCommonAssay01(CommonAssayReport report) {
        log.info("TRACE CoomonAssay BEGIN");

        Chronograph.start(3);

        try {
            XSSFWorkbook workbook = null;
            SXSSFWorkbook sxs_workbook = null;

            java.awt.Color color_weeds = new java.awt.Color(255, 192, 0);
            java.awt.Color color_diseases = new java.awt.Color(196, 215, 155);
            java.awt.Color color_pests = new java.awt.Color(146, 205, 220);

            InputStream in = resourceLoader.getResource("classpath:excel/report_common_01.xlsx").getInputStream();
            OPCPackage pkg = OPCPackage.open(in);
            workbook = new XSSFWorkbook(pkg);

            XSSFSheet sheet = workbook.getSheetAt(0);
            XSSFSheet sheet_template = workbook.getSheetAt(1);
            List<MeasureUnit> units = new ArrayList<>();

            if (report.getSplitCropTypeAndKindCultures() && report.getSplitSubjectTypes()) {

                Map<String, XSSFSheet> sheet_map = new HashMap<>();

                for (CropTypeAndKindCulture ctakc : report.getSelectedCropTypeAndKindCultures()) {
                    String sheet_sufix = "Сор - " + (getSheetSuffixByCtakc(ctakc));
                    String sheet_template_sufix = "Ш_" + sheet_sufix;

                    XSSFSheet sheet_clone_weeds = workbook.cloneSheet(0, sheet_sufix);
                    XSSFSheet sheet_clone_weeds_template = workbook.cloneSheet(1, sheet_template_sufix);

                    sheet_clone_weeds.setTabColor(new XSSFColor(color_weeds, null));

                    sheet_map.put(sheet_sufix, sheet_clone_weeds);
                    sheet_map.put(sheet_template_sufix, sheet_clone_weeds_template);
                }

                for (CropTypeAndKindCulture ctakc : report.getSelectedCropTypeAndKindCultures()) {
                    String sheet_sufix = "Бол - " + (getSheetSuffixByCtakc(ctakc));
                    String sheet_template_sufix = "Ш_" + sheet_sufix;

                    XSSFSheet sheet_clone_diseases = workbook.cloneSheet(0, sheet_sufix);
                    XSSFSheet sheet_clone_diseases_template = workbook.cloneSheet(1, sheet_template_sufix);

                    sheet_clone_diseases.setTabColor(new XSSFColor(color_diseases, null));

                    sheet_map.put(sheet_sufix, sheet_clone_diseases);
                    sheet_map.put(sheet_template_sufix, sheet_clone_diseases_template);
                }

                for (CropTypeAndKindCulture ctakc : report.getSelectedCropTypeAndKindCultures()) {
                    String sheet_sufix = "Врд - " + (getSheetSuffixByCtakc(ctakc));
                    String sheet_template_sufix = "Ш_" + sheet_sufix;

                    XSSFSheet sheet_clone_pests = workbook.cloneSheet(0, sheet_sufix);
                    XSSFSheet sheet_clone_pests_template = workbook.cloneSheet(1, sheet_template_sufix);

                    sheet_clone_pests.setTabColor(new XSSFColor(color_pests, null));

                    sheet_map.put(sheet_sufix, sheet_clone_pests);
                    sheet_map.put(sheet_template_sufix, sheet_clone_pests_template);
                }

                sxs_workbook = new SXSSFWorkbook(workbook, 1000000);

                for (CropTypeAndKindCulture ctakc : report.getSelectedCropTypeAndKindCultures()) {
                    String sheet_sufix = "Сор - " + (getSheetSuffixByCtakc(ctakc));
                    String sheet_template_sufix = "Ш_" + sheet_sufix;

                    List<CropTypeAndKindCulture> ctakc_list = new ArrayList<CropTypeAndKindCulture>();
                    ctakc_list.add(ctakc);

                    List<AssayCommonReport> assays_weeds = reporterService.findAssaysForWeedsCommonReport(
                            report,
                            ctakc_list);

                    if (!assays_weeds.isEmpty()) {
                        makeSheetReportCommonAssayState(workbook, sxs_workbook, sheet_map.get(sheet_sufix), sheet_map.get(sheet_template_sufix), assays_weeds, PhytoTypeEnum.WEEDS, report, cleanEmptySubjectsStates(report.getSelectedWeedsStates(), assays_weeds), new ArrayList<>(), new ArrayList<>());
                    } else {
                        workbook.removeSheetAt(workbook.getSheetIndex(sheet_map.get(sheet_sufix)));
                    }

                    workbook.removeSheetAt(workbook.getSheetIndex(sheet_map.get(sheet_template_sufix)));
                }

                for (CropTypeAndKindCulture ctakc : report.getSelectedCropTypeAndKindCultures()) {
                    String sheet_sufix = "Бол - " + (getSheetSuffixByCtakc(ctakc));
                    String sheet_template_sufix = "Ш_" + sheet_sufix;

                    List<CropTypeAndKindCulture> ctakc_list = new ArrayList<CropTypeAndKindCulture>();
                    ctakc_list.add(ctakc);

                    List<AssayCommonReport> assays_diseases = reporterService.findAssaysForDiseaseCommonReport(
                            report,
                            ctakc_list);

                    if (!assays_diseases.isEmpty()) {
                        makeSheetReportCommonAssayState(workbook, sxs_workbook, sheet_map.get(sheet_sufix), sheet_map.get(sheet_template_sufix), assays_diseases, PhytoTypeEnum.DISEASES, report, new ArrayList<>(), cleanEmptySubjectsStates(report.getSelectedDiseasesStates(), assays_diseases), new ArrayList<>());
                    } else {
                        workbook.removeSheetAt(workbook.getSheetIndex(sheet_map.get(sheet_sufix)));
                    }

                    workbook.removeSheetAt(workbook.getSheetIndex(sheet_map.get(sheet_template_sufix)));
                }

                for (CropTypeAndKindCulture ctakc : report.getSelectedCropTypeAndKindCultures()) {
                    String sheet_sufix = "Врд - " + (getSheetSuffixByCtakc(ctakc));
                    String sheet_template_sufix = "Ш_" + sheet_sufix;

                    List<CropTypeAndKindCulture> ctakc_list = new ArrayList<CropTypeAndKindCulture>();
                    ctakc_list.add(ctakc);

                    List<AssayCommonReport> assays_pests = reporterService.findAssaysForPestCommonReport(
                            report,
                            ctakc_list);

                    if (!assays_pests.isEmpty()) {
                        makeSheetReportCommonAssayState(workbook, sxs_workbook, sheet_map.get(sheet_sufix), sheet_map.get(sheet_template_sufix), assays_pests, PhytoTypeEnum.PESTS, report, new ArrayList<>(), new ArrayList<>(), cleanEmptySubjectsStates(report.getSelectedPestsStates(), assays_pests));
                    } else {
                        workbook.removeSheetAt(workbook.getSheetIndex(sheet_map.get(sheet_sufix)));
                    }

                    workbook.removeSheetAt(workbook.getSheetIndex(sheet_map.get(sheet_template_sufix)));
                }

                workbook.removeSheetAt(workbook.getSheetIndex(sheet));
                workbook.removeSheetAt(workbook.getSheetIndex(sheet_template));

                sheet_map.clear();

            } else if (!report.getSplitCropTypeAndKindCultures() && report.getSplitSubjectTypes()) {
                XSSFSheet sheet_clone_weeds = workbook.cloneSheet(0, "Сорняки");
                XSSFSheet sheet_clone_weeds_template = workbook.cloneSheet(1, "Ш_Сорняки");

                XSSFSheet sheet_clone_diseases = workbook.cloneSheet(0, "Болезни");
                XSSFSheet sheet_clone_diseases_template = workbook.cloneSheet(1, "Ш_Болезни");

                XSSFSheet sheet_clone_pests = workbook.cloneSheet(0, "Вредители");
                XSSFSheet sheet_clone_pests_template = workbook.cloneSheet(1, "Ш_Вредители");

                List<AssayCommonReport> assays_weeds = reporterService.findAssaysForWeedsCommonReport(report, report.getSelectedCropTypeAndKindCultures());
                List<AssayCommonReport> assays_diseases = reporterService.findAssaysForDiseaseCommonReport(report, report.getSelectedCropTypeAndKindCultures());
                List<AssayCommonReport> assays_pests = reporterService.findAssaysForPestCommonReport(report, report.getSelectedCropTypeAndKindCultures());

                sxs_workbook = new SXSSFWorkbook(workbook, 1000000);

                makeSheetReportCommonAssayState(workbook, sxs_workbook, sheet_clone_weeds, sheet_clone_weeds_template, assays_weeds, PhytoTypeEnum.WEEDS, report, report.getSelectedWeedsStates(), new ArrayList<>(), new ArrayList<>());
                makeSheetReportCommonAssayState(workbook, sxs_workbook, sheet_clone_diseases, sheet_clone_diseases_template, assays_diseases, PhytoTypeEnum.DISEASES, report, new ArrayList<>(), report.getSelectedDiseasesStates(), new ArrayList<>());
                makeSheetReportCommonAssayState(workbook, sxs_workbook, sheet_clone_pests, sheet_clone_pests_template, assays_pests, PhytoTypeEnum.PESTS, report, new ArrayList<>(), new ArrayList<>(), report.getSelectedPestsStates());

                sheet_clone_weeds.setTabColor(new XSSFColor(color_weeds, null));
                sheet_clone_diseases.setTabColor(new XSSFColor(color_diseases, null));
                sheet_clone_pests.setTabColor(new XSSFColor(color_pests, null));

                workbook.removeSheetAt(workbook.getSheetIndex(sheet_clone_weeds_template));
                workbook.removeSheetAt(workbook.getSheetIndex(sheet_clone_diseases_template));
                workbook.removeSheetAt(workbook.getSheetIndex(sheet_clone_pests_template));

                workbook.removeSheetAt(workbook.getSheetIndex(sheet));
                workbook.removeSheetAt(workbook.getSheetIndex(sheet_template));

            } else if (report.getSplitCropTypeAndKindCultures() && !report.getSplitSubjectTypes()) {

                Map<String, XSSFSheet> sheet_map = new HashMap<>();

                for (CropTypeAndKindCulture ctakc : report.getSelectedCropTypeAndKindCultures()) {
                    String sheet_sufix = getSheetSuffixByCtakc(ctakc);
                    String sheet_template_sufix = "Ш_" + sheet_sufix;

                    XSSFSheet sheet_clone = workbook.cloneSheet(0, sheet_sufix);
                    XSSFSheet sheet_clone_template = workbook.cloneSheet(1, "Ш_" + sheet_template_sufix);

                    sheet_map.put(sheet_sufix, sheet_clone);
                    sheet_map.put(sheet_template_sufix, sheet_clone_template);
                }

                sxs_workbook = new SXSSFWorkbook(workbook, 1000000);

                for (CropTypeAndKindCulture ctakc : report.getSelectedCropTypeAndKindCultures()) {
                    String sheet_sufix = getSheetSuffixByCtakc(ctakc);
                    String sheet_template_sufix = "Ш_" + sheet_sufix;

                    List<CropTypeAndKindCulture> ctakc_list = new ArrayList<CropTypeAndKindCulture>();
                    ctakc_list.add(ctakc);

                    List<AssayCommonReport> assays = reporterService.findOnlyAssaysStatesCommonReport(report, ctakc_list);

                    if (!assays.isEmpty()) {
                        makeSheetReportCommonAssayState(workbook, sxs_workbook, sheet_map.get(sheet_sufix), sheet_map.get(sheet_template_sufix), assays, null, report, cleanEmptySubjectsStates(report.getSelectedWeedsStates(), assays), cleanEmptySubjectsStates(report.getSelectedDiseasesStates(), assays), cleanEmptySubjectsStates(report.getSelectedPestsStates(), assays));
                    } else {
                        workbook.removeSheetAt(workbook.getSheetIndex(sheet_map.get(sheet_sufix)));
                    }

                    workbook.removeSheetAt(workbook.getSheetIndex(sheet_map.get(sheet_template_sufix)));
                }

                workbook.removeSheetAt(workbook.getSheetIndex(sheet));
                workbook.removeSheetAt(workbook.getSheetIndex(sheet_template));

                sheet_map.clear();
            } else {
                XSSFSheet sheet_clone = workbook.cloneSheet(0, "Обследования");
                XSSFSheet sheet_clone_template = workbook.cloneSheet(1, "Ш_Обследования");

                sxs_workbook = new SXSSFWorkbook(workbook, 1000000);

                List<AssayCommonReport> assays_all = reporterService.findOnlyAssaysStatesCommonReport(report, report.getSelectedCropTypeAndKindCultures());
                makeSheetReportCommonAssayState(workbook, sxs_workbook, sheet_clone, sheet_clone_template, assays_all, null, report, report.getSelectedWeedsStates(), report.getSelectedDiseasesStates(), report.getSelectedPestsStates());

                workbook.removeSheetAt(workbook.getSheetIndex(sheet_clone_template));

                workbook.removeSheetAt(workbook.getSheetIndex(sheet));
                workbook.removeSheetAt(workbook.getSheetIndex(sheet_template));
            }


            Chronograph.start(5);
            log.info("TRACE CoomonAssay WRITE BEGIN");

            try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {


                sxs_workbook.write(out);

                log.info("TRACE CoomonAssay WRITE CONTINUE " + +Chronograph.getTime(5));


                sxs_workbook.close();
                workbook.close();

                pkg.flush();
                pkg.clearRelationships();
                pkg.close();
                in.close();


                log.info("TRACE CoomonAssay WRITE END " + +Chronograph.getTime(5));

                log.info("TRACE CoomonAssay END request ... " + Chronograph.getTime(3));


                return out.toByteArray();
            }

//            return new DefaultStreamedContent(new ByteArrayInputStream(out.toByteArray()), "xlsx", "Отчет " +
//                    DateTimeFormatter.ofLocalizedDateTime(FormatStyle.MEDIUM).withLocale(new Locale("ru")).format(LocalDateTime.now()) + ".xlsx");
        } catch (IOException | InvalidFormatException e) {
            e.printStackTrace();
        }

        log.info("make report CoomonAssay END null " + Chronograph.getTime(3));

        return null;
    }


    private String getSheetSuffixByCtakc(CropTypeAndKindCulture ctakc) {
        return CropTypeEnum.SOWING.equals(ctakc.getCropType()) ? (ctakc.getCropKindCulture() != null ? ctakc.getCropKindCulture().getName() : "Без культуры") : Optional.ofNullable(ctakc.getCropType())
                .map(CropTypeEnum::getName)
                .orElse("Не указана культура и тип сева");
    }


    public String genArrayColumnRows(String column, List<Integer> rows) {
        StringBuilder sb = new StringBuilder();
        for (Integer row : rows) {
            if (sb.length() > 0) {
                sb.append(",");
            }
            sb.append(column).append(row);
        }

        return sb.toString();
    }

    class PhytotypeMethod {
        private PhytoTypeEnum phytoTypeEnum;
        private String method;

        public PhytotypeMethod(PhytoTypeEnum phytoTypeEnum, String method) {
            this.phytoTypeEnum = phytoTypeEnum;
            this.method = method;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PhytotypeMethod that = (PhytotypeMethod) o;
            return phytoTypeEnum == that.phytoTypeEnum && Objects.equals(method, that.method);
        }

        @Override
        public int hashCode() {
            return Objects.hash(phytoTypeEnum, method);
        }
    }
}
