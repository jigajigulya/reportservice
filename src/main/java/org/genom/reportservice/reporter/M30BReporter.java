package org.genom.reportservice.reporter;


import com.gnm.enums.DepartmentStructureTypeEnum;
import com.gnm.enums.RegionalTypeEnum;
import com.gnm.enums.ase.SeedProductionReportKindEnum;
import com.gnm.model.ase.mon.MonCCCommon;
import com.gnm.model.ase.wrapper.SeedsInfoWrapper;
import com.gnm.model.common.DepartmentStructure;
import com.gnm.model.common.geo.TerRegion;
import com.gnm.model.common.geo.TerTownship;
import com.gnm.utils.CollectionUtils;
import com.gnm.utils.DateUtils;
import com.gnm.utils.NumberUtils;
import com.gnm.utils.doc.DocTemplateXLSWorker;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.poi.hssf.usermodel.HSSFFont;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.streaming.SXSSFCell;
import org.apache.poi.xssf.streaming.SXSSFRow;
import org.apache.poi.xssf.streaming.SXSSFSheet;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbookFactory;
import org.genom.reportservice.interfaces.RowXlsCreatorEditorInt;
import org.genom.reportservice.interfaces.SeedProductionParameterInt;
import org.genom.reportservice.service.DepService;
import org.genom.reportservice.service.SeedProductionMonitoringService;
import org.genom.reportservice.utils.TreeNode;
import org.springframework.context.annotation.Scope;
import org.springframework.core.io.ResourceLoader;
import org.springframework.stereotype.Component;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static com.gnm.enums.ase.SeedProductionReportKindEnum.KIND_CULTURES_GROUPS;
import static com.gnm.enums.ase.SeedProductionReportKindEnum.KIND_REGIONS;


@Slf4j
@Component
@Scope("prototype")
@RequiredArgsConstructor
public class M30BReporter implements RowXlsCreatorEditorInt {


    private final SeedProductionMonitoringService service;
    private final DepService depService;
    private final ResourceLoader resourceLoader;

    private InputStream inputStream;

    private SeedProductionParameterInt seedProductionParameter;
    private SXSSFWorkbook sxs_workbook;


    private boolean coefsIgnore = false;

    private TreeNode rootTreeNode = null;

    private CellStyle cellStyleCat_template;

    private final Map<String, CellStyle> levelCellStyles = new HashMap<>();

    private XSSFRow row_country = null;
    private XSSFRow row_structure = null;
    ;
    private XSSFRow row_categrory = null;
    private XSSFRow row_backfiil = null;
    private XSSFRow row_info = null;


    /*public M30BReporter prepare(SeedProductionParameter seedProductionParameter, boolean coefsIgnore) {
        this.rootTreeNode = null;
        this.seedProductionParameter = seedProductionParameter;
        this.coefsIgnore = coefsIgnore;
        return this;
    }

    public M30BReporter prepare(SeedProductionParameter seedProductionParameter, boolean coefsIgnore, TreeNode rootTreeNode) {
        this.seedProductionParameter = seedProductionParameter;
        this.rootTreeNode = rootTreeNode;
        this.coefsIgnore = coefsIgnore;

        return this;
    }*/


    private String joinTownshipNames(List<TerTownship> townships) {
        return townships.stream()
                .map(TerTownship::getNameShort)
                .map(String::valueOf)
                .collect(Collectors.joining(", "));
    }

    private String joinRegionNames(List<TerRegion> regions) {
        return regions.stream()
                .map(TerRegion::getName)
                .map(String::valueOf)
                .collect(Collectors.joining(", "));
    }

    private String joinDepartmentsNames(List<DepartmentStructure> departments) {
        return departments.stream()
                .map(DepartmentStructure::getNameShort)
                .map(String::valueOf)
                .collect(Collectors.joining(", "));
    }


    public ByteArrayOutputStream write() {
        try (XSSFWorkbook workbook = XSSFWorkbookFactory.createWorkbook(OPCPackage.open(inputStream))) {

            this.sxs_workbook = new SXSSFWorkbook(workbook, 1000000);
            FormulaEvaluator evaluator = sxs_workbook.getCreationHelper().createFormulaEvaluator();


            XSSFSheet sheet = workbook.getSheetAt(0);
            XSSFSheet sheet_template = workbook.getSheetAt(1);

            for (int i = 3; i < 6; i++) {
                for (Cell cell : sheet.getRow(i)) {
                    evalCell(cell, "m30BReporter");
                }
            }

            String culture_group_sufix = "";

            if (SeedProductionReportKindEnum.KIND_REGIONS.equals(seedProductionParameter.getSeedProdReportKind()) && !Objects.isNull(seedProductionParameter.getFilterGroupName())) {
                culture_group_sufix = " - " + seedProductionParameter.getFilterGroupName();
            }

            String title_region = "";

            if (seedProductionParameter.isFederalOrAdmin()) {
                if (RegionalTypeEnum.BY_TERRITORIES.equals(seedProductionParameter.getRegionalType())) {
                    if (seedProductionParameter.isNotEmptyRegionsAndAllRegions()) {
                        if (seedProductionParameter.isRegionsContainsAllAllRegions()) {
                            title_region = "РФ (Все субъекты)";
                        } else {
                            title_region = seedProductionParameter.getRegionsJoinName();
                        }
                    }
                } else if (RegionalTypeEnum.BY_DEPARTMENTS.equals(seedProductionParameter.getRegionalType())) {
                    if (seedProductionParameter.isNotEmptyDepsAndAllDepsRegion()) {
                        if (seedProductionParameter.isRegionDepartmentsContainsAllAllDepartmentsRegionsCol()) {
                            title_region = "РФ (Все филиалы)";
                        } else {
                            title_region = joinDepartmentsNames(seedProductionParameter.getRegionDepartments());
                        }
                    }
                }
            } else {
                String townships_sufix = "";

                if (RegionalTypeEnum.BY_TERRITORIES.equals(seedProductionParameter.getRegionalType())) {
                    if (CollectionUtils.isNotNullOrNotEmpty(seedProductionParameter.getTownships())
                            && seedProductionParameter.getTerRegionTownships().size() != seedProductionParameter.getTownships().size()) {
                        townships_sufix = " - " + joinTownshipNames(seedProductionParameter.getTownships());
                    }
                } else if (RegionalTypeEnum.BY_DEPARTMENTS.equals(seedProductionParameter.getRegionalType())) {

                    Integer region_townDepsCount = depService.getCountDeps(DepartmentStructureTypeEnum.TOWNSHIP, seedProductionParameter.getDepartmentRegion().getId());

                    if (CollectionUtils.isNotNullOrNotEmpty(seedProductionParameter.getTownshipDepartments())
                            && region_townDepsCount != seedProductionParameter.getTownshipDepartments().size()) {
                        townships_sufix = " - " + joinDepartmentsNames(seedProductionParameter.getTownshipDepartments());
                    }
                }

                title_region = seedProductionParameter.getDepartmentRegion().getTerrainRegion().getName() + townships_sufix;
            }

            log.info("*** M30BReporter " + seedProductionParameter.isAssaysInclude());

            sheet.getRow(1).getCell(1).setCellValue(title_region + culture_group_sufix);

            if (seedProductionParameter.getMonitoringDate() != null) {
                sheet.getRow(2).getCell(1).setCellValue(DateUtils.DATE_FORMATTER.format(seedProductionParameter.getMonitoringDate().getDate()) + " - " + seedProductionParameter.getMonitoringDate().getTitle());
            } else {
                sheet.getRow(2).getCell(1).setCellValue("Опер.отчёт из базы данных");
            }

            LocalDateTime date_data_gen = LocalDateTime.now();

            if (seedProductionParameter.getMainMonitoringView() != null) {
                date_data_gen = seedProductionParameter.getMainMonitoringView().getDateCreated();
            }

            String title_date_data_gen = DateUtils.DATE_FORMATTER.format(date_data_gen) + " " + DateUtils.DATE_FORMATTER_DD_MM.format(date_data_gen);
            sheet.getRow(3).getCell(1).setCellValue(title_date_data_gen);

//            String title_dates_data_gen = utilsBean.dateShort(seedProductionParameter.getDateBegin()) + " - " + utilsBean.dateShort(seedProductionParameter.getDateFinish()) + " (" + seedProductionParameter.getSeedProdForHarvestYear() +" год урожая)";
            String title_dates_data_gen = seedProductionParameter.getSeedProdForHarvestYear() + " год урожая";
            sheet.getRow(3).getCell(9).setCellValue(title_dates_data_gen);

            String title_dataLayerSource = seedProductionParameter.getDataLayerSource().getTitle();
            sheet.getRow(3).getCell(13).setCellValue(title_dataLayerSource);


            row_structure = sheet_template.getRow(4);
            row_categrory = sheet_template.getRow(5);
            row_backfiil = sheet_template.getRow(6);
            row_info = sheet_template.getRow(7);

            row_country = sheet_template.getRow(8);

            initStyles();

            SXSSFSheet sxs_sheet = sxs_workbook.getSheet(sheet.getSheetName());

            DocTemplateXLSWorker my_worker = new DocTemplateXLSWorker() {

                public ByteArrayOutputStream write() {
                    rootTreeNode = null;
                    return null;
                }
            };

            int cur_row = 7;

            if (rootTreeNode == null) {
                rootTreeNode = service.initSeedProductionMonitoringStructure(seedProductionParameter);
            }

            if (!rootTreeNode.getChildren().isEmpty()) {
                for (TreeNode root_node : rootTreeNode.getChildren()) {
                    cur_row = fillTreeNode(root_node, sxs_sheet, my_worker, cur_row, 0);
                }
            }

            sxs_workbook.removeSheetAt(1);
            evaluator.evaluateAll();
            sxs_sheet.flushRows();


            ByteArrayOutputStream out = new ByteArrayOutputStream();
            sxs_workbook.write(out);
            sxs_workbook.close();
            workbook.close();

            this.rootTreeNode = null;
            return out;
        } catch (IOException | InvalidFormatException e) {
            e.printStackTrace();
            this.rootTreeNode = null;
            return null;
        }
    }

    private int fillTreeNode(TreeNode node, SXSSFSheet sxs_sheet, DocTemplateXLSWorker worker, int rowIndex, int treeLevel) {

        MonCCCommon mst = (MonCCCommon) node.getData();

        int curRow = rowIndex + 1;
        int new_treeLevel = treeLevel + 1;
//        int new_treeLevel = 0;
//
//        if (mst.getGroupLevel() != null) {
//            new_treeLevel = mst.getGroupLevel().intValue();
//        }

        int group_row_collapsed_begin = curRow + 1;

        SXSSFRow row = sxs_sheet.createRow(curRow);

        if (KIND_CULTURES_GROUPS.equals(seedProductionParameter.getSeedProdReportKind())) {
            if (treeLevel < 2 && "group".equals(mst.getType())) {
                worker.copyRow(row_country, row);
            } else {
                worker.copyRow(row_structure, row);
            }
        } else if (KIND_REGIONS.equals(seedProductionParameter.getSeedProdReportKind())) {
            if ("country".equals(mst.getType()) || "district".equals(mst.getType()) || "region".equals(mst.getType())) {
                worker.copyRow(row_country, row);
            } else {
                worker.copyRow(row_structure, row);
            }
        }


        SXSSFCell cell_name = row.getCell(0);

        cell_name.setCellStyle(genStyle(sxs_workbook, cell_name.getCellStyle(), (short) treeLevel, null));

        cell_name.setCellValue(mst.getName());

        int cc = 1;

        if (mst.getType() != null) {
            String type = mst.getType();

            if ("country".equals(mst.getType())) {
                type = "страна";
            } else if ("region".equals(mst.getType())) {
                type = "регион";
            } else if ("district".equals(mst.getType())) {
                type = "ф.округ";
            } else if ("group".equals(mst.getType())) {
                type = "группа культур";
            } else if ("culture".equals(mst.getType())) {
                type = "культура";
            } else if ("sort".equals(mst.getType())) {
                type = "сорт";
            }

            row.getCell(cc++).setCellValue(type);
        } else {
            row.getCell(cc++).setBlank();
        }

        if (mst.getStructureObjectId() != null) {
//            row.getCell(cc++).setCellValue(String.valueOf(mst.getStructureObjectId()));
            row.getCell(cc++).setBlank();
        } else {
            row.getCell(cc++).setBlank();
        }

        if (mst.getTerRegionName() != null) {
            row.getCell(cc++).setCellValue(mst.getTerRegionName());
        } else {
            row.getCell(cc++).setBlank();
        }

        if (mst.getCultureName() != null) {
            row.getCell(cc++).setCellValue(mst.getCultureName());
        } else {
            row.getCell(cc++).setBlank();
        }


        if (mst.getSortCode() != null) {
            row.getCell(cc++).setCellValue(mst.getSortCode());
        } else {
            row.getCell(cc++).setBlank();
        }


        if (mst.getSortName() != null) {
            row.getCell(cc++).setCellValue(mst.getSortName());
        } else {
            row.getCell(cc++).setBlank();
        }

        if (mst.getSeedprodSeedsKind() != null) {
            if ("SORT".equals(mst.getSeedprodSeedsKind())) {
                row.getCell(cc++).setCellValue("сорт");
            } else if ("HYBRID".equals(mst.getSeedprodSeedsKind())) {
                row.getCell(cc++).setCellValue("гибрид");
            } else {
                row.getCell(cc++).setCellValue(mst.getSeedprodSeedsKind());
            }
        } else {
            row.getCell(cc++).setBlank();
        }

        if (mst.getFillSeedFund() != null) {
            row.getCell(cc++).setCellValue(mst.getFillSeedFund());
        } else {
            row.getCell(cc++).setBlank();
        }

        if (mst.getFillSeedForSale() != null) {
            row.getCell(cc++).setCellValue(mst.getFillSeedForSale());
        } else {
            row.getCell(cc++).setBlank();
        }

        if (mst.getFillRemovedSeedFund() != null) {
            row.getCell(cc++).setCellValue(mst.getFillRemovedSeedFund());
        } else {
            row.getCell(cc++).setBlank();
        }


        if (mst.getRp_nc() != null) {
            row.getCell(cc++).setCellValue(mst.getRp_nc());
        } else {
            row.getCell(cc++).setBlank();
        }


        if (mst.getRp_os() != null) {
            row.getCell(cc++).setCellValue(mst.getRp_os());
        } else {
            row.getCell(cc++).setBlank();
        }

        if (mst.getRp_es() != null) {
            row.getCell(cc++).setCellValue(mst.getRp_es());
        } else {
            row.getCell(cc++).setBlank();
        }

        if (mst.getRp_rs_1() != null) {
            row.getCell(cc++).setCellValue(mst.getRp_rs_1());
        } else {
            row.getCell(cc++).setBlank();
        }

        if (mst.getRp_rs_1_rst() != null) {
            row.getCell(cc++).setCellValue(mst.getRp_rs_1_rst());
        } else {
            row.getCell(cc++).setBlank();
        }

        if (mst.getRp_rs_2() != null) {
            row.getCell(cc++).setCellValue(mst.getRp_rs_2());
        } else {
            row.getCell(cc++).setBlank();
        }

        if (mst.getRp_rs_2_rst() != null) {
            row.getCell(cc++).setCellValue(mst.getRp_rs_2_rst());
        } else {
            row.getCell(cc++).setBlank();
        }

        if (mst.getRp_rs_3() != null) {
            row.getCell(cc++).setCellValue(mst.getRp_rs_3());
        } else {
            row.getCell(cc++).setBlank();
        }

        if (mst.getRp_rs_3_rst() != null) {
            row.getCell(cc++).setCellValue(mst.getRp_rs_3_rst());
        } else {
            row.getCell(cc++).setBlank();
        }

        if (mst.getRp_rs_4() != null) {
            row.getCell(cc++).setCellValue(mst.getRp_rs_4());
        } else {
            row.getCell(cc++).setBlank();
        }

        if (mst.getRp_rs_4_rst() != null) {
            row.getCell(cc++).setCellValue(mst.getRp_rs_4_rst());
        } else {
            row.getCell(cc++).setBlank();
        }

        if (mst.getRp_f1() != null) {
            row.getCell(cc++).setCellValue(mst.getRp_f1());
        } else {
            row.getCell(cc++).setBlank();
        }

        if (mst.getRp_rs_5b() != null) {
            row.getCell(cc++).setCellValue(mst.getRp_rs_5b());
        } else {
            row.getCell(cc++).setBlank();
        }

        if (mst.getRp_rs_5b_rst() != null) {
            row.getCell(cc++).setCellValue(mst.getRp_rs_5b_rst());
        } else {
            row.getCell(cc++).setBlank();
        }

        if (mst.getRp_not() != null) {
            row.getCell(cc++).setCellValue(mst.getRp_not());
        } else {
            row.getCell(cc++).setBlank();
        }

        if (mst.getFillPurchaseEn() != null) {
            row.getCell(cc++).setCellValue(mst.getFillPurchaseEn());
        } else {
            row.getCell(cc++).setBlank();
        }

        if (mst.getFillPurchaseEnPercAll() != null) {
            row.getCell(cc++).setCellValue(mst.getFillPurchaseEnPercAll());
        } else {
            row.getCell(cc++).setBlank();
        }

        if (mst.getNr_fillAll() != null) {
            row.getCell(cc++).setCellValue(mst.getNr_fillAll());
        } else {
            row.getCell(cc++).setBlank();
        }

        if (mst.getNrFillPurchaseEnPercAll() != null) {
            row.getCell(cc++).setCellValue(mst.getNrFillPurchaseEnPercAll());
        } else {
            row.getCell(cc++).setBlank();
        }

        if (mst.getNr_fillPurchaseEn() != null) {
            row.getCell(cc++).setCellValue(mst.getNr_fillPurchaseEn());
        } else {
            row.getCell(cc++).setBlank();
        }


        if (mst.getRu_fillAll() != null) {
            row.getCell(cc++).setCellValue(mst.getRu_fillAll());
        } else {
            row.getCell(cc++).setBlank();
        }

        if (mst.getRuFillPurchaseEnPercAll() != null) {
            row.getCell(cc++).setCellValue(mst.getRuFillPurchaseEnPercAll());
        } else {
            row.getCell(cc++).setBlank();
        }

        if (mst.getRu_fillLocalized() != null) {
            row.getCell(cc++).setCellValue(mst.getRu_fillLocalized());
        } else {
            row.getCell(cc++).setBlank();
        }

        if (mst.getRu_fillPurchaseEn() != null) {
            row.getCell(cc++).setCellValue(mst.getRu_fillPurchaseEn());
        } else {
            row.getCell(cc++).setBlank();
        }


        if (mst.getEn_fillAll() != null) {
            row.getCell(cc++).setCellValue(mst.getEn_fillAll());
        } else {
            row.getCell(cc++).setBlank();
        }

        if (mst.getEnFillPurchaseEnPercAll() != null) {
            row.getCell(cc++).setCellValue(mst.getEnFillPurchaseEnPercAll());
        } else {
            row.getCell(cc++).setBlank();
        }

        if (mst.getEn_fillLocalized() != null) {
            row.getCell(cc++).setCellValue(mst.getEn_fillLocalized());
        } else {
            row.getCell(cc++).setBlank();
        }

        if (mst.getEn_fillPurchaseEn() != null) {
            row.getCell(cc++).setCellValue(mst.getEn_fillPurchaseEn());
        } else {
            row.getCell(cc++).setBlank();
        }

        if (mst.getSortType() != null) {

            if ("NORMAL".equals(mst.getSortType())) {
                row.getCell(cc++).setCellValue("В реестре");
            } else if ("NOSORT".equals(mst.getSortType())) {
                row.getCell(cc++).setCellValue("Не сортовой");
            } else if ("RNNS".equals(mst.getSortType())) {
                row.getCell(cc++).setCellValue("РННС");
            } else if ("NODOPUSK".equals(mst.getSortType())) {
                row.getCell(cc++).setCellValue("Нет допуска");
            } else if ("CANDIDAT".equals(mst.getSortType())) {
                row.getCell(cc++).setCellValue("Кандидат на исключение");
            } else if ("GIBRID".equals(mst.getSortType())) {
                row.getCell(cc++).setCellValue("Гибрид");
            } else {
                row.getCell(cc++).setCellValue(mst.getSortType());
            }

        } else {
            row.getCell(cc++).setBlank();
        }


        if (mst.getSortRegion() != null) {
            row.getCell(cc++).setCellValue(mst.getSortRegion());
        } else {
            row.getCell(cc++).setBlank();
        }


        if (mst.getSortYear() != null) {
            row.getCell(cc++).setCellValue(mst.getSortYear());
        } else {
            row.getCell(cc++).setBlank();
        }


        if (mst.getSortOriginatorMain() != null) {
            row.getCell(cc++).setCellValue(mst.getSortOriginatorMain());
        } else {
            row.getCell(cc++).setBlank();
        }

        if (mst.getSortOriginatorMainCountryCodeIso() != null) {
            row.getCell(cc++).setCellValue(mst.getSortOriginatorMainCountryName());
        } else {
            row.getCell(cc++).setBlank();
        }

        SXSSFRow row_ass = null;

        int node_child_size = node.getChildCount();

        for (int i = 0; i < node_child_size; i++) {
            curRow = fillTreeNode(node.getChildren().get(i), sxs_sheet, worker, curRow, new_treeLevel);
        }

        int group_row_sort_collapsed_begin = 0;

        if (seedProductionParameter.isAssaysInclude() && "sort".equals(mst.getType())) {
            group_row_sort_collapsed_begin = curRow;

            if (mst.getBackfillsData() != null && !mst.getBackfillsData().isEmpty()) {
                for (SeedsInfoWrapper siw : mst.getBackfillsData()) {
                    siw.clearZeros();
                    curRow++;
                    fillTreeNodeBackfill(mst, siw, sxs_sheet, worker, curRow);
                }
            }

            if (mst.getInfosData() != null && !mst.getInfosData().isEmpty()) {
                for (SeedsInfoWrapper siw : mst.getInfosData()) {
                    siw.clearZeros();
                    curRow++;
                    fillTreeNodeBackfill(mst, siw, sxs_sheet, worker, curRow);
                }
            }
        }

        int group_row_sort_collapsed_end = curRow;

        int group_row_collapsed_end = curRow;

        sxs_sheet.setRowSumsBelow(false);

        if (node_child_size > 0) {
            if (KIND_CULTURES_GROUPS.equals(seedProductionParameter.getSeedProdReportKind())) {
                if (treeLevel == 1) {
//                    sxs_sheet.groupRow(group_row_collapsed_begin, group_row_collapsed_end);
//                    sxs_sheet.setRowGroupCollapsed(group_row_collapsed_begin, false);
                }
                if (treeLevel == 2) {
                    sxs_sheet.groupRow(group_row_collapsed_begin, group_row_collapsed_end);
                    sxs_sheet.setRowGroupCollapsed(group_row_collapsed_begin, true);
                }
            } else if (KIND_REGIONS.equals(seedProductionParameter.getSeedProdReportKind())) {
                if (treeLevel == 2) {
                    sxs_sheet.groupRow(group_row_collapsed_begin, group_row_collapsed_end);
                    sxs_sheet.setRowGroupCollapsed(group_row_collapsed_begin, true);
                }

                if ("region".equals(mst.getType()) && (NumberUtils.greaterThan(mst.getCountBackfills(), 0L) || NumberUtils.greaterThan(mst.getCountInfos(), 0L))) {
                    sxs_sheet.groupRow(group_row_collapsed_begin, group_row_collapsed_end);
                    sxs_sheet.setRowGroupCollapsed(group_row_collapsed_begin, true);
                }
            }

            if ("culture".equals(mst.getType()) && (NumberUtils.greaterThan(mst.getCountBackfills(), 0L) || NumberUtils.greaterThan(mst.getCountInfos(), 0L))) {
                sxs_sheet.groupRow(group_row_collapsed_begin, group_row_collapsed_end);
                sxs_sheet.setRowGroupCollapsed(group_row_collapsed_begin, true);
            }
        }

        if (group_row_sort_collapsed_begin > 0) {
            sxs_sheet.groupRow(group_row_sort_collapsed_begin + 1, group_row_sort_collapsed_end);
//            sxs_sheet.setRowGroupCollapsed(group_row_sort_collapsed_end, true);
        }

        return curRow;
    }

    private int fillTreeNodeBackfill(MonCCCommon mst, SeedsInfoWrapper siw, SXSSFSheet sxs_sheet, DocTemplateXLSWorker worker, int rowIndex) {

        int curRow = rowIndex;

        SXSSFRow row = sxs_sheet.createRow(curRow);

        if ("RSC".equals(siw.getInfoKind())) {
            worker.copyRow(row_backfiil, row);
        } else {
            worker.copyRow(row_info, row);
        }

        SXSSFCell cell_name = row.getCell(0);

        cell_name.setCellStyle(genStyle(sxs_workbook, cell_name.getCellStyle(), (short) 0, null));

        cell_name.setCellValue(siw.getNameView());

        int cc = 1;

        if (siw.getInfoKind() != null) {
            String kind = siw.getInfoKind();

            if ("RSC".equals(siw.getInfoKind())) {
                kind = "засыпка";
            } else if ("OTHER".equals(siw.getInfoKind())) {
                kind = "данные стор.орг.";
            }

            row.getCell(cc++).setCellValue(kind);
        } else {
            row.getCell(cc++).setBlank();
        }

        if (siw.getId() != null) {
            row.getCell(cc++).setCellValue(String.valueOf(siw.getId()));
        } else {
            row.getCell(cc++).setBlank();
        }


        if (siw.getNatTerRegionName() != null) {
            row.getCell(cc++).setCellValue(siw.getNatTerRegionName());
        } else {
            row.getCell(cc++).setBlank();
        }

        if (mst.getCultureName() != null) {
            row.getCell(cc++).setCellValue(mst.getCultureName());
        } else {
            row.getCell(cc++).setBlank();
        }

        if (mst.getSortCode() != null) {
            row.getCell(cc++).setCellValue(mst.getSortCode());
        } else {
            row.getCell(cc++).setBlank();
        }


        if (mst.getSortName() != null) {
            row.getCell(cc++).setCellValue(mst.getSortName());
        } else {
            row.getCell(cc++).setBlank();
        }

        if (mst.getSeedprodSeedsKind() != null) {
            if ("SORT".equals(mst.getSeedprodSeedsKind())) {
                row.getCell(cc++).setCellValue("сорт");
            } else if ("HYBRID".equals(mst.getSeedprodSeedsKind())) {
                row.getCell(cc++).setCellValue("гибрид");
            } else {
                row.getCell(cc++).setCellValue(mst.getSeedprodSeedsKind());
            }
        } else {
            row.getCell(cc++).setBlank();
        }

        if (siw.getFillSeedFund() != null) {
            row.getCell(cc++).setCellValue(siw.getFillSeedFund());
        } else {
            row.getCell(cc++).setBlank();
        }

        if (siw.getFillSeedForSale() != null) {
            row.getCell(cc++).setCellValue(siw.getFillSeedForSale());
        } else {
            row.getCell(cc++).setBlank();
        }

        if (siw.getFillRemovedSeedFund() != null) {
            row.getCell(cc++).setCellValue(siw.getFillRemovedSeedFund());
        } else {
            row.getCell(cc++).setBlank();
        }


        if (siw.getRp_nc() != null) {
            row.getCell(cc++).setCellValue(siw.getRp_nc());
        } else {
            row.getCell(cc++).setBlank();
        }


        if (siw.getRp_os() != null) {
            row.getCell(cc++).setCellValue(siw.getRp_os());
        } else {
            row.getCell(cc++).setBlank();
        }

        if (siw.getRp_es() != null) {
            row.getCell(cc++).setCellValue(siw.getRp_es());
        } else {
            row.getCell(cc++).setBlank();
        }

        if (siw.getRp_rs_1() != null) {
            row.getCell(cc++).setCellValue(siw.getRp_rs_1());
        } else {
            row.getCell(cc++).setBlank();
        }

        if (siw.getRp_rs_1_rst() != null) {
            row.getCell(cc++).setCellValue(siw.getRp_rs_1_rst());
        } else {
            row.getCell(cc++).setBlank();
        }

        if (siw.getRp_rs_2() != null) {
            row.getCell(cc++).setCellValue(siw.getRp_rs_2());
        } else {
            row.getCell(cc++).setBlank();
        }

        if (siw.getRp_rs_2_rst() != null) {
            row.getCell(cc++).setCellValue(siw.getRp_rs_2_rst());
        } else {
            row.getCell(cc++).setBlank();
        }

        if (siw.getRp_rs_3() != null) {
            row.getCell(cc++).setCellValue(siw.getRp_rs_3());
        } else {
            row.getCell(cc++).setBlank();
        }

        if (siw.getRp_rs_3_rst() != null) {
            row.getCell(cc++).setCellValue(siw.getRp_rs_3_rst());
        } else {
            row.getCell(cc++).setBlank();
        }

        if (siw.getRp_rs_4() != null) {
            row.getCell(cc++).setCellValue(siw.getRp_rs_4());
        } else {
            row.getCell(cc++).setBlank();
        }

        if (siw.getRp_rs_4_rst() != null) {
            row.getCell(cc++).setCellValue(siw.getRp_rs_4_rst());
        } else {
            row.getCell(cc++).setBlank();
        }

        if (siw.getRp_f1() != null) {
            row.getCell(cc++).setCellValue(siw.getRp_f1());
        } else {
            row.getCell(cc++).setBlank();
        }

        if (siw.getRp_rs_5b() != null) {
            row.getCell(cc++).setCellValue(siw.getRp_rs_5b());
        } else {
            row.getCell(cc++).setBlank();
        }

        if (siw.getRp_rs_5b_rst() != null) {
            row.getCell(cc++).setCellValue(siw.getRp_rs_5b_rst());
        } else {
            row.getCell(cc++).setBlank();
        }

        if (siw.getRp_not() != null) {
            row.getCell(cc++).setCellValue(siw.getRp_not());
        } else {
            row.getCell(cc++).setBlank();
        }

        if (siw.getFillPurchaseEn() != null) {
            row.getCell(cc++).setCellValue(siw.getFillPurchaseEn());
        } else {
            row.getCell(cc++).setBlank();
        }

        if (siw.getFillPurchaseEnPercAll() != null) {
            row.getCell(cc++).setCellValue(siw.getFillPurchaseEnPercAll());
        } else {
            row.getCell(cc++).setBlank();
        }

        if (siw.getNr_fillAll() != null) {
            row.getCell(cc++).setCellValue(siw.getNr_fillAll());
        } else {
            row.getCell(cc++).setBlank();
        }

        if (siw.getNrFillPurchaseEnPercAll() != null) {
            row.getCell(cc++).setCellValue(siw.getNrFillPurchaseEnPercAll());
        } else {
            row.getCell(cc++).setBlank();
        }

        if (siw.getNr_fillPurchaseEn() != null) {
            row.getCell(cc++).setCellValue(siw.getNr_fillPurchaseEn());
        } else {
            row.getCell(cc++).setBlank();
        }


        if (siw.getRu_fillAll() != null) {
            row.getCell(cc++).setCellValue(siw.getRu_fillAll());
        } else {
            row.getCell(cc++).setBlank();
        }

        if (siw.getRuFillPurchaseEnPercAll() != null) {
            row.getCell(cc++).setCellValue(siw.getRuFillPurchaseEnPercAll());
        } else {
            row.getCell(cc++).setBlank();
        }

        if (siw.getRu_fillLocalized() != null) {
            row.getCell(cc++).setCellValue(siw.getRu_fillLocalized());
        } else {
            row.getCell(cc++).setBlank();
        }

        if (siw.getRu_fillPurchaseEn() != null) {
            row.getCell(cc++).setCellValue(siw.getRu_fillPurchaseEn());
        } else {
            row.getCell(cc++).setBlank();
        }


        if (siw.getEn_fillAll() != null) {
            row.getCell(cc++).setCellValue(siw.getEn_fillAll());
        } else {
            row.getCell(cc++).setBlank();
        }

        if (siw.getEnFillPurchaseEnPercAll() != null) {
            row.getCell(cc++).setCellValue(siw.getEnFillPurchaseEnPercAll());
        } else {
            row.getCell(cc++).setBlank();
        }

        if (siw.getEn_fillLocalized() != null) {
            row.getCell(cc++).setCellValue(siw.getEn_fillLocalized());
        } else {
            row.getCell(cc++).setBlank();
        }

        if (siw.getEn_fillPurchaseEn() != null) {
            row.getCell(cc++).setCellValue(siw.getEn_fillPurchaseEn());
        } else {
            row.getCell(cc++).setBlank();
        }

        if (mst.getSortType() != null) {

            if ("NORMAL".equals(mst.getSortType())) {
                row.getCell(cc++).setCellValue("В реестре");
            } else if ("NOSORT".equals(mst.getSortType())) {
                row.getCell(cc++).setCellValue("Не сортовой");
            } else if ("RNNS".equals(mst.getSortType())) {
                row.getCell(cc++).setCellValue("РННС");
            } else if ("NODOPUSK".equals(mst.getSortType())) {
                row.getCell(cc++).setCellValue("Нет допуска");
            } else if ("CANDIDAT".equals(mst.getSortType())) {
                row.getCell(cc++).setCellValue("Кандидат на исключение");
            } else if ("GIBRID".equals(mst.getSortType())) {
                row.getCell(cc++).setCellValue("Гибрид");
            } else {
                row.getCell(cc++).setCellValue(mst.getSortType());
            }

        } else {
            row.getCell(cc++).setBlank();
        }


        if (mst.getSortRegion() != null) {
            row.getCell(cc++).setCellValue(mst.getSortRegion());
        } else {
            row.getCell(cc++).setBlank();
        }


        if (mst.getSortYear() != null) {
            row.getCell(cc++).setCellValue(mst.getSortYear());
        } else {
            row.getCell(cc++).setBlank();
        }


        if (mst.getSortOriginatorMain() != null) {
            row.getCell(cc++).setCellValue(mst.getSortOriginatorMain());
        } else {
            row.getCell(cc++).setBlank();
        }

        if (mst.getSortOriginatorMainCountryCodeIso() != null) {
            row.getCell(cc++).setCellValue(mst.getSortOriginatorMainCountryName());
        } else {
            row.getCell(cc++).setBlank();
        }

        return curRow;
    }

    private void initStyles() {
        Font font_group = sxs_workbook.createFont();

        configureFonts(font_group);

        CellStyle cellStyleGroup = sxs_workbook.createCellStyle();
        cellStyleGroup.setFont(font_group);

        configureBorderStyles(cellStyleGroup);
    }

    public CellStyle genStyle(SXSSFWorkbook workbook, CellStyle template_style, short indention, Font font) {

        if (workbook != null && template_style != null) {
            String key_font = "";

            if (font == null) {
                key_font = "" + template_style.getFontIndex();
            } else {
                key_font = "" + font.getIndex();
            }

            String key = template_style.getIndex() + "." + key_font + "." + indention;
            CellStyle style_key = levelCellStyles.getOrDefault(key, null);

            if (style_key == null) {
                CellStyle style_new = workbook.createCellStyle();
                style_new.cloneStyleFrom(template_style);
                style_new.setFont(font);
                style_new.setIndention(indention);

                levelCellStyles.put(key, style_new);

                return style_new;
            }

            return style_key;
        }

        return null;
    }

    private void configureBorderStyles(CellStyle... cellStyles) {
        for (CellStyle style : cellStyles) {
            style.setBorderTop(BorderStyle.THIN);
            style.setBorderLeft(BorderStyle.THIN);
            style.setBorderRight(BorderStyle.THIN);
            style.setBorderBottom(BorderStyle.THIN);
        }
    }

    private void configureFonts(Font... fonts) {
        for (Font font : fonts) {
            font.setFontName(HSSFFont.FONT_ARIAL);
            font.setFontHeightInPoints((short) 10);
        }

    }

    public String getAreaUnit() {
        return seedProductionParameter.getAreaUnit();
    }

    public String getMassUnit() {
        return seedProductionParameter.getMassUnit();
    }

    @SneakyThrows
    public byte[] constructExcel(SeedProductionParameterInt parameter) {
        this.seedProductionParameter = parameter;
        try (InputStream inputStream = resourceLoader.getResource("classpath:/excel/SEEDCROPS_30B.xlsx").getInputStream()) {
            this.inputStream = inputStream;
            return write().toByteArray();
        }
    }
}
