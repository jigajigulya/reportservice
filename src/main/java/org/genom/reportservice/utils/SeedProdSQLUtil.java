package org.genom.reportservice.utils;

import com.gnm.criteria.SeedProductionParameter;
import com.gnm.enums.RegionalTypeEnum;
import com.gnm.enums.ase.SeedProductionReportTypeEnum;
import com.gnm.utils.ReportUtils;
import lombok.extern.slf4j.Slf4j;
import org.genom.reportservice.interfaces.SeedProductionParameterInt;

import static com.gnm.enums.ase.SeedProductionReportKindEnum.KIND_REGIONS;
import static com.gnm.enums.ase.SeedProductionServiceTypeEnum.APPROBATION;

@Slf4j
public class SeedProdSQLUtil {

    public static String constructDepClause(SeedProductionParameterInt pars, String table, String prefix) {
        return prefix + " " + constructDepClause(pars, table);
    }

    public static String constructDepClause(SeedProductionParameterInt pars, String table) {
        String where = "";

//        "scrop";


        log.info("pars ReportType " + pars.getSeedProdReportType() + "; ReportKind " + pars.getSeedProdReportKind() + " ter_reg - " + pars.getRegionsJoinName() + " sep_reg - " + pars.getRegionDepartments() + " twns - " + pars.getTownships() + " deps - " + pars.getTownshipDepartments());

        if (SeedProductionReportTypeEnum.TYPE_APPROBATION.equals(pars.getSeedProdReportType()) || SeedProductionReportTypeEnum.TYPE_REGISTRATION.equals(pars.getSeedProdReportType())) {
            if (pars.getSeedProductionServiceType() != null) {
                if (pars.getSeedProductionServiceType().equals(APPROBATION)) {
                    where += " AND (:table.service_type = '" + pars.getSeedProductionServiceType().name() + "' OR :table.service_type = 'SURVEY_RM') ";
                } else {
                    where += " AND :table.service_type = '" + pars.getSeedProductionServiceType().name() + "' ";
                }
            }
        }

        if (!KIND_REGIONS.equals(pars.getSeedProdReportKind())) {

            if (RegionalTypeEnum.BY_DEPARTMENTS.equals(pars.getRegionalType())) {
                if (pars.isFederalOrAdmin()) {
                    if (pars.getFederalDistricts() != null && !pars.getFederalDistricts().isEmpty()) {
                        where += " AND :table.dep_ter_federal_district_id IN (" + ReportUtils.joinFederalDistricts(pars.getFederalDistricts()) + ") ";
                    } else if (pars.getRegionDepartments() != null && !pars.getRegionDepartments().isEmpty()) {
                        where += " AND :table.dep_region_id IN (" + ReportUtils.joinDepartments(pars.getRegionDepartments()) + ") ";
                    }
                } else {
                    if (pars.getTownshipDepartments() != null && !pars.getTownshipDepartments().isEmpty()) {
                        where += " AND :table.dep_township_id IN (" + ReportUtils.joinDepartments(pars.getTownshipDepartments()) + ") ";
                    } else if (pars.getRegionDepartments() != null && !pars.getRegionDepartments().isEmpty()) {
                        where += " AND :table.dep_region_id IN (" + ReportUtils.joinDepartments(pars.getRegionDepartments()) + ") ";
                    } else {
                        where += " AND :table.dep_township_id IN (0) ";
                    }
                }
            } else if (RegionalTypeEnum.BY_TERRITORIES.equals(pars.getRegionalType())) {
                if (pars.isFederalOrAdmin()) {
                    if (pars.getFederalDistricts() != null && !pars.getFederalDistricts().isEmpty()) {
                        where += " AND :table.dep_ter_federal_district_id IN (" + ReportUtils.joinFederalDistricts(pars.getFederalDistricts()) + ") ";
                    } else if (pars.getRegions() != null && !pars.getRegions().isEmpty()) {
                        where += " AND :table.ter_region_id IN (" + ReportUtils.joinRegions(pars.getRegions()) + ") ";
                    }
                } else {
                    if (pars.getTownships() != null && !pars.getTownships().isEmpty()) {
                        where += " AND :table.ter_township_id IN (" + ReportUtils.joinTownships(pars.getTownships()) + ") ";
                    } else if (pars.getRegions() != null && !pars.getRegions().isEmpty()) {
                        where += " AND :table.ter_region_id IN (" + ReportUtils.joinRegions(pars.getRegions()) + ") ";
                    } else {
                        where += " AND :table.ter_township_id IN (0) ";
                    }
                }
            }
        }

//        log.info(where);

        return where.replaceAll(":table", table);
    }
}
