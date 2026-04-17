package org.genom.reportservice.model;

import com.gnm.enums.RegionalTypeEnum;
import com.gnm.enums.ase.DataLayerSourceEnum;
import com.gnm.enums.ase.SeedProductionReportKindEnum;
import com.gnm.enums.ase.SeedProductionReportTypeEnum;
import com.gnm.enums.ase.SeedProductionServiceTypeEnum;
import com.gnm.model.ase.calc.SeedProductionMonStructureCommon;
import com.gnm.model.ase.mon.AssaySeedProdFullInfo;
import com.gnm.model.ase.mon.MonitoringDate;
import com.gnm.model.ase.mon.MonitoringView;
import com.gnm.model.common.DepartmentStructure;
import com.gnm.model.common.geo.TerFederalDistrict;
import com.gnm.model.common.geo.TerRegion;
import com.gnm.model.common.geo.TerTownship;
import lombok.Builder;
import lombok.Data;
import org.genom.reportservice.interfaces.SeedProductionParameterInt;

import java.time.LocalDateTime;
import java.util.List;

@Data
@Builder
public class SeedProdParam implements SeedProductionParameterInt {

    private SeedProductionReportKindEnum seedProdReportKind;
    private String filterGroupName;
    private boolean federalOrAdmin;
    private RegionalTypeEnum regionalType;
    private boolean notEmptyRegionsAndAllRegions;
    private boolean regionsContainsAllAllRegions;
    private String regionsJoinName;
    private boolean notEmptyDepsAndAllDepsRegion;
    private boolean regionDepartmentsContainsAllAllDepartmentsRegionsCol;
    private List<DepartmentStructure> regionDepartments;
    private List<TerTownship> townships;
    private List<TerTownship> terRegionTownships;
    private DepartmentStructure departmentRegion;
    private List<DepartmentStructure> townshipDepartments;
    private boolean assaysInclude;
    private MonitoringDate monitoringDate;
    private MonitoringView mainMonitoringView;
    private String areaUnit;
    private String massUnit;
    private Integer seedProdForHarvestYear;
    private DataLayerSourceEnum dataLayerSource;
//
    private Long filterGroupId;
    private SeedProductionReportTypeEnum seedProdReportType;
    private List<Object> groupsAll;
    private List<AssaySeedProdFullInfo> assaysAll;
    private SeedProductionServiceTypeEnum seedProductionServiceType;
    private List<TerFederalDistrict>  federalDistricts;
    private List<TerRegion> regions;
    private Double areaCoef;
    private String filterGroupNameView;
    private List<SeedProductionMonStructureCommon> cultures;
    private List<SeedProductionMonStructureCommon> groups;
    private List<AssaySeedProdFullInfo> assaysSeedProd;
    private Long filterGroupObjectId;
    private boolean setDataPeriod;
    private LocalDateTime dataDateTimeFrom;
    private LocalDateTime dataDateTimeTo;
    private Double massCoef;















}
