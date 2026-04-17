package org.genom.reportservice.interfaces;

import com.gnm.bean.dialog.AssaySubjectChoiceDialog;
import com.gnm.enums.RegionalTypeEnum;
import com.gnm.enums.ase.DataLayerSourceEnum;
import com.gnm.enums.ase.SeedProductionReportKindEnum;
import com.gnm.enums.ase.SeedProductionReportTypeEnum;
import com.gnm.enums.ase.SeedProductionServiceTypeEnum;
import com.gnm.interfaces.DocType;
import com.gnm.model.ase.calc.SeedProductionMonStructureCommon;
import com.gnm.model.ase.mon.*;
import com.gnm.model.common.DepartmentStructure;
import com.gnm.model.common.geo.TerFederalDistrict;
import com.gnm.model.common.geo.TerRegion;
import com.gnm.model.common.geo.TerTownship;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public interface SeedProductionParameterInt {
    SeedProductionReportKindEnum getSeedProdReportKind();

    String getFilterGroupName();
    Long getFilterGroupId();

    boolean isFederalOrAdmin();

    RegionalTypeEnum getRegionalType();

    boolean isNotEmptyRegionsAndAllRegions();

    boolean isRegionsContainsAllAllRegions();

    String getRegionsJoinName();

    boolean isNotEmptyDepsAndAllDepsRegion();

    boolean isRegionDepartmentsContainsAllAllDepartmentsRegionsCol();

    List<DepartmentStructure> getRegionDepartments();

    List<TerTownship> getTownships();

    List<TerTownship> getTerRegionTownships();

    DepartmentStructure getDepartmentRegion();

    List<DepartmentStructure> getTownshipDepartments();

    boolean isAssaysInclude();

    MonitoringDate getMonitoringDate();

    MonitoringView getMainMonitoringView();

    String getAreaUnit();

    String getMassUnit();

    Integer getSeedProdForHarvestYear();

    DataLayerSourceEnum getDataLayerSource();

    SeedProductionReportTypeEnum getSeedProdReportType();

    List<Object> getGroupsAll();
    void setGroupsAll(List<Object> objects);

    void setAssaysAll(List<AssaySeedProdFullInfo> objects);

    SeedProductionServiceTypeEnum getSeedProductionServiceType();

    List<TerFederalDistrict> getFederalDistricts();

    List<TerRegion> getRegions();

    Double getAreaCoef();


    String getFilterGroupNameView();

    void setCultures(List<SeedProductionMonStructureCommon> cultures);

    void setGroups(List<SeedProductionMonStructureCommon> groups);

    void setAssaysSeedProd(List<AssaySeedProdFullInfo> assaysSeedProd);

    Long getFilterGroupObjectId();

    boolean isSetDataPeriod();

    LocalDateTime getDataDateTimeFrom();

    LocalDateTime getDataDateTimeTo();

    Double getMassCoef();
}
