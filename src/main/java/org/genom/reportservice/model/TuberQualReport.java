package org.genom.reportservice.model;

import com.gnm.enums.DefectPlantEnum;

public interface TuberQualReport {

    Long getAssayId();


    String getIndicatorTemplateName();

    Long getId();

    Double getFill();

    String getBatchNumber();

    Long getBackFillId();

    Long getContractorId();

    Long getCropQualityId();

    Double getChecked();

    Long getIndicatorResultId();

    String getIndicatorFieldName();

    boolean isTotalResult();

    Long getFieldSubjectCode();

    Long getFieldGroupId();

    Double getPercentInfected();

    Long getCultureId();

    boolean isTransferredFund();


    default  String prevAnalyzeName() {
        return null;
    }


    default  String formatDateQuality() {
        return null;
    }

    boolean phytoExpertizeProvided();


    String transferedFundName();

    default Long getParentId() {
        return null;
    }

    default DefectPlantEnum getDefect() {
        return null;
    }


    default Integer getPhytoTypeId() {
        return null;
    }


    Long getSeasonId();

    String getCultureName();

    default Integer getSamplesCount() {
        return null;
    }


    boolean isMassOrProductionReproduction();

    Long getRegionId();

}
