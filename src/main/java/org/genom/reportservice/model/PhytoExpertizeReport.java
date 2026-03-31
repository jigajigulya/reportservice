package org.genom.reportservice.model;

import com.gnm.model.ase.SeedsBackFill;
import com.gnm.model.common.Contractor;
import jakarta.persistence.*;
import lombok.*;
import lombok.experimental.SuperBuilder;


import java.io.Serializable;
import java.util.Objects;

@Entity
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@SuperBuilder
@Inheritance(strategy = InheritanceType.SINGLE_TABLE)
@DiscriminatorColumn(name = "class_type",
        discriminatorType = DiscriminatorType.STRING)
@DiscriminatorValue("expertize_report")
@EqualsAndHashCode
public class PhytoExpertizeReport implements Serializable, TuberQualReport {
    @Id
    protected Long id;
    @Column(name = "fill")
    protected Double fill;
    @Column(name = "batch_number")
    protected String batchNumber;
    @Column(name = "back_fill_id")
    protected Long backFillId;
    @Column(name = "contractor_id")
    protected Long contractorId;
    @Column(name = "crop_quality_id")
    protected Long cropQualityId;
    @Column(name = "checked")
    protected Double checked;
    @Column(name = "indicator_result_id")
    protected Long indicatorResultId;
    @Column(name = "indicator_field_name")
    protected String indicatorFieldName;
    @Column(name = "is_total_result")
    protected boolean totalResult;
    @Column(name = "field_subject_code")
    protected Long fieldSubjectCode;
    @Column(name = "field_group_id")
    protected Long fieldGroupId;
    @Column(name = "percent_infected")
    protected Double percentInfected;
    @Column(name = "culture_id")
    protected Long cultureId;
    @Column(name = "transfered_fund")
    protected boolean transferredFund;
    @Column(name = "culture_name")
    private String cultureName;
    @Column(name = "season_id")
    protected Long seasonId;
    @Column(name = "region_id")
    protected Long regionId;
    //for qual. Backfill may have another actual reproduction.
    @Column(name = "mass_or_production_reproduction")
    private boolean massOrProductionReproduction;

    public SeedsBackFill createBackFillForGrouping() {
        return SeedsBackFill.builder()
                .id(backFillId)
                .batchNumber(batchNumber)
                .contractor(Contractor.builder().id(contractorId).build())
                .fillAll(fill)
                .build();
    }

    public boolean phytoExpertizeProvided() {
        return Objects.nonNull(cropQualityId);
    }


    public String transferedFundName() {
        return transferredFund ? "Да" : "Нет";
    }


    @Override
    public Long getAssayId() {
        return null;
    }

    @Override
    public String getIndicatorTemplateName() {
        return "";
    }

    @Override
    public String prevAnalyzeName() {
        return null;
    }

    @Override
    public String formatDateQuality() {
        return null;
    }


}
