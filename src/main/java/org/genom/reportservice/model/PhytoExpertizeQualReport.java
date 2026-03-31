package org.genom.reportservice.model;

import com.gnm.enums.EntityStatusEnum;
import com.gnm.utils.DateUtils;
import jakarta.persistence.*;
import lombok.*;
import lombok.experimental.SuperBuilder;


import java.io.Serializable;
import java.time.LocalDateTime;

@Entity
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@SuperBuilder
@Inheritance(strategy = InheritanceType.SINGLE_TABLE)
@DiscriminatorColumn(name = "class_type",
        discriminatorType = DiscriminatorType.STRING)
@DiscriminatorValue("expertize_qual_report")
@EqualsAndHashCode
public class PhytoExpertizeQualReport extends PhytoExpertizeReport implements Serializable {

    @Column
    private boolean prevAnalyze;

    @Column
    private LocalDateTime dateCropQuality;

    @Column
    private String contractorName;

    @Column
    private String reproduction;

    @Column
    private String departmentName;

    @Column
    private String sortName;

    @Column
    private String measureError;

    @Column
    private String normsForNd;

    @Column
    protected String gost;

    @Column
    protected String town;

    @Column
    private Integer samplesCount;

    @Column
    private boolean massOrProductionReproduction;

    @Enumerated(EnumType.STRING)
    @Column
    private EntityStatusEnum statusEnum;

    @Column
    private String regionName;

    @Column
    private Integer cropYear;

    @Column
    private Integer seedProdForHarvestYear;

    @Column
    private String indicatorTemplateName;

    @Column
    private Boolean notFound;

    public String prevAnalyzeName() {
        return prevAnalyze ? "Да" : "Нет";
    }

    public String formatDateQuality() {
        return dateCropQuality == null ? "" : DateUtils.DATE_FORMATTER.format(dateCropQuality);
    }


    @Column
    private Long templateFieldId;


}
