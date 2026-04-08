package org.genom.reportservice.model;

import com.gnm.dao.ase.SeedsBackFillDAO;
import com.gnm.enums.*;
import com.gnm.enums.ase.QualityDocEndEnum;
import com.gnm.enums.ase.SeedProductionSeedsKindEnum;
import com.gnm.enums.ase.SeedsBackFillPurposeEnum;
import com.gnm.interfaces.SeedsBackFillChecker;
import com.gnm.model.ase.SeedsBackFill;
import com.gnm.model.ase.calc.ReproductionTypeViewInfo;
import com.gnm.utils.CollectionUtils;
import com.gnm.utils.StringUtils;
import jakarta.persistence.*;
import lombok.*;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static com.gnm.model.ase.calc.SeedsBackFillViewColumnsMeta.*;
import static com.gnm.utils.StringUtils.isNotNullOrNotEmpty;

@Entity
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@EqualsAndHashCode
public class SeedsBackFillView implements Serializable {
    @Column(name = ID)
    @Id
    private Long id;

    @Column(name = DEPARTMENT_ID)
    private Long departmentId;

    @Column(name = TOWNSHIP_ID)
    private Long townshipId;

    @Column(name = TER_REGION_NAME)
    private String terRegionName;

    @Column(name = LAST_ACTIVE_STATE_ENUM)
    @Getter(AccessLevel.PRIVATE)
    @Enumerated(EnumType.STRING)
    private SeedsBackFillStatusEnum lastActiveStateEnum;


    @Column(name = "status_correction_desc")
    private String statusCorrectionDesc;

    @Column(name = "entity_status_created")
    private LocalDateTime statusCreated;

    @Enumerated(value = EnumType.STRING)
    @Column(name = "entity_status_en")
    private EntityStatusEnum statusEnum;

    @Column(name = "entity_status_date_finish_edt")
    private LocalDateTime statusDateFinishEdt;


    @Column(name = CONTRACTOR_ID)
    private Long contractorId;

    @Column(name = CONTRACTOR_NAME_SHORT)
    private String contractorNameShort;

    @Column(name = CONTRACTOR_NAME_FULL)
    private String contractorNameFull;

    @Column(name = CONTRACTOR_MANAGER_NAME_SHORT)
    private String contractorManagerNameShort;

    @Column(name = CONTRACTOR_MANAGER_NAME_FULL)
    private String contractorManagerNameFull;

    @Column(name = CONTRACTOR_GROUP_INVESTOR_ID)
    private Long contractorGroupInvestorId;

    @Column(name = CONTRACTOR_ORGANIZATIONAL_FORM)
    private Long contractorOrganizationalForm;

    @Column(name = CONTRACTOR_PHONE_NUMBER)
    private String contractorPhoneNumber;

    @Column(name = CONTRACTOR_FAX_NUMBER)
    private String contractorFaxNumber;

    @Column(name = CONTRACTOR_EMAIL)
    private String contractorEmail;

    @Column(name = HAS_OUT_BT)
    protected boolean hasOutBt;

    @Column(name = HAS_GOV_TASK)
    protected boolean hasGovTask;

    @Column(name = HAS_CONTRACTOR_INFO)
    protected boolean hasContractorInfo;

    @Column(name = CULTURE_SORT_CODE)
    private String cultureSortCode;

    @Getter(AccessLevel.PRIVATE)
    @Column(name = SORT_NAME)
    private String sortName;

    @Getter(AccessLevel.PRIVATE)
    @Column(name = SORT_REGION)
    private String sortRegion;

    @Column(name = SEEDPROD_FOR_HARVESTYEAR)
    private Integer seedProdForHarvestYear;

    @Enumerated(EnumType.STRING)
    @Column(name = SEEDPROD_SEEDSKIND)
    private SeedProductionSeedsKindEnum seedProductionSeedsKind;

    @Column(name = CULTURE_SORT_ALLOW_ID)
    private Long cultureSortAllowId;

    @Column(name = CULTURE_ID)
    private Long cultureId;

    @Column(name = CULTURE_NAME)
    private String cultureName;

    @Column(name = CULTURE_GROUP_ID)
    private Long cultureGroupId;

    @Column(name = CULTURE_GROUP_SEASON_ID)
    private Long cultureGroupSeasonId;

    @Column(name = CULTURE_SEASON_ID)
    private Long cultureSeasonId;

    @Column(name = SEEDS_BACKFILL_REASON_ID)
    private Long reasonId;

    @Column(name = REASON_NAME)
    private String reasonName;

    @Column(name = CROP_YEAR)
    private Integer cropYear;

    @Column(name = PROVIDER_KIND_ID)
    private Long providerKindId;

    @Column(name = PROVIDER_KIND_NAME)
    private String providerKindName;

    @Column(name = PROVIDER)
    private String provider;

    @Column(name = SEED_FUND_TYPE_ID)
    private Long seedFundTypeId;

    @Column(name = FUND_TYPE_NAME)
    private String seedFundTypeName;

    @Column(name = FILL_ALL)
    private Double fillAll;

    @Column(name = BATCH_NUMBER)
    private String batchNumber;

    @Column(name = DATE_FINISHED)
    private LocalDateTime finished;

    @Column(name = ACTUAL_CROP_QUALITY_ID)
    private Long actualCropQualityId;

    @Column(name = DELETED)
    private LocalDateTime deleted;

    @Column(name = DATE_BEGIN)
    private LocalDateTime dateBegin;

    @Column(name = DATE_END)
    private LocalDateTime dateEnd;

    @Column(name = PREV_ANALYZE)
    private boolean prevAnalyze;

    @Column(name = FILL_UNIT_ID)
    private Long fillUnitId;

    @Column(name = MSU_NAME_SHORT)
    private String fillMeasureUnitName;

    @Column(name = CULTURE_MIX_ID)
    private Integer cultureMixId;

    @Column(name = PROVIDER_TYPE)
    private String providerType;

    @Enumerated(EnumType.STRING)
    @Column(name = PURPOSE_ENUM)
    private SeedsBackFillPurposeEnum purposeEnum;

    @Column(name = CURRENT_FILL)
    private Double currentFill;

    @Column(name = DEPARTMENT_NAME)
    protected String departmentName;

    @Column(name = DEPARTMENT_NAME_SHORT)
    private String departmentNameShort;

    @Column(name = TOWNSHIP_NAME)
    private String townshipName;

    @Column(name = QUAL_CHECKED)
    private Double qualChecked;

    @Column(name = QUAL_CONDITIONED)
    private Double qualConditioned;

    @Column(name = QUAL_NOTCONDITIONED_ALL)
    private Double qualNotConditionedAll;

    @Column(name = QUAL_NOTCONDITIONED_DEBRIS)
    private Double qualNotConditionedDebris;

    @Column(name = QUAL_NOTCONDITIONED_GERMINATION)
    private Double qualNotConditionedGermination;

    @Column(name = QUAL_NOTCONDITIONED_HUMIDITY)
    private Double qualNotConditionedHumidity;

    @Column(name = QUAL_NOTCONDITIONED_PESTS)
    private Double qualNotConditionedPests;

    @Enumerated(EnumType.STRING)
    @Column(name = DOC_QUALITY_TYPE)
    private DocQualityTypeEnum docQualityType;

    @Column(name = QUALITY_DOC_NUMBER)
    private String qualityDocNumber;

    @Column(name = QUALITY_DOC_DATE_EXPIRED)
    private LocalDate qualityDocDateExpired;

    @Enumerated(EnumType.STRING)
    @Column(name = "quality_doc_end_enum")
    private QualityDocEndEnum qualityDocEndEnum;

    @Enumerated(EnumType.STRING)
    @Column(name = SORT_DOC_TYPE)
    private SortDocTypeEnum sortDocType;

    @Column(name = CQ_SORT_DOC_NUMBER)
    private String sortDocNumber;

    @Column(name = CQ_SORT_DOC_DATE)
    private LocalDate sortDocDate;

    @Getter(AccessLevel.PRIVATE)
    @Setter(AccessLevel.PRIVATE)
    @Column(name = REPRODUCTION_ID)
    private Integer reproductionTypeId;

    @Getter(AccessLevel.PRIVATE)
    @Setter(AccessLevel.PRIVATE)
    @Column(name = REPRODUCTION_NAME_SHORT)
    private String reproductionNameShort;

    @Getter(AccessLevel.PRIVATE)
    @Setter(AccessLevel.PRIVATE)
    @Column(name = REPRODUCTION_NAME_VIEW)
    private String reproductionNameView;

    @Getter(AccessLevel.PRIVATE)
    @Setter(AccessLevel.PRIVATE)
    @Column(name = REPRODUCTION_NAME_FULL)
    private String reproductionNameFull;

    @Getter(AccessLevel.PRIVATE)
    @Setter(AccessLevel.PRIVATE)
    @Column(name = REPRODUCTION_TURN)
    private Integer reproductionTypeTurn;

    @Getter(AccessLevel.PRIVATE)
    @Setter(AccessLevel.PRIVATE)
    @Column(name = REPR_CATEGORY_ID)
    private Integer reprCategoryId;

    @Getter(AccessLevel.PRIVATE)
    @Setter(AccessLevel.PRIVATE)
    @Column(name = REPR_CATEGORY_NAME_SHORT)
    private String reprCategoryNameShort;

    @Getter(AccessLevel.PRIVATE)
    @Setter(AccessLevel.PRIVATE)
    @Column(name = REPR_CATEGORY_NAME_FULL)
    private String reprCategoryNameFull;

    @Getter(AccessLevel.PRIVATE)
    @Setter(AccessLevel.PRIVATE)
    @Enumerated(EnumType.STRING)
    @Column(name = REPR_CATEGORY_TYPE)
    private ReproductionCategoryEnum reprCategoryType;

    @Getter(AccessLevel.PRIVATE)
    @Setter(AccessLevel.PRIVATE)
    @Column(name = CATEGORY_GROUP_ID)
    private Integer categoryGroupId;

    @Getter(AccessLevel.PRIVATE)
    @Setter(AccessLevel.PRIVATE)
    @Column(name = CATEGORY_GROUP_NAME)
    private String categoryGroupName;

    @Column(name = GERMINATION)
    private Double germination;

    @Column(name = WEIGHTED_AVERAGE_1000SEEDS)
    private Double weightedAverage1000Seeds;

    @Column(name = HEALTH_PERC)
    private Double healthPerc;

    @Column(name = HUMIDITY_PERC)
    private Double humidityPerc;

    @Column(name = AKT_SEEDS_SELECTION_DATE)
    private LocalDate aktSeedsSelectionDate;

    @Column(name = AKT_SEEDS_SELECTION_NUMBER)
    private String aktSeedsSelectionNumber;

    @Column(name = MIX_TYPE_NAME)
    private String mixTypeName;

    @Column(name = MIX_SORT_PERCENTAGE)
    private Double mixSortPercentage;

    @Column(name = MIX_SORT_CODE)
    private String mixSortCode;

    @Getter(AccessLevel.PRIVATE)
    @Column(name = MIX_SORT_NAME)
    private String mixSortName;

    @Getter(AccessLevel.PRIVATE)
    @Column(name = MIX_SORT_REGION)
    private String mixSortRegion;

    @Getter(AccessLevel.PRIVATE)
    @Column(name = MIX_SORT_ALLOW_ID)
    private Long mixSortAllowId;

    @Getter(AccessLevel.PRIVATE)
    @Column(name = MIX_CULTURE_ID)
    private Long mixCultureId;

    @Getter(AccessLevel.PRIVATE)
    @Column(name = MIX_CULTURE_NAME)
    private String mixCultureName;

    @Column(name = MIX_CULTURE_GROUP_ID)
    private String mixCultureGroupName;

    @Column(name = MIX_CULTURE_GROUP_SEASON_ID)
    private String mixCultureGroupSeasonName;

    @Column(name = MIX_CULTURE_SEASON_ID)
    private Long mixCultureSeasonId;

    @Column(name = SOWN_SUM)
    private Double sownSum;

    @Column(name = SOWN_AREA_SUM)
    private Double sownAreaSum;

    @Column(name = DELETED_SUM)
    private Double deletedSum;

    @Column(name = SOLD_SUM)
    private Double soldSum;

    @Column(name = DEFECTED_SUM)
    private Double defectedSum;

    @Column(name = FED_SUM)
    private Double fedSum;

    @Column(name = "has_warns")
    @Getter
    private boolean hasWarns;

    @Column(name = "can_have_reproductions")
    @Getter
    private boolean canHaveReproductions;

    @Column(name = "has_expertize")
    @Getter
    private boolean hasExpertize;

    @Transient
    @Setter
    private ReproductionTypeViewInfo reproductionTypeViewInfo;

    @Transient
    @Setter
    private List<ReproductionTypeViewInfo> reproductionTypeViewInfoList;



    public ReproductionTypeViewInfo getReproductionTypeViewInfo() {
        if (Objects.isNull(reproductionTypeViewInfo)) {
            reproductionTypeViewInfo = ReproductionTypeViewInfo
                    .builder()
                    .id(reproductionTypeId)
                    .nameShort(reproductionNameShort)
                    .nameView(reproductionNameView)
                    .nameFull(reproductionNameFull)
                    .turn(reproductionTypeTurn)
                    .reprCategoryId(reprCategoryId)
                    .reprCategoryNameShort(reprCategoryNameShort)
                    .reprCategoryNameFull(reprCategoryNameFull)
                    .reprCategoryType(reprCategoryType)
                    .categoryGroupId(categoryGroupId)
                    .categoryGroupName(categoryGroupName)
                    .value(qualChecked)
                    .build();
        }
        return reproductionTypeViewInfo;
    }





    public List<ReproductionTypeViewInfo> getReproductionTypeViewInfoList() {
        if (CollectionUtils.isNullOrEmpty(reproductionTypeViewInfoList)) {
            reproductionTypeViewInfoList = new ArrayList<>();
            if (!getReproductionTypeViewInfo().isEmpty()) {
                reproductionTypeViewInfoList.add(getReproductionTypeViewInfo());
            }
        }
        return reproductionTypeViewInfoList;
    }







    public Long getCultureId() {
        return isMix() ? mixCultureId : cultureId;
    }

    public SeedsBackFillStatusEnum getCurrentStateEnum() {
        if (Objects.isNull(finished)) return null;
        return lastActiveStateEnum;
    }

    public void setCultureId(Long id) {
        if (isMix()) {
            this.mixCultureId = id;
        } else {
            this.cultureId = id;
        }
    }

    public String getCultureName() {
        return isMix() ? mixCultureName : cultureName;
    }

    public void setCultureName(String name) {
        if (isMix()) {
            this.mixCultureName = name;
        } else {
            this.cultureName = name;
        }
    }

    public String getCultureSortCode() {
        return isMix() ? mixSortCode : cultureSortCode;
    }

    public void setCultureSortCode(String code) {
        if (isMix()) {
            this.mixSortCode = code;
        } else {
            this.cultureSortCode = code;
        }
    }

    public Long getCultureSortAllowId() {
        return isMix() ? mixSortAllowId : cultureSortAllowId;
    }

    public void setCultureSortAllowId(Long id) {
        if (isMix()) {
            mixSortAllowId = id;
        } else {
            cultureSortAllowId = id;
        }
    }

    public String getCultureSortName() {
        return isMix() ? mixSortName : sortName;
    }

    public void setCultureSortName(String name) {
        if (isMix()) {
            mixSortName = name;
        } else {
            sortName = name;
        }
    }

    public String getCultureSortRegion() {
        return isMix() ? mixSortRegion : sortRegion;
    }

    public Long getCultureSeasonId() {
        return isMix() ? mixCultureSeasonId : cultureSeasonId;
    }

    public void setCultureSeasonId(Long seasonId) {
        if (isMix()) {
            mixCultureSeasonId = seasonId;
        } else {
            cultureSeasonId = seasonId;
        }
    }

    public boolean isMix() {
        return Objects.nonNull(cultureMixId);
    }

    public Double getQualNotSort() {
        return Long.valueOf(4).equals(getCultureSortAllowId()) ? getFillAll() : null;
    }

    public String sourceCustomerString() {
        List<String> srcCustomers = new ArrayList<>();
        if (hasGovTask) {
            srcCustomers.add(StringUtils.lowerCase(SourceCustomerEnum.GOV_TASK.getName()));
        }
        if (hasOutBt) {
            srcCustomers.add(StringUtils.lowerCase(SourceCustomerEnum.OUT_BUDGET.getName()));
        }

        if (hasContractorInfo) {
            srcCustomers.add(StringUtils.lowerCase(SourceCustomerEnum.CONTRACTOR_INFO.getName()));
        }
        return String.join(", ", srcCustomers);
    }

    public String getCultureSortCodeLabel() {
        String sortCode = getCultureSortCode();
        if (Objects.isNull(sortCode)) return "";
        if (sortCode.contains("*")) return "*";
        return sortCode;
    }

    public String getContractorContactsForDocs() {
        if (!isNotNullOrNotEmpty(contractorPhoneNumber) && !isNotNullOrNotEmpty(contractorFaxNumber) && !isNotNullOrNotEmpty(contractorEmail)) {
            return "Не указано";
        } else {
            return StringUtils.join(new ArrayList<String>() {{
                if (isNotNullOrNotEmpty(contractorPhoneNumber)) add(contractorPhoneNumber);
                if (isNotNullOrNotEmpty(contractorFaxNumber)) add(contractorFaxNumber);
                if (isNotNullOrNotEmpty(contractorEmail)) add(contractorEmail);
            }}, ", ");
        }
    }



    public boolean isForSale() {
        return Objects.equals(SeedsBackFillPurposeEnum.SAIL, purposeEnum);
    }




    public boolean nullFillAll() {
        return Objects.isNull(getFillAll());
    }


    public boolean currentFillEmpty() {
        return Objects.nonNull(getCurrentFill()) && getCurrentFill() < 0;
    }


    public boolean batchNumberIsEmpty() {
        return StringUtils.isStringNullOrEmpty(getBatchNumber());
    }


    public boolean contractorEmpty() {
        return Objects.isNull(getContractorId());
    }


    public boolean cultureSortEmptyOrSortMixIsEmpty() {
        return Objects.isNull(sortName) &&
                (StringUtils.isStringNullOrEmpty(mixSortName));
    }


    public boolean canHaveReproduction() {
        return canHaveReproductions;
    }


    public SeedsBackFill getSeedsBackFillDomain() {
        return new SeedsBackFillDAO().findById(id).orElse(null);
    }


    public boolean notSort() {
        return Objects.equals(cultureSortAllowId, 4L) || Objects.equals(mixSortAllowId, 4L);
    }



    public boolean nullReproduction() {
        return Objects.isNull(reproductionTypeId);
    }

}
