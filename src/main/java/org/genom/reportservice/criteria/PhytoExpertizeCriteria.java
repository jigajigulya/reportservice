package org.genom.reportservice.criteria;

import com.gnm.enums.DocTypeEnum;
import com.gnm.enums.ReproductionEnum;
import com.gnm.model.ase.SeedFundType;
import com.gnm.model.common.CropKindCulture;
import com.gnm.model.common.CultureSeason;
import com.gnm.model.common.DepartmentStructure;
import com.gnm.model.common.geo.TerTownship;
import com.gnm.model.pmon.PhytoDateRow;
import com.gnm.model.pmon.calc.CustomReport;
import com.gnm.utils.CollectionUtils;
import com.gnm.utils.RegistryConstans;
import com.gnm.utils.consts.PhytoExpertizeConstants;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.gnm.enums.DocTypeEnum.ATT_4A;
import static com.gnm.enums.DocTypeEnum.ATT_4V;
import static com.gnm.utils.criterias.CropQualityCriteria.REPRODUCTION_ENUMS_MASS_OR_PRODUCTION;

@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PhytoExpertizeCriteria {
    @Getter
    private Boolean reproductionMassOrProduction;
    private Collection<TerTownship> towns;
    @Getter
    private DocTypeEnum docTypeExpertize;
    private Collection<CropKindCulture> selectedCropKinds;
    @Getter
    private SeedFundType seedFundType;
    @Getter
    private CultureSeason season;
    @Getter
    private boolean onlyChecked = false;
    @Getter
    @Setter
    private Long contractorId;
    @Getter
    @Setter
    private Integer year;
    @Getter
    @Setter
    private boolean hasPhytoExp = false;
    @Getter
    @Setter
    private PhytoDateRow phytoDateRow;
    @Getter
    @Setter
    private Integer cropYear;
    @Getter
    @Setter
    private Integer seedProdForHarvestYear;
    @Getter
    @Setter
    private CustomReport report;

    @Getter
    @Setter
    private LocalDateTime dateBegin;
    @Getter
    @Setter
    private LocalDateTime dateFinish;
    @Getter
    @Setter
    private boolean onCultureSums = false;
    @Getter
    @Setter
    private boolean onConSums = false;
    @Getter
    @Setter
    private boolean onDepSums = false;
    @Getter
    @Setter
    private boolean onTownSums = false;

    private final List<Integer> inClauseCereals = List.of(315, 319, 477, 260, 261, 348, 410, 314, 318, 476, 347, 409,320,321,598,625,626,316);
    private final List<Integer> linenClause = List.of(191, 192, 193, 194);
    private final List<Integer> pulsesClause = List.of(80, 82, 83, 48, 49, 50, 51, 52, 388, 33, 217, 219, 220, 221, 258, 418, 448, 449);
    private List<DepartmentStructure> departments = new ArrayList<>();


    public List<Long> readDepartments() {
        return Stream.ofNullable(this.departments).flatMap(Collection::stream).map(DepartmentStructure::getId).collect(Collectors.toList());
    }


    public List<Integer> readCerealsCultureIds() {
        return inClauseCereals;
    }



    public String constructReproductionPredicate(String reproductionAlias) {
        if (reproductionMassOrProduction == null) {
            return "";
        }
        String isTradePredicate = " WHERE %s(" + reproductionAlias + ".type isnull or coalesce(qual.reproduction_trade,backfills.reproduction_trade,false) or " + reproductionAlias + ".type in (" + REPRODUCTION_ENUMS_MASS_OR_PRODUCTION.stream()
                .map(ReproductionEnum::name)
                .map(repr -> "'" + repr + "'")
                .collect(Collectors.joining(",")) + "))";
        if (!reproductionMassOrProduction) {
            return String.format(isTradePredicate, "not");
        } else {
            return String.format(isTradePredicate, "");
        }
    }

    public String otherSeasonPredicate(String templateAlias, String seasonExpr) {
        if (Objects.equals(DocTypeEnum.ATT_4ADGV, docTypeExpertize))
            return "";
        StringBuilder builder = new StringBuilder(" AND (" + templateAlias + "." + "id isnull" + " or " + templateAlias + ".id" + " %s " + PhytoExpertizeConstants.PHYTO_EXP_4G_OTHER_PREDICATE + ") ");
        if (is4GOtherReport()) {
            if (Objects.equals(DocTypeEnum.ATT_4G_OTHER_CENTER_SPR, docTypeExpertize)) {
                builder.append("AND ")
                        .append(seasonExpr)
                        .append(" = ")
                        .append(1)
                        .append(" ");
            } else if (Objects.equals(DocTypeEnum.ATT_4G_OTHER_CENTER_WINT, docTypeExpertize)) {
                builder.append("AND ")
                        .append(seasonExpr)
                        .append(" = ")
                        .append(2)
                        .append(" ");
            }
            return String.format(builder.toString(), " = ");
        } else if (is4DOther()) {
            return "";
        }
        return String.format(builder.toString(), " != ");
    }

    public List<Long> readTowns() {
        if (CollectionUtils.isNullOrEmpty(towns)) {
            return List.of(0L);
        }
        return towns.stream()
                .map(TerTownship::getId)
                .collect(Collectors.toList());
    }


    public PhytoExpertizeCriteria applyDocType(DocTypeEnum docTypeEnum) {
        this.docTypeExpertize = docTypeEnum;
        return this;
    }

    private String cropKindsPredicate(String cultureId) {
        if (CollectionUtils.isNullOrEmpty(selectedCropKinds)) {
            return null;
        }


        return selectedCropKinds
                .stream()
                .map(CropKindCulture::getId)
                .map(String::valueOf)
                .collect(Collectors.joining(", ",
                        cultureId + " in ( select id from common.cultures where crop_kind_culture_id in ( ", " )) "));


    }

    public String seedFundTypePredicate(String backFillAs) {
        if (seedFundType == null)
            return null;
        return backFillAs + "." + "seed_fund_type_id = " + seedFundType.getId();
    }

    public String buildPredicateCriteria() {
        String prefix = " WHERE ";
        String backfillsWhere = Stream.of(
                        cropKindsPredicate("coalesce(sort.culture_id, privelege_culture_mix.mix_culture_id, -1)"),
                        seedFundTypePredicate("backfills"),
                        cropYearPredicate("backfills"),
                        seedProdForHarvestYearPredicate("backfills")
                )
                .filter(Objects::nonNull)
                .collect(Collectors.joining(" AND ", prefix, ""));
        if (backfillsWhere.equals(prefix))
            return "";
        return backfillsWhere;
    }

    private String seedProdForHarvestYearPredicate(String backfills) {
        if (seedProdForHarvestYear == null)
            return null;
        return backfills + "." + "seedprod_for_harvest_year = " + seedProdForHarvestYear;

    }

    private String cropYearPredicate(String backfillsAs) {
        if (cropYear == null)
            return null;
        return backfillsAs + "." + "crop_year = " + cropYear;
    }

    public boolean twnsIsNull() {
        return towns == null;
    }

    public boolean depsIsNull() {
        return departments == null;
    }

    public String culturePredicate(String cultureAlias, String mixAliasId, String seasonEpxr) {
        if (docTypeExpertize != null && !DocTypeEnum.ATT_4ADGV.equals(docTypeExpertize)) {

            String mixIsnull = mixAliasId + " isnull ";
            String result = " AND " + cultureAlias + " not in ( " + RegistryConstans.POTATO_ID + " )";
            if (DocTypeEnum.ATT_4D_CENTER.equals(docTypeExpertize) || docTypeExpertize.name().contains(ATT_4A.name()) || docTypeExpertize.name().equals(DocTypeEnum.ATT_4D.name())) {
                result += " AND " + cultureAlias + " in ( " + inClauseCereals.stream().map(String::valueOf).collect(Collectors.joining(",")) + ") AND " + mixIsnull;
            }

            if (ATT_4V.equals(docTypeExpertize) || DocTypeEnum.ATT_4V_CENTER.equals(docTypeExpertize)) {
                result += " AND " + cultureAlias + " in ( " + linenClause.stream().map(String::valueOf).collect(Collectors.joining(",")) + ") AND " + mixIsnull;
            }

            if (DocTypeEnum.ATT_4G_CENTER.equals(docTypeExpertize) || DocTypeEnum.ATT_4G.equals(docTypeExpertize)) {
                result += " AND " + cultureAlias + " in ( " + pulsesClause.stream().map(String::valueOf).collect(Collectors.joining(",")) + ") AND " + mixIsnull;
            }

            if (is4DOther()) {
                result += " AND " + cultureAlias + " not in ( " + inClauseCereals.stream().map(String::valueOf).collect(Collectors.joining(",")) + ") ";
                if (DocTypeEnum.ATT_4D_OTHER_CENTER_SPRING.equals(docTypeExpertize)) {
                    result += " AND " + seasonEpxr + " =  1 ";
                } else if (DocTypeEnum.ATT_4D_OTHER_CENTER_WINTER.equals(docTypeExpertize)) {
                    result += " AND " + seasonEpxr + " =  2 ";
                }

            }
            return result;
        }
        return "";
    }

    private boolean is4DOther() {
        return docTypeExpertize.is4DOther();
    }


    public List<Integer> getCulturesListByReport() {
        if (DocTypeEnum.ATT_4D_CENTER.equals(docTypeExpertize) || docTypeExpertize.name().contains(ATT_4A.name()) || docTypeExpertize.name().equals(DocTypeEnum.ATT_4D.name())) {
            return inClauseCereals;
        }
        if (ATT_4V.equals(docTypeExpertize) || DocTypeEnum.ATT_4V_CENTER.equals(docTypeExpertize)) {
            return linenClause;
        }

        if (DocTypeEnum.ATT_4G_CENTER.equals(docTypeExpertize) || DocTypeEnum.ATT_4G.equals(docTypeExpertize)) {
            return pulsesClause;
        }
        List<Integer> all = new ArrayList<>(inClauseCereals);
        all.addAll(linenClause);
        all.addAll(pulsesClause);
        return all;
    }



    public static PhytoExpertizeCriteria buildByReport(CustomReport customReport) {
        return PhytoExpertizeCriteria.builder()
                .reproductionMassOrProduction(switch (customReport.getDocTypeEnum()) {
                    case ATT_4D, ATT_4D_CENTER, ATT_4D_OTHER_CENTER_SPRING, ATT_4D_OTHER_CENTER_WINTER -> true;
                    case ATT_4ADGV -> null;
                    default -> false;
                })
                .seedFundType(switch (customReport.getDocTypeEnum()) {
                    case ATT_4A_FUND_SPRING, ATT_4A_FUND_SPRING_CENTER, ATT_4A_FUND_WINT_CENTER ->
                            SeedFundType.builder().id(2L).build();
                    default -> SeedFundType.builder().id(0L).build();
                })
                .season(switch (customReport.getDocTypeEnum()) {
                    case ATT_4A_FUND_SPRING, ATT_4A_FUND_SPRING_CENTER, ATT_4G_OTHER_CENTER_SPR,
                            ATT_4D_OTHER_CENTER_SPRING -> CultureSeason.builder().id(1L).build();
                    case ATT_4A_FUND_WINT_CENTER, ATT_4G_OTHER_CENTER_WINT, ATT_4D_OTHER_CENTER_WINTER ->
                            CultureSeason.builder().id(2L).build();
                    default -> CultureSeason.builder().id(0L).build();
                })
                .docTypeExpertize(customReport.getDocTypeEnum())
                .build();
    }

    public boolean is4GOtherReport() {
        return Objects.equals(DocTypeEnum.ATT_4G_OTHER, docTypeExpertize) || Objects.equals(DocTypeEnum.ATT_4G_OTHER_CENTER_WINT, docTypeExpertize) || Objects.equals(DocTypeEnum.ATT_4G_OTHER_CENTER_SPR, docTypeExpertize);
    }



}
