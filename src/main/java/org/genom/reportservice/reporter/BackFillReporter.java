package org.genom.reportservice.reporter;


import com.gnm.enums.*;
import com.gnm.enums.ase.SeedNeedReportColumnEnum;
import com.gnm.enums.ase.SeedsBackFillPurposeEnum;
import com.gnm.model.ase.SeedFundType;
import com.gnm.model.ase.SeedsBackFillReason;
import com.gnm.model.ase.calc.ReproductionTypeViewInfo;
import com.gnm.model.common.*;
import com.gnm.model.common.geo.TerTownship;
import com.gnm.utils.*;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.poi.hssf.usermodel.HSSFFont;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.ss.util.CellRangeAddress;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbookFactory;
import org.genom.reportservice.criteria.SeedsBackFillCriteria;
import org.genom.reportservice.model.*;
import org.genom.reportservice.repository.BackFillRepository;
import org.genom.reportservice.repository.SeedNeedsRepo;
import org.genom.reportservice.utils.BackFillReporterFilterMapper;
import org.springframework.context.annotation.Scope;
import org.springframework.core.io.ResourceLoader;
import org.springframework.stereotype.Component;

import java.io.ByteArrayOutputStream;
import java.io.Serializable;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.gnm.enums.ase.SeedsBackFillReportColumnEnum.*;
import static com.gnm.utils.NumberUtils.isNullReturnZero;
import static com.gnm.utils.ReportUtils.*;
import static java.util.Comparator.*;
import static java.util.Optional.of;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.groupingBy;


@Slf4j
@Component
@Scope("prototype")
@RequiredArgsConstructor
public class BackFillReporter implements Serializable {

    private final BackFillRepository backFillRepository;
    private final ResourceLoader resourceLoader;
    private final SeedNeedsRepo seedNeedsRepo;


    private Map<ReportKeysForFilterEnum, Object> filterMap;

    private boolean renderedMainPanel;
    private static final short HEIGHT_ROW = 255;
    private static final float HEIGHT_IN_POINT_ROW = 12.75f;

    private static final int COLUMN_NAMES_ROW = 2;

    private List<SeedsBackFillView> exportList;
    private Workbook workbook;
    private Font groupingFont;
    private Font font;
    private Runnable runnable;
    private Comparator<? super SeedsBackFillView> seedBackComp;
    private CellStyle groupingStyleNum;
    private CellStyle groupingStyleWeight;
    private CellStyle groupingStylePerc;
    private CellStyle groupingStyleString;
    private CellStyle styleNum1;
    private CellStyle styleNum2;
    private CellStyle styleWeight;
    private CellStyle stylePerc;
    private CellStyle styleString;
    private CellStyle styleDateShort;
    private DataFormat dataFormat;
    private Predicate<SeedsBackFillView> numberPredicate;
    private Predicate<CalculationNeed> seedNeedPredicate;
    private Predicate<SeedsBackFillView> mainParamPredicate;
    private Integer currentNumRow;
    private Integer currentColumnOffset;
    private Map<String, List<String>> reproductionByCategoryColumns;
    private List<SeedsBackFillView> allSeedsView;

    private SeedsBackFillCriteria criteria;


    private Optional<String> getTownNameBySb(SeedsBackFillView sb) {
        return ofNullable(sb.getTownshipName());
    }

    private int compareForCulture(SeedsBackFillView sb1, SeedsBackFillView sb2) {
        Optional<String> cultureName1 = ofNullable(sb1.getCultureName());
        Optional<String> cultureName2 = ofNullable(sb2.getCultureName());
        if (cultureName1.isPresent() && cultureName2.isPresent()) {
            return cultureName1.get().compareTo(cultureName2.get());
        } else if (cultureName1.isPresent())
            return -1;
        else if (cultureName2.isPresent())
            return 1;
        else
            return 0;
    }

    public void initializeSeedBackComparator() {
        if (filterMap.get(ReportKeysForFilterEnum.GROUPING) instanceof GroupingTypeEnum && filterMap.get(ReportKeysForFilterEnum.GROUPING).equals(GroupingTypeEnum.CULTURE_AND_TOWNSHIPS)) {
            seedBackComp = (sb1, sb2) -> {
                Optional<String> twnOpt1 = getTownNameBySb(sb1);
                Optional<String> twnOpt2 = getTownNameBySb(sb2);
                if (twnOpt1.isPresent() && twnOpt2.isPresent())
                    if (twnOpt1.get().compareTo(twnOpt2.get()) == 0) {
                        return compareForCulture(sb1, sb2);
                    } else
                        return twnOpt1.get().compareTo(twnOpt2.get());
                return compareForCulture(sb1, sb2);
            };
            return;
        }
        if (filterMap.get(ReportKeysForFilterEnum.DETAL_BY_PARTIAL) instanceof Boolean) {
            if (filterMap.get(ReportKeysForFilterEnum.GROUPING) instanceof GroupingTypeEnum && filterMap.get(ReportKeysForFilterEnum.GROUPING).equals(GroupingTypeEnum.CULTURE) && (Boolean) filterMap.get(ReportKeysForFilterEnum.DETAL_BY_PARTIAL)) {
                seedBackComp = (sb1, sb2) -> {
                    int res = compareForCulture(sb1, sb2);
                    if (res == 0) {
                        res = helperMethodForComparator(sb1, sb2);
                    }
                    return res;
                };
                return;
            }
        }
        seedBackComp = this::helperMethodForComparator;
    }

    private int helperMethodForComparator(SeedsBackFillView sb1, SeedsBackFillView sb2) {

        Optional<String> twn1Opt = getTownNameBySb(sb1);
        Optional<String> twn2Opt = getTownNameBySb(sb2);
        if (twn1Opt.isPresent() && twn2Opt.isPresent()) {
            if (twn1Opt.get().compareTo(twn2Opt.get()) == 0) {
                return compareContractorsByNameShort(sb1, sb2);
            } else
                return twn1Opt.get().compareTo(twn2Opt.get());
        } else if (twn1Opt.isPresent()) {
            return -1;
        } else if (twn2Opt.isPresent())
            return 1;
        else
            return compareContractorsByNameShort(sb1, sb2);


    }

    private int compareContractorsByNameShort(SeedsBackFillView sb1, SeedsBackFillView sb2) {
        Optional<String> nameShort1 = ofNullable(sb1.getContractorNameShort());
        Optional<String> nameShort2 = ofNullable(sb2.getContractorNameShort());
        if (nameShort1.isPresent() && nameShort2.isPresent()) {
            return nameShort1.get().compareTo(nameShort2.get());
        } else if (nameShort1.isPresent())
            return -1;
        else if (nameShort2.isPresent())
            return 1;
        else
            return 1;
    }


    private void initializePredicateForNeed() {
        seedNeedPredicate = need -> true;
        for (ReportKeysForFilterEnum key : filterMap.keySet()) {
            switch (key) {
                case NEED:
                    seedNeedPredicate = seedNeedPredicate.and(sd -> {
                        Double[] temp = ((List<Double>) filterMap.get(key)).toArray(Double[]::new);
                        if (temp[0] != null && temp[1] != null) {
                            return temp[0] <= ofNullable(sd.getNeedCount()).orElse(0d) && temp[1] >= ofNullable(sd.getNeedCount()).orElse(0d);
                        } else
                            return true;
                    });


                    break;
                case PROC_NEED:
                    seedNeedPredicate = seedNeedPredicate.and(sd -> {
                        Double[] temp = ((List<Double>) filterMap.get(key)).toArray(Double[]::new);
                        if (temp[0] != null && temp[1] != null) {
                            Double perc = getProc(sd.getFillSum(), sd.getNeedCount()) * 100;
                            return temp[0] <= perc && temp[1] >= perc;
                        } else
                            return true;
                    });
                    break;
                case FILLED_UP:
                    seedNeedPredicate = seedNeedPredicate.and(sd -> {
                        Double[] temp = ((List<Double>) filterMap.get(key)).toArray(Double[]::new);
                        if (temp[0] != null && temp[1] != null) {
                            return temp[0] <= ofNullable(sd.getFillSum()).orElse(0d) && temp[1] >= ofNullable(sd.getFillSum()).orElse(0d);
                        } else
                            return true;
                    });
                    break;
                case CURRENT_FILL:
                    seedNeedPredicate = seedNeedPredicate.and(sd -> {
                        Double[] temp = ((List<Double>) filterMap.get(key)).toArray(Double[]::new);
                        if (temp[0] != null && temp[1] != null) {
                            return temp[0] <= ofNullable(sd.getCurrentFillSum()).orElse(0d) && temp[1] >= ofNullable(sd.getCurrentFillSum()).orElse(0d);
                        } else
                            return true;
                    });
                    break;
            }
        }
    }

    private void initializePredicatesForBackFill() {
        mainParamPredicate = s -> true;
        numberPredicate = s -> true;

        this.filterMap.forEach((key, value) -> {
            switch (key) {
                case PREV_ANALYZE:
                    if (Objects.isNull(value) || !(value instanceof Boolean)) break;
                    mainParamPredicate = mainParamPredicate.and(sb -> !((Boolean) value) || !sb.isPrevAnalyze());
                    break;
                case TOWNSHIP:
                    if (value instanceof TerTownship) {
                        mainParamPredicate = mainParamPredicate.and(sb -> ofNullable(sb.getTownshipId()).
                                map(id -> Objects.equals(id, ((TerTownship) value).getId())).
                                orElse(false));
                    }
                    break;
                case CONTRACTOR:
                    if (value instanceof Contractor) {
                        mainParamPredicate = mainParamPredicate.and(sb -> Objects.equals(((Contractor) value).getId(), sb.getContractorId()));
                    }
                    break;
                case SOURCE_CUSTOMER:
                    if (value instanceof SourceCustomerEnum) {
                        mainParamPredicate = mainParamPredicate.and(sb -> {
                            switch ((SourceCustomerEnum) value) {
                                case CONTRACTOR_INFO:
                                    return sb.isHasContractorInfo();
                                case GOV_TASK:
                                    return sb.isHasGovTask();
                                case OUT_BUDGET:
                                    return sb.isHasOutBt();
                            }
                            return false;
                        });
                    }
                    break;
                case ORG_TYPE:
                    if (value instanceof OrganizationalForm) {
                        mainParamPredicate = mainParamPredicate.and(sb -> Objects.equals(sb.getContractorOrganizationalForm(), ((OrganizationalForm) value).getId()));
                    }
                    break;
                case SH_FORM:
                    if (value instanceof GroupContractorInvestor) {
                        mainParamPredicate = mainParamPredicate.and(sb -> Objects.equals(sb.getContractorGroupInvestorId(), ((GroupContractorInvestor) value).getId()));
                    }
                    break;
                case DETAL_BY_PARTIAL:
                    break;

                case SEASON:
                    if (value instanceof CultureSeason) {
                        mainParamPredicate = mainParamPredicate.and(sb -> Objects.equals(sb.getCultureSeasonId(), ((CultureSeason) value).getId()));
                        break;
                    }
                case TYPE_CULTURE:
                    if (value instanceof CultureGroup) {
                        mainParamPredicate = mainParamPredicate.and(sb -> Objects.equals(sb.getCultureGroupId(), ((CultureGroup) value).getId()));
                    }
                    break;
                case SEED_FUND:
                    if (value instanceof SeedFundType) {
                        mainParamPredicate = mainParamPredicate.and(sb -> Objects.equals(sb.getSeedFundTypeId(), ((SeedFundType) value).getId()));
                    }
                    break;
                case CULTURE:
                    if (value instanceof List && CollectionUtils.isNotNullOrNotEmpty((List<Culture>) value)) {
                        mainParamPredicate = mainParamPredicate.and(sb -> ((List<Culture>) value).stream().anyMatch(culture -> Objects.equals(culture.getId(), sb.getCultureId())));
                    }
                    break;
                case SORT_CULTURE:
                    if (value instanceof CultureSort) {
                        mainParamPredicate = mainParamPredicate.and(sb -> Objects.equals(sb.getCultureSortCode(), ((CultureSort) value).getCode()));
                    }
                    break;
                case REGISTRATION:
                    if (value instanceof CultureSortAllow) {
                        mainParamPredicate = mainParamPredicate.and(sb -> Objects.equals(sb.getCultureSortAllowId(), ((CultureSortAllow) value).getId()));
                    }
                    break;
                case REASON:
                    if (value instanceof SeedsBackFillReason) {
                        mainParamPredicate = mainParamPredicate.and(sb -> Objects.equals(sb.getReasonId(), ((SeedsBackFillReason) value).getId()));
                    }
                    break;
                case PURPOSE:
                    if (Objects.isNull(value)) break;
                    if (value instanceof SeedsBackFillPurposeEnum) {
                        mainParamPredicate = mainParamPredicate.and(sb -> Objects.equals(sb.getPurposeEnum(), value));
                    }
                    break;
                case NEED:
                    break;

                case FILLED_UP:
                    if (listNullOrWrong(value))
                        break;
                    numberPredicate = numberPredicate.and(sb -> isNotDoubleOrBetween(ofNullable(sb.getFillAll()).orElse(0d), ((List<Double>) value).toArray(Double[]::new)));
                    break;
                case CURRENT_FILL:
                    if (listNullOrWrong(value))
                        break;
                    numberPredicate = numberPredicate.and(sb -> isNotDoubleOrBetween(ofNullable(sb.getCurrentFill()).orElse(0d), ((List<Double>) value).toArray(Double[]::new)));
                    break;
                case PROC_NEED:
                    break;

                case NUMBER_DOC:
                    if (value instanceof String) {
                        mainParamPredicate = mainParamPredicate.and(sb -> ofNullable(sb.getSortDocNumber()).
                                map(number -> StringUtils.matchesForSearch(number, (String) value)).
                                orElse(false));
                    }
                    break;
                case DATE_DOC:
                    if ((value instanceof LocalDate[] && !arrayIsNullOrEmpty((LocalDate[]) value)) || (value instanceof String[] && !arrayIsNullOrEmpty((String[]) value))) {
                        mainParamPredicate = mainParamPredicate.and(sb -> {
                            LocalDate[] arr;
                            if (value instanceof String[]) {
                                arr = Arrays.stream((String[]) value)
                                        .map(this::parseStringToLocalDate)
                                        .toArray(LocalDate[]::new);
                            } else
                                arr = (LocalDate[]) value;
                            if (arr[0] != null && arr[1] != null)
                                return DateUtils.isLocalDateBetween(arr[0], arr[1], sb.getSortDocDate());
                            return true;
                        });
                    }
                    break;
                case NUMBER_DOC_QUALITY:
                    if (value instanceof String) {
                        mainParamPredicate = mainParamPredicate.and(sb -> ofNullable(sb.getQualityDocNumber()).
                                map(number -> StringUtils.matchesForSearch(number, (String) value)).
                                orElse(false));
                    }
                    break;
                case DATE_DOC_ABOUT_QUALITY:
                    if ((value instanceof LocalDate[] && !arrayIsNullOrEmpty((LocalDate[]) value)) || (value instanceof String[] && !arrayIsNullOrEmpty((String[]) value))) {
                        mainParamPredicate = mainParamPredicate.and(sb -> {
                            LocalDate[] arr;
                            if (value instanceof String[]) {
                                arr = Arrays.stream((String[]) value)
                                        .map(this::parseStringToLocalDate)
                                        .toArray(LocalDate[]::new);
                            } else
                                arr = (LocalDate[]) value;
                            if (arr[0] != null && arr[1] != null)
                                return DateUtils.isLocalDateBetween(arr[0], arr[1], sb.getQualityDocDateExpired());
                            return true;
                        });
                    }
                    break;
                case CHECKED:
                    if (listNullOrWrong(value))
                        break;
                    numberPredicate = numberPredicate.and(sb -> isNotDoubleOrBetween(
                                    ofNullable(sb.getQualChecked()).orElse(0.d),  ((List<Double>) value).toArray(Double[]::new)
                            )
                    );
                    break;
                case CHECKED_PROCENT:
                    if (listNullOrWrong(value))
                        break;
                    numberPredicate = numberPredicate.and(sb -> isNotDoubleOrBetween(getProc(sb.getQualChecked(), sb.getCurrentFill()) * 100, ((List<Double>) value).toArray(Double[]::new)));
                    break;
                case CONDITION:
                    if (listNullOrWrong(value))
                        break;
                    numberPredicate = numberPredicate.and(sb -> isNotDoubleOrBetween(ofNullable(sb.getQualConditioned()).orElse(0d), ((List<Double>) value).toArray(Double[]::new)));
                    break;
                case CONDITION_PROCENT:
                    if (listNullOrWrong(value))
                        break;
                    numberPredicate = numberPredicate.and(sb -> isNotDoubleOrBetween(getProc(sb.getQualConditioned(), sb.getQualChecked()) * 100, ((List<Double>) value).toArray(Double[]::new)));
                    break;
                case NOT_CONDITION_SUMM:
                    if (listNullOrWrong(value))
                        break;
                    numberPredicate = numberPredicate.and(sb -> isNotDoubleOrBetween(ofNullable(sb.getQualNotConditionedAll()).orElse(0d), ((List<Double>) value).toArray(Double[]::new)));
                    break;
                case NOT_CONDITION_SUMM_PROC:
                    if (listNullOrWrong(value))
                        break;
                    numberPredicate = numberPredicate.and(sb -> isNotDoubleOrBetween(getProc(sb.getQualNotConditionedAll(), sb.getQualChecked()) * 100, ((List<Double>) value).toArray(Double[]::new)));
                    break;
                case NOT_CONDITION_DEBRIS:
                    if (listNullOrWrong(value))
                        break;
                    numberPredicate = numberPredicate.and(sb -> isNotDoubleOrBetween(ofNullable(sb.getQualNotConditionedDebris()).orElse(0d), ((List<Double>) value).toArray(Double[]::new)));
                    break;
                case NOT_CONDITION_HUMIDITY:
                    if (listNullOrWrong(value))
                        break;
                    numberPredicate = numberPredicate.and(sb -> isNotDoubleOrBetween(ofNullable(sb.getQualNotConditionedHumidity()).orElse(0d), ((List<Double>) value).toArray(Double[]::new)));
                    break;
                case NOT_CONDITION_GERMINATION:
                    if (listNullOrWrong(value))
                        break;
                    numberPredicate = numberPredicate.and(sb -> isNotDoubleOrBetween(ofNullable(sb.getQualNotConditionedGermination()).orElse(0d), ((List<Double>) value).toArray(Double[]::new)));
                    break;
                case NOT_CONDITION_DAMAGE:
                    if (listNullOrWrong(value))
                        break;
                    numberPredicate = numberPredicate.and(sb -> isNotDoubleOrBetween(ofNullable(sb.getQualNotConditionedPests()).orElse(0d), ((List<Double>) value).toArray(Double[]::new)));
                    break;
                case AVERAGE_MASS:
                    if (listNullOrWrong(value))
                        break;
                    mainParamPredicate = mainParamPredicate.and(sb -> isNotDoubleOrBetween(ofNullable(sb.getWeightedAverage1000Seeds()).orElse(0d), ((List<Double>) value).toArray(Double[]::new)));
                    break;
                case GERMINATION:
                    if (listNullOrWrong(value))
                        break;
                    mainParamPredicate = mainParamPredicate.and(sb -> isNotDoubleOrBetween(ofNullable(sb.getGermination()).orElse(0d), ((List<Double>) value).toArray(Double[]::new)));
                    break;
            }
        });
    }

    private boolean listNullOrWrong(Object value) {
        return !(value instanceof List) || CollectionUtils.isNullOrEmpty((List) value);
    }

    public boolean arrayIsNullOrEmpty(Object[] objects) {
        if (Objects.isNull(objects)) return true;
        for (Object o : objects) {
            if (Objects.nonNull(o)) return false;
        }
        return true;
    }

    private boolean isNotDoubleOrBetween(Double doubleValue, Double ... arr) {
        return (Objects.isNull(arr[0]) || arr[0] <= ofNullable(doubleValue).orElse(0d)) && (Objects.isNull(arr[1]) || arr[1] >= ofNullable(doubleValue).orElse(0d));
    }

    private LocalDate parseStringToLocalDate(String str) {
        try {
            return LocalDate.parse(str, DateTimeFormatter.ofPattern("dd.MM.yyyy"));
        } catch (DateTimeParseException e) {
            log.error("Err", e);
        }
        return null;
    }


    @SuppressWarnings("unchecked")
    public void createExportList() {
        BackFillReporterFilterMapper.mapToObject(filterMap);
        exportList = new ArrayList<>();
        initializePredicatesForBackFill();
        initializeSeedBackComparator();

        reproductionByCategoryColumns = new TreeMap<>(Comparator.nullsLast(Comparator.naturalOrder()));
        reproductionByCategoryColumns.putAll(getAllSeedsView()
                .stream()
                .map(SeedsBackFillView::getReproductionTypeViewInfo)
                .sorted(comparing(ReproductionTypeViewInfo::getTurn, nullsLast(naturalOrder())))
                .filter(reproductionTypeViewInfo -> !reproductionTypeViewInfo.isEmpty())
                .map(reproductionTypeViewInfo -> new AbstractMap.SimpleEntry<>(reproductionTypeViewInfo.getCategoryNameView(), reproductionTypeViewInfo.getNameView()))
                .distinct()
                .collect(Collectors.groupingBy(Map.Entry::getKey,
                        Collectors.mapping(Map.Entry::getValue, Collectors.toList()))));

        if (filterMap.get(ReportKeysForFilterEnum.GROUPING) instanceof GroupingTypeEnum && Boolean.FALSE.equals(filterMap.get(ReportKeysForFilterEnum.DETAL_BY_PARTIAL))) {
            helperForCreateExportList((GroupingTypeEnum) filterMap.get(ReportKeysForFilterEnum.GROUPING), s -> exportList.add((SeedsBackFillView) s), false);
        } else if (filterMap.get(ReportKeysForFilterEnum.GROUPING) == null && Boolean.TRUE.equals(filterMap.get(ReportKeysForFilterEnum.DETAL_BY_PARTIAL))) {
            exportList = getAllSeedsView().stream()
                    .filter(mainParamPredicate.and(numberPredicate))
                    .sorted(seedBackComp).collect(Collectors.toList());
        } else if (filterMap.get(ReportKeysForFilterEnum.GROUPING) != null && Boolean.TRUE.equals(filterMap.get(ReportKeysForFilterEnum.DETAL_BY_PARTIAL))) {
            helperForCreateExportList((GroupingTypeEnum) filterMap.get(ReportKeysForFilterEnum.GROUPING), list -> exportList.addAll((List<SeedsBackFillView>) list), true);
        }
    }

    private List<SeedsBackFillView> getAllSeedsView() {
        if (allSeedsView == null) {
            allSeedsView = backFillRepository.findForView(criteria);
        }
        return allSeedsView;
    }

    public static <T> Predicate<T> distinctByKey(Function<? super T, ?> keyExtractor) {
        Set<Object> seen = ConcurrentHashMap.newKeySet();
        return t -> seen.add(keyExtractor.apply(t));
    }

    @SuppressWarnings("unchecked")
    private void helperForCreateExportList(GroupingTypeEnum groupingTypeEnum, Consumer consumer, Boolean detalByPart) {
        switch (groupingTypeEnum) {
            case CONTRACTORS_AND_TOWNSHIPS:
                Map<Long, Map<Long, List<SeedsBackFillView>>> helperMapConTwn = allSeedsView.stream().
                        filter(mainParamPredicate.and(sb -> sb.getContractorId() != null))
                        .collect(groupingBy(SeedsBackFillView::getTownshipId, groupingBy(SeedsBackFillView::getContractorId)));
                if (detalByPart) {
                    helperMapConTwn.forEach((dep, contractorListMap) -> contractorListMap
                            .forEach((cul, list) -> ofNullable(groupingSeedsBackForContractorTowns(list)).map(s -> list).ifPresent(consumer)));
                } else
                    helperMapConTwn.forEach((dep, contractorListMap) -> contractorListMap.forEach((cul, list) -> ofNullable(groupingSeedsBackForContractorTowns(list)).ifPresent(consumer)));
                exportList.sort(seedBackComp);
                break;

            case CONTRACTORS:
                Map<Object, List<SeedsBackFillView>> helperMap = getAllSeedsView().stream().
                        filter(mainParamPredicate)
                        .collect(groupingBy(SeedsBackFillView::getContractorId));
                if (detalByPart)
                    helperMap.forEach((conId, list) -> ofNullable(groupingSeedsBack(list)).map(s -> list).ifPresent(consumer));
                else
                    helperMap.forEach((conId, list) -> ofNullable(groupingSeedsBack(list)).ifPresent(consumer));
                exportList.sort(seedBackComp);
                break;
            case TOWNSHIPS:
                helperMap = getAllSeedsView().stream().filter(mainParamPredicate)
                        .collect(groupingBy(sb ->
                                ofNullable(sb.getTownshipId()).
                                        orElse(-1L)));
                if (detalByPart)
                    helperMap.forEach((dep, list) -> ofNullable(groupingSeedsBackForTowns(list)).map(s -> list).ifPresent(consumer));
                else
                    helperMap.forEach((dep, list) -> ofNullable(groupingSeedsBackForTowns(list)).ifPresent(consumer));
                exportList.sort(seedBackComp);
                break;
            case CULTURE:
                helperMap = getAllSeedsView().stream().
                        filter(mainParamPredicate.and(sb -> sb.getCultureSortCode() != null))
                        .collect(groupingBy(SeedsBackFillView::getCultureId));
                if (detalByPart)

                    helperMap.forEach((cul, list) ->
                            ofNullable(groupingSeedsBackForCulture(list)).
                                    map(s -> list).ifPresent(consumer));
                else
                    helperMap.forEach((cul, list) -> ofNullable(groupingSeedsBackForCulture(list)).ifPresent(consumer));
                exportList.sort(seedBackComp);
                break;
            case CULTURE_AND_TOWNSHIPS:
                Map<Long, Map<Long, List<SeedsBackFillView>>> helperMapCulTwn = createTwnCulMap();
                if (detalByPart) {
                    helperMapCulTwn.forEach((dep, cultureListMap) -> cultureListMap.forEach((cul, list) -> ofNullable(groupingSeedsBackForCultureTowns(list)).map(s -> list).ifPresent(consumer)));
                } else
                    helperMapCulTwn.forEach((dep, cultureListMap) -> cultureListMap.forEach((cul, list) -> ofNullable(groupingSeedsBackForCultureTowns(list)).ifPresent(consumer)));
                exportList.sort(seedBackComp);
                break;
        }
    }

    private Map<Long, Map<Long, List<SeedsBackFillView>>> createTwnCulMap() {
        Map<Long, Map<Long, List<SeedsBackFillView>>> map = new HashMap<>();
        List<SeedsBackFillView> workList = getAllSeedsView().stream().filter(mainParamPredicate.and(sb -> sb.getCultureSortCode() != null)).collect(Collectors.toList());
        Supplier<Stream<SeedsBackFillView>> supplier = workList::stream;
        supplier.get().map(SeedsBackFillView::getTownshipId).filter(Objects::nonNull).forEach(twnId ->
                map.put(twnId, supplier.get().filter(sb -> Objects.equals(sb.getTownshipId(), twnId)).collect(groupingBy(SeedsBackFillView::getCultureId))));
        return map;
    }

    @SneakyThrows
    public byte[] constructExcel(BackFillApiParam backFillApiParam) {
        this.filterMap = backFillApiParam.getMap();
        this.criteria = backFillApiParam.getCriteria();


        double time = 0;
        try (ByteArrayOutputStream b = new ByteArrayOutputStream()) {
            Boolean flagDetal;
            currentNumRow = 3;

            GroupingTypeEnum groupingAndDetal = null;
            Sheet sheet;
            if (ReportTypeEnum.NEED.getName().equals(filterMap.get(ReportKeysForFilterEnum.REPORT_TYPE))) {
                try (XSSFWorkbook book = XSSFWorkbookFactory.createWorkbook(OPCPackage.open(resourceLoader.getResource("classpath:/excel/report_need.xlsx").getInputStream()))) {
                    workbook = book;

                    sheet = workbook.sheetIterator().next();
                    initializeGroupingFont();
                    generateRow(sheet.createRow(currentNumRow));
                    BackFillReporterFilterMapper.mapToObject(filterMap);
                    List<CalculationNeed> exportListNeeds = seedNeedsRepo.getNeedsByFilteredMap(filterMap, backFillApiParam.getCriteria().getDepartmentRegion());
                    flagDetal = (Boolean) filterMap.get(ReportKeysForFilterEnum.DETAL_BY_PARTIAL);
                    groupingAndDetal = (GroupingTypeEnum) filterMap.get(ReportKeysForFilterEnum.GROUPING);
                    // ? try fix
                    currentColumnOffset = 1;
                    if (!exportListNeeds.isEmpty()) {
                        if (!flagDetal && groupingAndDetal == null) {
                            addLastRowForNeed(sheet, currentNumRow, exportListNeeds, onlyGroupingType(filterMap));
                        } else if (flagDetal && groupingAndDetal != null) {
                            currentColumnOffset = 1;
                            switch (groupingAndDetal) {
                                case CONTRACTORS:
                                    Map<ContractorLite, List<CalculationNeed>> contractorCalculationNeedMap = exportListNeeds.stream()
                                            .collect(groupingBy(CalculationNeed::getContractor));
                                        /*contractorCalculationNeedMap.forEach((key, value) -> {
                                            editCellsForNeed(sheet, exportListNeeds, currentNumRow);
                                        });
*/
                                    List<ContractorLite> collection = contractorCalculationNeedMap.keySet().stream().sorted(sortByContractor()).collect(Collectors.toList());
                                    for (ContractorLite contractor : collection) {
                                        editCellsForNeed(sheet, contractorCalculationNeedMap.get(contractor).stream().sorted(comparing(CalculationNeed::getCultureName, nullsLast(naturalOrder()))).collect(Collectors.toList()));
                                        addGroupingRowForNeed(sheet.createRow(currentNumRow), groupingAndDetal, contractor, null, contractorCalculationNeedMap.get(contractor));
                                        currentNumRow = currentNumRow + 1;
                                    }
                                    break;
                                case TOWNSHIPS:
                                    Map<TerTownshipLite, List<CalculationNeed>> terTownshipListMap = exportListNeeds.stream().
                                            collect(groupingBy(CalculationNeed::getTownship));
                                    for (TerTownshipLite terTownship : terTownshipListMap.keySet().stream().sorted(comparing(TerTownshipLite::getName)).collect(Collectors.toList())) {
                                        editCellsForNeed(sheet, terTownshipListMap.get(terTownship).stream().sorted(sortByContractorCulture()).collect(Collectors.toList()));
                                        addGroupingRowForNeed(sheet.createRow(currentNumRow++), groupingAndDetal, null, null, terTownshipListMap.get(terTownship));
                                    }
                                    break;
                                case CULTURE:
                                    Map<CultureLite, List<CalculationNeed>> cultureCalculationNeedMap = exportListNeeds.stream()
                                            .collect(groupingBy(CalculationNeed::getCulture));
                                    for (CultureLite culture : cultureCalculationNeedMap.keySet().stream().sorted(comparing(CultureLite::getName, nullsLast(naturalOrder()))).collect(Collectors.toList())) {
                                        editCellsForNeed(sheet, cultureCalculationNeedMap.get(culture).stream().sorted(sortByTwnContractor()).collect(Collectors.toList()));
                                        addGroupingRowForNeed(sheet.createRow(currentNumRow++), groupingAndDetal, null, culture, cultureCalculationNeedMap.get(culture));
                                    }
                                    break;
                                case CONTRACTORS_AND_TOWNSHIPS:
                                    Map<TerTownshipLite, Map<ContractorLite, List<CalculationNeed>>> contrTownCalculationNeedMap = exportListNeeds.stream().
                                            collect(groupingBy(CalculationNeed::getTownship,
                                                    groupingBy(CalculationNeed::getContractor)));
                                    for (TerTownshipLite terTownship : contrTownCalculationNeedMap.keySet().stream().sorted(comparing(TerTownshipLite::getName)).collect(Collectors.toList())) {
                                        for (ContractorLite contractor : contrTownCalculationNeedMap.get(terTownship).keySet().stream().sorted(comparing(ContractorLite::getNameShort, nullsLast(naturalOrder()))).collect(Collectors.toList())) {
                                            editCellsForNeed(sheet, contrTownCalculationNeedMap.get(terTownship).get(contractor).stream().sorted(sortByCulture()).collect(Collectors.toList()));
                                            addGroupingRowForNeed(sheet.createRow(currentNumRow++), groupingAndDetal, contractor, null, contrTownCalculationNeedMap.get(terTownship).get(contractor));
                                        }
                                        addGroupingRowForNeed(sheet.createRow(currentNumRow++), groupingAndDetal, null, null, contrTownCalculationNeedMap.get(terTownship).values().stream().flatMap(Collection::stream).collect(Collectors.toList()));
                                    }
                                    break;
                                case CULTURE_AND_TOWNSHIPS:
                                    Map<TerTownshipLite, Map<CultureLite, List<CalculationNeed>>> cultureTownCalculationNeedMap = exportListNeeds.stream().
                                            collect(groupingBy(CalculationNeed::getTownship,
                                                    groupingBy(CalculationNeed::getCulture)));
                                    for (TerTownshipLite terTownship : cultureTownCalculationNeedMap.keySet().stream().sorted(comparing(TerTownshipLite::getName)).collect(Collectors.toList())) {
                                        for (CultureLite culture : cultureTownCalculationNeedMap.get(terTownship).keySet().stream().sorted(comparing(CultureLite::getName, nullsLast(naturalOrder()))).collect(Collectors.toList())) {
                                            editCellsForNeed(sheet, cultureTownCalculationNeedMap.get(terTownship).get(culture).stream().sorted(sortByContractorCalc()).collect(Collectors.toList()));
                                            addGroupingRowForNeed(sheet.createRow(currentNumRow++), groupingAndDetal, null, culture, cultureTownCalculationNeedMap.get(terTownship).get(culture));
                                        }
                                        addGroupingRowForNeed(sheet.createRow(currentNumRow++), groupingAndDetal, null, null, cultureTownCalculationNeedMap.get(terTownship).values().stream().flatMap(Collection::stream).collect(Collectors.toList()));
                                    }
                                    break;
                            }
                            addLastRowForNeed(sheet, currentNumRow, exportListNeeds, onlyGroupingType(filterMap));
                        } else if (onlyGroupingType(filterMap)) {
                            initializePredicateForNeed();
                            switch (groupingAndDetal) {
                                case CONTRACTORS:
                                    Map<ContractorLite, List<CalculationNeed>> contractorCalculationNeedMap = exportListNeeds.stream()
                                            .collect(groupingBy(CalculationNeed::getContractor));
                                    for (ContractorLite contractor : contractorCalculationNeedMap.keySet().stream().sorted(sortByContractor()).collect(Collectors.toList())) {
                                        editCellForNeed(sheet, helperMethodForGroupingNeed(contractorCalculationNeedMap.get(contractor), contractor, null, null, GroupingTypeEnum.CONTRACTORS), false);
                                    }
                                    break;
                                case TOWNSHIPS:
                                    Map<TerTownshipLite, List<CalculationNeed>> terTownshipListMap = exportListNeeds.stream().
                                            collect(groupingBy(CalculationNeed::getTownship));
                                    for (TerTownshipLite terTownship : terTownshipListMap.keySet().stream().sorted(comparing(TerTownshipLite::getName)).collect(Collectors.toList())) {
                                        editCellForNeed(sheet, helperMethodForGroupingNeed(terTownshipListMap.get(terTownship), null, null, terTownship, GroupingTypeEnum.TOWNSHIPS), false);
                                    }
                                    break;
                                case CULTURE:
                                    Map<CultureLite, List<CalculationNeed>> cultureCalculationNeedMap = exportListNeeds.stream()
                                            .collect(groupingBy(CalculationNeed::getCulture));
                                    for (CultureLite culture : cultureCalculationNeedMap.keySet().stream().sorted(comparing(CultureLite::getName, nullsLast(naturalOrder()))).collect(Collectors.toList())) {
                                        editCellForNeed(sheet, helperMethodForGroupingNeed(cultureCalculationNeedMap.get(culture), null, culture, null, GroupingTypeEnum.CULTURE), false);
                                    }
                                    break;
                                case CONTRACTORS_AND_TOWNSHIPS:
                                    Map<TerTownshipLite, Map<ContractorLite, List<CalculationNeed>>> contrTownCalculationNeedMap = exportListNeeds.stream().
                                            collect(groupingBy(CalculationNeed::getTownship,
                                                    groupingBy(CalculationNeed::getContractor)));

                                    for (TerTownshipLite terTownship : contrTownCalculationNeedMap.keySet().stream().sorted(comparing(TerTownshipLite::getName)).collect(Collectors.toList())) {
                                        CalculationNeed clGr = helperMethodForGroupingNeed(contrTownCalculationNeedMap.get(terTownship).values().stream().flatMap(Collection::stream).collect(Collectors.toList()), null, null, terTownship, GroupingTypeEnum.TOWNSHIPS);
                                        for (ContractorLite contractor : contrTownCalculationNeedMap.get(terTownship).keySet().stream().sorted(comparing(ContractorLite::getNameShort, nullsLast(naturalOrder()))).collect(Collectors.toList())) {
                                            if (clGr != null)
                                                editCellForNeed(sheet, helperMethodForGroupingNeed(contrTownCalculationNeedMap.get(terTownship).get(contractor), contractor, null, null, GroupingTypeEnum.CONTRACTORS_AND_TOWNSHIPS), false);
                                            else
                                                break;
                                        }
                                        editCellForNeed(sheet, helperMethodForGroupingNeed(contrTownCalculationNeedMap.get(terTownship).values().stream().flatMap(Collection::stream).collect(Collectors.toList()), null, null, terTownship, GroupingTypeEnum.CONTRACTORS_AND_TOWNSHIPS), true);
                                    }
                                    break;
                                case CULTURE_AND_TOWNSHIPS:
                                    Map<TerTownshipLite, Map<CultureLite, List<CalculationNeed>>> cultureTownCalculationNeedMap = exportListNeeds.stream().
                                            collect(groupingBy(CalculationNeed::getTownship,
                                                    groupingBy(CalculationNeed::getCulture)));
                                    for (TerTownshipLite terTownship : cultureTownCalculationNeedMap.keySet().stream().sorted(comparing(TerTownshipLite::getName)).collect(Collectors.toList())) {
                                        CalculationNeed group = helperMethodForGroupingNeed(cultureTownCalculationNeedMap.get(terTownship).values().stream().flatMap(Collection::stream).collect(Collectors.toList()), null, null, terTownship, GroupingTypeEnum.TOWNSHIPS);
                                        for (CultureLite culture : cultureTownCalculationNeedMap.get(terTownship).keySet().stream().sorted(comparing(CultureLite::getName, nullsLast(naturalOrder()))).collect(Collectors.toList())) {
                                            if (group != null)
                                                editCellForNeed(sheet, helperMethodForGroupingNeed(cultureTownCalculationNeedMap.get(terTownship).get(culture), null, culture, terTownship, GroupingTypeEnum.CULTURE_AND_TOWNSHIPS), false);
                                            else
                                                break;
                                        }
                                        editCellForNeed(sheet, helperMethodForGroupingNeed(cultureTownCalculationNeedMap.get(terTownship).values().stream().flatMap(Collection::stream).collect(Collectors.toList()), null, null, terTownship, GroupingTypeEnum.CULTURE_AND_TOWNSHIPS), true);
                                    }
                                    break;
                            }
                            addLastRowForNeed(sheet, currentNumRow, exportListNeeds, onlyGroupingType(filterMap));
                        } else {
                            for (CalculationNeed seedsNeed : exportListNeeds) {
                                Row row = sheet.getRow(currentNumRow);

                                editCell(row, SeedNeedReportColumnEnum.NUMBER.ordinal(), String.valueOf(currentNumRow - 2), styleString);
                                editCell(row, SeedNeedReportColumnEnum.TOWNSHIP.ordinal(), seedsNeed.getTownship().getName(), styleString);
                                editCell(row, SeedNeedReportColumnEnum.CONTRACTOR.ordinal(), seedsNeed.getContractor().getNameShort(), styleString);
                                editCell(row, SeedNeedReportColumnEnum.CULTURE.ordinal(), seedsNeed.getCulture().getName(), styleString);
                                BigDecimal temp = ofNullable(seedsNeed.getFillSum()).map(d -> BigDecimal.valueOf(d).setScale(6, BigDecimal.ROUND_HALF_DOWN)).orElse(null);
                                editCell(row, SeedNeedReportColumnEnum.FILL_ALL.ordinal(), ofNullable(temp).map(BigDecimal::doubleValue).orElse(null), styleWeight, CellType.NUMERIC);
                                temp = ofNullable(seedsNeed.getCurrentFillSum()).map(d -> BigDecimal.valueOf(d).setScale(6, BigDecimal.ROUND_HALF_DOWN)).orElse(null);
                                editCell(row, SeedNeedReportColumnEnum.CURRENT_FILL.ordinal(), ofNullable(temp).map(BigDecimal::doubleValue).orElse(null), styleWeight, CellType.NUMERIC);
                                editCell(row, SeedNeedReportColumnEnum.NEED_COUNT.ordinal(), seedsNeed.getNeedCount(), styleWeight, CellType.NUMERIC);
                                editCell(row, SeedNeedReportColumnEnum.NEED_COUNT_PERC.ordinal(), row.getCell(SeedNeedReportColumnEnum.CURRENT_FILL.ordinal()).getNumericCellValue() / row.getCell(SeedNeedReportColumnEnum.NEED_COUNT.ordinal()).getNumericCellValue(), stylePerc, CellType.BLANK);

                                if (exportListNeeds.size() - 1 != exportListNeeds.indexOf(seedsNeed)) {
                                    currentNumRow++;
                                    //generateSeedBackGroupAndAddGroupingRow(groupingAndDetal, sb, sheet, currentNumRow, false);
                                    generateRow(sheet.createRow(currentNumRow));
                                } else
                                    currentNumRow++;
                            }
                            addLastRowForNeed(sheet, currentNumRow, exportListNeeds, onlyGroupingType(filterMap));
                        }
                    }
                    workbook.write(b);
                }

            } else {
                currentColumnOffset = 0;
                try (XSSFWorkbook book = XSSFWorkbookFactory.createWorkbook(OPCPackage.open(resourceLoader.getResource("classpath:/excel/report.xlsx").getInputStream()))) {
                    workbook = book;

                    sheet = workbook.sheetIterator().next();
                    initializeGroupingFont();
                    createExportList();
                    expandTableWthRepr(sheet);
                    generateRow(sheet.createRow(currentNumRow));
                    if (filterMap.get(ReportKeysForFilterEnum.DETAL_BY_PARTIAL) instanceof Boolean) {
                        flagDetal = (Boolean) filterMap.get(ReportKeysForFilterEnum.DETAL_BY_PARTIAL);
                        if (filterMap.get(ReportKeysForFilterEnum.GROUPING) instanceof GroupingTypeEnum && flagDetal)
                            groupingAndDetal = (GroupingTypeEnum) filterMap.get(ReportKeysForFilterEnum.GROUPING);
                    } else
                        flagDetal = false;
                    if (CollectionUtils.isNotNullOrNotEmpty(exportList)) {
                        if (flagDetal) {
                            for (SeedsBackFillView sb : exportList) {
                                currentColumnOffset = 1;
                                Row row = sheet.getRow(currentNumRow);
                                editCell(row, 0, String.valueOf(sb.getId()), styleString);
                                editCell(row, NUMBER.ordinal() + currentColumnOffset, String.valueOf(exportList.indexOf(sb) + 1), styleString);
                                editCell(row, TOWNSHIP.ordinal() + currentColumnOffset, getTownNameBySb(sb).orElse(null), styleString);
                                editCell(row, CONTRACTOR.ordinal() + currentColumnOffset, ofNullable(sb.getContractorNameShort()).orElse(null), styleString);
                                editCell(row, CULTURE.ordinal() + currentColumnOffset, sb.getCultureName(), styleString);
                                editCell(row, CULTURE_SORT.ordinal() + currentColumnOffset, sb.getCultureSortName(), styleString);
                                editCell(row, CATEGORY.ordinal() + currentColumnOffset, sb.getReproductionTypeViewInfo().getCategoryNameView(), styleString);
                                editCell(row, REPRODUCTION.ordinal() + currentColumnOffset, sb.getReproductionTypeViewInfo().getNameView(), styleString);
                                BigDecimal temp = ofNullable(sb.getFillAll()).map(d -> BigDecimal.valueOf(d).setScale(6, BigDecimal.ROUND_HALF_DOWN)).orElse(null);
                                editCell(row, FILL_ALL.ordinal() + currentColumnOffset, ofNullable(temp).map(BigDecimal::doubleValue).orElse(null), styleWeight, CellType.NUMERIC);
                                temp = ofNullable(sb.getCurrentFill()).map(d -> BigDecimal.valueOf(d).setScale(6, BigDecimal.ROUND_HALF_DOWN)).orElse(null);
                                editCell(row, CURRENT_FILL.ordinal() + currentColumnOffset, ofNullable(temp).map(BigDecimal::doubleValue).orElse(null), styleWeight, CellType.NUMERIC);
                                temp = ofNullable(sb.getSownSum()).map(d -> BigDecimal.valueOf(d).setScale(6, BigDecimal.ROUND_HALF_DOWN)).orElse(null);
                                editCell(row, SOWN_SUM.ordinal() + currentColumnOffset, ofNullable(temp).map(BigDecimal::doubleValue).orElse(null), styleWeight, CellType.NUMERIC);
                                temp = ofNullable(sb.getSoldSum()).map(d -> BigDecimal.valueOf(d).setScale(6, BigDecimal.ROUND_HALF_DOWN)).orElse(null);
                                editCell(row, SOLD_SUM.ordinal() + currentColumnOffset, ofNullable(temp).map(BigDecimal::doubleValue).orElse(null), styleWeight, CellType.NUMERIC);
                                temp = ofNullable(sb.getDefectedSum()).map(d -> BigDecimal.valueOf(d).setScale(6, BigDecimal.ROUND_HALF_DOWN)).orElse(null);
                                editCell(row, DEFECTED_SUM.ordinal() + currentColumnOffset, ofNullable(temp).map(BigDecimal::doubleValue).orElse(null), styleWeight, CellType.NUMERIC);
                                temp = ofNullable(sb.getFedSum()).map(d -> BigDecimal.valueOf(d).setScale(6, BigDecimal.ROUND_HALF_DOWN)).orElse(null);
                                editCell(row, FED_SUM.ordinal() + currentColumnOffset, ofNullable(temp).map(BigDecimal::doubleValue).orElse(null), styleWeight, CellType.NUMERIC);
                                editCell(row, SOURCE_CUSTOMER.ordinal() + currentColumnOffset, sb.sourceCustomerString(), styleString);
                                editCell(row, PURPOSE.ordinal() + currentColumnOffset, ofNullable(sb.getPurposeEnum()).map(SeedsBackFillPurposeEnum::getName).orElse(null), styleString);
                                editCell(row, BATCH_NUMBER.ordinal() + currentColumnOffset, sb.getBatchNumber(), styleString);
                                editCell(row, FUND_TYPE.ordinal() + currentColumnOffset, ofNullable(sb.getSeedFundTypeName()).orElse(null), styleString);
                                editCell(row, CROP_YEAR.ordinal() + currentColumnOffset, ofNullable(sb.getCropYear()).map(String::valueOf).orElse(null), styleString);
                                editCell(row, SORT_DOC_NUMBER.ordinal() + currentColumnOffset, sb.getSortDocNumber(), styleString);
                                editCell(row, SORT_DOC_DATE.ordinal() + currentColumnOffset, sb.getSortDocDate(), styleDateShort);
                                editCell(row, QUAL_DOC_NUMBER.ordinal() + currentColumnOffset, sb.getQualityDocNumber(), styleString);
                                editCell(row, QUAL_DOC_DATE_EXPIRED.ordinal() + currentColumnOffset, sb.getQualityDocDateExpired(), styleDateShort);
                                editCell(row, QUAL_CHECKED.ordinal() + currentColumnOffset, sb.getQualChecked(), styleWeight, CellType.NUMERIC);
                                editCell(row, QUAL_CHECKED_PERC.ordinal() + currentColumnOffset, getProc(sb.getQualChecked(), sb.getCurrentFill()), stylePerc, CellType.BLANK);
                                editCell(row, QUAL_CONDITIONED.ordinal() + currentColumnOffset, sb.getQualConditioned(), styleWeight, CellType.NUMERIC);
                                editCell(row, QUAL_CONDITIONED_PERC.ordinal() + currentColumnOffset, getProc(sb.getQualConditioned(), sb.getQualChecked()), stylePerc, CellType.BLANK);
                                editCell(row, QUAL_NOT_CONDITIONED_ALL.ordinal() + currentColumnOffset, ofNullable(sb.getQualNotConditionedAll()).orElse(0d), styleWeight, CellType.NUMERIC);
                                editCell(row, QUAL_NOT_CONDITIONED_ALL_PERC.ordinal() + currentColumnOffset, of(getProc(sb.getQualNotConditionedAll(), sb.getQualChecked())).orElse(0d), stylePerc, CellType.BLANK);
                                editCell(row, QUAL_NOT_CONDITIONED_DEBRIS.ordinal() + currentColumnOffset, ofNullable(sb.getQualNotConditionedDebris()).orElse(0d), styleWeight, CellType.NUMERIC);
                                editCell(row, QUAL_NOT_CONDITIONED_HUMIDITY.ordinal() + currentColumnOffset, ofNullable(sb.getQualNotConditionedHumidity()).orElse(0d), styleWeight, CellType.NUMERIC);
                                editCell(row, QUAL_NOT_CONDITIONED_GERMINATION.ordinal() + currentColumnOffset, ofNullable(sb.getQualNotConditionedGermination()).orElse(0d), styleWeight, CellType.NUMERIC);
                                editCell(row, QUAL_NOT_CONDITIONED_PESTS.ordinal() + currentColumnOffset, ofNullable(sb.getQualNotConditionedPests()).orElse(0d), styleWeight, CellType.NUMERIC);


                                fillReproductionColumns(sb, row);
                                currentColumnOffset = reproductionCategoryColumnsFullSize() - 1 + currentColumnOffset;
                                editCell(row, WEIGHTED_AVERAGE_1000_SEEDS.ordinal() + currentColumnOffset, sb.getWeightedAverage1000Seeds(), styleNum2, CellType.NUMERIC);
                                editCell(row, GERMINATION.ordinal() + currentColumnOffset, ofNullable(sb.getGermination()).map(germ -> germ / 100).orElse(null), stylePerc, CellType.BLANK);

                                if (exportList.size() - 1 != exportList.indexOf(sb)) {
                                    currentNumRow++;
                                    generateSeedBackGroupAndAddGroupingRow(groupingAndDetal, sb, sheet, false);
                                    Chronograph.start(55);
                                    generateRow(sheet.createRow(currentNumRow));
                                    time += Chronograph.stop(55);
                                } else
                                    currentNumRow++;
                            }
                        } else {
                            if (filterMap.get(ReportKeysForFilterEnum.GROUPING) instanceof GroupingTypeEnum) {
                                GroupingTypeEnum gr = (GroupingTypeEnum) filterMap.get(ReportKeysForFilterEnum.GROUPING);
                                generateRow(sheet.createRow(currentNumRow));
                                for (SeedsBackFillView sb : exportList) {
                                    currentColumnOffset = 1;
                                    Row row = sheet.getRow(currentNumRow);
                                    editCell(row, 0, String.valueOf(sb.getId()), styleString);
                                    editCell(row, NUMBER.ordinal() + currentColumnOffset, String.valueOf(exportList.indexOf(sb) + 1), styleString);
                                    editCell(row, TOWNSHIP.ordinal() + currentColumnOffset, ofNullable(sb.getTownshipName()).orElse(""), styleString);
                                    editCell(row, CONTRACTOR.ordinal() + currentColumnOffset, gr.equals(GroupingTypeEnum.TOWNSHIPS) ? "" : ofNullable(sb.getContractorNameShort()).orElse(""), styleString);
                                    editCell(row, CULTURE.ordinal() + currentColumnOffset, gr.equals(GroupingTypeEnum.CULTURE) || gr.equals(GroupingTypeEnum.CULTURE_AND_TOWNSHIPS) ? sb.getCultureName() : "", styleString);
                                    nullCell(row, CULTURE_SORT.ordinal() + currentColumnOffset, groupingStyleString);
                                    nullCell(row, CATEGORY.ordinal() + currentColumnOffset, groupingStyleString);
                                    nullCell(row, REPRODUCTION.ordinal() + currentColumnOffset, groupingStyleString);
                                    BigDecimal temp = ofNullable(sb.getFillAll()).map(d -> BigDecimal.valueOf(d).setScale(6, BigDecimal.ROUND_HALF_DOWN)).orElse(null);
                                    editCell(row, FILL_ALL.ordinal() + currentColumnOffset, ofNullable(temp).map(BigDecimal::doubleValue).orElse(0d), groupingStyleWeight, CellType.NUMERIC);
                                    temp = ofNullable(sb.getCurrentFill()).map(d -> BigDecimal.valueOf(d).setScale(6, BigDecimal.ROUND_HALF_DOWN)).orElse(null);
                                    editCell(row, CURRENT_FILL.ordinal() + currentColumnOffset, ofNullable(temp).map(BigDecimal::doubleValue).orElse(0d), groupingStyleWeight, CellType.NUMERIC);
                                    temp = ofNullable(sb.getSownSum()).map(d -> BigDecimal.valueOf(d).setScale(6, BigDecimal.ROUND_HALF_DOWN)).orElse(null);
                                    editCell(row, SOWN_SUM.ordinal() + currentColumnOffset, ofNullable(temp).map(BigDecimal::doubleValue).orElse(null), groupingStyleWeight, CellType.NUMERIC);
                                    temp = ofNullable(sb.getSoldSum()).map(d -> BigDecimal.valueOf(d).setScale(6, BigDecimal.ROUND_HALF_DOWN)).orElse(null);
                                    editCell(row, SOLD_SUM.ordinal() + currentColumnOffset, ofNullable(temp).map(BigDecimal::doubleValue).orElse(null), groupingStyleWeight, CellType.NUMERIC);
                                    temp = ofNullable(sb.getDefectedSum()).map(d -> BigDecimal.valueOf(d).setScale(6, BigDecimal.ROUND_HALF_DOWN)).orElse(null);
                                    editCell(row, DEFECTED_SUM.ordinal() + currentColumnOffset, ofNullable(temp).map(BigDecimal::doubleValue).orElse(null), groupingStyleWeight, CellType.NUMERIC);
                                    temp = ofNullable(sb.getFedSum()).map(d -> BigDecimal.valueOf(d).setScale(6, BigDecimal.ROUND_HALF_DOWN)).orElse(null);
                                    editCell(row, FED_SUM.ordinal() + currentColumnOffset, ofNullable(temp).map(BigDecimal::doubleValue).orElse(null), groupingStyleWeight, CellType.NUMERIC);

                                    nullCell(row, SOURCE_CUSTOMER.ordinal() + currentColumnOffset, groupingStyleString);
                                    nullCell(row, PURPOSE.ordinal() + currentColumnOffset, groupingStyleString);
                                    nullCell(row, BATCH_NUMBER.ordinal() + currentColumnOffset, groupingStyleString);
                                    nullCell(row, FUND_TYPE.ordinal() + currentColumnOffset, groupingStyleString);
                                    nullCell(row, CROP_YEAR.ordinal() + currentColumnOffset, groupingStyleString);
                                    nullCell(row, SORT_DOC_NUMBER.ordinal() + currentColumnOffset, groupingStyleString);
                                    nullCell(row, SORT_DOC_DATE.ordinal() + currentColumnOffset, groupingStyleString);
                                    nullCell(row, QUAL_DOC_NUMBER.ordinal() + currentColumnOffset, groupingStyleString);
                                    nullCell(row, QUAL_DOC_DATE_EXPIRED.ordinal() + currentColumnOffset, groupingStyleString);

                                    editCell(row, QUAL_CHECKED.ordinal() + currentColumnOffset, sb.getQualChecked(), groupingStyleWeight, CellType.NUMERIC);
                                    editCell(row, QUAL_CHECKED_PERC.ordinal() + currentColumnOffset, getProc(sb.getQualChecked(), sb.getCurrentFill()), groupingStylePerc, CellType.BLANK);
                                    editCell(row, QUAL_CONDITIONED.ordinal() + currentColumnOffset, sb.getQualConditioned(), groupingStyleWeight, CellType.NUMERIC);
                                    editCell(row, QUAL_CONDITIONED_PERC.ordinal() + currentColumnOffset, getProc(sb.getQualConditioned(), sb.getQualChecked()), groupingStylePerc, CellType.BLANK);
                                    editCell(row, QUAL_NOT_CONDITIONED_ALL.ordinal() + currentColumnOffset, ofNullable(sb.getQualNotConditionedAll()).orElse(0d), groupingStyleWeight, CellType.NUMERIC);
                                    editCell(row, QUAL_NOT_CONDITIONED_ALL_PERC.ordinal() + currentColumnOffset, of(getProc(sb.getQualNotConditionedAll(), sb.getQualChecked())).orElse(0d), groupingStylePerc, CellType.BLANK);
                                    editCell(row, QUAL_NOT_CONDITIONED_DEBRIS.ordinal() + currentColumnOffset, ofNullable(sb.getQualNotConditionedDebris()).orElse(0d), groupingStyleWeight, CellType.NUMERIC);
                                    editCell(row, QUAL_NOT_CONDITIONED_HUMIDITY.ordinal() + currentColumnOffset, ofNullable(sb.getQualNotConditionedHumidity()).orElse(0d), groupingStyleWeight, CellType.NUMERIC);
                                    editCell(row, QUAL_NOT_CONDITIONED_GERMINATION.ordinal() + currentColumnOffset, ofNullable(sb.getQualNotConditionedGermination()).orElse(0d), groupingStyleWeight, CellType.NUMERIC);
                                    editCell(row, QUAL_NOT_CONDITIONED_PESTS.ordinal() + currentColumnOffset, ofNullable(sb.getQualNotConditionedPests()).orElse(0d), groupingStyleWeight, CellType.NUMERIC);

                                    fillReproductionColumnsGrouped(row, sb.getReproductionTypeViewInfoList(), sb.getQualChecked());
                                    currentColumnOffset = reproductionCategoryColumnsFullSize() - 1 + currentColumnOffset;

                                    editCell(row, WEIGHTED_AVERAGE_1000_SEEDS.ordinal() + currentColumnOffset, null, groupingStyleWeight, CellType.NUMERIC);
                                    editCell(row, GERMINATION.ordinal() + currentColumnOffset, null, groupingStyleWeight, CellType.NUMERIC);

                                    if (exportList.size() - 1 != exportList.indexOf(sb)) {
                                        currentNumRow++;
                                        if ((gr.equals(GroupingTypeEnum.CONTRACTORS_AND_TOWNSHIPS) || gr.equals(GroupingTypeEnum.CULTURE_AND_TOWNSHIPS)) && !matchesLastSbByTwn(sb)) {
                                            switch (gr) {
                                                case CONTRACTORS_AND_TOWNSHIPS:
                                                    SeedsBackFillView seedGroup = groupingSeedsBackForTowns(this.exportList.stream().
                                                            filter(sbgr -> ofNullable(sbgr.getTownshipId()).
                                                                    map(twn -> twn.equals(ofNullable(sb.getTownshipId()).orElse(null))).orElse(false)).
                                                            collect(Collectors.toList()));
                                                    addGroupingRow(sheet.createRow(currentNumRow++), seedGroup, "", gr);
                                                    break;
                                                case CULTURE_AND_TOWNSHIPS:
                                                    SeedsBackFillView seedGroupCul = groupingSeedsBackForTowns(this.exportList.stream().
                                                            filter(sbgr -> ofNullable(sbgr.getTownshipId()).
                                                                    map(twn -> twn.equals(ofNullable(sb.getTownshipId()).orElse(null))).orElse(false)).
                                                            collect(Collectors.toList()));
                                                    addGroupingRow(sheet.createRow(currentNumRow++), seedGroupCul, sb.getContractorNameShort(), gr);
                                                    break;
                                            }

                                        }
                                        generateRow(sheet.createRow(currentNumRow));
                                    } else
                                        currentNumRow++;
                                }
                            }
                        }


                    }
                    //TODO subtotal have the meaning where exportList.size() > 1
                    if (groupingAndDetal != null && exportList.size() > 1) {
                        generateSeedBackGroupAndAddGroupingRow(groupingAndDetal, exportList.get(exportList.size() - 2), sheet, true);
                        addLastRow(sheet, currentNumRow);
                    } else {
                        addLastRow(sheet, currentNumRow);
                    }

                    sheet.setAutoFilter(new CellRangeAddress(2, currentNumRow, 0, 60));
                    workbook.write(b);
                }
                catch (Exception e) {
                    log.error("Err", e);
                }
            }



            return b.toByteArray();
        }

    }

    private void expandTableWthRepr(Sheet sheet) {
        currentColumnOffset = 1;
        int reprColSize = reproductionCategoryColumnsFullSize();
        addColumns(sheet, REPRODUCTIONS.ordinal() + 1, reprColSize - 1, REPRODUCTIONS.ordinal());
        sheet.addMergedRegion(new CellRangeAddress(COLUMN_NAMES_ROW - 2, COLUMN_NAMES_ROW - 2, REPRODUCTIONS.ordinal() + currentColumnOffset, REPRODUCTIONS.ordinal() + reprColSize - 1 + currentColumnOffset));
        sheet.addMergedRegion(new CellRangeAddress(COLUMN_NAMES_ROW - 1, COLUMN_NAMES_ROW - 1, REPRODUCTIONS.ordinal() + currentColumnOffset, REPRODUCTIONS.ordinal() + reprColSize - 1 + currentColumnOffset));
        fillReproductionColumnNames(sheet.getRow(COLUMN_NAMES_ROW));
    }

    private void fillReproductionColumnNames(Row row) {
        int counter = 0;
        if (Objects.isNull(reproductionByCategoryColumns) || reproductionByCategoryColumns.isEmpty() || Objects.isNull(row))
            return;

        currentColumnOffset = 1;
        for (Map.Entry<String, List<String>> reprColNames : reproductionByCategoryColumns.entrySet()) {
            List<String> reproductionNames = reprColNames.getValue();
            if (CollectionUtils.isNotNullOrNotEmpty(reproductionNames)) {
                for (String name : reproductionNames) {
                    editCell(row, REPRODUCTIONS.ordinal() + counter + currentColumnOffset, name, groupingStyleString);
                    counter++;
                }
            }
            editCell(row, REPRODUCTIONS.ordinal() + counter + currentColumnOffset, reprColNames.getKey(), groupingStyleString);
            counter++;
        }
    }

    private void fillReproductionColumnsGrouped(Row row, List<ReproductionTypeViewInfo> reproductions, Double groupedQualChecked) {
        currentColumnOffset = 1;
        for (int i = REPRODUCTIONS.ordinal(); i < REPRODUCTIONS.ordinal() + reproductionCategoryColumnsFullSize(); i++) {
            editCell(row, i + currentColumnOffset, 0d, groupingStyleWeight, CellType.NUMERIC);
        }
        if (CollectionUtils.isNullOrEmpty(reproductions) || Objects.isNull(row)) return;
        reproductions.stream()
                .collect(groupingBy(ReproductionTypeViewInfo::getCategoryNameView, Collectors.summingDouble(ReproductionTypeViewInfo::getValue)))
                .forEach((key, value) -> rewriteCell(row,
                        REPRODUCTIONS.ordinal() + reproductionColumnNumber(null, key) + currentColumnOffset,
                        getProc(ofNullable(value).orElse(0d), ofNullable(groupedQualChecked).orElse(0d)),
                        groupingStylePerc,
                        CellType.NUMERIC));
        for (ReproductionTypeViewInfo reproduction : reproductions) {
            rewriteCell(row, REPRODUCTIONS.ordinal() + currentColumnOffset + reproductionColumnNumber(reproduction.getNameView(), reproduction.getCategoryNameView()), ofNullable(reproduction.getValue()).orElse(0d), groupingStyleWeight, CellType.NUMERIC);
        }
    }

    private void fillReproductionColumns(SeedsBackFillView sb, Row row) {
        currentColumnOffset = 1;

        ReproductionTypeViewInfo reproductionTypeViewInfo = sb.getReproductionTypeViewInfo();
        for (int i = REPRODUCTIONS.ordinal(); i < REPRODUCTIONS.ordinal() + reproductionCategoryColumnsFullSize(); i++) {
            editCell(row, i + currentColumnOffset, 0d, groupingStyleWeight, CellType.NUMERIC);
        }
        if (Objects.nonNull(reproductionTypeViewInfo) && !reproductionTypeViewInfo.isEmpty()) {
            Double qualChecked = sb.getQualChecked();
            rewriteCell(row, REPRODUCTIONS.ordinal() + currentColumnOffset + reproductionColumnNumber(reproductionTypeViewInfo.getNameView(), reproductionTypeViewInfo.getCategoryNameView()), ofNullable(qualChecked).orElse(0d), styleWeight, CellType.NUMERIC);
            rewriteCell(row, REPRODUCTIONS.ordinal() + currentColumnOffset + reproductionColumnNumber(null, reproductionTypeViewInfo.getCategoryNameView()), getProc(ofNullable(qualChecked).orElse(0d), ofNullable(qualChecked).orElse(0d)), groupingStylePerc, CellType.NUMERIC);
        }
    }

    private int reproductionCategoryColumnsFullSize() {
        if (Objects.isNull(reproductionByCategoryColumns) || reproductionByCategoryColumns.isEmpty()) return 0;
        return reproductionByCategoryColumns.size()
                + (CollectionUtils.isNotNullOrNotEmpty(reproductionByCategoryColumns.values())
                ? reproductionByCategoryColumns.values().stream()
                .filter(CollectionUtils::isNotNullOrNotEmpty)
                .mapToInt(List::size)
                .sum()
                : 0);
    }

    private int categoryColumnNumber(String reprCategoryNameView) {
        if (Objects.isNull(reproductionByCategoryColumns) || reproductionByCategoryColumns.isEmpty()) return 0;
        int counter = -1;
        Iterator<Map.Entry<String, List<String>>> iterator = reproductionByCategoryColumns.entrySet().iterator();
        Map.Entry<String, List<String>> currentEntry = iterator.next();
        while (iterator.hasNext() && !Objects.equals(currentEntry.getKey(), reprCategoryNameView)) {
            counter += currentEntry.getValue().size() + 1;
            currentEntry = iterator.next();
        }
        if (iterator.hasNext() || Objects.equals(currentEntry.getKey(), reprCategoryNameView)) {
            return counter;
        }
        return 0;
    }

    private int reproductionColumnNumber(String reprNameView, String reprCategoryNameView) {
        int counter = 0;
        if (Objects.isNull(reproductionByCategoryColumns) || reproductionByCategoryColumns.isEmpty()) return 0;
        for (Map.Entry<String, List<String>> reprColNames : reproductionByCategoryColumns.entrySet()) {
            List<String> reproductionNames = reprColNames.getValue();
            if (CollectionUtils.isNotNullOrNotEmpty(reproductionNames)) {
                for (String name : reproductionNames) {
                    if (name.equals(reprNameView) && Objects.equals(reprColNames.getKey(), reprCategoryNameView))
                        return counter;
                    counter++;
                }
            }
            if (StringUtils.isStringNullOrEmpty(reprNameView) && Objects.equals(reprColNames.getKey(), reprCategoryNameView))
                return counter;
            counter++;
        }
        return 0;
    }

    private void addGroupingRowForNeed(Row row, GroupingTypeEnum groupingTypeEnum, ContractorLite contractor, CultureLite culture, List<CalculationNeed> calculationNeeds) {
        editCell(row, TOWNSHIP.ordinal() + currentColumnOffset, groupingTypeEnum.equals(GroupingTypeEnum.CULTURE) ? "" : ofNullable(calculationNeeds.get(0).getTownshipName()).
                orElse(""), groupingStyleString);
        boolean groupByCulture = groupingTypeEnum.equals(GroupingTypeEnum.CULTURE) || groupingTypeEnum.equals(GroupingTypeEnum.CULTURE_AND_TOWNSHIPS);
        editCell(row, SeedNeedReportColumnEnum.CONTRACTOR.ordinal() + currentColumnOffset, groupByCulture ? "" : ofNullable(contractor).map(ContractorLite::getNameShort).orElse(""), groupingStyleString);
        editCell(row, SeedNeedReportColumnEnum.CULTURE.ordinal() + currentColumnOffset, groupByCulture ? ofNullable(culture).map(CultureLite::getName).orElse(null) : null, groupingStyleString);
        editCell(row, SeedNeedReportColumnEnum.FILL_ALL.ordinal() + currentColumnOffset, calculationNeeds.stream().mapToDouble(cneed -> Optional.ofNullable(cneed.getFillSum()).orElse(0.0)).sum(), groupingStyleWeight, CellType.NUMERIC);
        editCell(row, SeedNeedReportColumnEnum.CURRENT_FILL.ordinal() + currentColumnOffset, calculationNeeds.stream().mapToDouble(cneed -> Optional.ofNullable(cneed.getCurrentFillSum()).orElse(0.0)).sum(), groupingStyleWeight, CellType.NUMERIC);
        editCell(row, SeedNeedReportColumnEnum.NEED_COUNT.ordinal() + currentColumnOffset, calculationNeeds.stream().mapToDouble(cneed -> Optional.ofNullable(cneed.getNeedCount()).orElse(0.0)).sum(), groupingStyleWeight, CellType.NUMERIC);
        editCell(row, SeedNeedReportColumnEnum.NEED_COUNT_PERC.ordinal() + currentColumnOffset, row.getCell(SeedNeedReportColumnEnum.CURRENT_FILL.ordinal() + currentColumnOffset).getNumericCellValue() / row.getCell(SeedNeedReportColumnEnum.NEED_COUNT.ordinal() + currentColumnOffset).getNumericCellValue(), groupingStylePerc, CellType.BLANK);
    }


    private Boolean matchesLastSbByTwn(SeedsBackFillView sb) {
        return ofNullable(sb.getTownshipId()).
                map(twn -> ofNullable(exportList.get(exportList.indexOf(sb) + 1)).
                        map(sbNext -> ofNullable(sbNext.getTownshipId()).
                                map(twn::equals).
                                orElse(true)).
                        orElse(true)).
                orElse(true);
    }

    private void editCellsForNeed(Sheet sheet, List<CalculationNeed> list) {
        Row row = sheet.getRow(currentNumRow);
        if (row == null) {
            generateRow(sheet.createRow(currentNumRow));
        }

        for (CalculationNeed seedsNeed : list) {
            row = sheet.getRow(currentNumRow);

            editCell(row, SeedNeedReportColumnEnum.NUMBER.ordinal() + currentColumnOffset, String.valueOf(currentNumRow - 2), styleString);
            editCell(row, SeedNeedReportColumnEnum.TOWNSHIP.ordinal() + currentColumnOffset, seedsNeed.getTownship().getName(), styleString);
            editCell(row, SeedNeedReportColumnEnum.CONTRACTOR.ordinal() + currentColumnOffset, seedsNeed.getContractor().getNameShort(), styleString);
            editCell(row, SeedNeedReportColumnEnum.CULTURE.ordinal() + currentColumnOffset, seedsNeed.getCulture().getName(), styleString);
            BigDecimal temp = ofNullable(seedsNeed.getFillSum()).map(d -> BigDecimal.valueOf(d).setScale(6, BigDecimal.ROUND_HALF_DOWN)).orElse(null);
            editCell(row, SeedNeedReportColumnEnum.FILL_ALL.ordinal() + currentColumnOffset, ofNullable(temp).map(BigDecimal::doubleValue).orElse(null), styleWeight, CellType.NUMERIC);
            temp = ofNullable(seedsNeed.getCurrentFillSum()).map(d -> BigDecimal.valueOf(d).setScale(6, BigDecimal.ROUND_HALF_DOWN)).orElse(null);
            editCell(row, SeedNeedReportColumnEnum.CURRENT_FILL.ordinal() + currentColumnOffset, ofNullable(temp).map(BigDecimal::doubleValue).orElse(null), styleWeight, CellType.NUMERIC);
            editCell(row, SeedNeedReportColumnEnum.NEED_COUNT.ordinal() + currentColumnOffset, seedsNeed.getNeedCount(), styleWeight, CellType.NUMERIC);
            editCell(row, SeedNeedReportColumnEnum.NEED_COUNT_PERC.ordinal() + currentColumnOffset, row.getCell(SeedNeedReportColumnEnum.CURRENT_FILL.ordinal() + currentColumnOffset).getNumericCellValue() / row.getCell(SeedNeedReportColumnEnum.NEED_COUNT.ordinal() + currentColumnOffset).getNumericCellValue(), stylePerc, CellType.BLANK);

            if (list.size() - 1 != list.indexOf(seedsNeed)) {
                currentNumRow++;
                generateRow(sheet.createRow(currentNumRow));
            } else
                currentNumRow++;
        }
    }

    private void editCellForNeed(Sheet sheet, CalculationNeed seedsNeed, Boolean groupStyle) {
        if (seedsNeed != null) {
            Row row = sheet.getRow(currentNumRow);
            if (row == null) {
                generateRow(sheet.createRow(currentNumRow));
            }
            row = sheet.getRow(currentNumRow);
            CellStyle styleString = groupStyle ? this.groupingStyleString : this.styleString;
            CellStyle styleNum = groupStyle ? this.groupingStyleWeight : this.styleWeight;
            CellStyle stylePerc = groupStyle ? this.groupingStylePerc : this.stylePerc;

            editCell(row, SeedNeedReportColumnEnum.NUMBER.ordinal() + currentColumnOffset, String.valueOf(currentNumRow - 2), styleString);
            editCell(row, SeedNeedReportColumnEnum.TOWNSHIP.ordinal() + currentColumnOffset, ofNullable(seedsNeed.getTownship()).map(TerTownshipLite::getName).orElse(null), styleString);
            editCell(row, SeedNeedReportColumnEnum.CONTRACTOR.ordinal() + currentColumnOffset, ofNullable(seedsNeed.getContractor()).map(ContractorLite::getNameShort).orElse(null), styleString);
            editCell(row, SeedNeedReportColumnEnum.CULTURE.ordinal() + currentColumnOffset, ofNullable(seedsNeed.getCulture()).map(CultureLite::getName).orElse(null), styleString);
            BigDecimal temp = ofNullable(seedsNeed.getFillSum()).map(d -> BigDecimal.valueOf(d).setScale(6, BigDecimal.ROUND_HALF_DOWN)).orElse(null);
            editCell(row, SeedNeedReportColumnEnum.FILL_ALL.ordinal() + currentColumnOffset, ofNullable(temp).map(BigDecimal::doubleValue).orElse(null), styleNum, CellType.NUMERIC);
            temp = ofNullable(seedsNeed.getCurrentFillSum()).map(d -> BigDecimal.valueOf(d).setScale(6, BigDecimal.ROUND_HALF_DOWN)).orElse(null);
            editCell(row, SeedNeedReportColumnEnum.CURRENT_FILL.ordinal() + currentColumnOffset, ofNullable(temp).map(BigDecimal::doubleValue).orElse(null), styleNum, CellType.NUMERIC);
            editCell(row, SeedNeedReportColumnEnum.NEED_COUNT.ordinal() + currentColumnOffset, seedsNeed.getNeedCount(), styleNum, CellType.NUMERIC);
            editCell(row, SeedNeedReportColumnEnum.NEED_COUNT_PERC.ordinal() + currentColumnOffset, row.getCell(SeedNeedReportColumnEnum.CURRENT_FILL.ordinal() + currentColumnOffset).getNumericCellValue() / row.getCell(SeedNeedReportColumnEnum.NEED_COUNT.ordinal() + currentColumnOffset).getNumericCellValue(), stylePerc, CellType.BLANK);

            currentNumRow = currentNumRow + 1;
        }


    }

    private Boolean matchesLastByCon(SeedsBackFillView sb) {
        return ofNullable(sb.getContractorId()).
                map(con -> ofNullable(exportList.get(exportList.indexOf(sb) + 1)).
                        map(sbNext -> ofNullable(sbNext.getContractorId()).
                                map(con::equals).
                                orElse(true)).
                        orElse(true)).
                orElse(true);
    }

    private Boolean matchesLastByCul(SeedsBackFillView sb) {
        return ofNullable(sb.getCultureId()).
                map(cul -> ofNullable(exportList.get(exportList.indexOf(sb) + 1)).
                        map(sbNext -> ofNullable(sbNext.getCultureId()).
                                map(cul::equals).
                                orElse(true)).
                        orElse(true)).
                orElse(true);
    }

    private void actionsAfterGroupTowns(SeedsBackFillView sb, Sheet sheet, Integer i, GroupingTypeEnum finalGroupingAndDetal) {
        Optional<SeedsBackFillView> optional = ofNullable(groupingSeedsBackForTowns(this.exportList.stream().
                filter(sbgr -> ofNullable(sbgr.getTownshipId()).map(twn -> twn.equals(ofNullable(sb.getTownshipId()).orElse(null))).orElse(false)).
                collect(Collectors.toList())));
        if (optional.isPresent())
            addGroupingRow(sheet.createRow(i++), optional.get(), "", finalGroupingAndDetal);
    }

    private void generateSeedBackGroupAndAddGroupingRow(GroupingTypeEnum groupingAndDetal, SeedsBackFillView sb, Sheet sheet, Boolean last) {
        if (groupingAndDetal != null) {
            switch (groupingAndDetal) {
                case CONTRACTORS:
                    if (last || !matchesLastByCon(sb)) {
                        SeedsBackFillView seedGroup = groupingSeedsBackForTowns(this.exportList.stream().
                                filter(sbgr -> ofNullable(sb.getContractorId()).map(con -> con.equals(ofNullable(sbgr.getContractorId()).orElse(null))).orElse(false)).collect(Collectors.toList()));
                        addGroupingRow(sheet.createRow(currentNumRow++), seedGroup, sb.getContractorNameShort(), groupingAndDetal);
                    }
                    break;
                case TOWNSHIPS:
                    if (last || !matchesLastSbByTwn(sb)) {
                        SeedsBackFillView seedGroup = groupingSeedsBackForTowns(this.exportList.stream().
                                filter(sbgr -> ofNullable(sbgr.getTownshipId()).
                                        map(twn -> twn.equals(ofNullable(sb.getTownshipId()).orElse(null))).
                                        orElse(false)).
                                collect(Collectors.toList()));
                        addGroupingRow(sheet.createRow(currentNumRow++), seedGroup, sb.getContractorNameShort(), groupingAndDetal);
                    }
                    break;
                case CULTURE:
                    if (last || !matchesLastByCul(sb)) {
                        Optional<SeedsBackFillView> optSb = ofNullable(groupingSeedsBackForCulture(this.exportList.stream().
                                filter(sbgr -> ofNullable(sbgr.getCultureId()).
                                        map(cul -> cul.equals(ofNullable(sb.getCultureId()).orElse(null))).orElse(false)).
                                collect(Collectors.toList())));
                        optSb.ifPresent(SeedsBackFillView -> addGroupingRow(sheet.createRow(currentNumRow++), SeedsBackFillView, null, groupingAndDetal));
                    }
                    break;
                case CONTRACTORS_AND_TOWNSHIPS:
                    if (last || !matchesLastByCon(sb)) {
                        Optional<SeedsBackFillView> sbOpt = ofNullable(groupingSeedsBackForContractorTowns(this.exportList.stream().
                                filter(sbgr -> ofNullable(sb.getContractorId()).
                                        map(con -> con.equals(ofNullable(sbgr.getContractorId()).
                                                orElse(null))).
                                        orElse(false)).
                                collect(Collectors.toList())));
                        sbOpt.ifPresent(SeedsBackFillView -> addGroupingRow(sheet.createRow(currentNumRow++), SeedsBackFillView, sb.getContractorNameShort(), groupingAndDetal));

                    }
                    if (last || !matchesLastSbByTwn(sb)) {
                        actionsAfterGroupTowns(sb, sheet, currentNumRow, groupingAndDetal);
                    }
                    break;
                case CULTURE_AND_TOWNSHIPS:
                    if (last || !matchesLastByCul(sb)) {
                        Optional<SeedsBackFillView> optional = ofNullable(groupingSeedsBackForCultureTowns(this.exportList.stream().
                                filter(sbgr -> ofNullable(sbgr.getTownshipId()).map(twn -> twn.equals(ofNullable(sb.getTownshipId()).orElse(null))).orElse(false)
                                        && ofNullable(sbgr.getCultureId()).
                                        map(cul -> cul.equals(ofNullable(sb.getCultureId()).orElse(null))).orElse(false)).
                                collect(Collectors.toList())));
                        optional.ifPresent(SeedsBackFillView -> addGroupingRow(sheet.createRow(currentNumRow++), SeedsBackFillView, sb.getContractorNameShort(), groupingAndDetal));

                    }
                    if (last || !matchesLastSbByTwn(sb)) {
                        actionsAfterGroupTowns(sb, sheet, currentNumRow, groupingAndDetal);
                    }
                    break;
            }
        }
    }

    private void addGroupingRow(Row row, SeedsBackFillView seedGroup, String contractorNameShort, GroupingTypeEnum groupingTypeEnum) {
        currentColumnOffset = 1;
        editCell(row, TOWNSHIP.ordinal() + currentColumnOffset, groupingTypeEnum.equals(GroupingTypeEnum.CULTURE) ? "" : ofNullable(seedGroup.getTownshipName()).orElse(""), groupingStyleString);
        editCell(row, CONTRACTOR.ordinal() + currentColumnOffset, (groupingTypeEnum.equals(GroupingTypeEnum.CULTURE) || groupingTypeEnum.equals(GroupingTypeEnum.TOWNSHIPS) || groupingTypeEnum.equals(GroupingTypeEnum.CULTURE_AND_TOWNSHIPS)) ? "" : ofNullable(contractorNameShort).orElse(""), groupingStyleString);
        editCell(row, CULTURE.ordinal() + currentColumnOffset, groupingTypeEnum.equals(GroupingTypeEnum.CULTURE) || groupingTypeEnum.equals(GroupingTypeEnum.CULTURE_AND_TOWNSHIPS) ? ofNullable(seedGroup.getCultureName()).orElse(null) : null, groupingStyleString);
        nullCell(row, CULTURE_SORT.ordinal() + currentColumnOffset, groupingStyleString);
        nullCell(row, CATEGORY.ordinal() + currentColumnOffset, styleString);
        nullCell(row, REPRODUCTION.ordinal() + currentColumnOffset, styleString);
        editCell(row, FILL_ALL.ordinal() + currentColumnOffset, seedGroup.getFillAll(), groupingStyleWeight, CellType.NUMERIC);
        editCell(row, CURRENT_FILL.ordinal() + currentColumnOffset, seedGroup.getCurrentFill(), groupingStyleWeight, CellType.NUMERIC);
        editCell(row, SOWN_SUM.ordinal() + currentColumnOffset, seedGroup.getSownSum(), groupingStyleWeight, CellType.NUMERIC);
        editCell(row, SOLD_SUM.ordinal() + currentColumnOffset, seedGroup.getSoldSum(), groupingStyleWeight, CellType.NUMERIC);
        editCell(row, DEFECTED_SUM.ordinal() + currentColumnOffset, seedGroup.getDefectedSum(), groupingStyleWeight, CellType.NUMERIC);
        editCell(row, FED_SUM.ordinal() + currentColumnOffset, seedGroup.getFedSum(), groupingStyleWeight, CellType.NUMERIC);

        nullCell(row, SOURCE_CUSTOMER.ordinal() + currentColumnOffset, groupingStyleString);
        nullCell(row, PURPOSE.ordinal() + currentColumnOffset, groupingStyleString);
        nullCell(row, BATCH_NUMBER.ordinal() + currentColumnOffset, groupingStyleString);
        nullCell(row, FUND_TYPE.ordinal() + currentColumnOffset, groupingStyleString);
        nullCell(row, CROP_YEAR.ordinal() + currentColumnOffset, groupingStyleNum);
        nullCell(row, SORT_DOC_NUMBER.ordinal() + currentColumnOffset, groupingStyleString);
        nullCell(row, SORT_DOC_DATE.ordinal() + currentColumnOffset, groupingStyleString);
        nullCell(row, QUAL_DOC_NUMBER.ordinal() + currentColumnOffset, groupingStyleString);
        nullCell(row, QUAL_DOC_DATE_EXPIRED.ordinal() + currentColumnOffset, groupingStyleString);

        editCell(row, QUAL_CHECKED_PERC.ordinal() + currentColumnOffset, getProc(seedGroup.getQualChecked(), seedGroup.getCurrentFill()), groupingStylePerc, CellType.BLANK);
        editCell(row, QUAL_CONDITIONED.ordinal() + currentColumnOffset, seedGroup.getQualConditioned(), groupingStyleWeight, CellType.NUMERIC);
        Double sum_perc_cond = getProc(seedGroup.getQualConditioned(), seedGroup.getQualChecked());
        editCell(row, QUAL_CHECKED_PERC.ordinal() + currentColumnOffset, sum_perc_cond, groupingStylePerc, CellType.BLANK);
        editCell(row, QUAL_NOT_CONDITIONED_ALL.ordinal() + currentColumnOffset, seedGroup.getQualNotConditionedAll(), groupingStyleWeight, CellType.NUMERIC);
        Double sum_perc_nocond_check = getProc(seedGroup.getQualNotConditionedAll(), seedGroup.getQualChecked());
        editCell(row, QUAL_NOT_CONDITIONED_ALL_PERC.ordinal() + currentColumnOffset, sum_perc_nocond_check, groupingStylePerc, CellType.BLANK);
        editCell(row, QUAL_NOT_CONDITIONED_DEBRIS.ordinal() + currentColumnOffset, seedGroup.getQualNotConditionedDebris(), groupingStyleWeight, CellType.NUMERIC);
        editCell(row, QUAL_NOT_CONDITIONED_HUMIDITY.ordinal() + currentColumnOffset, seedGroup.getQualNotConditionedHumidity(), groupingStyleWeight, CellType.NUMERIC);
        editCell(row, QUAL_NOT_CONDITIONED_GERMINATION.ordinal() + currentColumnOffset, seedGroup.getQualNotConditionedGermination(), groupingStyleWeight, CellType.NUMERIC);
        editCell(row, QUAL_NOT_CONDITIONED_PESTS.ordinal() + currentColumnOffset, seedGroup.getQualNotConditionedPests(), groupingStyleWeight, CellType.NUMERIC);

        fillReproductionColumnsGrouped(row, seedGroup.getReproductionTypeViewInfoList(), seedGroup.getQualChecked());
        currentColumnOffset += reproductionCategoryColumnsFullSize() - 1;

        nullCell(row, WEIGHTED_AVERAGE_1000_SEEDS.ordinal() + currentColumnOffset, groupingStyleString);
        nullCell(row, GERMINATION.ordinal() + currentColumnOffset, groupingStyleString);
    }

    private SeedsBackFillView groupingSeedsBack(List<SeedsBackFillView> list) {
        SeedsBackFillView seedsBackFill = new SeedsBackFillView();
        if (CollectionUtils.isNotNullOrNotEmpty(list)) {
            SeedsBackFillView from = list.get(0);
            copySeedsBackfillViewContractor(from, seedsBackFill);
            copySeedsBackfillViewTownship(from, seedsBackFill);
        }
        return helperMethodForGrouping(seedsBackFill, list);
    }

    private SeedsBackFillView groupingSeedsBackForCulture(List<SeedsBackFillView> list) {
        SeedsBackFillView seedsBackFill = new SeedsBackFillView();
        if (CollectionUtils.isNotNullOrNotEmpty(list)) {
            SeedsBackFillView from = list.get(0);
            copySeedsBackfillViewCultureSort(from, seedsBackFill);
            copySeedsBackfillViewCulture(from, seedsBackFill);
        }
        return helperMethodForGrouping(seedsBackFill, ofNullable(list).orElse(new ArrayList<>()));
    }

    private SeedsBackFillView groupingSeedsBackForCultureTowns(List<SeedsBackFillView> list) {
        SeedsBackFillView seedsBackFill = new SeedsBackFillView();
        if (CollectionUtils.isNotNullOrNotEmpty(list)) {
            SeedsBackFillView from = list.get(0);
            copySeedsBackfillViewTownship(from, seedsBackFill);
            copySeedsBackfillViewCultureSort(from, seedsBackFill);
            copySeedsBackfillViewCulture(from, seedsBackFill);
        }
        return helperMethodForGrouping(seedsBackFill, list);
    }

    private SeedsBackFillView groupingSeedsBackForContractorTowns(List<SeedsBackFillView> list) {
        SeedsBackFillView seedsBackFill = new SeedsBackFillView();
        if (CollectionUtils.isNotNullOrNotEmpty(list)) {
            SeedsBackFillView from = list.get(0);
            copySeedsBackfillViewTownship(from, seedsBackFill);
            copySeedsBackfillViewContractor(from, seedsBackFill);
        }
        return helperMethodForGrouping(seedsBackFill, list);
    }

    private SeedsBackFillView groupingSeedsBackForTowns(List<SeedsBackFillView> list) {
        SeedsBackFillView seedsBackFill = new SeedsBackFillView();
        if (CollectionUtils.isNotNullOrNotEmpty(list)) {
            copySeedsBackfillViewTownship(list.get(0), seedsBackFill);
        }
        return helperMethodForGrouping(seedsBackFill, of(list).orElse(new ArrayList<>()));
    }

    private void copySeedsBackfillViewContractor(@NonNull SeedsBackFillView from, @NonNull SeedsBackFillView to) {
        to.setContractorId(from.getContractorId());
        to.setContractorNameShort(from.getContractorNameShort());
        to.setContractorNameFull(from.getContractorNameFull());
        to.setContractorManagerNameFull(from.getContractorManagerNameFull());
        to.setContractorManagerNameShort(from.getContractorManagerNameShort());
    }

    private void copySeedsBackfillViewTownship(@NonNull SeedsBackFillView from, @NonNull SeedsBackFillView to) {
        to.setTownshipId(from.getTownshipId());
        to.setTownshipName(from.getTownshipName());
    }

    private void copySeedsBackfillViewCultureSort(@NonNull SeedsBackFillView from, @NonNull SeedsBackFillView to) {
        to.setCultureSortCode(from.getCultureSortCode());
        to.setCultureSortName(from.getCultureSortName());
        to.setCultureSortAllowId(from.getCultureSortAllowId());
    }

    private void copySeedsBackfillViewCulture(@NonNull SeedsBackFillView from, @NonNull SeedsBackFillView to) {
        to.setCultureId(from.getCultureId());
        to.setCultureName(from.getCultureName());
        to.setCultureGroupId(from.getCultureGroupId());
        to.setCultureGroupSeasonId(from.getCultureGroupSeasonId());
    }

    private SeedsBackFillView helperMethodForGrouping(SeedsBackFillView seedsBackFill, List<SeedsBackFillView> list) {

        seedsBackFill.setFillAll(list.stream().mapToDouble(sb -> ofNullable(sb.getFillAll()).orElse(0d)).sum());
        seedsBackFill.setCurrentFill(list.stream().mapToDouble(sb -> ofNullable(sb.getCurrentFill()).orElse(0d)).sum());
        seedsBackFill.setQualChecked(list.stream().mapToDouble(sb -> isNullReturnZero(sb.getQualChecked())).sum());
        seedsBackFill.setQualConditioned(list.stream().mapToDouble(sb -> isNullReturnZero(sb.getQualConditioned())).sum());
        seedsBackFill.setQualNotConditionedAll(list.stream().mapToDouble(sb -> isNullReturnZero(sb.getQualNotConditionedAll())).sum());
        seedsBackFill.setQualNotConditionedDebris(list.stream().mapToDouble(sb -> isNullReturnZero(sb.getQualNotConditionedDebris())).sum());
        seedsBackFill.setQualNotConditionedHumidity(list.stream().mapToDouble(sb -> isNullReturnZero(sb.getQualNotConditionedHumidity())).sum());
        seedsBackFill.setQualNotConditionedGermination(list.stream().mapToDouble(sb -> isNullReturnZero(sb.getQualNotConditionedGermination())).sum());
        seedsBackFill.setQualNotConditionedPests(list.stream().mapToDouble(sb -> isNullReturnZero(sb.getQualNotConditionedPests())).sum());
        seedsBackFill.setSownSum(list.stream().mapToDouble(sb -> ofNullable(sb.getSownSum()).orElse(0d)).sum());
        seedsBackFill.setSoldSum(list.stream().mapToDouble(sb -> ofNullable(sb.getSoldSum()).orElse(0d)).sum());
        seedsBackFill.setDefectedSum(list.stream().mapToDouble(sb -> ofNullable(sb.getDefectedSum()).orElse(0d)).sum());
        seedsBackFill.setFedSum(list.stream().mapToDouble(sb -> ofNullable(sb.getFedSum()).orElse(0d)).sum());

        seedsBackFill.setReproductionTypeViewInfoList(groupSbfReproductions(list));

        return Optional.of(seedsBackFill).
                filter(numberPredicate).
                orElse(null);
    }

    public List<ReproductionTypeViewInfo> groupSbfReproductions(List<SeedsBackFillView> list) {
        return helperMethodForReproductionsGrouping(list.stream()
                .map(SeedsBackFillView::getReproductionTypeViewInfoList)
                .flatMap(Collection::stream)
                .collect(Collectors.toList()));
    }

    public List<ReproductionTypeViewInfo> helperMethodForReproductionsGrouping(List<ReproductionTypeViewInfo> originList) {
        List<ReproductionTypeViewInfo> reproductions = new ArrayList<>();
        if (CollectionUtils.isNullOrEmpty(originList)) return reproductions;
        originList.forEach(reproductionTypeViewInfo -> {
            boolean contains = false;
            Iterator<ReproductionTypeViewInfo> iter = reproductions.listIterator();
            ReproductionTypeViewInfo curReproduction = null;
            while (iter.hasNext() && !contains) {
                curReproduction = iter.next();
                contains = Objects.equals(curReproduction.getNameView(), reproductionTypeViewInfo.getNameView())
                        && Objects.equals(curReproduction.getCategoryNameView(), reproductionTypeViewInfo.getCategoryNameView());
            }

            if (Objects.nonNull(curReproduction) && contains) {
                curReproduction.setValue(curReproduction.getValue() + NumberUtils.isNullReturnZero(reproductionTypeViewInfo.getValue()));
            } else if (StringUtils.isNotNullOrNotTrimEmpty(reproductionTypeViewInfo.getNameView()) || StringUtils.isNotNullOrNotTrimEmpty(reproductionTypeViewInfo.getCategoryNameView())) {
                ReproductionTypeViewInfo newReproduction = CloneUtil.cloneReproductionTypeViewInfo(reproductionTypeViewInfo);
                newReproduction.setValue(NumberUtils.isNullReturnZero(reproductionTypeViewInfo.getValue()));
                reproductions.add(newReproduction);
            }
        });
        return reproductions;
    }

    private CalculationNeed helperMethodForGroupingNeed(Collection<CalculationNeed> list, ContractorLite contractor, CultureLite culture, TerTownshipLite terTownship, GroupingTypeEnum groupingTypeEnum) {
        switch (groupingTypeEnum) {
            case CONTRACTORS:
                return Optional.of(CalculationNeed.builder().
                                contractor(contractor).
                                township(contractor.getTerrainTownship()).
                                needCount(list.stream().mapToDouble(cl -> cl.getNeedCount() == null ? 0.0 : cl.getNeedCount()).sum()).
                                fillSum(list.stream().mapToDouble(cl -> cl.getFillSum() == null ? 0.0 : cl.getFillSum()).sum()).
                                currentFillSum(list.stream().mapToDouble(cl -> cl.getCurrentFillSum() == null ? 0.0 : cl.getCurrentFillSum()).sum()).
                                build()).
                        filter(seedNeedPredicate).
                        orElse(null);

            case TOWNSHIPS:
                return Optional.of(CalculationNeed.builder().
                                township(terTownship).
                                needCount(list.stream().mapToDouble(cl -> cl.getNeedCount() == null ? 0.0 : cl.getNeedCount()).sum()).
                                fillSum(list.stream().mapToDouble(cl -> cl.getFillSum() == null ? 0.0 : cl.getFillSum()).sum()).
                                currentFillSum(list.stream().mapToDouble(cl -> cl.getCurrentFillSum() == null ? 0.0 : cl.getCurrentFillSum()).sum()).
                                build()).
                        filter(seedNeedPredicate).
                        orElse(null);

            case CULTURE:
                return Optional.of(CalculationNeed.builder().
                                culture(culture).
                                needCount(list.stream().mapToDouble(cl -> cl.getNeedCount() == null ? 0.0 : cl.getNeedCount()).sum()).
                                fillSum(list.stream().mapToDouble(cl -> cl.getFillSum() == null ? 0.0 : cl.getFillSum()).sum()).
                                currentFillSum(list.stream().mapToDouble(cl -> cl.getCurrentFillSum() == null ? 0.0 : cl.getCurrentFillSum()).sum()).
                                build()).
                        filter(seedNeedPredicate).
                        orElse(null);

            case CONTRACTORS_AND_TOWNSHIPS:
                if (terTownship == null)
                    return Optional.of(CalculationNeed.builder().
                                    contractor(contractor).
                                    township(contractor.getTerrainTownship()).
                                    needCount(list.stream().mapToDouble(cl -> cl.getNeedCount() == null ? 0.0 : cl.getNeedCount()).sum()).
                                    fillSum(list.stream().mapToDouble(cl -> cl.getFillSum() == null ? 0.0 : cl.getFillSum()).sum()).
                                    currentFillSum(list.stream().mapToDouble(cl -> cl.getCurrentFillSum() == null ? 0.0 : cl.getCurrentFillSum()).sum()).
                                    build()).
                            orElse(null);
                else
                    return Optional.of(CalculationNeed.builder().
                                    contractor(contractor).
                                    township(terTownship).
                                    needCount(list.stream().mapToDouble(cl -> cl.getNeedCount() == null ? 0.0 : cl.getNeedCount()).sum()).
                                    fillSum(list.stream().mapToDouble(cl -> cl.getFillSum() == null ? 0.0 : cl.getFillSum()).sum()).
                                    currentFillSum(list.stream().mapToDouble(cl -> cl.getCurrentFillSum() == null ? 0.0 : cl.getCurrentFillSum()).sum()).
                                    build()).
                            filter(seedNeedPredicate).
                            orElse(null);

            case CULTURE_AND_TOWNSHIPS:
                if (culture != null)
                    return Optional.of(CalculationNeed.builder().
                                    culture(culture).
                                    township(terTownship).
                                    needCount(list.stream().mapToDouble(cl -> cl.getNeedCount() == null ? 0.0 : cl.getNeedCount()).sum()).
                                    fillSum(list.stream().mapToDouble(cl -> cl.getFillSum() == null ? 0.0 : cl.getFillSum()).sum()).
                                    currentFillSum(list.stream().mapToDouble(cl -> cl.getCurrentFillSum() == null ? 0.0 : cl.getCurrentFillSum()).sum()).
                                    build()).
                            orElse(null);
                else
                    return Optional.of(CalculationNeed.builder().
                                    township(terTownship).
                                    needCount(list.stream().mapToDouble(cl -> cl.getNeedCount() == null ? 0.0 : cl.getNeedCount()).sum()).
                                    fillSum(list.stream().mapToDouble(cl -> cl.getFillSum() == null ? 0.0 : cl.getFillSum()).sum()).
                                    currentFillSum(list.stream().mapToDouble(cl -> cl.getCurrentFillSum() == null ? 0.0 : cl.getCurrentFillSum()).sum()).
                                    build()).
                            filter(seedNeedPredicate).
                            orElse(null);
        }
        return null;
    }


    private void generateRow(Row result) {
        result.setHeight(HEIGHT_ROW);
        result.setHeightInPoints(HEIGHT_IN_POINT_ROW);
        result.setZeroHeight(false);
    }

    private void initializeGroupingFont() {
        this.dataFormat = workbook.createDataFormat();
        this.groupingStyleNum = workbook.createCellStyle();
        this.groupingStyleWeight = workbook.createCellStyle();
        this.groupingStylePerc = workbook.createCellStyle();
        this.groupingStyleString = workbook.createCellStyle();
        this.styleNum1 = workbook.createCellStyle();
        this.styleNum2 = workbook.createCellStyle();
        this.styleWeight = workbook.createCellStyle();
        this.stylePerc = workbook.createCellStyle();
        this.styleString = workbook.createCellStyle();

        this.styleDateShort = workbook.createCellStyle();

        CreationHelper createHelper = workbook.getCreationHelper();
        this.styleDateShort.setDataFormat(createHelper.createDataFormat().getFormat("dd.MM.yyyy"));

//        for (int f = 0; f < 100; f ++) {
//            LOGGER.info(f + " - "  + createHelper.createDataFormat().getFormat((short) f));
//        }

        this.styleNum1.setDataFormat(dataFormat.getFormat(ColumnExcelEnum.ApacheNumberFormatEnum.ONE.getName()));
        this.styleNum2.setDataFormat(dataFormat.getFormat(ColumnExcelEnum.ApacheNumberFormatEnum.TWO.getName()));
        this.styleWeight.setDataFormat(dataFormat.getFormat(ColumnExcelEnum.ApacheNumberFormatEnum.WEIGHT.getName()));
        this.stylePerc.setDataFormat(dataFormat.getFormat(ColumnExcelEnum.ApacheNumberFormatEnum.PERC.getName()));
        this.groupingStyleNum.setDataFormat(dataFormat.getFormat(ColumnExcelEnum.ApacheNumberFormatEnum.ONE.getName()));
        this.groupingStyleWeight.setDataFormat(dataFormat.getFormat(ColumnExcelEnum.ApacheNumberFormatEnum.WEIGHT.getName()));
        this.groupingStylePerc.setDataFormat(dataFormat.getFormat(ColumnExcelEnum.ApacheNumberFormatEnum.PERC.getName()));
        this.groupingFont = workbook.createFont();
        this.font = workbook.createFont();
        this.groupingFont.setFontName(HSSFFont.FONT_ARIAL);
        this.groupingFont.setFontHeightInPoints((short) 7);
        this.groupingFont.setBold(true);
        this.font.setFontName(HSSFFont.FONT_ARIAL);
        this.font.setFontHeightInPoints((short) 7);
        helperMethodForInitializeStyles(groupingFont, groupingStyleNum, groupingStyleWeight, groupingStylePerc, groupingStyleString);
        helperMethodForInitializeStyles(font, styleNum1, styleNum2, styleWeight, stylePerc, styleString, styleDateShort);
    }


    private void helperMethodForInitializeStyles(Font font, CellStyle... styles) {
        for (CellStyle style : styles) {
            style.setFont(font);
            style.setBorderTop(BorderStyle.THIN);
            style.setBorderLeft(BorderStyle.THIN);
            style.setBorderRight(BorderStyle.THIN);
            style.setBorderBottom(BorderStyle.THIN);
        }
    }

    private void addLastRow(Sheet sheet, Integer row_num) {
        currentColumnOffset = 1;
        List<SeedsBackFillView> total;
        if (filterMap.get(ReportKeysForFilterEnum.GROUPING) == null && !Boolean.TRUE.equals(filterMap.get(ReportKeysForFilterEnum.DETAL_BY_PARTIAL)))
            total = getAllSeedsView().stream().filter(mainParamPredicate.and(numberPredicate)).collect(Collectors.toList());
        else
            total = exportList;
        Row row = sheet.createRow(row_num);
        editCell(row, NUMBER.ordinal() + currentColumnOffset, "ИТОГО:", groupingStyleString);
        editCell(row, TOWNSHIP.ordinal() + currentColumnOffset, "", groupingStyleString);
        editCell(row, CONTRACTOR.ordinal() + currentColumnOffset, "", groupingStyleString);
        editCell(row, CULTURE.ordinal() + currentColumnOffset, "", groupingStyleString);
        editCell(row, CULTURE_SORT.ordinal() + currentColumnOffset, "", groupingStyleString);
        editCell(row, CATEGORY.ordinal() + currentColumnOffset, "", styleString);
        editCell(row, REPRODUCTION.ordinal() + currentColumnOffset, "", styleString);

        sheet.addMergedRegion(new CellRangeAddress(row_num, row_num, (short) NUMBER.ordinal() + currentColumnOffset, (short) REPRODUCTION.ordinal() + currentColumnOffset));
        double sum_sem_fall_all = total.stream().mapToDouble(sb -> ofNullable(sb.getFillAll()).orElse(0d)).sum();
        editCell(row, FILL_ALL.ordinal() + currentColumnOffset, sum_sem_fall_all, groupingStyleWeight, CellType.NUMERIC);
        double sum_sem_current_fill = total.stream().mapToDouble(sb -> ofNullable(sb.getCurrentFill()).orElse(0d)).sum();
        editCell(row, CURRENT_FILL.ordinal() + currentColumnOffset, sum_sem_current_fill, groupingStyleWeight, CellType.NUMERIC);

        double sum_sem_sown = total.stream().mapToDouble(sb -> ofNullable(sb.getSownSum()).orElse(0d)).sum();
        editCell(row, SOWN_SUM.ordinal() + currentColumnOffset, sum_sem_sown, groupingStyleWeight, CellType.NUMERIC);

        double sum_sem_sold = total.stream().mapToDouble(sb -> ofNullable(sb.getSoldSum()).orElse(0d)).sum();
        editCell(row, SOLD_SUM.ordinal() + currentColumnOffset, sum_sem_sold, groupingStyleWeight, CellType.NUMERIC);

        double sum_sem_defected = total.stream().mapToDouble(sb -> ofNullable(sb.getDefectedSum()).orElse(0d)).sum();
        editCell(row, DEFECTED_SUM.ordinal() + currentColumnOffset, sum_sem_defected, groupingStyleWeight, CellType.NUMERIC);

        double sum_sem_fed = total.stream().mapToDouble(sb -> ofNullable(sb.getFedSum()).orElse(0d)).sum();
        editCell(row, FED_SUM.ordinal() + currentColumnOffset, sum_sem_fed, groupingStyleWeight, CellType.NUMERIC);
//      editCell(row_num, , sum_perc_want_perc, sheet, false); // %%%%%
        editCell(row, SOURCE_CUSTOMER.ordinal() + currentColumnOffset, "-", groupingStyleString);
        editCell(row, PURPOSE.ordinal() + currentColumnOffset, "-", groupingStyleString);
        editCell(row, BATCH_NUMBER.ordinal() + currentColumnOffset, "-", groupingStyleString);
        editCell(row, FUND_TYPE.ordinal() + currentColumnOffset, "-", groupingStyleString);
        editCell(row, CROP_YEAR.ordinal() + currentColumnOffset, "-", groupingStyleString);
        editCell(row, SORT_DOC_NUMBER.ordinal() + currentColumnOffset, "-", groupingStyleString);
        editCell(row, SORT_DOC_DATE.ordinal() + currentColumnOffset, "-", groupingStyleString);
        editCell(row, QUAL_DOC_NUMBER.ordinal() + currentColumnOffset, "-", groupingStyleString);
        editCell(row, QUAL_DOC_DATE_EXPIRED.ordinal() + currentColumnOffset, "-", groupingStyleString);

        double sum_quality_sem_check = total.stream().mapToDouble(sb -> ofNullable(sb.getQualChecked()).orElse(0d)).sum();
        editCell(row, QUAL_CHECKED.ordinal() + currentColumnOffset, sum_quality_sem_check, groupingStyleWeight, CellType.NUMERIC);

        Double sum_perc_quality = getProc(sum_quality_sem_check, total.stream().mapToDouble(sb -> ofNullable(sb.getCurrentFill()).orElse(0d)).sum());
        editCell(row, QUAL_CHECKED_PERC.ordinal() + currentColumnOffset, sum_perc_quality, groupingStylePerc, CellType.BLANK);

        double sum_quality_sem_conditioned = total.stream().mapToDouble(sb -> ofNullable(sb.getQualConditioned()).orElse(0d)).sum();
        editCell(row, QUAL_CONDITIONED.ordinal() + currentColumnOffset, sum_quality_sem_conditioned, groupingStyleWeight, CellType.NUMERIC);

        Double sum_perc_cond = getProc(sum_quality_sem_conditioned, sum_quality_sem_check);
        editCell(row, QUAL_CONDITIONED_PERC.ordinal() + currentColumnOffset, sum_perc_cond, groupingStylePerc, CellType.BLANK);

        double sum_quality_sem_noconditioned_all = total.stream().mapToDouble(sb -> ofNullable(sb.getQualNotConditionedAll()).orElse(0d)).sum();
        editCell(row, QUAL_NOT_CONDITIONED_ALL.ordinal() + currentColumnOffset, sum_quality_sem_noconditioned_all, groupingStyleWeight, CellType.NUMERIC);

        Double sum_perc_nocond_check = getProc(sum_quality_sem_noconditioned_all, sum_quality_sem_check);
        editCell(row, QUAL_NOT_CONDITIONED_ALL_PERC.ordinal() + currentColumnOffset, sum_perc_nocond_check, groupingStylePerc, CellType.BLANK);

        double sum_quality_sem_noconditioned_inf = total.stream().mapToDouble(sb -> ofNullable(sb.getQualNotConditionedDebris()).orElse(0d)).sum();
        editCell(row, QUAL_NOT_CONDITIONED_DEBRIS.ordinal() + currentColumnOffset, sum_quality_sem_noconditioned_inf, groupingStyleWeight, CellType.NUMERIC);

        double sum_quality_sem_noconditioned_hum = total.stream().mapToDouble(sb -> ofNullable(sb.getQualNotConditionedHumidity()).orElse(0d)).sum();
        editCell(row, QUAL_NOT_CONDITIONED_HUMIDITY.ordinal() + currentColumnOffset, sum_quality_sem_noconditioned_hum, groupingStyleWeight, CellType.NUMERIC);

        double sum_quality_sem_noconditioned_cap = total.stream().mapToDouble(sb -> ofNullable(sb.getQualNotConditionedGermination()).orElse(0d)).sum();
        editCell(row, QUAL_NOT_CONDITIONED_GERMINATION.ordinal() + currentColumnOffset, sum_quality_sem_noconditioned_cap, groupingStyleWeight, CellType.NUMERIC);

        double sum_quality_sem_noconditioned_sab = total.stream().mapToDouble(sb -> ofNullable(sb.getQualNotConditionedPests()).orElse(0d)).sum();
        editCell(row, QUAL_NOT_CONDITIONED_PESTS.ordinal() + currentColumnOffset, sum_quality_sem_noconditioned_sab, groupingStyleWeight, CellType.NUMERIC);

        fillReproductionColumnsGrouped(row, groupSbfReproductions(total), sum_quality_sem_check);
        currentColumnOffset += reproductionCategoryColumnsFullSize() - 1;

        editCell(row, WEIGHTED_AVERAGE_1000_SEEDS.ordinal() + currentColumnOffset, "-", groupingStyleString);
        editCell(row, GERMINATION.ordinal() + currentColumnOffset, "-", groupingStyleString);
    }

    private void addLastRowForNeed(Sheet sheet, Integer row_num, List<CalculationNeed> seedsNeeds, Boolean onlyGrouping) {
        Row row = sheet.createRow(row_num);
        double sum_sem_fall_all = 0;
        double sum_sem_current_fill = 0;
        double sum_need = 0;
        double avg_need = 0;
        if (onlyGrouping) {
            editCell(row, SeedNeedReportColumnEnum.NUMBER.ordinal() + currentColumnOffset, "ИТОГО:", groupingStyleString);
            sheet.addMergedRegion(new CellRangeAddress(row_num, row_num, (short) SeedNeedReportColumnEnum.NUMBER.ordinal() + currentColumnOffset, (short) (SeedNeedReportColumnEnum.CULTURE.ordinal() + currentColumnOffset)));
            if (row_num != 3)
                for (Row cells : sheet) {
                    if (cells.getRowNum() > 2)
                        for (Cell cell : cells) {
                            if (SeedNeedReportColumnEnum.FILL_ALL.ordinal() + currentColumnOffset == cell.getColumnIndex()) {
                                sum_sem_fall_all += cell.getNumericCellValue();
                            }

                            if (SeedNeedReportColumnEnum.CURRENT_FILL.ordinal() + currentColumnOffset == cell.getColumnIndex()) {
                                sum_sem_current_fill += cell.getNumericCellValue();
                            }

                            if (SeedNeedReportColumnEnum.NEED_COUNT.ordinal() + currentColumnOffset == cell.getColumnIndex()) {
                                sum_need += cell.getNumericCellValue();
                            }
                        }
                }

            editCell(row, SeedNeedReportColumnEnum.FILL_ALL.ordinal() + currentColumnOffset, sum_sem_fall_all, groupingStyleWeight, CellType.NUMERIC);
            editCell(row, SeedNeedReportColumnEnum.CURRENT_FILL.ordinal() + currentColumnOffset, sum_sem_current_fill, groupingStyleWeight, CellType.NUMERIC);
            editCell(row, SeedNeedReportColumnEnum.NEED_COUNT.ordinal() + currentColumnOffset, sum_need, groupingStyleWeight, CellType.NUMERIC);
            editCell(row, SeedNeedReportColumnEnum.NEED_COUNT_PERC.ordinal() + currentColumnOffset, sum_need == 0 ? 0 : getProc(sum_sem_current_fill, sum_need), groupingStylePerc, CellType.BLANK);
        } else {
            editCell(row, SeedNeedReportColumnEnum.NUMBER.ordinal() + currentColumnOffset, "ИТОГО:", groupingStyleString);
            sheet.addMergedRegion(new CellRangeAddress(row_num, row_num, (short) SeedNeedReportColumnEnum.NUMBER.ordinal() + currentColumnOffset, (short) (SeedNeedReportColumnEnum.CULTURE.ordinal() + currentColumnOffset)));

            sum_sem_fall_all = seedsNeeds.stream().mapToDouble(sn -> sn.getFillSum() == null ? 0 : sn.getFillSum()).sum();
            editCell(row, SeedNeedReportColumnEnum.FILL_ALL.ordinal() + currentColumnOffset, sum_sem_fall_all, groupingStyleWeight, CellType.NUMERIC);

            sum_sem_current_fill = seedsNeeds.stream().mapToDouble(sn -> sn.getCurrentFillSum() == null ? 0 : sn.getCurrentFillSum()).sum();
            editCell(row, SeedNeedReportColumnEnum.CURRENT_FILL.ordinal() + currentColumnOffset, sum_sem_current_fill, groupingStyleWeight, CellType.NUMERIC);

            sum_need = seedsNeeds.stream().mapToDouble(sn -> sn.getNeedCount() == null ? 0 : sn.getNeedCount()).sum();
            editCell(row, SeedNeedReportColumnEnum.NEED_COUNT.ordinal() + currentColumnOffset, sum_need, groupingStyleWeight, CellType.NUMERIC);

            editCell(row, SeedNeedReportColumnEnum.NEED_COUNT_PERC.ordinal() + currentColumnOffset, sum_need == 0 ? 0 : getProc(sum_sem_fall_all, sum_need), groupingStylePerc, CellType.BLANK);
        }


    }

    protected void addColumns(Sheet sheetDoc, int position, int count) {
        addColumns(sheetDoc, position, count, -1);
    }

    protected void addColumns(Sheet sheetDoc, int position, int count, int copyColumnStyleIndex) {
        if (Objects.isNull(sheetDoc) || count == 0) return;
        Iterator<Row> rowIterator = sheetDoc.rowIterator();
        while (rowIterator.hasNext()) {
            Row curRow = rowIterator.next();
            if (curRow.getLastCellNum() != -1) {
                CellStyle cellStyle = curRow.getCell(copyColumnStyleIndex).getCellStyle();
                boolean needToMove = curRow.getLastCellNum() > position;
                for (int i = 0; i < count; ++i) {
                    short currentColumnIndex = curRow.getLastCellNum();
                    Cell newCell = curRow.createCell(currentColumnIndex);

                    if (needToMove && currentColumnIndex - count >= position) {
                        moveCell(curRow.getCell(currentColumnIndex - count), newCell);
                        curRow.getCell(currentColumnIndex - count).setCellStyle(cellStyle);
                    } else if (copyColumnStyleIndex >= 0) {
                        newCell.setCellStyle(cellStyle);
                    }
                }
            }
        }
    }

    private void moveCell(Cell cellFrom, Cell cellTo) {
        cloneCell(cellFrom, cellTo);
        clearCell(cellFrom);
        moveMergeRegionFromCellToCellIfExists(cellFrom, cellTo);
    }

    private void moveMergeRegionFromCellToCellIfExists(Cell cellFrom, Cell cellTo) {
        Sheet sheetFrom;
        if (Objects.isNull(cellFrom) || Objects.isNull(cellTo) || Objects.isNull(sheetFrom = cellFrom.getSheet()))
            return;

        for (int i = 0; i < sheetFrom.getNumMergedRegions(); ++i) {
            CellRangeAddress curMergeRegion = sheetFrom.getMergedRegion(i);
            if (Objects.equals(curMergeRegion.getFirstRow(), cellFrom.getRowIndex()) && Objects.equals(curMergeRegion.getFirstColumn(), cellFrom.getColumnIndex())) {
                copyMergeRegionOnCell(curMergeRegion, cellTo);
                sheetFrom.removeMergedRegion(i);
                return;
            }
        }
    }

    private void copyMergeRegionOnCell(CellRangeAddress cellRangeAddress, Cell cellTo) {
        if (Objects.isNull(cellRangeAddress) || Objects.isNull(cellTo)) return;
        Sheet sheetTo = ofNullable(cellTo.getSheet()).orElse(null);
        if (Objects.isNull(sheetTo)) return;
        int rangeHeight = cellRangeAddress.getLastRow() - cellRangeAddress.getFirstRow();
        int rangeWidth = cellRangeAddress.getLastColumn() - cellRangeAddress.getFirstColumn();
        sheetTo.addMergedRegion(new CellRangeAddress(cellTo.getRowIndex(), cellTo.getRowIndex() + rangeHeight, cellTo.getColumnIndex(), cellTo.getColumnIndex() + rangeWidth));
    }

    private Cell cloneCell(Cell srcCell, Cell targetCell) {
        CellStyle sourceCellStyle = srcCell.getCellStyle();
        CellStyle clonedCellStyle = workbook.createCellStyle();
        clonedCellStyle.cloneStyleFrom(sourceCellStyle);
        targetCell.setCellStyle(clonedCellStyle);
        switch (srcCell.getCellType()) {
            case _NONE:
                break;
            case NUMERIC:
                editXlsCell(targetCell.getRow(), targetCell.getColumnIndex(), srcCell.getNumericCellValue());
                break;
            case STRING:
                editXlsCell(targetCell.getRow(), targetCell.getColumnIndex(), srcCell.getStringCellValue());
                break;
            case FORMULA:
                targetCell.setCellFormula(srcCell.getCellFormula());
                break;
            case BLANK:
                break;
            case BOOLEAN:
                editXlsCell(targetCell.getRow(), targetCell.getColumnIndex(), srcCell.getBooleanCellValue());
                break;
            case ERROR:
                break;
        }
        return targetCell;
    }

    public static void editXlsCell(Row row, int column, String value) {
        Cell hCell;
        hCell = row.getCell(column);
        if (value != null) {
            hCell.setCellValue(value);
        } else
            hCell.setCellValue("");
    }

    public static void editXlsCell(Row row, int column, boolean value) {
        Cell hCell;
        hCell = row.getCell(column);
        hCell.setCellValue(value);
    }

    public static void editXlsCell(Row row, int column, Double value) {
        Cell hCell;
        hCell = row.getCell(column);
        if (value != null) {
            if (value != 0.d) {
                if (value.isNaN()) {
                    hCell.setCellValue(0.0);
                } else {
                    hCell.setCellValue(value);
                }
            } else
                hCell.setCellValue(0.0);
        } else
            hCell.setCellValue(0.0);
    }

    protected void clearCell(Cell cell) {
        Row row;
        if (Objects.isNull(cell) || Objects.isNull(row = cell.getRow())) return;
        row.createCell(cell.getColumnIndex());
    }

    private void nullCell(Row row, int column, CellStyle cellStyle) {
        Cell h_cell;
        if (row.getCell(column) == null) {
            h_cell = row.createCell(column);
            h_cell.setCellStyle(cellStyle);
        } else {
            h_cell = row.getCell(column);
        }

        h_cell.setCellValue("");
    }


    private void editCell(Row row, int column, String value, CellStyle cellStyle) {
        Cell h_cell;
        if (row.getCell(column) == null) {
            h_cell = row.createCell(column, CellType.STRING);
            h_cell.setCellStyle(cellStyle);
        } else {
            h_cell = row.getCell(column);
        }

        if (value != null) {
            h_cell.setCellValue(value);
        } else
            h_cell.setCellValue("");
    }

    private void editCell(Row row, int column, LocalDate value, CellStyle cellStyle) {
        Cell h_cell;
        if (row.getCell(column) == null) {
            h_cell = row.createCell(column);
            h_cell.setCellStyle(cellStyle);
        } else {
            h_cell = row.getCell(column);
        }

        if (value != null) {
            h_cell.setCellValue(value);
        } else
            h_cell.setCellValue("");
    }

    private void rewriteCell(Row row, int column, Double value, CellStyle cellStyle, CellType cellType) {
        Cell h_cell;

        if (NumberUtils.isNotNullOrNotEmpty(value)) {
            h_cell = row.createCell(column, cellType);
            h_cell.setCellStyle(cellStyle);
        } else {
            h_cell = row.createCell(column, CellType.BLANK);
            h_cell.setCellStyle(styleString);
        }
        h_cell = row.getCell(column);

        if (value != null) {
            if (!(value == 0.d)) {
                if (value.isNaN()) {
                    h_cell.setCellValue(0.0);
                } else {
                    h_cell.setCellValue(value);
                }
            }
        }
    }

    private void editCell(Row row, int column, Double value, CellStyle cellStyle, CellType cellType) {
        Cell h_cell;
        if (row.getCell(column) == null) {
            if (NumberUtils.isNotNullOrNotEmpty(value)) {
                h_cell = row.createCell(column, cellType);
                h_cell.setCellStyle(cellStyle);
            } else {
                h_cell = row.createCell(column, CellType.BLANK);
                h_cell.setCellStyle(styleString);
            }

        } else {
            h_cell = row.getCell(column);
        }


        if (value != null) {
            if (!(value == 0.d)) {
                if (value.isNaN()) {
                    h_cell.setCellValue(0.0);
                } else {
                    h_cell.setCellValue(value);
                }
            }
        }
    }

    private Double getProc(Double expr, Double from) {
        double temp = from != null && from != 0d ? ofNullable(expr).orElse(0d) / from : 0d;
        return BigDecimal.valueOf(temp).setScale(3, BigDecimal.ROUND_HALF_DOWN).doubleValue();
    }

    /*public DefaultStreamedContent restoreExcel() {
        return ofNullable(seedsBackFillsModelBean.getSelectedReportRestore()).
                map(rp -> {
                    setFilterMap(reportRestoreController.restoreFilterMap(rp.getId()));
                    return constructExcel();
                }).orElseGet(() -> {
                    Map<ReportKeysForFilterEnum, Object> all = ReportKeysForFilterEnum.getMapValues();
                    all.put(ReportKeysForFilterEnum.NAME, "Полный детализированный список");
                    all.put(ReportKeysForFilterEnum.DETAL_BY_PARTIAL, true);
                    setFilterMap(all);

                    return constructExcel();
                });
    }*/


}
