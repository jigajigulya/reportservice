package org.genom.reportservice.utils;

import com.gnm.enums.GroupingTypeEnum;
import com.gnm.enums.ReportKeysForFilterEnum;
import com.gnm.enums.SourceCustomerEnum;
import com.gnm.enums.ase.SeedsBackFillPurposeEnum;
import com.gnm.model.ase.SeedFundType;
import com.gnm.model.ase.SeedsBackFillReason;
import com.gnm.model.common.*;
import com.gnm.model.common.geo.TerTownship;
import com.gnm.utils.CollectionUtils;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.gnm.enums.ReportKeysForFilterEnum.*;

public class BackFillReporterFilterMapper {


    public static Map<ReportKeysForFilterEnum, Object> mapToObject(Map<ReportKeysForFilterEnum, Object> filterMap) {
        Stream.of(
                GROUPING,
                TOWNSHIP,
                CULTURE,
                CONTRACTOR,
                SOURCE_CUSTOMER,
                ORG_TYPE,
                SH_FORM,
                SEASON,
                TYPE_CULTURE,
                SEED_FUND,
                SORT_CULTURE,
                REGISTRATION,
                REASON,
                PURPOSE
        ).forEach(
                obj -> {
                    switch (obj) {
                        case GROUPING -> filterMap.computeIfPresent(obj, (key, val) -> GroupingTypeEnum.valueOf((String)val));
                        case TOWNSHIP -> filterMap.computeIfPresent(obj, (key, val) -> TerTownship.builder()
                                .id(Long.valueOf((int) ((LinkedHashMap) val).get("id")))
                                .build());
                        case CONTRACTOR -> filterMap.computeIfPresent(obj, (key, val) -> Contractor.builder()
                                .id(Long.valueOf((int) ((LinkedHashMap) val).get("id")))
                                .build());
                        case ORG_TYPE -> filterMap.computeIfPresent(obj, (key, val) -> OrganizationalForm.builder()
                                .id(Long.valueOf((int) ((LinkedHashMap) val).get("id")))
                                .build());
                        case SH_FORM -> filterMap.computeIfPresent(obj, (key, val) -> GroupContractorInvestor.builder()
                                .id(Long.valueOf((int) ((LinkedHashMap) val).get("id")))
                                .build());

                        case SEASON -> filterMap.computeIfPresent(obj, (key, val) -> CultureSeason.builder()
                                .id(Long.valueOf((int) ((LinkedHashMap) val).get("id")))
                                .build());
                        case TYPE_CULTURE -> {
                            filterMap.computeIfPresent(obj, (key, val) -> CultureGroup.builder()
                                    .id(Long.valueOf((int) ((LinkedHashMap) val).get("id")))
                                    .build());
                        }
                        case CULTURE -> {

                            filterMap.computeIfPresent(obj, (key, val) -> {
                                List list = (List) val;
                                if (CollectionUtils.isNotNullOrNotEmpty(list)) {
                                    return list.stream()
                                                    .map(itemCulture -> {
                                                        return Culture.builder()
                                                                .id(Long.valueOf((int)((LinkedHashMap) itemCulture).get("id")))
                                                                .build();
                                                    })
                                            .collect(Collectors.toList());
                                } return null;

                            });
                        }
                        case SORT_CULTURE -> {
                            filterMap.computeIfPresent(obj, (key, val) -> CultureSort.builder()
                                    .id(Long.valueOf((int) ((LinkedHashMap) val).get("id")))
                                    .build());
                        }
                        case REGISTRATION -> {
                            filterMap.computeIfPresent(obj, (key, val) -> CultureSortAllow.builder()
                                    .id(Long.valueOf((int) ((LinkedHashMap) val).get("id")))
                                    .build());
                        }
                        case REASON -> {
                            filterMap.computeIfPresent(obj, (key, val) -> SeedsBackFillReason.builder()
                                    .id(Long.valueOf((int) ((LinkedHashMap) val).get("id")))
                                    .build());
                        }
                        case PURPOSE -> {
                            filterMap.computeIfPresent(obj, (key, val) -> SeedsBackFillPurposeEnum.valueOf((String)val));
                        }
                        case SOURCE_CUSTOMER -> {
                            filterMap.computeIfPresent(obj, (key, val) -> SourceCustomerEnum.valueOf((String)val));
                        }
                        case SEED_FUND -> {
                            filterMap.computeIfPresent(obj, (key, val) -> SeedFundType.builder()

                                    .id(Long.valueOf((int) ((LinkedHashMap) val).get("id"))).build());
                        }
                    }
                }
        );
        return filterMap;
    }
}
