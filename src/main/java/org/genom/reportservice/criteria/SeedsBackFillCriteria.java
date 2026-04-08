package org.genom.reportservice.criteria;


import com.gnm.enums.SeedsBackFillStatusEnum;
import com.gnm.model.ase.SeedFundType;
import com.gnm.model.common.Contractor;
import com.gnm.model.common.DepartmentStructure;
import com.gnm.model.common.Laboratory;
import com.gnm.model.common.geo.TerRegion;
import com.gnm.utils.CollectionUtils;
import com.gnm.utils.DateUtils;
import com.google.gson.annotations.Expose;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;


@Getter
@SuperBuilder
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class SeedsBackFillCriteria {


    public static final Long REASON_SEED = 1L;
    public static final Long REASON_TRADE = 3L;

    @Expose
    private TerRegion terRegion;
    @Expose
    private DepartmentStructure departmentRegion;
    @Expose
    private Contractor contractor;
    @Expose
    private boolean filterDeleted;
    @Expose
    private boolean filterEnded;
    @Expose
    private boolean forAnalytics;
    @Expose
    private boolean forSale;
    @Expose
    private boolean filterByStatus;
    @Expose
    private SeedsBackFillStatusEnum statusEnum;
    @Expose
    private LocalDateTime statusDateBegin;
    @Expose
    private LocalDateTime statusDateFinish;
    @Expose
    private LocalDateTime archiveDateBegin;
    @Expose
    private LocalDateTime archiveDateFinish;
    @Builder.Default
    private List<SeedFundType> fundTypeList = new ArrayList<>();
    @Expose
    private boolean archive;
    @Expose
    private Laboratory laboratory;
    @Expose
    private Long cultureId;
    @Expose
    private List<DepartmentStructure> departments;
    @Expose
    private LocalDateTime dateBegin;
    @Expose
    private LocalDateTime dateFinish;

    public LocalDateTime preparedDateBegin(LocalDateTime dateBegin) {
        return DateUtils.getTheStartOfDay(dateBegin);
    }

    public LocalDateTime preparedDateFinish(LocalDateTime dateFinish) {
        return DateUtils.getTheEndOfDay(dateFinish);
    }


    public String getTerRegionSQLClause() {
        if (Objects.isNull(terRegion)) return "";
        return " and ter_regions.id = " + terRegion.getId() + "\n";
    }

    public String getDepartmentRegionSQLClause() {
        if (Objects.isNull(departmentRegion)) return "";
        return " and department_structure.parent_id = " + departmentRegion.getId() + "\n";
    }

    public String fundTypePredicateSQL() {
        if (fundTypeList.isEmpty()) return "";
        return " and seeds_backfills.seed_fund_type_id in (" + fundTypeList.stream().map(SeedFundType::getId).map(String::valueOf).collect(Collectors.joining(",")) + ") \n";
    }

    public String getDepartmentsSQLClause() {
        if (CollectionUtils.isNullOrEmpty(departments)) return "";
        return " and department_structure.id in (" + departments.stream()
                .map(DepartmentStructure::getId)
                .filter(Objects::nonNull)
                .map(id -> Long.toString(id))
                .collect(Collectors.joining(", "))
                + ")\n";
    }

    public String getContractorSqlClause() {
        if (contractor == null) return "";
        return " and seeds_backfills.contractor_id = " + contractor.getId() + "\n";
    }

    public String getEndedSQLClause() {
        return isFilterEnded() ? "" : getDateEndSqlClauseLeftBorder(Optional.ofNullable(archiveDateBegin)
                .orElse(Optional.ofNullable(dateBegin)
                        .orElse(LocalDateTime.now())));
    }

    public static String getDateEndSqlClauseLeftBorder(LocalDateTime dateBegin) {
        return getDateEndSqlClauseLeftBorder(dateBegin, "seeds_backfills.");
    }

    public static String getDateEndSqlClauseLeftBorder(LocalDateTime dateBegin, String tableName) {
        if (Objects.isNull(tableName)) tableName = "";
        return Objects.nonNull(dateBegin) ? " and (" + tableName + "date_end is null or " + tableName + "date_end  >= '" + Timestamp.valueOf(dateBegin) + "')" : "";
    }

    public static String getDateBeginSqlClauseLeftBorder(LocalDateTime dateBegin, String tableName) {
        if (Objects.isNull(tableName)) tableName = "";
        return Objects.nonNull(dateBegin) ? " and " + tableName + "date_begin >= '" + Timestamp.valueOf(dateBegin) + "'" : "";
    }

    public static String getDateBeginSqlClauseRightBorder(LocalDateTime dateEnd, String tableName) {
        if (Objects.isNull(tableName)) tableName = "";
        return Objects.nonNull(dateEnd) ? " and " + tableName + "date_begin <=  '" + DateUtils.getTheEndOfDay(Timestamp.valueOf(dateEnd)) + "'" : "";
    }

    public String getDeletedSQLClause() {
        return isFilterDeleted() ? "" : " and seeds_backfills.deleted is null\n";
    }

    public String getDateBeginBetweenSQLClause() {
        if (Objects.isNull(dateBegin) && Objects.isNull(dateFinish)) return "";
        StringBuilder clause = new StringBuilder();
        final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        if (Objects.nonNull(dateBegin)) {
            clause.append(" and '").append(dateTimeFormatter.format(dateBegin)).append("' <= seeds_backfills.date_begin\n");
        }

        if (Objects.nonNull(dateFinish)) {
            clause.append(" and '").append(dateTimeFormatter.format(dateFinish)).append("' >= seeds_backfills.date_begin\n");
        }

        return clause.toString();
    }

}
