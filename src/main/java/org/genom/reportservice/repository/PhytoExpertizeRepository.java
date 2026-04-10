package org.genom.reportservice.repository;

import com.gnm.enums.ReproductionEnum;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import jakarta.persistence.Query;
import lombok.extern.slf4j.Slf4j;
import org.genom.reportservice.LogExecTime;
import org.genom.reportservice.criteria.PhytoExpertizeCriteria;
import org.genom.reportservice.model.PhytoExpertizeQualReport;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.gnm.enums.ReproductionEnum.*;
import static com.gnm.enums.ReproductionEnum.RSM;
import static java.time.LocalDateTime.now;

@Repository
@Slf4j
public class PhytoExpertizeRepository {

    @PersistenceContext
    private EntityManager em;
    public static final List<ReproductionEnum> REPRODUCTION_ENUMS_MASS_OR_PRODUCTION = List.of(RS5, RS6, RS7, RS8, RS9, RS10, RS10, RST, RSM);

    private String getBackFillSQL(PhytoExpertizeCriteria phytoExpertizeCriteria) {
        /*language=SQL*/
        final String BACK_FILL_EXPERTIZE_SQL = "with \n" +
                "     expertizes as (SELECT qual.id, back_fill_id,qual.reproduction_type_id, phyto_exp_checked,date,count(seeds_samples.id) as samplesCount,qual.reproduction_trade,qual.actual_ent_status_id\n" +
                "                    FROM ase.crop_qualities qual JOIN ase.seeds_backfills backfills ON backfills.id = qual.back_fill_id" +
                "  LEFT JOIN ase.seeds_samples ON qual.id = seeds_samples.crop_quality_id and seeds_samples.good_enum like 'PHYTO_EXPERTIZE'\n" +
                "  LEFT JOIN common.entity_statuses status ON status.id = qual.actual_ent_status_id \n" +
                "                    WHERE %s jsonb_exists(crop_quality_good_enums,'PHYTO_EXPERTIZE')\n" +
                "                      AND qual.date BETWEEN :dateBegin and :dateEnd and case when :onlyChecked then status like 'CHECKED' else true end  GROUP BY qual.id),\n" +
                "       backfills as (SELECT backfills.*, backfills.info_reproduction_type_id as  reproduction_type_id,reprType.type as reprTypeName,backfills.info_reproduction_trade as reproduction_trade\n " +
                "\n" +
                "                   FROM ase.seeds_backfills backfills " +
                "                   LEFT JOIN common.reproduction_types reprType ON reprType.id = backfills.info_reproduction_type_id\n" +
                "                   WHERE %s \n" +
                "                      (((backfills.date_end isnull or (backfills.date_end >= :dateBegin and exists(select id from expertizes where expertizes.back_fill_id = backfills.id))) and\n" +
                "                          (backfills.date_begin <= :dateEnd))\n" +
                "                       or backfills.date_finished isnull)\n" +
                "                     and backfills.deleted isnull)," +
                "     privelege_culture_mix as (\n" +
                "         select distinct on (backfills.id) backfills.id                 as backfill_id,\n" +
                "                                           mix_type.id                  as mix_type_id,\n" +
                "                                           mix_type.name                as mix_type_name,\n" +
                "                                           sbf_ms.percentage            as mix_sort_percentage,\n" +
                "                                           cs.code                      as mix_sort_code,\n" +
                "                                           cs.name                      as mix_sort_name,\n" +
                "                                           cs.culture_sort_allow_id     as mix_sort_allow_id,\n" +
                "                                           cs.region                    as mix_sort_region,\n" +
                "                                           cult.id                      as mix_culture_id,\n" +
                "                                           concat(mix_type.name,' ',cult.name)                    as mix_culture_name,\n" +
                "                                           cult.culture_group_id        as mix_culture_group_id,\n" +
                "                                           cult.culture_group_season_id as mix_culture_group_season_id,\n" +
                "                                           cult.culture_season_id       as mix_culture_season_id\n" +
                "         from backfills\n" +
                "                  join common.culture_mixes cm on backfills.culture_mix_id = cm.id\n" +
                "                  join common.cultures mix_type on mix_type.id = cm.mix_type\n" +
                "                  left join ase.seeds_backfill_mix_sorts sbf_ms on sbf_ms.seeds_backfill_id = backfills.id\n" +
                "                  left join common.culture_sorts cs on cs.code = sbf_ms.culture_sort_code\n" +
                "                  left join common.cultures cult on cult.id = cs.culture_id\n" +
                "         order by backfills.id, mix_sort_percentage desc, mix_sort_name\n" +
                "     )\n";

        if (!phytoExpertizeCriteria.twnsIsNull() || !phytoExpertizeCriteria.depsIsNull()) {
            String twnClause = phytoExpertizeCriteria.twnsIsNull() ? null : " backfills.township_id in (:towns) ";
            String depClause = phytoExpertizeCriteria.depsIsNull() ? null : " backfills.department_id in (:departments) ";
            String depClauseExpertize = depClause == null ? "" : " qual.department_id in (:departments) AND ";
            List<String> combineCollection = Stream.of(twnClause, depClause)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
            String combineClause = "";
            if (!combineCollection.isEmpty()) {
                combineClause = " ( " + String.join(" AND ", combineCollection) + " ) AND ";
            }
            return String.format(
                    BACK_FILL_EXPERTIZE_SQL,
                    depClauseExpertize,
                    combineClause
            );
        } else {
            return String.format(
                    BACK_FILL_EXPERTIZE_SQL,
                    " true and ",
                    " true and "
            );
        }


    }

    /**
     * @param phytoExpertizeCriteria
     * @return phytoExpertize by cropqualities report
     */
    @Transactional
    @LogExecTime
    public List<PhytoExpertizeQualReport> findForQualReport(PhytoExpertizeCriteria phytoExpertizeCriteria) {
        log.info("get expertize for report {}", phytoExpertizeCriteria);


        //date_end isnull because, may occur for exist phytoExpertize by specified interval dates
        Query query  = em.createNativeQuery(getBackFillSQL(phytoExpertizeCriteria) +
                        "SELECT row_number() over ()                                                as id,\n" +
                        "       backfills.fill_all                                                  as fill,\n" +
                        "       backfills.batch_number                                              as batch_number,\n" +
                        "       backfills.id                                                        as back_fill_id,\n" +
                        "       backfills.contractor_id                                             as contractor_id,\n" +
                        "       qual.id                                                             as crop_quality_id,\n" +
                        "       qual.phyto_exp_checked                                                             as checked,\n" +
                        "       result.id                                                           as indicator_result_id,\n" +
                        "       result.name                                                         as indicator_field_name,\n" +
                        "       coalesce(result.turn = 1, false)                                       is_total_result,\n" +
                        "       result.phyto_subject_code                                           as field_subject_code,\n" +
                        "       result.phyto_subject_group_id                                       as field_group_id,\n" +
                        "       case when result.is_not_found then  0 else cast(nullif(replace(replace(result.value, ',', '.'),' ',''),'') as double precision) end           as percent_infected,\n" +
                        "       result.is_not_found as not_found," +

                        "       coalesce(sort.culture_id, privelege_culture_mix.mix_culture_id, -1) as culture_id,\n" +
                        "       backfills.seed_fund_type_id = 2                                     as transfered_fund,\n" +
                        "       backfills.prev_analyze                                     as prev_analyze,\n" +
                        "       repr_type.name_short                                     as reproduction,\n" +
                        "       con.name_short                                     as contractor_name,\n" +
                        "       qual.date                                     as date_crop_quality,\n" +
                        "       dep.name                                     as department_name,\n" +
                        "       town.name                                     as town,\n" +
                        "       region.name                                     as region_name,\n" +
                        "       coalesce(culture.name, privelege_culture_mix.mix_culture_name)                                     as culture_name,\n" +
                        "       coalesce(sort.name, privelege_culture_mix.mix_sort_name)                                     as sort_name,\n" +
                        "       result.measure_error                                     as measure_error,\n" +
                        "       result.norms_for_nd                                     as norms_for_nd,\n" +
                        "       coalesce(culture.culture_season_id, privelege_culture_mix.mix_culture_season_id)                                     as season_id,\n" +
                        "       result.gost                                     as gost,\n" +
                        "       result.indicator_template_field_id                                     as template_field_id,\n" +
                        "       qual.samplesCount                                     as samples_count,\n" +
                        "       coalesce(repr_type.type,backfills.reprTypeName) isnull or coalesce(repr_type.type,backfills.reprTypeName)  in (:reprsMassOrProduction) or coalesce(qual.reproduction_trade,backfills.reproduction_trade,false) as mass_or_production_reproduction,\n" +
                        "       null as region_id,\n" +
                        "       entStat.status as status_enum,\n" +
                        "       template.name    as indicator_template_name," +
                        "       'expertize_qual_report'                                     as class_type,\n" +
                        "       backFills.crop_year                                     as crop_year,\n" +
                        "       backFills.seedprod_for_harvest_year                                     as seed_prod_for_harvest_year\n" +
                        "FROM backfills\n" +
                        "         LEFT JOIN common.contractors con ON con.id = backfills.contractor_id\n" +
                        "         JOIN common.department_structure dep ON dep.id = backfills.department_id\n" +
                        "         JOIN common.ter_townships town ON town.id = backfills.township_id\n" +
                        "         JOIN common.ter_regions region ON region.id = town.region_id\n" +
                        "         LEFT JOIN common.culture_sorts sort ON sort.code = backfills.culture_sort_code\n" +
                        "         LEFT JOIN common.cultures culture ON culture.id = sort.culture_id\n" +
                        "         JOIN expertizes qual ON qual.back_fill_id = backfills.id\n" +
                        "         LEFT JOIN common.entity_statuses entStat ON entStat.id = qual.actual_ent_status_id\n" +
                        "         LEFT JOIN common.reproduction_types repr_type ON qual.reproduction_type_id = repr_type.id\n" +
                        "         LEFT JOIN ase.indicator_result_wrappers wrapper ON qual.id = wrapper.crop_quality_id\n" +
                        "         LEFT JOIN ase.indicator_templates template ON wrapper.indicator_template_id = template.id\n" +
                        "         LEFT JOIN ase.indicator_results result ON result.indicator_result_wrapper_id = wrapper.id AND\n" +
                        "                                                   template.crop_quality_good_enum like 'PHYTO_EXPERTIZE'\n" +
                        "         LEFT JOIN privelege_culture_mix ON backfills.id = privelege_culture_mix.backfill_id " + phytoExpertizeCriteria.buildPredicateCriteria() +
                        "         ORDER BY region.federal_district_id, region.turn,dep.name,town.name, con.name_short,culture_name, qual.date", PhytoExpertizeQualReport.class)

                .setParameter("dateBegin", phytoExpertizeCriteria.getDateBegin())
                .setParameter("dateEnd", phytoExpertizeCriteria.getDateFinish())
                .setParameter("reprsMassOrProduction", REPRODUCTION_ENUMS_MASS_OR_PRODUCTION.stream().map(ReproductionEnum::name).collect(Collectors.toList()))
                .setParameter("onlyChecked", phytoExpertizeCriteria.isOnlyChecked());
        prepareQueryTownDepParams(query, phytoExpertizeCriteria);
        return query.getResultList();

    }

    private void prepareQueryTownDepParams(Query query, PhytoExpertizeCriteria criteria) {
        if (!criteria.twnsIsNull())
            query.setParameter("towns", criteria.readTowns());
        if (!criteria.depsIsNull())
            query.setParameter("departments", criteria.readDepartments());
    }
}
