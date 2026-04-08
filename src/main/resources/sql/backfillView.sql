with active_states as (select seed_back_fill_id, date, status
from ase.seeds_backfills_state state
where deleted is null
order by seed_back_fill_id
    ),
    last_active_state as (select distinct on (seed_back_fill_id) active_states.*
from active_states
order by seed_back_fill_id, date desc),
    backfills as ( %s),
    privelege_culture_mix as (
select distinct on (backfills.id) backfills.id                 as backdill_id,
    mix_type.id                  as mix_type_id,
    mix_type.name                as mix_type_name,
    sbf_ms.percentage            as mix_sort_percentage,
    cs.code                      as mix_sort_code,
    cs.name                      as mix_sort_name,
    cs.culture_sort_allow_id     as mix_sort_allow_id,
    cs.region                    as mix_sort_region,
    cult.id                      as mix_culture_id,
    cult.name                    as mix_culture_name,
    cult.culture_group_id        as mix_culture_group_id,
    cult.culture_group_season_id as mix_culture_group_season_id,
    cult.culture_season_id       as mix_culture_season_id
from backfills
    join common.culture_mixes cm on backfills.culture_mix_id = cm.id
    join common.cultures mix_type on mix_type.id = cm.mix_type
    left join ase.seeds_backfill_mix_sorts sbf_ms on sbf_ms.seeds_backfill_id = backfills.id
    left join common.culture_sorts cs on cs.code = sbf_ms.culture_sort_code
    left join common.cultures cult on cult.id = cs.culture_id
order by backfills.id, mix_sort_percentage desc, mix_sort_name
    ),
    backfill_src_customers as (
select backfills.id                                            as backfill_id,
    cast(max(cast((src.source_customer = 'OUT_BUDGET') as int)) as boolean) as has_out_bt,
    cast(max(cast((src.source_customer = 'GOV_TASK') as int)) as boolean)   as has_gov_task,
    cast(max(cast((src.source_customer = 'CONTRACTOR_INFO') as int)) as boolean)   as has_contractor_info
from ase.seeds_backfills_source_customers src
    join backfills on backfills.id = src.seeds_backfill_id
group by backfill_id
    ),
    backfills_sums_by_statuses as (
select backfills.id as backfill_id,
    sum(case when sbs.status = 'SOWN' then volume end) as sown_sum,
    sum(case when sbs.status = 'SOWN' then seeded_area end) as sown_area_sum,
    sum(case when sbs.status = 'DELETED' then volume end) as deleted_sum,
    sum(case when sbs.status = 'SOLID' then volume end) as sold_sum,
    sum(case when sbs.status = 'DEFECTED' then volume end) as defected_sum,
    sum(case when sbs.status = 'FED' then volume end) as fed_sum
from backfills
    join ase.seeds_backfills_state sbs on sbs.seed_back_fill_id = backfills.id
where sbs.deleted is null
group by backfills.id
    )
select backfills.*,
       stat_sum.*,
       entStat.date_finish_edit as entity_status_date_finish_edt,
       entStat.status as entity_status_en,
       entStat.created as entity_status_created,
       entStat.status_correction_desc,
       departments.name as department_name,
       departments.name_short as department_name_short,
       twn.name         as township_name,
       regions.name     as ter_region_name,
       last_active_state.status as last_active_state_enum,
       meashure_units.msu_name_short,
       coalesce(src_customers.has_out_bt, false)   as has_out_bt,
       coalesce(src_customers.has_gov_task, false) as has_gov_task,
       coalesce(src_customers.has_contractor_info, false) as has_contractor_info,
       prov_kind.name                              as provider_kind_name,
       reasons.name                                as reason_name,
       fund.name                                   as fund_type_name,
       con.name_view                              as contractor_name_short,
       con.name_full                               as contractor_name_full,
       con.manager_name_short                      as contractor_manager_name_short,
       con.manager_name_full                       as contractor_manager_name_full,
       con.group_investor_id                       as contractor_group_investor_id,
       con.organizational_form                     as contractor_organizational_form,
       con.phone                                   as contractor_phone_number,
       con.fax                                     as contractor_fax_number,
       con.email                                   as contractor_email,
       cq.qual_checked,
       cq.qual_conditioned,
       cq.qual_notconditioned_all,
       cq.qual_notconditioned_debris,
       cq.qual_notconditioned_germination,
       cq.qual_notconditioned_humidity,
       cq.qual_notconditioned_pests,
       cq.doc_quality_type,
       cq.quality_doc_number,
       cq.quality_doc_date_expired,
       cq.sort_doc_type,
       cq.sort_doc_number as cq_sort_doc_number,
       cq.sort_doc_date as cq_sort_doc_date,
       repr_type.id                                as reproduction_id,
       repr_type.name_short                        as reproduction_name_short,
       repr_type.name_view                        as reproduction_name_view,
       repr_type.name_full                         as reproduction_name_full,
       repr_type.turn                         as reproduction_turn,
       category.id                                 as repr_category_id,
       category.name_short                         as repr_category_name_short,
       category.name_full                         as repr_category_name_full,
       category.type                               as repr_category_type,
       category_group.id                           as category_group_id,
       category_group.name                         as category_group_name,
       ind.germination,

       ind.health_perc,
       ind.humidity_perc,
       ind.weighted_average_1000seeds,
       ind.akt_seeds_selection_date,
       ind.akt_seeds_selection_number,
       privelege_culture_mix.*,
       cs.name                                     as sort_name,
       cs.region                                   as sort_region,
       cs.culture_sort_allow_id,
       culture.id                                  as culture_id,
       culture.culture_group_id,
       culture.culture_group_season_id,
       culture.name                                as culture_name,
       'back_fill_view_p'                                as class_type,

       (select count(id) > 0 from ase.crop_qualities where crop_qualities.back_fill_id = backfills.id and jsonb_exists(crop_qualities.crop_quality_good_enums, 'PHYTO_EXPERTIZE')) as has_expertize,
       cq.quality_doc_end_enum                                as quality_doc_end_enum,
       case when culture notnull then culture.category_group_id notnull else false end as can_have_reproductions,
       culture.culture_season_id                   as culture_season_id

from backfills
         join common.entity_statuses entStat on entStat.id = backfills.actual_ent_status_id
         join common.contractors con on con.id = backfills.contractor_id
         left join common.culture_sorts cs on cs.code = backfills.culture_sort_code
         left join common.cultures culture on culture.id = cs.culture_id
         left join common.culture_sort_allows cs_allow on cs_allow.id = cs.culture_sort_allow_id
         left join ase.crop_qualities cq on cq.id = backfills.actual_crop_quality_id
         left join common.reproduction_types repr_type on repr_type.id = cq.reproduction_type_id
         left join common.reproduction_categories category on category.id = repr_type.category_id
         left join common.reproduction_category_groups category_group on category_group.id = repr_type.category_group_id
         left join ase.seeds_indicators ind on ind.id = cq.indicator_id
         left join privelege_culture_mix on privelege_culture_mix.backdill_id = backfills.id
         left join ase.seed_fund_types fund on fund.id = backfills.seed_fund_type_id
         left join ase.seeds_backfill_reasons reasons on reasons.id = backfills.seeds_backfill_reason_id
         left join backfill_src_customers src_customers on src_customers.backfill_id = backfills.id
         left join common.provider_kinds prov_kind on prov_kind.id = backfills.provider_kind_id
         left join common.department_structure departments on departments.id = backfills.department_id
         left join common.ter_townships twn on backfills.township_id = twn.id
         left join common.ter_regions regions on regions.id = twn.region_id
         left join backfills_sums_by_statuses stat_sum on stat_sum.backfill_id = backfills.id
         left join common.meashure_units on backfills.fill_unit_id = meashure_units.msu_id
         left join last_active_state on backfills.id = last_active_state.seed_back_fill_id
order by twn.name, con.name_short, backfills.date_begin, culture.name, mix_culture_name, sort_name, mix_sort_name, cq.date, backfills.created desc
