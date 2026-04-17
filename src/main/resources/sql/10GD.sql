WITH scrop AS (
    SELECT
        id,
        date_assay,
        area_seeded,

        is_inc,

        (CASE WHEN is_inc THEN id ELSE null END) AS id_harvest,

        (CASE WHEN is_inc THEN area_harvest ELSE null END) AS area_harvest,
        (CASE WHEN is_inc THEN area_harvest_forsale ELSE null END) AS area_harvest_forsale,
        (CASE WHEN is_inc THEN yield ELSE null END) AS yield,
        (CASE WHEN is_inc THEN productivity_fact ELSE null END) AS productivity_fact,

        seedprod_crop_category,
        (CASE WHEN is_inc THEN sort_code ELSE null END) AS sort_code,
        (CASE WHEN is_inc THEN sort_name ELSE null END) AS sort_name,
        sort_sign_2,
        culture_id,
        culture_name,
        seedprod_culture_season_id,

        ter_township_id,
        ter_township_name,
        ter_region_id,
        ter_region_name,
        ter_federal_district_id,

        dep_township_id,
        dep_township_name,
        dep_region_id,
        dep_region_name,
        dep_ter_region_id,
        dep_ter_federal_district_id,

        (CASE WHEN is_inc AND sort_ru = 1 THEN id ELSE null END) AS ru_assay_id,
        (CASE WHEN is_inc AND sort_ru = 1 THEN sort_code ELSE null END) AS ru_sort_code,
        (CASE WHEN is_inc AND sort_ru = 1 THEN area_seeded ELSE null END) AS ru_area_seeded,
        (CASE WHEN is_inc AND sort_ru = 1 THEN area_harvest ELSE null END) AS ru_area_harvest,
        (CASE WHEN is_inc AND sort_ru = 1 THEN area_harvest_forsale ELSE null END) AS ru_area_harvest_forsale,
        (CASE WHEN is_inc AND sort_ru = 1 THEN yield ELSE null END) AS ru_yield,

        (CASE WHEN is_inc AND sort_en = 1 THEN id ELSE null END) AS en_assay_id,
        (CASE WHEN is_inc AND sort_en = 1 THEN sort_code ELSE null END) AS en_sort_code,
        (CASE WHEN is_inc AND sort_en = 1 THEN area_seeded ELSE null END) AS en_area_seeded,
        (CASE WHEN is_inc AND sort_en = 1 THEN area_harvest ELSE null END) AS en_area_harvest,
        (CASE WHEN is_inc AND sort_en = 1 THEN area_harvest_forsale ELSE null END) AS en_area_harvest_forsale,
        (CASE WHEN is_inc AND sort_en = 1 THEN yield ELSE null END) AS en_yield,

        (CASE WHEN is_inc AND sort_ru = 0 AND sort_en = 0 THEN id ELSE null END) AS nr_assay_id,
        (CASE WHEN is_inc AND sort_ru = 0 AND sort_en = 0 THEN sort_code ELSE null END) AS nr_sort_code,
        (CASE WHEN is_inc AND sort_ru = 0 AND sort_en = 0 THEN area_seeded ELSE null END) AS nr_area_seeded,
        (CASE WHEN is_inc AND sort_ru = 0 AND sort_en = 0 THEN area_harvest ELSE null END) AS nr_area_harvest,
        (CASE WHEN is_inc AND sort_ru = 0 AND sort_en = 0 THEN area_harvest_forsale ELSE null END) AS nr_area_harvest_forsale,
        (CASE WHEN is_inc AND sort_ru = 0 AND sort_en = 0 THEN yield ELSE null END) AS nr_yield

    FROM (
             SELECT
                 assays.id,
                 assays.date_assay,
                 assays.cropfield_area AS area_seeded,

                 (CASE WHEN '10G' = :rep_type THEN true WHEN '10D' = :rep_type AND (assays.cropfield_harvest_area IS NOT null) THEN true ELSE false END) AS is_inc,

                 assays.cropfield_harvest_area AS area_harvest,
                 assays.cropfield_forsale_area AS area_harvest_forsale,
                 assays.cropfield_fact_productivity AS productivity_fact,
                 coalesce(assays.cropfield_fact_productivity, 0.0) * coalesce(assays.cropfield_harvest_area, 0.0) AS yield,

                 assays.seedprod_crop_category,
                 assays.crop_current_culturesort_code AS sort_code,
                 join_sorts.name AS sort_name,
                 join_sorts.sign_2 AS sort_sign_2,
                 join_sorts.culture_id AS culture_id,
                 assays.seedprod_culture_season_id,
                 join_cultures.name AS culture_name,

                 assays.township_id               AS ter_township_id,
                 join_ter_township.name             AS ter_township_name,

                 join_ter_township.region_id           AS ter_region_id,
                 join_ter_region.name              AS ter_region_name,
                 join_ter_region.federal_district_id       AS ter_federal_district_id,

                 assays.department_id              AS dep_township_id,
                 join_dep_township.name_short          AS dep_township_name,
                 join_dep_township.parent_id           AS dep_region_id,
                 join_dep_region.name_short           AS dep_region_name,
                 join_dep_region.ter_region_id          AS dep_ter_region_id,
                 join_dep_region_ter_region.federal_district_id AS dep_ter_federal_district_id,

                 (CASE WHEN join_sorts.type = 'RNNS' THEN 0 WHEN join_sorts.country_code_iso ISNULL THEN (CASE WHEN join_sorts_contractors.country_code_iso = '643' THEN 1 ELSE 0 END) WHEN join_sorts.country_code_iso = '643' THEN 1 ELSE 0 END) sort_ru,
                 (CASE WHEN join_sorts.type = 'RNNS' THEN 0 WHEN (join_sorts.country_code_iso ISNULL OR trim(join_sorts.country_code_iso) = '') THEN (CASE WHEN join_sorts_contractors.country_code_iso = '643' THEN 0 ELSE (CASE WHEN (join_sorts_contractors.country_code_iso ISNULL OR trim(join_sorts_contractors.country_code_iso) = '') THEN 0 ELSE 1 END) END) WHEN join_sorts.country_code_iso = '643' THEN 0 ELSE 1 END) sort_en

             FROM assays
                      LEFT JOIN common.culture_sorts AS join_sorts ON join_sorts.code = assays.crop_current_culturesort_code
                      LEFT JOIN common.cultures AS join_cultures ON join_cultures.id = join_sorts.culture_id
                      LEFT JOIN common.culture_sorts_contractors AS join_sorts_contractors ON join_sorts_contractors.id = join_sorts.originator_main

                      LEFT JOIN common.ter_townships AS join_ter_township ON join_ter_township.id = assays.township_id
                      LEFT JOIN common.ter_regions AS join_ter_region ON join_ter_region.id = join_ter_township.region_id
                      LEFT JOIN common.ter_federal_districts AS join_ter_federal_districts ON join_ter_federal_districts.id = join_ter_region.federal_district_id

                      LEFT JOIN common.department_structure AS join_dep_township ON join_dep_township.id = assays.department_id
                      LEFT JOIN common.department_structure AS join_dep_region ON join_dep_region.id = join_dep_township.parent_id
                      LEFT JOIN common.ter_regions AS join_dep_region_ter_region ON join_dep_region_ter_region.id = join_dep_region.ter_region_id

             WHERE assays.is_seedprod AND assays.seedprod_crop_kind = 'SEEDS' AND township_id IS NOT null AND seedprod_for_harvest_year = :harvest_year
               AND (join_ter_region.exclude_monitoring ISNULL OR join_ter_region.exclude_monitoring = false)
               AND (join_dep_region_ter_region.exclude_monitoring ISNULL OR join_dep_region_ter_region.exclude_monitoring = false)
               AND assays.crop_current_culturesort_code IS NOT null AND join_sorts.mark_01 IS DISTINCT FROM 'ns'
                 ?
         ) AS ass
    WHERE true ?
),

     ass_sorts AS (
         SELECT
             scrop.id,
             scrop.date_assay,
             scrop.area_seeded,

             scrop.is_inc,
             scrop.id_harvest,

             scrop.area_harvest,
             scrop.area_harvest_forsale,
             scrop.productivity_fact,
             scrop.yield,

             scrop.seedprod_crop_category,
             scrop.sort_code,
             scrop.sort_name,
             scrop.sort_sign_2,
             scrop.culture_id,
             scrop.culture_name,
             seedprod_culture_season_id,

             ter_township_id,
             ter_township_name,
             ter_region_id,
             ter_region_name,

             dep_township_id,
             dep_township_name,
             dep_region_id,
             dep_region_name,

             ru_assay_id,
             ru_sort_code,
             ru_area_seeded,
             ru_area_harvest,
             ru_area_harvest_forsale,
             ru_yield,

             en_assay_id,
             en_sort_code,
             en_area_seeded,
             en_area_harvest,
             en_area_harvest_forsale,
             en_yield,

             nr_assay_id,
             nr_sort_code,
             nr_area_seeded,
             nr_area_harvest,
             nr_area_harvest_forsale,
             nr_yield

         FROM scrop
     ),

     cultures_cats AS (
         SELECT
             scats.id,
             scats.type,

             scats.parent_id,

             scats.view_name,

             scats.name,

             scats.stc_object_id,
             scats.stc_parent_id,

             scats.tree_parent_id,

             scats.group_level,
             scats.group_crop_category,

             scats.cnt_cultures,
             scats.cnt_groups,

             scats.parent_wcat_id,

             scats_par.cnt_cultures AS par_cnt_cultures,
             scats_par.cnt_groups AS par_cnt_groups,

             scats.culture_id,
             scats.category,

             scats.turn_all,
             scats.turn_in,

             0 AS ter_federal_district_turn,
             0 AS ter_region_turn,

             0 AS ter_region_id,
             '' AS ter_region_name,
             0 AS ter_federal_district_id,
             '' AS ter_federal_district_name,

             (CASE WHEN (:detail_assays = true) AND (scats.culture_id IS NOT NULL) AND (scats.category IS NOT NULL) AND (COUNT(ass_sorts.id) > 0)
                       THEN
                       jsonb_agg(cast(ROW (

                           ass_sorts.id,
                           scats.id,
                           ass_sorts.date_assay,

                           ass_sorts.dep_township_name,
                           ass_sorts.dep_township_id,
                           ass_sorts.dep_region_name,
                           ass_sorts.dep_region_id,

                           ass_sorts.ter_township_name,
                           ass_sorts.ter_township_id,
                           ass_sorts.ter_region_name,
                           ass_sorts.ter_region_id,

                           ass_sorts.culture_id,
                           ass_sorts.culture_name,
                           ass_sorts.sort_code,
                           ass_sorts.sort_name,

                           ass_sorts.area_seeded,
                           ass_sorts.area_harvest,
                           ass_sorts.area_harvest_forsale,
                           ass_sorts.productivity_fact,
                           ass_sorts.yield,

                           (CASE WHEN ass_sorts.ru_assay_id ISNULL THEN false ELSE true END),
                           (CASE WHEN ass_sorts.en_assay_id ISNULL THEN false ELSE true END),
                           (CASE WHEN ass_sorts.nr_assay_id ISNULL THEN false ELSE true END)
                           ) as assay_seedprod_category_wrapper))
                   ELSE null END) AS assays,


             array_to_string(array_agg(DISTINCT ass_sorts.sort_code), ',') AS all_sorts_codes,

             COUNT(ass_sorts.id) AS count_assays,
             COUNT(ass_sorts.id_harvest) AS count_harvest_assays,

             SUM(ass_sorts.area_seeded) AS sum_all_area_seeded,
             SUM(ass_sorts.area_harvest) AS sum_all_area_harvest,
             SUM(ass_sorts.area_harvest_forsale) AS sum_all_area_harvest_forsale,
             SUM(ass_sorts.yield) AS sum_all_yield,

             COUNT(ass_sorts.ru_assay_id) AS ru_count_assays,
             array_to_string(array_agg(DISTINCT ass_sorts.ru_sort_code), ',') AS ru_sorts_codes,
             SUM(ass_sorts.ru_area_seeded) AS ru_sum_area_seeded,
             SUM(ass_sorts.ru_area_harvest) AS ru_sum_area_harvest,
             SUM(ass_sorts.ru_area_harvest_forsale) AS ru_sum_area_harvest_forsale,
             SUM(ass_sorts.ru_yield) AS ru_sum_yield,

             COUNT(ass_sorts.en_assay_id) AS en_count_assays,
             array_to_string(array_agg(DISTINCT ass_sorts.en_sort_code), ',') AS en_sorts_codes,
             SUM(ass_sorts.en_area_seeded) AS en_sum_area_seeded,
             SUM(ass_sorts.en_area_harvest) AS en_sum_area_harvest,
             SUM(ass_sorts.en_area_harvest_forsale) AS en_sum_area_harvest_forsale,
             SUM(ass_sorts.en_yield) AS en_sum_yield,

             COUNT(ass_sorts.nr_assay_id) AS nr_count_assays,
             array_to_string(array_agg(DISTINCT ass_sorts.nr_sort_code), ',') AS nr_sorts_codes,
             SUM(ass_sorts.nr_area_seeded) AS nr_sum_area_seeded,
             SUM(ass_sorts.nr_area_harvest) AS nr_sum_area_harvest,
             SUM(ass_sorts.nr_area_harvest_forsale) AS nr_sum_area_harvest_forsale,
             SUM(ass_sorts.nr_yield) AS nr_sum_yield

         FROM ase.seed_production_cultures_groups_w_categories AS scats
                  LEFT JOIN ase.seed_production_cultures_groups_w_categories AS scats_par ON scats_par.id = scats.parent_wcat_id
                  LEFT JOIN common.cultures AS join_cultures ON join_cultures.id = scats.culture_id
                  LEFT JOIN ass_sorts ON (CASE WHEN scats.category ISNULL THEN true ELSE ass_sorts.seedprod_crop_category = scats.category END) AND ass_sorts.culture_id = scats.culture_id
             AND (CASE WHEN scats.flag_01 IS NOT NULL THEN ass_sorts.sort_sign_2 = scats.flag_01 ELSE true END)
             AND (CASE WHEN scats.flag_01_anti IS NOT NULL THEN ass_sorts.sort_sign_2 NOT IN (SELECT unnest(string_to_array(scats.flag_01_anti,','))) ELSE true END)
             AND (CASE WHEN scats.culture_season_id ISNULL THEN true ELSE scats.culture_season_id = (CASE WHEN ass_sorts.seedprod_culture_season_id ISNULL THEN join_cultures.default_season_id ELSE ass_sorts.seedprod_culture_season_id END) END)

         WHERE scats.crop_category ISNULL

         GROUP BY scats.id, scats.type, scats.parent_id, scats.countassays, scats.view_name, scats.name, scats.group_level, scats.group_crop_category, scats.stc_object_id, scats.stc_parent_id, scats.tree_parent_id, scats.cnt_cultures, scats.cnt_groups, scats.parent_wcat_id, scats_par.cnt_cultures, scats_par.cnt_groups, scats.culture_id, scats.culture_season_id, scats.category, scats.turn_all, scats.turn_in
     ),

     cultures_cats_ter AS (
         SELECT
             scats.id,
             scats.type,

             scats.parent_id,

             scats.view_name,

             scats.name,

             scats.stc_object_id,
             scats.stc_parent_id,

             scats.tree_parent_id,

             scats.group_level,
             scats.group_crop_category,

             scats.cnt_cultures,
             scats.cnt_groups,

             scats.parent_wcat_id,

             scats_par.cnt_cultures AS par_cnt_cultures,
             scats_par.cnt_groups AS par_cnt_groups,

             scats.culture_id,
             scats.category,

             scats.turn_all,
             scats.turn_in,

             join_federal_districts.turn AS ter_federal_district_turn,
             join_regions.turn AS ter_region_turn,

             join_regions.id AS ter_region_id,
             join_regions.name AS ter_region_name,
             join_federal_districts.id AS ter_federal_district_id,
             join_federal_districts.name AS ter_federal_district_name,

             (CASE WHEN (:detail_assays = true) AND (scats.culture_id IS NOT NULL) AND (scats.category IS NOT NULL) AND (COUNT(ass_sorts.id) > 0)
                       THEN
                       jsonb_agg(cast(ROW (

                           ass_sorts.id,
                           scats.id,
                           ass_sorts.date_assay,

                           ass_sorts.dep_township_name,
                           ass_sorts.dep_township_id,
                           ass_sorts.dep_region_name,
                           ass_sorts.dep_region_id,

                           ass_sorts.ter_township_name,
                           ass_sorts.ter_township_id,
                           ass_sorts.ter_region_name,
                           ass_sorts.ter_region_id,

                           ass_sorts.culture_id,
                           ass_sorts.culture_name,
                           ass_sorts.sort_code,
                           ass_sorts.sort_name,

                           ass_sorts.area_seeded,
                           ass_sorts.area_harvest,
                           ass_sorts.area_harvest_forsale,
                           ass_sorts.productivity_fact,
                           ass_sorts.yield,

                           (CASE WHEN ass_sorts.ru_assay_id ISNULL THEN false ELSE true END),
                           (CASE WHEN ass_sorts.en_assay_id ISNULL THEN false ELSE true END),
                           (CASE WHEN ass_sorts.nr_assay_id ISNULL THEN false ELSE true END)
                           ) as assay_seedprod_category_wrapper))
                   ELSE null END) AS assays,


             array_to_string(array_agg(DISTINCT ass_sorts.sort_code), ',') AS all_sorts_codes,

             COUNT(ass_sorts.id) AS count_assays,
             COUNT(ass_sorts.id_harvest) AS count_harvest_assays,

             SUM(ass_sorts.area_seeded) AS sum_all_area_seeded,
             SUM(ass_sorts.area_harvest) AS sum_all_area_harvest,
             SUM(ass_sorts.area_harvest_forsale) AS sum_all_area_harvest_forsale,
             SUM(ass_sorts.yield) AS sum_all_yield,

             COUNT(ass_sorts.ru_assay_id) AS ru_count_assays,
             array_to_string(array_agg(DISTINCT ass_sorts.ru_sort_code), ',') AS ru_sorts_codes,
             SUM(ass_sorts.ru_area_seeded) AS ru_sum_area_seeded,
             SUM(ass_sorts.ru_area_harvest) AS ru_sum_area_harvest,
             SUM(ass_sorts.ru_area_harvest_forsale) AS ru_sum_area_harvest_forsale,
             SUM(ass_sorts.ru_yield) AS ru_sum_yield,

             COUNT(ass_sorts.en_assay_id) AS en_count_assays,
             array_to_string(array_agg(DISTINCT ass_sorts.en_sort_code), ',') AS en_sorts_codes,
             SUM(ass_sorts.en_area_seeded) AS en_sum_area_seeded,
             SUM(ass_sorts.en_area_harvest) AS en_sum_area_harvest,
             SUM(ass_sorts.en_area_harvest_forsale) AS en_sum_area_harvest_forsale,
             SUM(ass_sorts.en_yield) AS en_sum_yield,

             COUNT(ass_sorts.nr_assay_id) AS nr_count_assays,
             array_to_string(array_agg(DISTINCT ass_sorts.nr_sort_code), ',') AS nr_sorts_codes,
             SUM(ass_sorts.nr_area_seeded) AS nr_sum_area_seeded,
             SUM(ass_sorts.nr_area_harvest) AS nr_sum_area_harvest,
             SUM(ass_sorts.nr_area_harvest_forsale) AS nr_sum_area_harvest_forsale,
             SUM(ass_sorts.nr_yield) AS nr_sum_yield

         FROM ase.seed_production_cultures_groups_w_categories AS scats
                  JOIN common.ter_regions AS join_regions ON true
                  LEFT JOIN common.cultures AS join_cultures ON join_cultures.id = scats.culture_id
                  LEFT JOIN common.ter_federal_districts AS join_federal_districts ON join_federal_districts.id = join_regions.federal_district_id
                  LEFT JOIN ase.seed_production_cultures_groups_w_categories AS scats_par ON scats_par.id = scats.parent_wcat_id
                  LEFT JOIN ass_sorts ON ass_sorts.ter_region_id = join_regions.id
             AND (CASE WHEN scats.category ISNULL THEN true ELSE ass_sorts.seedprod_crop_category = scats.category END) AND ass_sorts.culture_id = scats.culture_id
             AND (CASE WHEN scats.flag_01 IS NOT NULL THEN ass_sorts.sort_sign_2 = scats.flag_01 ELSE true END)
             AND (CASE WHEN scats.flag_01_anti IS NOT NULL THEN ass_sorts.sort_sign_2 NOT IN (SELECT unnest(string_to_array(scats.flag_01_anti,','))) ELSE true END)
             AND (CASE WHEN scats.culture_season_id ISNULL THEN true ELSE scats.culture_season_id = (CASE WHEN ass_sorts.seedprod_culture_season_id ISNULL THEN join_cultures.default_season_id ELSE ass_sorts.seedprod_culture_season_id END) END)

         WHERE scats.crop_category ISNULL
           AND (join_regions.exclude_monitoring ISNULL OR join_regions.exclude_monitoring = false)

         GROUP BY scats.id, scats.type, scats.parent_id, scats.countassays, scats.view_name, scats.name, scats.group_level, scats.group_crop_category, scats.stc_object_id, scats.stc_parent_id, scats.tree_parent_id, scats.cnt_cultures, scats.cnt_groups, scats.parent_wcat_id, scats_par.cnt_cultures, scats_par.cnt_groups, scats.culture_id, scats.category, scats.turn_all, scats.turn_in,
                  join_regions.turn, join_regions.id, join_regions.name, join_federal_districts.turn, join_federal_districts.id, join_federal_districts.name
     ),

     cats_raw AS (
         SELECT * FROM cultures_cats_ter WHERE (CASE WHEN :sel_type = 'KIND_REGIONS' THEN true ELSE false END)
         UNION
         SELECT * FROM cultures_cats WHERE (CASE WHEN :sel_type = 'KIND_CULTURES_GROUPS' THEN true ELSE false END)
     ),

     grp AS (
         SELECT
             (array_agg(DISTINCT com.id) || array_agg(DISTINCT com_01.id) || array_agg(DISTINCT com_02.id) || array_agg(DISTINCT com_03.id) || array_agg(DISTINCT com_04.id) || array_agg(DISTINCT com_05.id) || array_agg(DISTINCT com_06.id)) AS com_ids
         FROM ase.seed_production_cultures_groups_w_categories AS com
                  LEFT JOIN ase.seed_production_cultures_groups_w_categories AS com_01 ON com_01.parent_wcat_id = com.id OR com_01.tree_parent_id = com.id
                  LEFT JOIN ase.seed_production_cultures_groups_w_categories AS com_02 ON com_02.parent_wcat_id = com_01.id OR com_02.tree_parent_id = com_01.id
                  LEFT JOIN ase.seed_production_cultures_groups_w_categories AS com_03 ON com_03.parent_wcat_id = com_02.id OR com_03.tree_parent_id = com_02.id
                  LEFT JOIN ase.seed_production_cultures_groups_w_categories AS com_04 ON com_04.parent_wcat_id = com_03.id OR com_04.tree_parent_id = com_03.id
                  LEFT JOIN ase.seed_production_cultures_groups_w_categories AS com_05 ON com_05.parent_wcat_id = com_04.id OR com_05.tree_parent_id = com_04.id
                  LEFT JOIN ase.seed_production_cultures_groups_w_categories AS com_06 ON com_06.parent_wcat_id = com_05.id OR com_06.tree_parent_id = com_05.id
         WHERE com.id = :need_id
         GROUP BY com.id
     ),

     grpn AS (
         SELECT uid FROM (SELECT DISTINCT unnest(grp.com_ids) AS uid FROM grp) AS uids
         WHERE uid IS NOT NULL
     ),

     cultures_cats_all AS (
         SELECT
             cats_all.id,
             cats_all.type,

             cats_all.parent_id,

             cats_all.view_name,

             cats_all.name,

             cats_all.stc_object_id,
             cats_all.stc_parent_id,

             cats_all.tree_parent_id,

             cats_all.cnt_cultures,
             cats_all.cnt_groups,

             cats_all.group_level,
             cats_all.group_crop_category,

             cats_all.parent_wcat_id,

             cats_all.par_cnt_cultures,
             cats_all.par_cnt_groups,

             cats_all.culture_id,
             cats_all.category,

             cats_all.turn_all,
             cats_all.turn_in,

             cats_all.ter_federal_district_turn,
             cats_all.ter_region_turn,

             cats_all.ter_region_id, cats_all.ter_region_name, cats_all.ter_federal_district_id, cats_all.ter_federal_district_name,


             cats_all.assays,

             (CASE WHEN cats_all.all_sorts_codes isnull THEN '' ELSE cats_all.all_sorts_codes||',' END) || coalesce(string_agg(cats_m1.all_sorts_codes, ','),'') || coalesce(string_agg(cats_m2.all_sorts_codes, ','),'') || coalesce(string_agg(cats_m3.all_sorts_codes, ','),'') || coalesce(string_agg(cats_m4.all_sorts_codes, ','),'') || coalesce(string_agg(cats_m5.all_sorts_codes, ','),'') AS all_sorts_codes,

             coalesce(SUM(cats_all.count_assays), 0) + coalesce(SUM(cats_m1.count_assays), 0) + coalesce(SUM(cats_m2.count_assays), 0) + coalesce(SUM(cats_m3.count_assays), 0) + coalesce(SUM(cats_m4.count_assays), 0) + coalesce(SUM(cats_m5.count_assays), 0) AS count_assays,
             coalesce(SUM(cats_all.count_harvest_assays), 0) + coalesce(SUM(cats_m1.count_harvest_assays), 0) + coalesce(SUM(cats_m2.count_harvest_assays), 0) + coalesce(SUM(cats_m3.count_harvest_assays), 0) + coalesce(SUM(cats_m4.count_harvest_assays), 0) + coalesce(SUM(cats_m5.count_harvest_assays), 0) AS count_harvest_assays,

             coalesce(SUM(cats_all.sum_all_area_seeded), 0.0) + coalesce(SUM(cats_m1.sum_all_area_seeded), 0.0) + coalesce(SUM(cats_m2.sum_all_area_seeded), 0.0) + coalesce(SUM(cats_m3.sum_all_area_seeded), 0.0) + coalesce(SUM(cats_m4.sum_all_area_seeded), 0.0) + coalesce(SUM(cats_m5.sum_all_area_seeded), 0.0) AS sum_all_area_seeded,
             coalesce(SUM(cats_all.sum_all_area_harvest), 0.0) + coalesce(SUM(cats_m1.sum_all_area_harvest), 0.0) + coalesce(SUM(cats_m2.sum_all_area_harvest), 0.0) + coalesce(SUM(cats_m3.sum_all_area_harvest), 0.0) + coalesce(SUM(cats_m4.sum_all_area_harvest), 0.0) + coalesce(SUM(cats_m5.sum_all_area_harvest), 0.0) AS sum_all_area_harvest,
             coalesce(SUM(cats_all.sum_all_area_harvest_forsale), 0.0) + coalesce(SUM(cats_m1.sum_all_area_harvest_forsale), 0.0) + coalesce(SUM(cats_m2.sum_all_area_harvest_forsale), 0.0) + coalesce(SUM(cats_m3.sum_all_area_harvest_forsale), 0.0) + coalesce(SUM(cats_m4.sum_all_area_harvest_forsale), 0.0) + coalesce(SUM(cats_m5.sum_all_area_harvest_forsale), 0.0) AS sum_all_area_harvest_forsale,
             coalesce(SUM(cats_all.sum_all_yield), 0.0) + coalesce(SUM(cats_m1.sum_all_yield), 0.0) + coalesce(SUM(cats_m2.sum_all_yield), 0.0) + coalesce(SUM(cats_m3.sum_all_yield), 0.0) + coalesce(SUM(cats_m4.sum_all_yield), 0.0) + coalesce(SUM(cats_m5.sum_all_yield), 0.0) AS sum_all_yield,

             coalesce(SUM(cats_all.ru_count_assays), 0) + coalesce(SUM(cats_m1.ru_count_assays), 0) + coalesce(SUM(cats_m2.ru_count_assays), 0) + coalesce(SUM(cats_m3.ru_count_assays), 0) + coalesce(SUM(cats_m4.ru_count_assays), 0) + coalesce(SUM(cats_m5.ru_count_assays), 0) AS ru_count_assays,
             (CASE WHEN cats_all.ru_sorts_codes isnull THEN '' ELSE cats_all.ru_sorts_codes||',' END) || coalesce(string_agg(cats_m1.ru_sorts_codes, ','),'') || coalesce(string_agg(cats_m2.ru_sorts_codes, ','),'') || coalesce(string_agg(cats_m3.ru_sorts_codes, ','),'') || coalesce(string_agg(cats_m4.ru_sorts_codes, ','),'') || coalesce(string_agg(cats_m5.ru_sorts_codes, ','),'') AS ru_sorts_codes,
             coalesce(SUM(cats_all.ru_sum_area_seeded), 0.0) + coalesce(SUM(cats_m1.ru_sum_area_seeded), 0.0) + coalesce(SUM(cats_m2.ru_sum_area_seeded), 0.0) + coalesce(SUM(cats_m3.ru_sum_area_seeded), 0.0) + coalesce(SUM(cats_m4.ru_sum_area_seeded), 0.0) + coalesce(SUM(cats_m5.ru_sum_area_seeded), 0.0) AS ru_sum_area_seeded,
             coalesce(SUM(cats_all.ru_sum_area_harvest), 0.0) + coalesce(SUM(cats_m1.ru_sum_area_harvest), 0.0) + coalesce(SUM(cats_m2.ru_sum_area_harvest), 0.0) + coalesce(SUM(cats_m3.ru_sum_area_harvest), 0.0) + coalesce(SUM(cats_m4.ru_sum_area_harvest), 0.0) + coalesce(SUM(cats_m5.ru_sum_area_harvest), 0.0) AS ru_sum_area_harvest,
             coalesce(SUM(cats_all.ru_sum_area_harvest_forsale), 0.0) + coalesce(SUM(cats_m1.ru_sum_area_harvest_forsale), 0.0) + coalesce(SUM(cats_m2.ru_sum_area_harvest_forsale), 0.0) + coalesce(SUM(cats_m3.ru_sum_area_harvest_forsale), 0.0) + coalesce(SUM(cats_m4.ru_sum_area_harvest_forsale), 0.0) + coalesce(SUM(cats_m5.ru_sum_area_harvest_forsale), 0.0) AS ru_sum_area_harvest_forsale,
             coalesce(SUM(cats_all.ru_sum_yield), 0.0) + coalesce(SUM(cats_m1.ru_sum_yield), 0.0) + coalesce(SUM(cats_m2.ru_sum_yield), 0.0) + coalesce(SUM(cats_m3.ru_sum_yield), 0.0) + coalesce(SUM(cats_m4.ru_sum_yield), 0.0) + coalesce(SUM(cats_m5.ru_sum_yield), 0.0) AS ru_sum_yield,

             coalesce(SUM(cats_all.en_count_assays), 0) + coalesce(SUM(cats_m1.en_count_assays), 0) + coalesce(SUM(cats_m2.en_count_assays), 0) + coalesce(SUM(cats_m3.en_count_assays), 0) + coalesce(SUM(cats_m4.en_count_assays), 0) + coalesce(SUM(cats_m5.en_count_assays), 0) AS en_count_assays,
             (CASE WHEN cats_all.en_sorts_codes isnull THEN '' ELSE cats_all.en_sorts_codes||',' END) || coalesce(string_agg(cats_m1.en_sorts_codes, ','),'') || coalesce(string_agg(cats_m2.en_sorts_codes, ','),'') || coalesce(string_agg(cats_m3.en_sorts_codes, ','),'') || coalesce(string_agg(cats_m4.en_sorts_codes, ','),'') || coalesce(string_agg(cats_m5.en_sorts_codes, ','),'') AS en_sorts_codes,
             coalesce(SUM(cats_all.en_sum_area_seeded), 0.0) + coalesce(SUM(cats_m1.en_sum_area_seeded), 0.0) + coalesce(SUM(cats_m2.en_sum_area_seeded), 0.0) + coalesce(SUM(cats_m3.en_sum_area_seeded), 0.0) + coalesce(SUM(cats_m4.en_sum_area_seeded), 0.0) + coalesce(SUM(cats_m5.en_sum_area_seeded), 0.0) AS en_sum_area_seeded,
             coalesce(SUM(cats_all.en_sum_area_harvest), 0.0) + coalesce(SUM(cats_m1.en_sum_area_harvest), 0.0) + coalesce(SUM(cats_m2.en_sum_area_harvest), 0.0) + coalesce(SUM(cats_m3.en_sum_area_harvest), 0.0) + coalesce(SUM(cats_m4.en_sum_area_harvest), 0.0) + coalesce(SUM(cats_m5.en_sum_area_harvest), 0.0) AS en_sum_area_harvest,
             coalesce(SUM(cats_all.en_sum_area_harvest_forsale), 0.0) + coalesce(SUM(cats_m1.en_sum_area_harvest_forsale), 0.0) + coalesce(SUM(cats_m2.en_sum_area_harvest_forsale), 0.0) + coalesce(SUM(cats_m3.en_sum_area_harvest_forsale), 0.0) + coalesce(SUM(cats_m4.en_sum_area_harvest_forsale), 0.0) + coalesce(SUM(cats_m5.en_sum_area_harvest_forsale), 0.0) AS en_sum_area_harvest_forsale,
             coalesce(SUM(cats_all.en_sum_yield), 0.0) + coalesce(SUM(cats_m1.en_sum_yield), 0.0) + coalesce(SUM(cats_m2.en_sum_yield), 0.0) + coalesce(SUM(cats_m3.en_sum_yield), 0.0) + coalesce(SUM(cats_m4.en_sum_yield), 0.0) + coalesce(SUM(cats_m5.en_sum_yield), 0.0) AS en_sum_yield,

             coalesce(SUM(cats_all.nr_count_assays), 0) + coalesce(SUM(cats_m1.nr_count_assays), 0) + coalesce(SUM(cats_m2.nr_count_assays), 0) + coalesce(SUM(cats_m3.nr_count_assays), 0) + coalesce(SUM(cats_m4.nr_count_assays), 0) + coalesce(SUM(cats_m5.nr_count_assays), 0) AS nr_count_assays,
             (CASE WHEN cats_all.nr_sorts_codes isnull THEN '' ELSE cats_all.nr_sorts_codes||',' END) || coalesce(string_agg(cats_m1.nr_sorts_codes, ','),'') || coalesce(string_agg(cats_m2.nr_sorts_codes, ','),'') || coalesce(string_agg(cats_m3.nr_sorts_codes, ','),'') || coalesce(string_agg(cats_m4.nr_sorts_codes, ','),'') || coalesce(string_agg(cats_m5.nr_sorts_codes, ','),'') AS nr_sorts_codes,
             coalesce(SUM(cats_all.nr_sum_area_seeded), 0.0) + coalesce(SUM(cats_m1.nr_sum_area_seeded), 0.0) + coalesce(SUM(cats_m2.nr_sum_area_seeded), 0.0) + coalesce(SUM(cats_m3.nr_sum_area_seeded), 0.0) + coalesce(SUM(cats_m4.nr_sum_area_seeded), 0.0) + coalesce(SUM(cats_m5.nr_sum_area_seeded), 0.0) AS nr_sum_area_seeded,
             coalesce(SUM(cats_all.nr_sum_area_harvest), 0.0) + coalesce(SUM(cats_m1.nr_sum_area_harvest), 0.0) + coalesce(SUM(cats_m2.nr_sum_area_harvest), 0.0) + coalesce(SUM(cats_m3.nr_sum_area_harvest), 0.0) + coalesce(SUM(cats_m4.nr_sum_area_harvest), 0.0) + coalesce(SUM(cats_m5.nr_sum_area_harvest), 0.0) AS nr_sum_area_harvest,
             coalesce(SUM(cats_all.nr_sum_area_harvest_forsale), 0.0) + coalesce(SUM(cats_m1.nr_sum_area_harvest_forsale), 0.0) + coalesce(SUM(cats_m2.nr_sum_area_harvest_forsale), 0.0) + coalesce(SUM(cats_m3.nr_sum_area_harvest_forsale), 0.0) + coalesce(SUM(cats_m4.nr_sum_area_harvest_forsale), 0.0) + coalesce(SUM(cats_m5.nr_sum_area_harvest_forsale), 0.0) AS nr_sum_area_harvest_forsale,
             coalesce(SUM(cats_all.nr_sum_yield), 0.0) + coalesce(SUM(cats_m1.nr_sum_yield), 0.0) + coalesce(SUM(cats_m2.nr_sum_yield), 0.0) + coalesce(SUM(cats_m3.nr_sum_yield), 0.0) + coalesce(SUM(cats_m4.nr_sum_yield), 0.0) + coalesce(SUM(cats_m5.nr_sum_yield), 0.0) AS nr_sum_yield

         FROM cats_raw AS cats_all
                  LEFT JOIN cats_raw AS cats_m1 ON cats_m1.parent_wcat_id = cats_all.id AND cats_m1.ter_region_id = cats_all.ter_region_id
                  LEFT JOIN cats_raw AS cats_m2 ON cats_m2.parent_wcat_id = cats_m1.id AND cats_m2.ter_region_id = cats_m1.ter_region_id
                  LEFT JOIN cats_raw AS cats_m3 ON cats_m3.parent_wcat_id = cats_m2.id AND cats_m3.ter_region_id = cats_m2.ter_region_id
                  LEFT JOIN cats_raw AS cats_m4 ON cats_m4.parent_wcat_id = cats_m3.id AND cats_m4.ter_region_id = cats_m3.ter_region_id
                  LEFT JOIN cats_raw AS cats_m5 ON cats_m5.parent_wcat_id = cats_m4.id AND cats_m5.ter_region_id = cats_m4.ter_region_id

         WHERE (CASE WHEN :need_id ISNULL THEN true ELSE cats_all.id IN (SELECT * FROM grpn) OR cats_all.tree_parent_id IN (SELECT * FROM grpn) END)

         GROUP BY cats_all.id, cats_all.type, cats_all.assays, cats_all.all_sorts_codes, cats_all.ru_sorts_codes, cats_all.en_sorts_codes, cats_all.nr_sorts_codes, cats_all.parent_id, cats_all.view_name, cats_all.turn_all, cats_all.turn_in, cats_all.name, cats_all.group_level, cats_all.group_crop_category, cats_all.parent_wcat_id, cats_all.par_cnt_cultures, cats_all.par_cnt_groups, cats_all.stc_object_id, cats_all.stc_parent_id, cats_all.tree_parent_id, cats_all.cnt_cultures, cats_all.cnt_groups, cats_all.culture_id, cats_all.category, cats_all.turn_all, cats_all.turn_in,
                  cats_all.ter_federal_district_turn, cats_all.ter_region_turn, cats_all.ter_region_id, cats_all.ter_region_name, cats_all.ter_federal_district_id, cats_all.ter_federal_district_name
     ),



     sel_solv AS (
         SELECT
             id,
             type,

             parent_id,

             view_name,

             name,

             (SELECT array_agg(DISTINCT unnest) FROM unnest(string_to_array(all_sorts_codes,',')) WHERE unnest IS DISTINCT FROM '' AND unnest IS NOT null) AS all_sorts_codes,

             stc_object_id,
             stc_parent_id,

             tree_parent_id,

             cnt_cultures,
             cnt_groups,

             group_level,
             group_crop_category,

             parent_wcat_id,

             par_cnt_cultures,
             par_cnt_groups,

             culture_id,
             category,

             turn_all,
             turn_in,

             ter_federal_district_turn,
             ter_region_turn,

             ter_region_id, ter_region_name, ter_federal_district_id, ter_federal_district_name,

             assays,

             count_assays,
             count_harvest_assays,

             sum_all_area_seeded,
             sum_all_area_harvest,
             sum_all_area_harvest_forsale,
             sum_all_yield,

             ru_count_assays,
             (SELECT array_agg(DISTINCT unnest) FROM unnest(string_to_array(ru_sorts_codes,',')) WHERE unnest IS DISTINCT FROM '' AND unnest IS NOT null) AS ru_sorts_codes,
             ru_sum_area_seeded,
             ru_sum_area_harvest,
             ru_sum_area_harvest_forsale,
             ru_sum_yield,

             en_count_assays,
             (SELECT array_agg(DISTINCT unnest) FROM unnest(string_to_array(en_sorts_codes,',')) WHERE unnest IS DISTINCT FROM '' AND unnest IS NOT null) AS en_sorts_codes,
             en_sum_area_seeded,
             en_sum_area_harvest,
             en_sum_area_harvest_forsale,
             en_sum_yield,

             nr_count_assays,
             (SELECT array_agg(DISTINCT unnest) FROM unnest(string_to_array(nr_sorts_codes,',')) WHERE unnest IS DISTINCT FROM '' AND unnest IS NOT null) AS nr_sorts_codes,
             nr_sum_area_seeded,
             nr_sum_area_harvest,
             nr_sum_area_harvest_forsale,
             nr_sum_yield
         FROM cultures_cats_all
         WHERE NOT (type = 'category' AND (count_assays isnull OR count_assays = 0))
         ORDER BY ter_federal_district_turn, ter_region_turn, turn_all, turn_in
     ),

     regions_solv AS (
         SELECT
             join_ter_regions.id,
             'region' AS type,

             join_ter_regions.federal_district_id AS parent_id,

             join_ter_regions.name_short || ' - ' || gps.name AS view_name,
             join_ter_regions.name_short || ' - ' || gps.name AS name,

             all_sorts_codes,

             stc_object_id,
             stc_parent_id,

             join_ter_regions.federal_district_id AS tree_parent_id,

             cnt_cultures,
             cnt_groups,

             group_level,
             group_crop_category,

             parent_wcat_id,

             par_cnt_cultures,
             par_cnt_groups,

             culture_id,
             category,

             0 AS turn_all,
             0 AS turn_in,

             join_ter_federal_districts.turn AS ter_federal_district_turn,
             join_ter_regions.turn AS ter_region_turn,

             join_ter_regions.id AS ter_region_id,
             join_ter_regions.name AS ter_region_name,
             join_ter_federal_districts.id AS ter_federal_district_id,
             join_ter_federal_districts.name AS ter_federal_district_name,

             assays,

             count_assays,
             count_harvest_assays,

             sum_all_area_seeded,
             sum_all_area_harvest,
             sum_all_area_harvest_forsale,
             sum_all_yield,

             ru_count_assays,
             ru_sorts_codes,
             ru_sum_area_seeded,
             ru_sum_area_harvest,
             ru_sum_area_harvest_forsale,
             ru_sum_yield,

             en_count_assays,
             en_sorts_codes,
             en_sum_area_seeded,
             en_sum_area_harvest,
             en_sum_area_harvest_forsale,
             en_sum_yield,

             nr_count_assays,
             nr_sorts_codes,
             nr_sum_area_seeded,
             nr_sum_area_harvest,
             nr_sum_area_harvest_forsale,
             nr_sum_yield

         FROM sel_solv AS gps
                  LEFT JOIN common.ter_regions AS join_ter_regions ON gps.ter_region_id = join_ter_regions.id
                  LEFT JOIN common.ter_federal_districts AS join_ter_federal_districts ON join_ter_federal_districts.id = join_ter_regions.federal_district_id

         WHERE gps.id = (CASE WHEN :need_id ISNULL THEN 0 ELSE :need_id END) AND (join_ter_regions.exclude_monitoring ISNULL OR join_ter_regions.exclude_monitoring = false)
         ORDER BY join_ter_federal_districts.turn, join_ter_regions.turn
     ),

     districts_solv AS (
         SELECT
             dts.id AS id,
             'district' AS type,

             0 AS parent_id,

             dts.name_short AS view_name,
             dts.name_short AS name,

             (SELECT array_agg(DISTINCT unnest) FROM unnest(string_to_array(coalesce(string_agg(array_to_string(all_sorts_codes,','), ','),''),',')) WHERE unnest IS DISTINCT FROM '' AND unnest IS NOT null) AS all_sorts_codes,

             0 AS stc_object_id,
             0 AS stc_parent_id,

             0 AS tree_parent_id,

             cast(null AS bigint) AS cnt_cultures,
             cast(null AS bigint) AS cnt_groups,

             1 AS group_level,
             cast(null AS text) AS group_crop_category,

             cast(null AS bigint) AS parent_wcat_id,

             cast(null AS bigint) AS par_cnt_cultures,
             cast(null AS bigint) AS par_cnt_groups,

             cast(null AS bigint) AS culture_id,
             cast(null AS text) AS category,

             0 AS turn_all,
             0 AS turn_in,

             dts.turn AS ter_federal_district_turn,
             0 AS ter_region_turn,

             cast(null AS bigint) AS ter_region_id,
             cast(null AS text) AS ter_region_name,
             dts.id AS ter_federal_district_id,
             dts.name AS ter_federal_district_name,

             cast(null AS jsonb) AS assays,

             SUM(count_assays) AS count_assays,
             SUM(count_harvest_assays) AS count_harvest_assays,

             SUM(sum_all_area_seeded) AS sum_all_area_seeded,
             SUM(sum_all_area_harvest) AS sum_all_area_harvest,
             SUM(sum_all_area_harvest_forsale) AS sum_all_area_harvest_forsale,
             SUM(sum_all_yield) AS sum_all_yield,

             SUM(ru_count_assays) AS ru_count_assays,
             (SELECT array_agg(DISTINCT unnest) FROM unnest(string_to_array(coalesce(string_agg(array_to_string(ru_sorts_codes,','), ','),''),',')) WHERE unnest IS DISTINCT FROM '' AND unnest IS NOT null) AS ru_sorts_codes,
             SUM(ru_sum_area_seeded) AS ru_sum_area_seeded,
             SUM(ru_sum_area_harvest) AS ru_sum_area_harvest,
             SUM(ru_sum_area_harvest_forsale) AS ru_sum_area_harvest_forsale,
             SUM(ru_sum_yield) AS ru_sum_yield,

             SUM(en_count_assays) AS en_count_assays,
             (SELECT array_agg(DISTINCT unnest) FROM unnest(string_to_array(coalesce(string_agg(array_to_string(en_sorts_codes,','), ','),''),',')) WHERE unnest IS DISTINCT FROM '' AND unnest IS NOT null) AS en_sorts_codes,
             SUM(en_sum_area_seeded) AS en_sum_area_seeded,
             SUM(en_sum_area_harvest) AS en_sum_area_harvest,
             SUM(en_sum_area_harvest_forsale) AS en_sum_area_harvest_forsale,
             SUM(en_sum_yield) AS en_sum_yield,

             SUM(nr_count_assays) AS nr_count_assays,
             (SELECT array_agg(DISTINCT unnest) FROM unnest(string_to_array(coalesce(string_agg(array_to_string(nr_sorts_codes,','), ','),''),',')) WHERE unnest IS DISTINCT FROM '' AND unnest IS NOT null) AS nr_sorts_codes,
             SUM(nr_sum_area_seeded) AS nr_sum_area_seeded,
             SUM(nr_sum_area_harvest) AS nr_sum_area_harvest,
             SUM(nr_sum_area_harvest_forsale) AS nr_sum_area_harvest_forsale,
             SUM(nr_sum_yield) AS nr_sum_yield

         FROM common.ter_federal_districts AS dts
                  LEFT JOIN regions_solv AS rgs ON rgs.ter_federal_district_id = dts.id

         GROUP BY dts.id, dts.turn, dts.name_short
         ORDER BY dts.turn
     ),

     country_solv AS (
         SELECT
             0              AS id,
             'country'          AS type,

             0 AS parent_id,

             'РФ' AS view_name,
             'РФ' AS name,

             (SELECT array_agg(DISTINCT unnest) FROM unnest(string_to_array(coalesce(string_agg(array_to_string(all_sorts_codes,','), ','),''),',')) WHERE unnest IS DISTINCT FROM '' AND unnest IS NOT null) AS all_sorts_codes,

             0 AS stc_object_id,
             0 AS stc_parent_id,

             0 AS tree_parent_id,

             cast(null AS bigint) AS cnt_cultures,
             cast(null AS bigint) AS cnt_groups,

             0 AS group_level,
             cast(null AS text) AS group_crop_category,

             cast(null AS bigint) AS parent_wcat_id,

             cast(null AS bigint) AS par_cnt_cultures,
             cast(null AS bigint) AS par_cnt_groups,

             cast(null AS bigint) AS culture_id,
             cast(null AS text) AS category,

             0 AS turn_all,
             0 AS turn_in,

             0 AS ter_federal_district_turn,
             0 AS ter_region_turn,

             cast(null AS bigint) AS ter_region_id,
             cast(null AS text) AS ter_region_name,
             cast(null AS bigint) AS ter_federal_district_id,
             cast(null AS text) AS ter_federal_district_name,

             cast(null AS jsonb) AS assays,

             SUM(count_assays) AS count_assays,
             SUM(count_harvest_assays) AS count_harvest_assays,

             SUM(sum_all_area_seeded) AS sum_all_area_seeded,
             SUM(sum_all_area_harvest) AS sum_all_area_harvest,
             SUM(sum_all_area_harvest_forsale) AS sum_all_area_harvest_forsale,
             SUM(sum_all_yield) AS sum_all_yield,

             SUM(ru_count_assays) AS ru_count_assays,
             (SELECT array_agg(DISTINCT unnest) FROM unnest(string_to_array(coalesce(string_agg(array_to_string(ru_sorts_codes,','), ','),''),',')) WHERE unnest IS DISTINCT FROM '' AND unnest IS NOT null) AS ru_sorts_codes,
             SUM(ru_sum_area_seeded) AS ru_sum_area_seeded,
             SUM(ru_sum_area_harvest) AS ru_sum_area_harvest,
             SUM(ru_sum_area_harvest_forsale) AS ru_sum_area_harvest_forsale,
             SUM(ru_sum_yield) AS ru_sum_yield,

             SUM(en_count_assays) AS en_count_assays,
             (SELECT array_agg(DISTINCT unnest) FROM unnest(string_to_array(coalesce(string_agg(array_to_string(en_sorts_codes,','), ','),''),',')) WHERE unnest IS DISTINCT FROM '' AND unnest IS NOT null) AS en_sorts_codes,
             SUM(en_sum_area_seeded) AS en_sum_area_seeded,
             SUM(en_sum_area_harvest) AS en_sum_area_harvest,
             SUM(en_sum_area_harvest_forsale) AS en_sum_area_harvest_forsale,
             SUM(en_sum_yield) AS en_sum_yield,

             SUM(nr_count_assays) AS nr_count_assays,
             (SELECT array_agg(DISTINCT unnest) FROM unnest(string_to_array(coalesce(string_agg(array_to_string(nr_sorts_codes,','), ','),''),',')) WHERE unnest IS DISTINCT FROM '' AND unnest IS NOT null) AS nr_sorts_codes,
             SUM(nr_sum_area_seeded) AS nr_sum_area_seeded,
             SUM(nr_sum_area_harvest) AS nr_sum_area_harvest,
             SUM(nr_sum_area_harvest_forsale) AS nr_sum_area_harvest_forsale,
             SUM(nr_sum_yield) AS nr_sum_yield
         FROM regions_solv AS rgs
     ),

     all_solv AS (
         SELECT * FROM sel_solv
         UNION ALL
         SELECT * FROM country_solv
         UNION ALL
         SELECT * FROM districts_solv
         UNION ALL
         SELECT * FROM regions_solv
     ),

     result_query AS (
         SELECT row_number() over () AS rid, all_solv.* FROM all_solv WHERE (CASE WHEN :sel_type = 'KIND_REGIONS' THEN true ELSE false END)
         UNION
         SELECT row_number() over () AS rid, sel_solv.* FROM sel_solv WHERE (CASE WHEN :sel_type = 'KIND_CULTURES_GROUPS' THEN true ELSE false END)
     )

SELECT
    result_query.*
FROM result_query
ORDER BY ter_federal_district_turn, ter_region_turn, turn_all, turn_in
