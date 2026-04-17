WITH bfs_states AS (
    SELECT
        sbs.id,
        sbs.seed_back_fill_id,
        sbs.status,

--         (CASE WHEN sbs.status = 'SOWN' AND DATE_PART('year', sbs.date_finished) >= (:harvest_year - 1) THEN sbs.volume ELSE null END) AS volume_SOWN,
--         (CASE WHEN sbs.status = 'SOWN' AND DATE_PART('year', sbs.date_finished) < (:harvest_year - 1) THEN sbs.volume ELSE null END) AS volume_SOWN_old,

        (CASE WHEN sbs.status = 'SOWN' THEN sbs.volume ELSE null END) AS volume_SOWN,

        (CASE WHEN sbs.status = 'DELETED' THEN sbs.volume ELSE null END) AS volume_DELETED,
        (CASE WHEN sbs.status = 'SOLID' THEN sbs.volume ELSE null END) AS volume_SOLID,
        (CASE WHEN sbs.status = 'DEFECTED' THEN sbs.volume ELSE null END) AS volume_DEFECTED,
        (CASE WHEN sbs.status = 'FED' THEN sbs.volume ELSE null END) AS volume_FED,
        (CASE WHEN sbs.status = 'REFORMED' THEN sbs.volume ELSE null END) AS volume_REFORMED,
        (CASE WHEN sbs.status = 'TRANS_FUND' THEN sbs.volume ELSE null END) AS volume_TRANS_FUND

    FROM ase.seeds_backfills_state AS sbs
    WHERE sbs.deleted ISNULL
),

--      info_bfs_pre AS (
--          SELECT
--              null AS backfill_id,
--              id AS info_id,
--
--              (CASE WHEN :sel_type = 'KIND_CULTURES_GROUPS' THEN 0 ELSE ter_federal_district_turn END) AS ter_federal_district_turn,
--              (CASE WHEN :sel_type = 'KIND_CULTURES_GROUPS' THEN 0 ELSE ter_region_turn END) AS ter_region_turn,
--
--              (CASE WHEN :sel_type = 'KIND_CULTURES_GROUPS' THEN 0 ELSE ter_region_id END) AS ter_region_id,
--              (CASE WHEN :sel_type = 'KIND_CULTURES_GROUPS' THEN '' ELSE ter_region_name END) AS ter_region_name,
--              (CASE WHEN :sel_type = 'KIND_CULTURES_GROUPS' THEN 0 ELSE ter_federal_district_id END) AS ter_federal_district_id,
--              (CASE WHEN :sel_type = 'KIND_CULTURES_GROUPS' THEN '' ELSE ter_federal_district_name END) AS ter_federal_district_name,
--
--              ter_region_name AS nat_ter_region_name,
--              concat(to_char(date , 'dd.MM.YY'), ' ', ter_township_name, ' (', dep_township_name, ')')  AS view_tmp_name,
--
--              culture_name,
--              culture_id, culture_season_id, culture_kind_plant_material,
--
--              cast(null AS bigint) AS culture_mix_id,
--              cast(null AS text) AS culture_mix_name,
--              cast(null AS bigint) AS culture_mix_season_id,
--
--              sort_code,
--              sort_name,
--              sort_sign_2,
--
--              sort_type,
--              sort_region,
--              sort_year,
--
--              sort_originator_main,
--              sort_originator_main_name,
--              sort_originator_main_country_code_iso,
--              sort_originator_main_country_name,
--              sort_country_code_iso,
--              sort_country_name,
--
--              seedprod_seeds_kind,
--
--              fill_seed_fund AS fill_all,
--              fill_seed_fund,
--              0 AS fill_seed_for_sale,
--              0 AS fill_removed_seed_fund,
--
--              0 AS fill_AVALIABLE,
--
--              0 AS fill_SOWN,
--              0 AS fill_DELETED,
--              0 AS fill_SOLD,
--              0 AS fill_DEFECTED,
--              0 AS fill_FED,
--              0 AS fill_REFORMED,
--
--              null AS ru_backfill_ids,
--              (CASE WHEN sort_ru = 1 THEN fill_seed_fund ELSE null END) AS ru_fill_all,
--              (CASE WHEN sort_ru = 1 AND sort_localized THEN fill_seed_fund ELSE null END) AS ru_fill_localized,
--              0 AS ru_fill_purchase_en,
--
--              null AS en_backfill_id,
--              (CASE WHEN sort_en = 1 THEN fill_seed_fund ELSE null END) AS en_fill_all,
--              (CASE WHEN sort_en = 1 AND sort_localized THEN fill_seed_fund ELSE null END) AS en_fill_localized,
--              0 AS en_fill_purchase_en,
--
--              null AS nr_backfill_id,
--              (CASE WHEN sort_ru = 0 AND sort_en = 0 THEN fill_seed_fund ELSE null END) AS nr_fill_all,
--              0 AS nr_fill_purchase_en,
--
--              rp_NC,
--              rp_OS,
--              rp_ES,
--
--              rp_RS_1,
--              rp_RS_1_RST,
--              rp_RS_2,
--              rp_RS_2_RST,
--              rp_RS_3,
--              rp_RS_3_RST,
--              rp_RS_4,
--              rp_RS_4_RST,
--              rp_F1,
--              rp_RS_5B,
--              rp_RS_5B_RST,
--              rp_NOT,
--
--              sort_localized,
--              sort_ru,
--              sort_en,
--
--              fill_purchase_en
--
--          FROM
--              (SELECT
--                   ifs.id,
--
--                   township_id          AS ter_township_id,
--                   join_ter_township.name       AS ter_township_name,
--
--                   join_ter_region.id        AS ter_region_id,
--                   join_ter_region.name       AS ter_region_name,
--                   join_ter_region.federal_district_id    AS ter_federal_district_id,
--                   join_ter_federal_districts.name     AS ter_federal_district_name,
--
--                   join_ter_federal_districts.turn     AS ter_federal_district_turn,
--                   join_ter_region.turn       AS ter_region_turn,
--
--                   department_id         AS dep_township_id,
--                   join_dep_township.name_short     AS dep_township_name,
--                   join_dep_township.parent_id      AS dep_region_id,
--                   join_dep_region.name_short      AS dep_region_name,
--                   join_dep_region.ter_region_id     AS dep_ter_region_id,
--                   join_dep_region_ter_region.federal_district_id AS dep_ter_federal_district_id,
--
--
--                   join_cultures.name AS culture_name,
--                   join_sorts.culture_id AS culture_id,
--                   COALESCE(join_cultures.default_season_id, join_cultures.culture_season_id, 1) AS culture_season_id,
--                   COALESCE(join_cultures.default_kind_plant_material, 'SEEDS') AS culture_kind_plant_material,
--
--                   culture_sort_code AS sort_code,
--                   join_sorts.name AS sort_name,
--                   join_sorts.type AS sort_type,
--                   join_sorts.sign_2 AS sort_sign_2,
--                   join_sorts.region AS sort_region,
--                   join_sorts.year AS sort_year,
--
--                   join_sorts.originator_main AS sort_originator_main,
--                   join_sorts_contractors.title AS sort_originator_main_name,
--                   join_sorts_contractors.country_code_iso AS sort_originator_main_country_code_iso,
--                   join_sorts_contractors_country.name_short AS sort_originator_main_country_name,
--                   join_sorts.country_code_iso AS sort_country_code_iso,
--                   join_sorts_country.name_short AS sort_country_name,
--
--                   seeds_kind_enum AS seedprod_seeds_kind,
--
--                   date,
--                   fill   AS fill_seed_fund,
--                   import_total AS fill_purchase_en,
--
--                   rnns   AS rp_NC,
--                   os    AS rp_OS,
--                   es    AS rp_ES,
--
--                   rs1    AS rp_RS_1,
--                   rs1_rst   AS rp_RS_1_RST,
--                   rs2    AS rp_RS_2,
--                   rs2_rst   AS rp_RS_2_RST,
--                   rs3    AS rp_RS_3,
--                   rs3_rst   AS rp_RS_3_RST,
--                   rs4    AS rp_RS_4,
--                   rs4_rst   AS rp_RS_4_RST,
--                   f1    AS rp_F1,
--                   rs5    AS rp_RS_5B,
--                   rs5_rst   AS rp_RS_5B_RST,
--
--                   undefined_cat AS rp_NOT,
--
--                   (CASE WHEN join_sorts.region isnull OR trim(join_sorts.region) = '' THEN false ELSE
--                       (CASE WHEN join_sorts.region LIKE '*' THEN true ELSE
--                           (CASE WHEN CAST(join_sorts_regions_regions.culture_sort_region_id AS text) IN (SELECT trim(unnest(string_to_array(join_sorts.region,',')))) THEN true ELSE
--                               false
--                               END)
--                           END)
--                       END) AS sort_localized,
--
--                   (CASE WHEN join_sorts.type = 'RNNS' THEN 0 WHEN join_sorts.country_code_iso ISNULL THEN (CASE WHEN join_sorts_contractors.country_code_iso = '643' THEN 1 ELSE 0 END) WHEN join_sorts.country_code_iso = '643' THEN 1 ELSE 0 END) AS sort_ru,
--                   (CASE WHEN join_sorts.type = 'RNNS' THEN 0 WHEN (join_sorts.country_code_iso ISNULL OR trim(join_sorts.country_code_iso) = '') THEN (CASE WHEN join_sorts_contractors.country_code_iso = '643' THEN 0 ELSE (CASE WHEN (join_sorts_contractors.country_code_iso ISNULL OR trim(join_sorts_contractors.country_code_iso) = '') THEN 0 ELSE 1 END) END) WHEN join_sorts.country_code_iso = '643' THEN 0 ELSE 1 END) AS sort_en
--
--               FROM ase.info_seed_production_common_sorts AS ifs
--                        LEFT JOIN common.culture_sorts AS join_sorts ON join_sorts.code = ifs.culture_sort_code
--                        LEFT JOIN common.cultures AS join_cultures ON join_cultures.id = join_sorts.culture_id
--                        LEFT JOIN common.culture_sorts_contractors AS join_sorts_contractors ON join_sorts_contractors.id = join_sorts.originator_main
--                        LEFT JOIN common.iso_countres AS join_sorts_contractors_country ON join_sorts_contractors_country.code = join_sorts_contractors.country_code_iso
--                        LEFT JOIN common.iso_countres AS join_sorts_country ON join_sorts_country.code = join_sorts.country_code_iso
--
--                        LEFT JOIN common.ter_townships AS join_ter_township ON join_ter_township.id = ifs.township_id
--                        LEFT JOIN common.ter_regions AS join_ter_region ON join_ter_region.id = join_ter_township.region_id
--                        LEFT JOIN common.ter_federal_districts AS join_ter_federal_districts ON join_ter_federal_districts.id = join_ter_region.federal_district_id
--
--                        LEFT JOIN common.department_structure AS join_dep_township ON join_dep_township.id = ifs.department_id
--                        LEFT JOIN common.department_structure AS join_dep_region ON join_dep_region.id = join_dep_township.parent_id
--                        LEFT JOIN common.ter_regions AS join_dep_region_ter_region ON join_dep_region_ter_region.id = join_dep_region.ter_region_id
--
--                        LEFT JOIN common.culture_sort_regions_ter_regions AS join_sorts_regions_regions ON join_sorts_regions_regions.ter_region_id = join_ter_township.region_id
--
--               WHERE
--
--                   FALSE
--
--                 AND ifs.for_harvest_year = :harvest_year
--              ) AS seeds_and_info_simple
--
--          WHERE :dep_ter_structure
--
--      ),

     bfs_pre AS (
         SELECT
             id,
             post_id,
             post_sale_id,

             (CASE WHEN :sel_type = 'KIND_CULTURES_GROUPS' THEN 0 ELSE ter_federal_district_turn END) AS ter_federal_district_turn,
             (CASE WHEN :sel_type = 'KIND_CULTURES_GROUPS' THEN 0 ELSE ter_region_turn END) AS ter_region_turn,

             (CASE WHEN :sel_type = 'KIND_CULTURES_GROUPS' THEN 0 ELSE ter_region_id END) AS ter_region_id,
             (CASE WHEN :sel_type = 'KIND_CULTURES_GROUPS' THEN '' ELSE ter_region_name END) AS ter_region_name,
             (CASE WHEN :sel_type = 'KIND_CULTURES_GROUPS' THEN 0 ELSE ter_federal_district_id END) AS ter_federal_district_id,
             (CASE WHEN :sel_type = 'KIND_CULTURES_GROUPS' THEN '' ELSE ter_federal_district_name END) AS ter_federal_district_name,

             ter_region_name AS nat_ter_region_name,
             concat((CASE WHEN qual_date IS NOT NULL THEN to_char(qual_date , 'dd.MM.YY') ELSE '[без даты]' END), ' - ',
                    (CASE WHEN qual_doc_number IS NOT NULL AND trim(qual_doc_number) != '' THEN trim(qual_doc_number) ELSE '[без документа]' END), ', ',
                    (CASE WHEN rep_trade THEN concat('РСт (', rep_type_name_view, ')') ELSE coalesce(rep_type_name_view, '[Репродукция не указана]') END), ', ',
                    contractor_name, ', ',
                    ter_township_name, ' (', dep_township_name, ')')  AS view_tmp_name,

             culture_name,
             culture_id, culture_season_id, culture_kind_plant_material,
             culture_mix_id,
             culture_mix_name,
             culture_mix_season_id,
             sort_code,
             sort_name,
             sort_sign_2,

             sort_type,
             sort_region,
             sort_year,

             sort_originator_main,
             sort_originator_main_name,
             sort_originator_main_country_code_iso,
             sort_originator_main_country_name,
             sort_country_code_iso,
             sort_country_name,

             seedprod_seeds_kind,

             fill_all,

             fill_AVALIABLE,
--              fill_SOWN,
--              fill_DELETED,
--              fill_SOLD,
--              fill_DEFECTED,
--              fill_FED,
--              fill_REFORMED,

--              (CASE WHEN sort_ru = 1 THEN id ELSE null END) AS ru_backfill_id,
--
--              (CASE WHEN sort_ru = 1 THEN fill_seed_fund ELSE null END) AS ru_fill_all,
--              (CASE WHEN sort_ru = 1 AND sort_localized THEN fill_seed_fund ELSE null END) AS ru_fill_localized,
--              (CASE WHEN (sort_ru = 1 AND purchase_en = 1) THEN fill_seed_fund ELSE null END) AS ru_fill_purchase_en,

--              (CASE WHEN sort_en = 1 THEN id ELSE null END) AS en_backfill_id,
--              (CASE WHEN sort_en = 1 THEN fill_seed_fund ELSE null END) AS en_fill_all,
--              (CASE WHEN sort_en = 1 AND sort_localized THEN fill_seed_fund ELSE null END) AS en_fill_localized,
--              (CASE WHEN (sort_en = 1 AND purchase_en = 1) THEN fill_seed_fund ELSE null END) AS en_fill_purchase_en,

--              (CASE WHEN sort_ru = 0 AND sort_en = 0 THEN id ELSE null END) AS nr_backfill_id,
--              (CASE WHEN sort_ru = 0 AND sort_en = 0 THEN fill_seed_fund ELSE null END) AS nr_fill_all,
--              (CASE WHEN (sort_ru = 0 AND sort_en = 0 AND purchase_en = 1) THEN fill_seed_fund ELSE null END) AS nr_fill_purchase_en,

             (CASE WHEN rep_category_type = 'NC' THEN fill_all ELSE 0 END)              AS rp_NC,
             (CASE WHEN rep_category_type = 'OS' THEN fill_all ELSE 0 END)              AS rp_OS,
             (CASE WHEN rep_category_type = 'ES' THEN fill_all ELSE 0 END)              AS rp_ES,
             (CASE WHEN rep_type IN ('RS', 'RS1') AND rep_trade != TRUE THEN fill_all ELSE 0 END)        AS rp_RS_1,
             (CASE WHEN rep_type IN ('RS', 'RS1') AND rep_trade = TRUE THEN fill_all ELSE 0 END)         AS rp_RS_1_RST,
             (CASE WHEN rep_type IN ('RS2') AND rep_trade != TRUE THEN fill_all ELSE 0 END)          AS rp_RS_2,
             (CASE WHEN rep_type IN ('RS2') AND rep_trade = TRUE THEN fill_all ELSE 0 END)          AS rp_RS_2_RST,
             (CASE WHEN rep_type IN ('RS3') AND rep_trade != TRUE THEN fill_all ELSE 0 END)          AS rp_RS_3,
             (CASE WHEN rep_type IN ('RS3') AND rep_trade = TRUE THEN fill_all ELSE 0 END)          AS rp_RS_3_RST,
             (CASE WHEN rep_type IN ('RS4') AND rep_trade != TRUE THEN fill_all ELSE 0 END)          AS rp_RS_4,
             (CASE WHEN rep_type IN ('RS4') AND rep_trade = TRUE THEN fill_all ELSE 0 END)          AS rp_RS_4_RST,
             (CASE WHEN rep_type = 'F1' THEN fill_all ELSE 0 END)                AS rp_F1,
             (CASE WHEN rep_type IN ('RS5', 'RS6', 'RS7', 'RS8', 'RS9', 'RS10') AND rep_trade != TRUE THEN fill_all ELSE 0 END) AS rp_RS_5B,
             (CASE WHEN rep_type IN ('RS5', 'RS6', 'RS7', 'RS8', 'RS9', 'RS10') AND rep_trade = TRUE THEN fill_all ELSE 0 END) AS rp_RS_5B_RST,

             (CASE WHEN rep_id ISNULL OR (rep_category_type NOT IN ('NC', 'OS', 'ES') AND rep_type NOT IN ('RS', 'RS1', 'RS2', 'RS3', 'RS4', 'F1' , 'RS5', 'RS6', 'RS7', 'RS8', 'RS9', 'RS10')) THEN fill_all ELSE 0 END) AS rp_NOT,

         --              fill_seed_fund,
             fill_seed_for_sale
--              fill_removed_seed_fund,
--              purchase_en,

--              (CASE WHEN purchase_en = 1 THEN fill_seed_fund ELSE 0 END)                AS fill_purchase_en

         FROM (
                  SELECT

                      (CASE WHEN is_trans_fund AND is_need_hyear THEN id ELSE null END) AS id,
                      (CASE WHEN NOT is_need_hyear AND NOT is_for_sail THEN id ELSE null END) AS post_id,
                      (CASE WHEN NOT is_need_hyear AND is_for_sail THEN id ELSE null END) AS post_sale_id,

                      is_trans_fund,
                      is_need_hyear,
                      is_for_sail,

                      ter_region_id, ter_region_name, ter_federal_district_id, ter_federal_district_name,
                      ter_federal_district_turn, ter_region_turn,

                      ter_township_name, dep_township_name,

                      date_begin,

                      qual_date,
                      qual_doc_number,
                      rep_type_name_view,
                      contractor_name,

                      culture_name,
                      culture_id, culture_season_id, culture_kind_plant_material,
                      culture_mix_id,
                      culture_mix_name,
                      culture_mix_season_id,
                      sort_code,
                      sort_name,
                      sort_sign_2,

                      sort_type,
                      sort_region,
                      sort_localized,
                      sort_year,

                      sort_originator_main,
                      sort_originator_main_name,
                      sort_originator_main_country_code_iso,
                      sort_originator_main_country_name,
                      sort_country_code_iso,
                      sort_country_name,

                      seedprod_seeds_kind,


                      (CASE WHEN is_trans_fund AND is_need_hyear THEN fill_all ELSE null END) AS fill_all,

                      (CASE WHEN (NOT is_need_hyear AND NOT is_for_sail) THEN coalesce(fill_all, 0.0) - coalesce(fill_SOWN, 0.0) -coalesce(fill_DELETED, 0.0) - coalesce(fill_SOLD, 0.0) - coalesce(fill_DEFECTED, 0.0) - coalesce(fill_FED, 0.0) - coalesce(fill_REFORMED, 0.0) - coalesce(fill_TRANS_FUND, 0.0) ELSE null END) AS fill_AVALIABLE,

                      (CASE WHEN (NOT is_need_hyear AND is_for_sail) THEN coalesce(fill_all, 0.0) - coalesce(fill_SOWN, 0.0) -coalesce(fill_DELETED, 0.0) - coalesce(fill_SOLD, 0.0) - coalesce(fill_DEFECTED, 0.0) - coalesce(fill_FED, 0.0) - coalesce(fill_REFORMED, 0.0) - coalesce(fill_TRANS_FUND, 0.0) ELSE null END) AS fill_seed_for_sale,

--                       (coalesce(fill_SOWN, 0.0) + coalesce(fill_SOWN_old, 0.0)) AS fill_SOWN,
--                       fill_DELETED,
--                       fill_SOLD,
--                       fill_DEFECTED,
--                       fill_FED,
--                       fill_REFORMED,

--                       sort_ru,
--                       sort_en,

                      rep_category_id,
                      rep_category_type,
                      rep_id,
                      rep_type,
                      rep_trade

                  --                       (CASE WHEN fill_all_for_OWN > 0
--                                 THEN (CASE WHEN (coalesce(fill_all_for_OWN, 0.0) - coalesce(fill_SOWN_old, 0.0) - coalesce(fill_DELETED, 0.0) - coalesce(fill_SOLD, 0.0) - coalesce(fill_DEFECTED, 0.0) - coalesce(fill_FED, 0.0) - coalesce(fill_REFORMED, 0.0)) > 0
--                                                THEN coalesce(fill_all_for_OWN, 0.0) - coalesce(fill_SOWN_old, 0.0) - coalesce(fill_DELETED, 0.0) - coalesce(fill_SOLD, 0.0) - coalesce(fill_DEFECTED, 0.0) - coalesce(fill_FED, 0.0) - coalesce(fill_REFORMED, 0.0)
--                                            ELSE null
--                               END)
--                             ELSE null END) AS fill_seed_fund,
--
--                       (CASE WHEN fill_all_for_SAIL > 0
--                                 THEN (CASE WHEN (coalesce(fill_all_for_SAIL, 0.0) - coalesce(fill_DELETED, 0.0) - coalesce(fill_SOLD, 0.0) - coalesce(fill_DEFECTED, 0.0) - coalesce(fill_FED, 0.0) - coalesce(fill_REFORMED, 0.0)) > 0
--                                                THEN coalesce(fill_all_for_SAIL, 0.0) - coalesce(fill_DELETED, 0.0) - coalesce(fill_SOLD, 0.0) - coalesce(fill_DEFECTED, 0.0) - coalesce(fill_FED, 0.0) - coalesce(fill_REFORMED, 0.0)
--                                            ELSE null
--                               END)
--                             ELSE null END) AS fill_seed_for_sale,
--
--                       (CASE WHEN fill_all_for_OWN > 0
--                                 THEN (CASE WHEN (coalesce(fill_SOWN_old, 0.0) + coalesce(fill_DELETED, 0.0) + coalesce(fill_SOLD, 0.0) + coalesce(fill_DEFECTED, 0.0) + coalesce(fill_FED, 0.0) + coalesce(fill_REFORMED, 0.0)) > 0
--                                                THEN coalesce(fill_SOWN_old, 0.0) + coalesce(fill_DELETED, 0.0) + coalesce(fill_SOLD, 0.0) + coalesce(fill_DEFECTED, 0.0) + coalesce(fill_FED, 0.0) + coalesce(fill_REFORMED, 0.0)
--                                            ELSE null
--                               END)
--                             ELSE null END) AS fill_removed_seed_fund,

--                       cast(null AS double precision) AS fill_seed_fund,
--                       cast(null AS double precision) AS fill_seed_for_sale,
--                       cast(null AS double precision) AS fill_removed_seed_fund

--                       purchase_en

                  FROM
                      (SELECT
                           (CASE WHEN seed_fund_type_id NOTNULL AND seed_fund_type_id = 2 THEN true ELSE false END) AS is_trans_fund,
                           (CASE WHEN bfs.seedprod_for_harvest_year = :harvest_year THEN true ELSE false END) AS is_need_hyear,

                           (CASE WHEN bfs.purpose_enum = 'SAIL' THEN true ELSE false END) AS is_for_sail,

                           bfs.id,
                           bfs.township_id         AS ter_township_id,
                           join_ter_township.name       AS ter_township_name,

                           join_ter_region.id        AS ter_region_id,
                           join_ter_region.name       AS ter_region_name,
                           join_ter_region.federal_district_id    AS ter_federal_district_id,
                           join_ter_federal_districts.name     AS ter_federal_district_name,

                           bfs.department_id        AS dep_township_id,
                           join_dep_township.name_short     AS dep_township_name,
                           join_dep_township.parent_id      AS dep_region_id,
                           join_dep_region.name_short      AS dep_region_name,
                           join_dep_region.ter_region_id     AS dep_ter_region_id,
                           join_dep_region_ter_region.federal_district_id AS dep_ter_federal_district_id,

                           join_ter_federal_districts.turn     AS ter_federal_district_turn,
                           join_ter_region.turn       AS ter_region_turn,

                           bfs.date_begin,

                           (CASE WHEN bfs.actual_crop_quality_id IS NOT NULL THEN qual.date ELSE null END) AS qual_date,
                           (CASE WHEN bfs.actual_crop_quality_id IS NOT NULL THEN qual.quality_doc_number ELSE null END) AS qual_doc_number,
                           rep_types.name_view AS rep_type_name_view,
                           coalesce(join_contractor.name_view, join_contractor.name_short, join_contractor.name_full) AS contractor_name,


                           join_cultures.name AS culture_name,
                           join_sorts.culture_id,
                           COALESCE(join_cultures.default_season_id, join_cultures.culture_season_id, 1) AS culture_season_id,
                           COALESCE(bfs.kind_plant_material, join_cultures.default_kind_plant_material, 'SEEDS') AS culture_kind_plant_material,
                           join_culture_mixes.id AS culture_mix_id,
                           join_culture_mixes.name AS culture_mix_name,
                           join_culture_mixes.culture_season_id AS culture_mix_season_id,
                           bfs.culture_sort_code AS sort_code,
                           join_sorts.name AS sort_name,
                           join_sorts.sign_2 AS sort_sign_2,

                           join_sorts.type AS sort_type,
                           join_sorts.region AS sort_region,


                           (CASE WHEN join_sorts.region isnull OR trim(join_sorts.region) = '' THEN false ELSE
                               (CASE WHEN join_sorts.region LIKE '*' THEN true ELSE
                                   (CASE WHEN CAST(join_sorts_regions_regions.culture_sort_region_id AS text) IN (SELECT trim(unnest(string_to_array(join_sorts.region,',')))) THEN true ELSE
                                       false
                                       END)
                                   END)
                               END) AS sort_localized,


                           join_sorts.year AS sort_year,

                           join_sorts.originator_main AS sort_originator_main,
                           join_sorts_contractors.title AS sort_originator_main_name,
                           join_sorts_contractors.country_code_iso AS sort_originator_main_country_code_iso,
                           join_sorts_contractors_country.name_short AS sort_originator_main_country_name,
                           join_sorts.country_code_iso AS sort_country_code_iso,
                           join_sorts_country.name_short AS sort_country_name,

                           bfs.info_reproduction_type_id,
                           bfs.info_reproduction_trade,
                           bfs.seedprod_seeds_kind,

                           bfs.fill_all,

                           SUM(states_all.volume_SOWN) AS fill_SOWN,
--                            SUM(states_all.volume_SOWN_old) AS fill_SOWN_old,
                           SUM(states_all.volume_DELETED) AS fill_DELETED,
                           SUM(states_all.volume_SOLID) AS fill_SOLD,
                           SUM(states_all.volume_DEFECTED) AS fill_DEFECTED,
                           SUM(states_all.volume_FED) AS fill_FED,
                           SUM(states_all.volume_REFORMED) AS fill_REFORMED,
                           SUM(states_all.volume_TRANS_FUND) AS fill_TRANS_FUND,


--                            (CASE WHEN bfs.purpose_enum = 'OWN' OR bfs.purpose_enum = 'SAIL_REGION' OR bfs.purpose_enum ISNULL THEN bfs.fill_all ELSE null END) AS fill_all_for_OWN,
--                            (CASE WHEN bfs.purpose_enum = 'SAIL' THEN bfs.fill_all ELSE null END) AS fill_all_for_SAIL,

--                            (CASE WHEN join_sorts.type = 'RNNS' THEN 0 WHEN join_sorts.country_code_iso ISNULL THEN (CASE WHEN join_sorts_contractors.country_code_iso = '643' THEN 1 ELSE 0 END) WHEN join_sorts.country_code_iso = '643' THEN 1 ELSE 0 END) sort_ru,
--                            (CASE WHEN join_sorts.type = 'RNNS' THEN 0 WHEN (join_sorts.country_code_iso ISNULL OR trim(join_sorts.country_code_iso) = '') THEN (CASE WHEN join_sorts_contractors.country_code_iso = '643' THEN 0 ELSE (CASE WHEN (join_sorts_contractors.country_code_iso ISNULL OR trim(join_sorts_contractors.country_code_iso) = '') THEN 0 ELSE 1 END) END) WHEN join_sorts.country_code_iso = '643' THEN 0 ELSE 1 END) sort_en,

                           rep_categories.id AS rep_category_id,
                           rep_categories.type AS rep_category_type,
                           rep_types.id AS rep_id,
                           rep_types.type AS rep_type,

                           (CASE WHEN bfs.actual_crop_quality_id IS NOT NULL THEN qual.reproduction_trade ELSE bfs.info_reproduction_trade END) AS rep_trade

--                            (CASE WHEN (bfs.provider_kind_id = 1 AND bfs.purchase_type_enum = 'ABROAD') THEN 1 ELSE 0 END) AS purchase_en

                       FROM ase.seeds_backfills AS bfs

                                LEFT JOIN common.culture_mixes AS join_culture_mixes ON join_culture_mixes.id = bfs.culture_mix_id
                                LEFT JOIN common.cultures AS join_mixes_culture ON join_mixes_culture.id = join_culture_mixes.mix_type
                                LEFT JOIN common.culture_sorts AS join_sorts ON join_sorts.code = (CASE WHEN bfs.culture_mix_id ISNULL THEN bfs.culture_sort_code ELSE join_mixes_culture.not_sorted_sort_id END)
                                LEFT JOIN common.cultures AS join_cultures ON join_cultures.id = (CASE WHEN bfs.culture_mix_id ISNULL THEN join_sorts.culture_id ELSE join_culture_mixes.mix_type END)
                                LEFT JOIN common.contractors AS join_contractor ON bfs.contractor_id = join_contractor.id

                                LEFT JOIN common.culture_sorts_contractors AS join_sorts_contractors ON join_sorts_contractors.id = join_sorts.originator_main
                                LEFT JOIN common.iso_countres AS join_sorts_contractors_country ON join_sorts_contractors_country.code = join_sorts_contractors.country_code_iso
                                LEFT JOIN common.iso_countres AS join_sorts_country ON join_sorts_country.code = join_sorts.country_code_iso

                                LEFT JOIN common.ter_townships AS join_ter_township ON join_ter_township.id = bfs.township_id
                                LEFT JOIN common.ter_regions AS join_ter_region ON join_ter_region.id = join_ter_township.region_id
                                LEFT JOIN common.ter_federal_districts AS join_ter_federal_districts ON join_ter_federal_districts.id = join_ter_region.federal_district_id

                                LEFT JOIN common.department_structure AS join_dep_township ON join_dep_township.id = bfs.department_id
                                LEFT JOIN common.department_structure AS join_dep_region ON join_dep_region.id = join_dep_township.parent_id
                                LEFT JOIN common.ter_regions AS join_dep_region_ter_region ON join_dep_region_ter_region.id = join_dep_region.ter_region_id

                                LEFT JOIN ase.crop_qualities AS qual ON qual.id = bfs.actual_crop_quality_id

                                LEFT JOIN common.culture_sort_regions_ter_regions AS join_sorts_regions_regions ON join_sorts_regions_regions.ter_region_id = join_ter_township.region_id

                                LEFT JOIN common.reproduction_types AS rep_types ON rep_types.id = (CASE WHEN bfs.actual_crop_quality_id IS NOT NULL THEN qual.reproduction_type_id ELSE bfs.info_reproduction_type_id END)
                                LEFT JOIN common.reproduction_categories AS rep_categories ON rep_categories.id = rep_types.category_id

                                LEFT JOIN bfs_states AS states_all ON states_all.seed_back_fill_id = bfs.id

                       WHERE true
                         AND (bfs.seedprod_for_harvest_year = :harvest_year OR bfs.seedprod_for_harvest_year = :harvest_year - 1)
                         AND (bfs.prev_analyze IS NULL OR bfs.prev_analyze = false)
                         AND bfs.deleted IS NULL
                         AND bfs.township_id IS NOT NULL
                         AND bfs.department_id IS NOT NULL
                         AND bfs.fill_all > 0.00000001
                         AND bfs.fill_unit_id = 36

                       GROUP BY
                           bfs.id, bfs.township_id, bfs.department_id, bfs.culture_sort_code, bfs.info_reproduction_type_id, bfs.info_reproduction_trade, bfs.seedprod_seeds_kind, bfs.fill_all, bfs.provider_kind_id, bfs.purchase_type_enum,

                           join_ter_township.name, join_ter_township.region_id,
                           join_ter_region.id, join_ter_region.name, join_ter_region.federal_district_id,

                           join_dep_township.name_short, join_dep_township.parent_id,
                           join_dep_region.name_short, join_dep_region.ter_region_id,
                           join_dep_region_ter_region.federal_district_id,
                           join_ter_federal_districts.name,
                           join_ter_federal_districts.turn, join_ter_region.turn,

                           join_cultures.name, join_cultures.default_season_id, join_cultures.culture_season_id, join_cultures.default_kind_plant_material,

                           join_culture_mixes.id, join_culture_mixes.name, join_culture_mixes.culture_season_id,

                           join_sorts.culture_id, join_sorts.name, join_sorts.sign_2, join_sorts.type, join_sorts.region, join_sorts.year, join_sorts.originator_main, join_sorts.country_code_iso,
                           join_sorts_contractors.country_code_iso, join_sorts_contractors.title,
                           join_sorts_contractors_country.name_short, join_sorts_country.name_short,

                           rep_categories.id, rep_categories.type,

                           rep_types.id, rep_types.type,
                           qual.reproduction_trade, qual.date, qual.quality_doc_number, join_contractor.name_full, join_contractor.name_short, join_contractor.name_view,
                           join_sorts_regions_regions.culture_sort_region_id

                      ) AS seeds_and_info_simple

                  WHERE :dep_ter_structure

              ) AS bfs_fundF

         WHERE
             (CASE WHEN is_trans_fund AND is_need_hyear THEN bfs_fundF.fill_all > 0.00000001 ELSE false END) OR
             (CASE WHEN NOT is_need_hyear AND NOT is_for_sail THEN bfs_fundF.fill_AVALIABLE > 0.00000001 ELSE false END) OR
             (CASE WHEN NOT is_need_hyear AND is_for_sail THEN bfs_fundF.fill_seed_for_sale > 0.00000001 ELSE false END)

--              (bfs_fundF.fill_all > 0.0 OR bfs_fundF.fill_AVALIABLE > 0.0)
     ),

     bfs_sorts_kinds AS (
         SELECT
             COUNT(bfs_pre.id) AS count_backfills,
             cast(null AS jsonb) AS backfills_data,

--              0 AS count_infos,
             coalesce(COUNT(bfs_pre.post_id), 0) + coalesce(COUNT(bfs_pre.post_sale_id), 0) AS count_infos,
             cast(null AS jsonb) AS infos_data,

             CASE WHEN COUNT(bfs_pre.id) > 0 THEN array_agg(bfs_pre.id) FILTER (WHERE bfs_pre.id NOTNULL) ELSE array[cast(0 AS bigint)] END AS ids_backfills,

--              CASE WHEN COUNT(bfs_pre.post_id) > 0 THEN array_agg(bfs_pre.post_id) ELSE array[0] END AS ids_infos,

             CASE
                 WHEN COUNT(bfs_pre.post_id) > 0 AND COUNT(bfs_pre.post_sale_id) > 0
                     THEN array_cat(array_agg(bfs_pre.post_id) FILTER (WHERE bfs_pre.post_id NOTNULL), array_agg(bfs_pre.post_sale_id) FILTER (WHERE bfs_pre.post_sale_id NOTNULL))
                 WHEN COUNT(bfs_pre.post_id) > 0
                     THEN array_agg(bfs_pre.post_id) FILTER (WHERE bfs_pre.post_id NOTNULL)
                 WHEN COUNT(bfs_pre.post_sale_id) > 0
                     THEN array_agg(bfs_pre.post_sale_id) FILTER (WHERE bfs_pre.post_sale_id NOTNULL)
                 ELSE array[cast(0 AS bigint)]
             END AS ids_infos,

             ter_region_id, ter_region_name, ter_federal_district_id, ter_federal_district_name,
             ter_federal_district_turn, ter_region_turn,

             culture_name,
             culture_id, culture_season_id, culture_kind_plant_material,
--              culture_mix_id,
--              culture_mix_name,
             culture_mix_season_id,
             sort_code,
             sort_name,
             sort_sign_2,

             sort_type,
             sort_region,
             sort_year,

             sort_originator_main,
             sort_originator_main_name,
             sort_originator_main_country_code_iso,
             sort_originator_main_country_name,
             sort_country_code_iso,
             sort_country_name,

             seedprod_seeds_kind,

             SUM(fill_all) AS fill_all,

--              SUM(fill_seed_fund) AS fill_seed_fund,
--              SUM(fill_seed_for_sale) AS fill_seed_for_sale,
--              SUM(fill_removed_seed_fund) AS fill_removed_seed_fund,

             cast(null AS double precision) AS fill_seed_fund,
             SUM(fill_seed_for_sale) AS fill_seed_for_sale,
             cast(null AS double precision) AS fill_removed_seed_fund,

             SUM(fill_AVALIABLE) AS fill_AVALIABLE,

--              SUM(fill_SOWN) AS fill_SOWN,
--              SUM(fill_DELETED) AS fill_DELETED,
--              SUM(fill_SOLD) AS fill_SOLD,
--              SUM(fill_DEFECTED) AS fill_DEFECTED,
--              SUM(fill_FED) AS fill_FED,
--              SUM(fill_REFORMED) AS fill_REFORMED,

             cast(null AS double precision) AS fill_SOWN,
             cast(null AS double precision) AS fill_DELETED,
             cast(null AS double precision) AS fill_SOLD,
             cast(null AS double precision) AS fill_DEFECTED,
             cast(null AS double precision) AS fill_FED,
             cast(null AS double precision) AS fill_REFORMED,

--              null AS ru_backfill_ids,
--              SUM(ru_fill_all) AS ru_fill_all,
--              SUM(ru_fill_localized) AS ru_fill_localized,
--              SUM(ru_fill_purchase_en) AS ru_fill_purchase_en,
--
--              null AS en_backfill_id,
--              SUM(en_fill_all) AS en_fill_all,
--              SUM(en_fill_localized) AS en_fill_localized,
--              SUM(en_fill_purchase_en) AS en_fill_purchase_en,
--
--              null AS nr_backfill_id,
--              SUM(nr_fill_all) AS nr_fill_all,
--              SUM(nr_fill_purchase_en) AS nr_fill_purchase_en,

             null AS ru_backfill_ids,
             cast(null AS double precision) AS ru_fill_all,
             cast(null AS double precision) AS ru_fill_localized,
             cast(null AS double precision) AS ru_fill_purchase_en,

             null AS en_backfill_id,
             cast(null AS double precision) AS en_fill_all,
             cast(null AS double precision) AS en_fill_localized,
             cast(null AS double precision) AS en_fill_purchase_en,

             null AS nr_backfill_id,
             cast(null AS double precision) AS nr_fill_all,
             cast(null AS double precision) AS nr_fill_purchase_en,

             SUM(rp_NC) AS rp_NC,
             SUM(rp_OS) AS rp_OS,
             SUM(rp_ES) AS rp_ES,
             SUM(rp_RS_1) AS rp_RS_1,
             SUM(rp_RS_1_RST) AS rp_RS_1_RST,
             SUM(rp_RS_2) AS rp_RS_2,
             SUM(rp_RS_2_RST) AS rp_RS_2_RST,
             SUM(rp_RS_3) AS rp_RS_3,
             SUM(rp_RS_3_RST) AS rp_RS_3_RST,
             SUM(rp_RS_4) AS rp_RS_4,
             SUM(rp_RS_4_RST) AS rp_RS_4_RST,
             SUM(rp_F1) AS rp_F1,
             SUM(rp_RS_5B) AS rp_RS_5B,
             SUM(rp_RS_5B_RST) AS rp_RS_5B_RST,
             SUM(rp_NOT) AS rp_NOT,

--              SUM(fill_purchase_en) AS fill_purchase_en

             cast(null AS double precision) AS fill_purchase_en

         FROM bfs_pre
         GROUP BY culture_name, culture_id, culture_season_id, culture_kind_plant_material, sort_code, sort_name, sort_sign_2, sort_type, sort_region, sort_year, seedprod_seeds_kind,
--                   culture_mix_id, culture_mix_name,
                  culture_mix_season_id,
                  sort_originator_main, sort_originator_main_name, sort_originator_main_country_code_iso, sort_originator_main_country_name, sort_country_code_iso, sort_country_name,
                  ter_region_id, ter_region_name, ter_federal_district_id, ter_federal_district_name, ter_federal_district_turn, ter_region_turn
     ),



--      info_sorts_kinds AS (
--          SELECT
--              0 AS count_backfills,
--              cast(null AS jsonb) AS backfills_data,
--
--              COUNT(info_bfs_pre.info_id) AS count_infos,
--              cast(null AS jsonb) AS infos_data,
--
--              ter_region_id, ter_region_name, ter_federal_district_id, ter_federal_district_name,
--              ter_federal_district_turn, ter_region_turn,
--
--              culture_name,
--              culture_id, culture_season_id, culture_kind_plant_material,
--              culture_mix_id,
--              culture_mix_name,
--              culture_mix_season_id,
--              sort_code,
--              sort_name,
--              sort_sign_2,
--
--              sort_type,
--              sort_region,
--              sort_year,
--
--              sort_originator_main,
--              sort_originator_main_name,
--              sort_originator_main_country_code_iso,
--              sort_originator_main_country_name,
--              sort_country_code_iso,
--              sort_country_name,
--
--              seedprod_seeds_kind,
--
--              SUM(fill_all) AS fill_all,
--
--              SUM(fill_seed_fund) AS fill_seed_fund,
--              SUM(fill_seed_for_sale) AS fill_seed_for_sale,
--              SUM(fill_removed_seed_fund) AS fill_removed_seed_fund,
--
--              SUM(fill_AVALIABLE) AS fill_AVALIABLE,
--
--              SUM(fill_SOWN) AS fill_SOWN,
--              SUM(fill_DELETED) AS fill_DELETED,
--              SUM(fill_SOLD) AS fill_SOLD,
--              SUM(fill_DEFECTED) AS fill_DEFECTED,
--              SUM(fill_FED) AS fill_FED,
--              SUM(fill_REFORMED) AS fill_REFORMED,
--
--              null AS ru_backfill_ids,
--              SUM(ru_fill_all) AS ru_fill_all,
--              SUM(ru_fill_localized) AS ru_fill_localized,
--              SUM(ru_fill_purchase_en) AS ru_fill_purchase_en,
--
--              null AS en_backfill_id,
--              SUM(en_fill_all) AS en_fill_all,
--              SUM(en_fill_localized) AS en_fill_localized,
--              SUM(en_fill_purchase_en) AS en_fill_purchase_en,
--
--              null AS nr_backfill_id,
--              SUM(nr_fill_all) AS nr_fill_all,
--              SUM(nr_fill_purchase_en) AS nr_fill_purchase_en,
--
--              SUM(rp_NC) AS rp_NC,
--              SUM(rp_OS) AS rp_OS,
--              SUM(rp_ES) AS rp_ES,
--              SUM(rp_RS_1) AS rp_RS_1,
--              SUM(rp_RS_1_RST) AS rp_RS_1_RST,
--              SUM(rp_RS_2) AS rp_RS_2,
--              SUM(rp_RS_2_RST) AS rp_RS_2_RST,
--              SUM(rp_RS_3) AS rp_RS_3,
--              SUM(rp_RS_3_RST) AS rp_RS_3_RST,
--              SUM(rp_RS_4) AS rp_RS_4,
--              SUM(rp_RS_4_RST) AS rp_RS_4_RST,
--              SUM(rp_F1) AS rp_F1,
--              SUM(rp_RS_5B) AS rp_RS_5B,
--              SUM(rp_RS_5B_RST) AS rp_RS_5B_RST,
--              SUM(rp_NOT) AS rp_NOT,
--              SUM(fill_purchase_en) AS fill_purchase_en
--
--          FROM info_bfs_pre
--          GROUP BY culture_name, culture_id, culture_season_id, culture_kind_plant_material, sort_code, sort_name, sort_sign_2, sort_type, sort_region, sort_year, seedprod_seeds_kind,
--                   culture_mix_id, culture_mix_name, culture_mix_season_id,
--                   sort_originator_main, sort_originator_main_name, sort_originator_main_country_code_iso, sort_originator_main_country_name, sort_country_code_iso, sort_country_name,
--                   ter_region_id, ter_region_name, ter_federal_district_id, ter_federal_district_name, ter_federal_district_turn, ter_region_turn
--      ),

--      all_sorts_kinds AS (
--          SELECT
--              SUM(all_pre.count_backfills) AS count_backfills,
--              cast(null AS jsonb) AS backfills_data,
--
--              SUM(all_pre.count_infos) AS count_infos,
--              cast(null AS jsonb) AS infos_data,
--
--              (SELECT array_agg(DISTINCT unnest) FROM unnest(array_agg(all_pre.ids_backfills)) WHERE unnest IS NOT null) AS ids_backfills,
--              (SELECT array_agg(DISTINCT unnest) FROM unnest(array_agg(all_pre.ids_infos)) WHERE unnest IS NOT null) AS ids_infos,
--
-- --              cast(array[0] AS bigint[]) AS ids_backfills,
-- --              cast(array[0] AS bigint[]) AS ids_infos,
--
-- --              CASE WHEN COUNT(bfs_pre.id) > 0 THEN array_agg(bfs_pre.id) ELSE cast(null AS bigint[]) END AS ids_backfills,
-- --              CASE WHEN COUNT(bfs_pre.post_id) > 0 THEN array_agg(bfs_pre.post_id) ELSE cast(null AS bigint[]) END AS ids_infos,
--
--              ter_region_id, ter_region_name, ter_federal_district_id, ter_federal_district_name,
--              ter_federal_district_turn, ter_region_turn,
--
--              culture_name,
--              culture_id, culture_season_id, culture_kind_plant_material,
-- --              culture_mix_id,
-- --              culture_mix_name,
--              culture_mix_season_id,
--              sort_code,
--              sort_name,
--              sort_sign_2,
--
--              sort_type,
--              sort_region,
--              sort_year,
--
--              sort_originator_main,
--              sort_originator_main_name,
--              sort_originator_main_country_code_iso,
--              sort_originator_main_country_name,
--              sort_country_code_iso,
--              sort_country_name,
--
--              seedprod_seeds_kind,
--
--              SUM(fill_all) AS fill_all,
--
--              SUM(fill_seed_fund) AS fill_seed_fund,
--              SUM(fill_seed_for_sale) AS fill_seed_for_sale,
--              SUM(fill_removed_seed_fund) AS fill_removed_seed_fund,
--
--              SUM(fill_AVALIABLE) AS fill_AVALIABLE,
--
--              SUM(fill_SOWN) AS fill_SOWN,
--              SUM(fill_DELETED) AS fill_DELETED,
--              SUM(fill_SOLD) AS fill_SOLD,
--              SUM(fill_DEFECTED) AS fill_DEFECTED,
--              SUM(fill_FED) AS fill_FED,
--              SUM(fill_REFORMED) AS fill_REFORMED,
--
--              null AS ru_backfill_ids,
--              SUM(ru_fill_all) AS ru_fill_all,
--              SUM(ru_fill_localized) AS ru_fill_localized,
--              SUM(ru_fill_purchase_en) AS ru_fill_purchase_en,
--
--              null AS en_backfill_id,
--              SUM(en_fill_all) AS en_fill_all,
--              SUM(en_fill_localized) AS en_fill_localized,
--              SUM(en_fill_purchase_en) AS en_fill_purchase_en,
--
--              null AS nr_backfill_id,
--              SUM(nr_fill_all) AS nr_fill_all,
--              SUM(nr_fill_purchase_en) AS nr_fill_purchase_en,
--
--              SUM(rp_NC) AS rp_NC,
--              SUM(rp_OS) AS rp_OS,
--              SUM(rp_ES) AS rp_ES,
--              SUM(rp_RS_1) AS rp_RS_1,
--              SUM(rp_RS_1_RST) AS rp_RS_1_RST,
--              SUM(rp_RS_2) AS rp_RS_2,
--              SUM(rp_RS_2_RST) AS rp_RS_2_RST,
--              SUM(rp_RS_3) AS rp_RS_3,
--              SUM(rp_RS_3_RST) AS rp_RS_3_RST,
--              SUM(rp_RS_4) AS rp_RS_4,
--              SUM(rp_RS_4_RST) AS rp_RS_4_RST,
--              SUM(rp_F1) AS rp_F1,
--              SUM(rp_RS_5B) AS rp_RS_5B,
--              SUM(rp_RS_5B_RST) AS rp_RS_5B_RST,
--              SUM(rp_NOT) AS rp_NOT,
--              SUM(fill_purchase_en) AS fill_purchase_en
--
--          FROM
--              (
-- --              SELECT * FROM info_sorts_kinds
-- --               WHERE (CASE WHEN :data_layer = 'FULL' OR :data_layer = 'OTHER' THEN true ELSE false END)
-- --               UNION
--                  SELECT * FROM bfs_sorts_kinds
-- --               WHERE (CASE WHEN :data_layer = 'FULL' OR :data_layer = 'RSC' THEN true ELSE false END)
-- --                 WHERE (CASE WHEN :data_layer = 'FULL' OR :data_layer = 'RSC' THEN true ELSE true END)
--              ) AS all_pre
--
--          GROUP BY culture_name, culture_id, culture_season_id, culture_kind_plant_material, sort_code, sort_name, sort_sign_2, sort_type, sort_region, sort_year, seedprod_seeds_kind,
-- --                   culture_mix_id, culture_mix_name,
--                   culture_mix_season_id,
--                   sort_originator_main, sort_originator_main_name, sort_originator_main_country_code_iso, sort_originator_main_country_name, sort_country_code_iso, sort_country_name,
--                   ter_region_id, ter_region_name, ter_federal_district_id, ter_federal_district_name, ter_federal_district_turn, ter_region_turn
--      ),

     sorts_w_culcats_pre AS (
         SELECT
                     row_number() over () AS id,
                     'sort' AS type,
                     gcom.id AS tree_parent_id,
                     gcom.stc_object_id,
                     gcom.stc_parent_id,

                     gcom.group_level + 1 AS group_level,

                     gcom.turn_all,
                     gcom.turn_in + 1 AS turn_in,
                     bfs.ter_federal_district_turn, bfs.ter_region_turn,

                     concat(repeat(' ', gcom.group_level + 1), (CASE WHEN bfs.sort_code ISNULL THEN 'Нет сорта' ELSE (CASE WHEN bfs.sort_type = 'RNNS' THEN concat('РННС, ', bfs.sort_name) WHEN bfs.sort_type = 'NOSORT' THEN 'Несортовой' ELSE concat(coalesce(bfs.sort_code), ', ', bfs.sort_name) END) END)) AS view_name,
                     (CASE WHEN bfs.sort_code ISNULL THEN 'Нет сорта' ELSE (CASE WHEN bfs.sort_type = 'RNNS' THEN concat('РННС, ', bfs.sort_name) WHEN bfs.sort_type = 'NOSORT' THEN 'Несортовой' ELSE concat(coalesce(bfs.sort_code), ', ', bfs.sort_name) END) END) AS name,

                     0 AS cnt_groups,
                     0 AS cnt_cultures,

                     SUM(bfs.count_backfills) AS count_backfills,
                     cast(null AS jsonb) AS backfills_data,

                     SUM(bfs.count_infos) AS count_infos,
                     cast(null AS jsonb) AS infos_data,

--                      CASE WHEN SUM(bfs.count_backfills) > 0 THEN (SELECT array_agg(DISTINCT unnest) FROM unnest(array_agg(bfs.ids_backfills)) WHERE unnest IS NOT null) ELSE array[0]::bigint[] END AS ids_backfills,
--                      CASE WHEN SUM(bfs.count_infos) > 0 THEN (SELECT array_agg(DISTINCT unnest) FROM unnest(array_agg(bfs.ids_infos)) WHERE unnest IS NOT null) ELSE array[0]::bigint[] END AS ids_infos,

--                      (SELECT array_agg(DISTINCT unnest) FROM unnest(array_agg(bfs.ids_backfills)) WHERE unnest IS NOT null AND NOT (unnest = cast(null AS bigint))) AS ids_backfills,
--                      (SELECT array_agg(DISTINCT unnest) FROM unnest(array_agg(bfs.ids_infos)) WHERE unnest IS NOT null AND NOT (unnest = cast(null AS bigint))) AS ids_infos,

--                       array_agg(bfs.ids_backfills) AS ids_backfills,
--                       array_agg(bfs.ids_infos) AS ids_infos,

--                      (SELECT array_agg(DISTINCT unnest) FROM unnest(array_agg(bfs.ids_backfills)) WHERE unnest IS NOT null) AS ids_backfills,
--                      (SELECT array_agg(DISTINCT unnest) FROM unnest(array_agg(bfs.ids_infos)) WHERE unnest IS NOT null) AS ids_infos,

                     ids_backfills,
                     ids_infos,


                     bfs.ter_region_id, bfs.ter_region_name, bfs.ter_federal_district_id, bfs.ter_federal_district_name,

                     join_cultures.name AS culture_name,
                     gcom.culture_id,
                     gcom.flag_01,
                     gcom.flag_01_anti,
                     gcom.culture_season_id,
                     gcom.kind_plant_material,

                     bfs.sort_type,
                     bfs.sort_code,
                     bfs.sort_name,

                     bfs.sort_region,
                     bfs.sort_year,

                     bfs.sort_originator_main,
                     bfs.sort_originator_main_name,
                     bfs.sort_originator_main_country_code_iso,
                     bfs.sort_originator_main_country_name,
                     bfs.sort_country_code_iso,
                     bfs.sort_country_name,

                     bfs.seedprod_seeds_kind,

                     SUM(bfs.fill_all) AS fill_all,

                     SUM(bfs.fill_seed_fund) AS fill_seed_fund,
                     SUM(bfs.fill_seed_for_sale) AS fill_seed_for_sale,
                     SUM(bfs.fill_removed_seed_fund) AS fill_removed_seed_fund,

                     SUM(bfs.fill_AVALIABLE) AS fill_AVALIABLE,

                     SUM(bfs.fill_SOWN) AS fill_SOWN,
                     SUM(bfs.fill_DELETED) AS fill_DELETED,
                     SUM(bfs.fill_SOLD) AS fill_SOLD,
                     SUM(bfs.fill_DEFECTED) AS fill_DEFECTED,
                     SUM(bfs.fill_FED) AS fill_FED,
                     SUM(bfs.fill_REFORMED) AS fill_REFORMED,

                     SUM(bfs.ru_fill_all) AS ru_fill_all,
                     SUM(bfs.ru_fill_localized) AS ru_fill_localized,
                     SUM(bfs.ru_fill_purchase_en) AS ru_fill_purchase_en,

                     SUM(bfs.en_fill_all) AS en_fill_all,
                     SUM(bfs.en_fill_localized) AS en_fill_localized,
                     SUM(bfs.en_fill_purchase_en) AS en_fill_purchase_en,

                     SUM(bfs.nr_fill_all) AS nr_fill_all,
                     SUM(bfs.nr_fill_purchase_en) AS nr_fill_purchase_en,

                     SUM(bfs.rp_NC) AS rp_NC,
                     SUM(bfs.rp_OS) AS rp_OS,
                     SUM(bfs.rp_ES) AS rp_ES,
                     SUM(bfs.rp_RS_1) AS rp_RS_1,
                     SUM(bfs.rp_RS_1_RST) AS rp_RS_1_RST,
                     SUM(bfs.rp_RS_2) AS rp_RS_2,
                     SUM(bfs.rp_RS_2_RST) AS rp_RS_2_RST,
                     SUM(bfs.rp_RS_3) AS rp_RS_3,
                     SUM(bfs.rp_RS_3_RST) AS rp_RS_3_RST,
                     SUM(bfs.rp_RS_4) AS rp_RS_4,
                     SUM(bfs.rp_RS_4_RST) AS rp_RS_4_RST,
                     SUM(bfs.rp_F1) AS rp_F1,
                     SUM(bfs.rp_RS_5B) AS rp_RS_5B,
                     SUM(bfs.rp_RS_5B_RST) AS rp_RS_5B_RST,
                     SUM(bfs.rp_NOT) AS rp_NOT,
                     SUM(bfs.fill_purchase_en) AS fill_purchase_en

         FROM ase.seed_production_cultures_groups_com AS gcom
                  LEFT JOIN bfs_sorts_kinds AS bfs ON bfs.culture_id = gcom.culture_id
                  LEFT JOIN common.cultures AS join_cultures ON join_cultures.id = gcom.culture_id
         WHERE gcom.culture_id IS NOT NULL
--              AND sort_code NOTNULL
--              AND sort_code NOTNULL OR (bfs.culture_id IN (100000, 100001))
           AND (CASE WHEN gcom.flag_01 IS NOT NULL THEN bfs.sort_sign_2 = gcom.flag_01 ELSE true END)
           AND (CASE WHEN gcom.flag_01_anti IS NOT NULL THEN bfs.sort_sign_2 NOT IN (SELECT unnest(string_to_array(gcom.flag_01_anti,','))) ELSE true END)
           AND (CASE WHEN gcom.culture_season_id ISNULL THEN true ELSE gcom.culture_season_id = coalesce(bfs.culture_mix_season_id, bfs.culture_season_id) END)
           AND (CASE WHEN gcom.kind_plant_material ISNULL THEN true ELSE gcom.kind_plant_material = bfs.culture_kind_plant_material END)
         GROUP BY gcom.id, gcom.tree_parent_id, gcom.stc_object_id, gcom.stc_parent_id, gcom.view_name, gcom.group_level, gcom.culture_id, gcom.turn_all, gcom.turn_in,
                  join_cultures.name,
                  bfs.ids_backfills, bfs.ids_infos,
                  bfs.sort_code, bfs.sort_name, bfs.sort_type, bfs.sort_region, bfs.sort_year, bfs.seedprod_seeds_kind,
                  bfs.sort_originator_main, bfs.sort_originator_main_name, bfs.sort_originator_main_country_code_iso, bfs.sort_originator_main_country_name, bfs.sort_country_code_iso, bfs.sort_country_name,
                  bfs.ter_region_id, bfs.ter_region_name, bfs.ter_federal_district_id, bfs.ter_federal_district_name, bfs.ter_federal_district_turn, bfs.ter_region_turn
     ),


--
--
--      sorts_w_culcats_pre_null_sorts AS (
--          SELECT
--                      row_number() over () AS id,
--                      'sort' AS type,
--                      gcom.id AS tree_parent_id,
--                      gcom.stc_object_id,
--                      gcom.stc_parent_id,
--
--                      gcom.group_level + 1 AS group_level,
--
--                      gcom.turn_all,
--                      gcom.turn_in + 1 AS turn_in,
--                      bfs.ter_federal_district_turn, bfs.ter_region_turn,
--
--                      concat(repeat(' ', gcom.group_level + 1), (CASE WHEN bfs.sort_code ISNULL THEN 'Нет сорта' ELSE (CASE WHEN bfs.sort_type = 'RNNS' THEN concat('РННС, ', bfs.sort_name) WHEN bfs.sort_type = 'NOSORT' THEN 'Несортовой' ELSE concat(coalesce(bfs.sort_code), ', ', bfs.sort_name) END) END)) AS view_name,
--                      (CASE WHEN bfs.sort_code ISNULL THEN 'Нет сорта' ELSE (CASE WHEN bfs.sort_type = 'RNNS' THEN concat('РННС, ', bfs.sort_name) WHEN bfs.sort_type = 'NOSORT' THEN 'Несортовой' ELSE concat(coalesce(bfs.sort_code), ', ', bfs.sort_name) END) END) AS name,
--
--                      0 AS cnt_groups,
--                      0 AS cnt_cultures,
--
--                      SUM(bfs.count_backfills) AS count_backfills,
--                      cast(null AS jsonb) AS backfills_data,
--
--                      SUM(bfs.count_infos) AS count_infos,
--                      cast(null AS jsonb) AS infos_data,
--
-- --                      CASE WHEN SUM(bfs.count_backfills) > 0 THEN (SELECT array_agg(DISTINCT unnest) FROM unnest(array_agg(bfs.ids_backfills)) WHERE unnest IS NOT null) ELSE array[0]::bigint[] END AS ids_backfills,
-- --                      CASE WHEN SUM(bfs.count_infos) > 0 THEN (SELECT array_agg(DISTINCT unnest) FROM unnest(array_agg(bfs.ids_infos)) WHERE unnest IS NOT null) ELSE array[0]::bigint[] END AS ids_infos,
--
-- --                      (SELECT array_agg(DISTINCT unnest) FROM unnest(array_agg(bfs.ids_backfills)) WHERE unnest IS NOT null AND NOT (unnest = cast(null AS bigint))) AS ids_backfills,
-- --                      (SELECT array_agg(DISTINCT unnest) FROM unnest(array_agg(bfs.ids_infos)) WHERE unnest IS NOT null AND NOT (unnest = cast(null AS bigint))) AS ids_infos,
--
-- --                       array_agg(bfs.ids_backfills) AS ids_backfills,
-- --                       array_agg(bfs.ids_infos) AS ids_infos,
--
--                      cast(array[0] AS bigint[]) AS ids_backfills,
--                      cast(array[0] AS bigint[]) AS ids_infos,
--
-- --                      cast(null AS bigint[]) AS ids_backfills,
-- --                      cast(null AS bigint[]) AS ids_infos,
--
--
--                      bfs.ter_region_id, bfs.ter_region_name, bfs.ter_federal_district_id, bfs.ter_federal_district_name,
--
--                      join_cultures.name AS culture_name,
--                      gcom.culture_id,
--                      gcom.flag_01,
--                      gcom.flag_01_anti,
--                      gcom.culture_season_id,
--                      gcom.kind_plant_material,
--
--                      bfs.sort_type,
--                      bfs.sort_code,
--                      bfs.sort_name,
--
--                      bfs.sort_region,
--                      bfs.sort_year,
--
--                      bfs.sort_originator_main,
--                      bfs.sort_originator_main_name,
--                      bfs.sort_originator_main_country_code_iso,
--                      bfs.sort_originator_main_country_name,
--                      bfs.sort_country_code_iso,
--                      bfs.sort_country_name,
--
--                      bfs.seedprod_seeds_kind,
--
--                      SUM(bfs.fill_all) AS fill_all,
--
--                      SUM(bfs.fill_seed_fund) AS fill_seed_fund,
--                      SUM(bfs.fill_seed_for_sale) AS fill_seed_for_sale,
--                      SUM(bfs.fill_removed_seed_fund) AS fill_removed_seed_fund,
--
--                      SUM(bfs.fill_AVALIABLE) AS fill_AVALIABLE,
--
--                      SUM(bfs.fill_SOWN) AS fill_SOWN,
--                      SUM(bfs.fill_DELETED) AS fill_DELETED,
--                      SUM(bfs.fill_SOLD) AS fill_SOLD,
--                      SUM(bfs.fill_DEFECTED) AS fill_DEFECTED,
--                      SUM(bfs.fill_FED) AS fill_FED,
--                      SUM(bfs.fill_REFORMED) AS fill_REFORMED,
--
--                      SUM(bfs.ru_fill_all) AS ru_fill_all,
--                      SUM(bfs.ru_fill_localized) AS ru_fill_localized,
--                      SUM(bfs.ru_fill_purchase_en) AS ru_fill_purchase_en,
--
--                      SUM(bfs.en_fill_all) AS en_fill_all,
--                      SUM(bfs.en_fill_localized) AS en_fill_localized,
--                      SUM(bfs.en_fill_purchase_en) AS en_fill_purchase_en,
--
--                      SUM(bfs.nr_fill_all) AS nr_fill_all,
--                      SUM(bfs.nr_fill_purchase_en) AS nr_fill_purchase_en,
--
--                      SUM(bfs.rp_NC) AS rp_NC,
--                      SUM(bfs.rp_OS) AS rp_OS,
--                      SUM(bfs.rp_ES) AS rp_ES,
--                      SUM(bfs.rp_RS_1) AS rp_RS_1,
--                      SUM(bfs.rp_RS_1_RST) AS rp_RS_1_RST,
--                      SUM(bfs.rp_RS_2) AS rp_RS_2,
--                      SUM(bfs.rp_RS_2_RST) AS rp_RS_2_RST,
--                      SUM(bfs.rp_RS_3) AS rp_RS_3,
--                      SUM(bfs.rp_RS_3_RST) AS rp_RS_3_RST,
--                      SUM(bfs.rp_RS_4) AS rp_RS_4,
--                      SUM(bfs.rp_RS_4_RST) AS rp_RS_4_RST,
--                      SUM(bfs.rp_F1) AS rp_F1,
--                      SUM(bfs.rp_RS_5B) AS rp_RS_5B,
--                      SUM(bfs.rp_RS_5B_RST) AS rp_RS_5B_RST,
--                      SUM(bfs.rp_NOT) AS rp_NOT,
--                      SUM(bfs.fill_purchase_en) AS fill_purchase_en
--
--          FROM ase.seed_production_cultures_groups_com AS gcom
--                   LEFT JOIN all_sorts_kinds AS bfs ON bfs.culture_id = gcom.culture_id
--                   LEFT JOIN common.cultures AS join_cultures ON join_cultures.id = gcom.culture_id
--
--          WHERE gcom.culture_id IS NOT NULL
--            AND sort_code IS NULL
--            AND (CASE WHEN gcom.flag_01 IS NOT NULL THEN bfs.sort_sign_2 = gcom.flag_01 ELSE true END)
--            AND (CASE WHEN gcom.flag_01_anti IS NOT NULL THEN bfs.sort_sign_2 NOT IN (SELECT unnest(string_to_array(gcom.flag_01_anti,','))) ELSE true END)
--            AND (CASE WHEN gcom.culture_season_id ISNULL THEN true ELSE gcom.culture_season_id = coalesce(bfs.culture_mix_season_id, bfs.culture_season_id) END)
--            AND (CASE WHEN gcom.kind_plant_material ISNULL THEN true ELSE gcom.kind_plant_material = bfs.culture_kind_plant_material END)
--          GROUP BY gcom.id, gcom.tree_parent_id, gcom.stc_object_id, gcom.stc_parent_id, gcom.view_name, gcom.group_level, gcom.culture_id, gcom.turn_all, gcom.turn_in,
--                   join_cultures.name,
--                   bfs.sort_code, bfs.sort_name, bfs.sort_type, bfs.sort_region, bfs.sort_year, bfs.seedprod_seeds_kind,
--                   bfs.sort_originator_main, bfs.sort_originator_main_name, bfs.sort_originator_main_country_code_iso, bfs.sort_originator_main_country_name, bfs.sort_country_code_iso, bfs.sort_country_name,
--                   bfs.ter_region_id, bfs.ter_region_name, bfs.ter_federal_district_id, bfs.ter_federal_district_name, bfs.ter_federal_district_turn, bfs.ter_region_turn
--      ),

     sorts_w_culcats AS (
         SELECT
             spr.id, spr.type, spr.tree_parent_id, spr.stc_object_id, spr.stc_parent_id, spr.group_level, spr.turn_all, spr.turn_in, spr.ter_federal_district_turn, spr.ter_region_turn, spr.view_name, spr.name, spr.cnt_groups, spr.cnt_cultures, spr.count_backfills,
--              spr.flag_01, spr.flag_01_anti, spr.culture_season_id, spr.kind_plant_material,

             CASE WHEN :details AND (spr.count_backfills > 0 OR spr.count_infos > 0) THEN jsonb_agg(cast(ROW (
                 coalesce(bfs_pre.post_id, bfs_pre.post_sale_id, bfs_pre.id),
                 bfs_pre.nat_ter_region_name,
                 CASE WHEN bfs_pre.post_id NOTNULL THEN 'RSC_PAST' WHEN bfs_pre.post_sale_id NOTNULL THEN 'RSC_PAST_SALE' ELSE 'RSC' END,

                 bfs_pre.view_tmp_name,
                 bfs_pre.sort_code,

                 null, -- qual_checked
                 null, -- qual_conditioned
                 null, -- qual_ncon_all
                 null, -- qual_ncon_pests
                 null, -- qual_ncon_debris
                 null, -- qual_ncon_hum
                 null, -- qual_ncon_germ
                 null, -- qual_ncon_germ_lt_10
                 null, -- qual_ncon_germ_10_20

                 bfs_pre.fill_all,
                 null, -- bfs_pre.fill_seed_fund,
                 bfs_pre.fill_seed_for_sale,
                 null, -- bfs_pre.fill_removed_seed_fund,
                 bfs_pre.fill_avaliable,
                 null, -- bfs_pre.fill_sown,
                 null, -- bfs_pre.fill_deleted,
                 null, -- bfs_pre.fill_sold,
                 null, -- bfs_pre.fill_defected,
                 null, -- bfs_pre.fill_fed,
                 null, -- bfs_pre.fill_reformed,
                 null, -- bfs_pre.ru_fill_all,
                 null, -- bfs_pre.ru_fill_localized,
                 null, -- bfs_pre.ru_fill_purchase_en,
                 null, -- bfs_pre.en_fill_all,
                 null, -- bfs_pre.en_fill_localized,
                 null, -- bfs_pre.en_fill_purchase_en,
                 null, -- en_fill_purchase_en_processed
                 null, -- bfs_pre.nr_fill_all,
                 null, -- bfs_pre.nr_fill_purchase_en,
                 bfs_pre.rp_nc,
                 bfs_pre.rp_os,
                 bfs_pre.rp_es,

                 null, -- rp_rep_PP1
                 null, -- rp_rep_SSE
                 null, -- rp_rep_SE

                 bfs_pre.rp_rs_1,
                 bfs_pre.rp_rs_1_rst,
                 bfs_pre.rp_rs_2,
                 bfs_pre.rp_rs_2_rst,
                 bfs_pre.rp_rs_3,
                 bfs_pre.rp_rs_3_rst,
                 bfs_pre.rp_rs_4,
                 bfs_pre.rp_rs_4_rst,
                 bfs_pre.rp_f1,
                 bfs_pre.rp_rs_5b,
                 bfs_pre.rp_rs_5b_rst,
                 bfs_pre.rp_not,

                 null, -- sa_all

                 null, -- sa_nc
                 null, -- sa_os
                 null, -- sa_es

                 null, -- sa_rep_PP1
                 null, -- sa_rep_SSE
                 null, -- sa_rep_SE

                 null, -- sa_rs_1
                 null, -- sa_rs_1_rst
                 null, -- sa_rs_2
                 null, -- sa_rs_2_rst
                 null, -- sa_rs_3
                 null, -- sa_rs_3_rst
                 null, -- sa_rs_4
                 null, -- sa_rs_4_rst
                 null, -- sa_f1
                 null, -- sa_rs_5b
                 null, -- sa_rs_5b_rst
                 null, -- sa_not

                 null, -- bfs_pre.fill_purchase_en,
                 null -- fill_purchase_en_processed
                 ) as seeds_info_wrapper)) ELSE cast(null AS jsonb) END AS backfills_data,

             spr.count_infos,
             cast(null AS jsonb) AS infos_data,



             spr.ids_backfills,
             spr.ids_infos,


             spr.ter_region_id, spr.ter_region_name, spr.ter_federal_district_id, spr.ter_federal_district_name, spr.culture_name, spr.culture_id, spr.sort_type, spr.sort_code, spr.sort_name, spr.sort_region, spr.sort_year, spr.sort_originator_main, spr.sort_originator_main_name, spr.sort_originator_main_country_code_iso, spr.sort_originator_main_country_name, spr.sort_country_code_iso, spr.sort_country_name, spr.seedprod_seeds_kind, spr.fill_all, spr.fill_seed_fund, spr.fill_seed_for_sale, spr.fill_removed_seed_fund, spr.fill_AVALIABLE, spr.fill_SOWN, spr.fill_DELETED, spr.fill_SOLD, spr.fill_DEFECTED, spr.fill_FED, spr.fill_REFORMED, spr.ru_fill_all, spr.ru_fill_localized, spr.ru_fill_purchase_en, spr.en_fill_all, spr.en_fill_localized, spr.en_fill_purchase_en, spr.nr_fill_all, spr.nr_fill_purchase_en, spr.rp_NC, spr.rp_OS, spr.rp_ES, spr.rp_RS_1, spr.rp_RS_1_RST, spr.rp_RS_2, spr.rp_RS_2_RST, spr.rp_RS_3, spr.rp_RS_3_RST, spr.rp_RS_4, spr.rp_RS_4_RST, spr.rp_F1, spr.rp_RS_5B, spr.rp_RS_5B_RST, spr.rp_NOT, spr.fill_purchase_en
         FROM
--              (SELECT * FROM sorts_w_culcats_pre
--               UNION SELECT * FROM sorts_w_culcats_pre_null_sorts
--              )

                 sorts_w_culcats_pre AS spr
                LEFT JOIN bfs_pre ON (CASE WHEN :details THEN (
             bfs_pre.ter_region_id = spr.ter_region_id
                 AND bfs_pre.culture_id = spr.culture_id
                 AND bfs_pre.seedprod_seeds_kind = spr.seedprod_seeds_kind
                 AND (CASE WHEN spr.sort_code IS NOT NULL THEN bfs_pre.sort_code = spr.sort_code ELSE bfs_pre.sort_code ISNULL END)
                 AND (CASE WHEN spr.flag_01 IS NOT NULL THEN bfs_pre.sort_sign_2 = spr.flag_01 ELSE true END)
                 AND (CASE WHEN spr.flag_01_anti IS NOT NULL THEN bfs_pre.sort_sign_2 NOT IN (SELECT unnest(string_to_array(spr.flag_01_anti,','))) ELSE true END)
                 AND (CASE WHEN spr.culture_season_id ISNULL THEN true ELSE spr.culture_season_id = coalesce(bfs_pre.culture_mix_season_id, bfs_pre.culture_season_id) END)
                 AND (CASE WHEN spr.kind_plant_material ISNULL THEN true ELSE spr.kind_plant_material = bfs_pre.culture_kind_plant_material END)) ELSE false END)
         GROUP BY spr.id, spr.type, spr.tree_parent_id, spr.stc_object_id, spr.stc_parent_id, spr.group_level, spr.turn_all, spr.turn_in, spr.ter_federal_district_turn, spr.ter_region_turn, spr.view_name, spr.name, spr.cnt_groups, spr.cnt_cultures, spr.count_backfills, spr.count_infos,
                  spr.ids_backfills, ids_infos,
                  spr.ter_region_id, spr.ter_region_name, spr.ter_federal_district_id, spr.ter_federal_district_name, spr.culture_name, spr.culture_id, spr.sort_type, spr.sort_code, spr.sort_name, spr.sort_region, spr.sort_year, spr.sort_originator_main, spr.sort_originator_main_name, spr.sort_originator_main_country_code_iso, spr.sort_originator_main_country_name, spr.sort_country_code_iso, spr.sort_country_name, spr.seedprod_seeds_kind, spr.fill_all, spr.fill_seed_fund, spr.fill_seed_for_sale, spr.fill_removed_seed_fund, spr.fill_AVALIABLE, spr.fill_SOWN, spr.fill_DELETED, spr.fill_SOLD, spr.fill_DEFECTED, spr.fill_FED, spr.fill_REFORMED, spr.ru_fill_all, spr.ru_fill_localized, spr.ru_fill_purchase_en, spr.en_fill_all, spr.en_fill_localized, spr.en_fill_purchase_en, spr.nr_fill_all, spr.nr_fill_purchase_en, spr.rp_NC, spr.rp_OS, spr.rp_ES, spr.rp_RS_1, spr.rp_RS_1_RST, spr.rp_RS_2, spr.rp_RS_2_RST, spr.rp_RS_3, spr.rp_RS_3_RST, spr.rp_RS_4, spr.rp_RS_4_RST, spr.rp_F1, spr.rp_RS_5B, spr.rp_RS_5B_RST, spr.rp_NOT, spr.fill_purchase_en,
                  spr.flag_01, spr.flag_01_anti, spr.culture_season_id, spr.kind_plant_material
     ),


--      sorts_w_culcats AS (
--          SELECT
--              spr.id, spr.type, spr.tree_parent_id, spr.stc_object_id, spr.stc_parent_id, spr.group_level, spr.turn_all, spr.turn_in, spr.ter_federal_district_turn, spr.ter_region_turn, spr.view_name, spr.name, spr.cnt_groups, spr.cnt_cultures,
--              spr.count_backfills, spr.backfills_data,
--
--              spr.count_infos,
-- --              CASE WHEN :details AND spr.count_infos > 0 THEN jsonb_agg(cast(ROW (
-- --                  info_bfs_pre.info_id,
-- --                  info_bfs_pre.nat_ter_region_name,
-- --                  'OTHER',
-- --                  info_bfs_pre.view_tmp_name,
-- --                  info_bfs_pre.sort_code,
-- --
-- --                  null, -- qual_checked
-- --                  null, -- qual_conditioned
-- --                  null, -- qual_ncon_all
-- --                  null, -- qual_ncon_pests
-- --                  null, -- qual_ncon_debris
-- --                  null, -- qual_ncon_hum
-- --                  null, -- qual_ncon_germ
-- --                  null, -- qual_ncon_germ_lt_10
-- --                  null, -- qual_ncon_germ_10_20
-- --
-- --                  info_bfs_pre.fill_all,
-- --                  info_bfs_pre.fill_seed_fund,
-- --                  info_bfs_pre.fill_seed_for_sale,
-- --                  info_bfs_pre.fill_removed_seed_fund,
-- --                  info_bfs_pre.fill_avaliable,
-- --                  info_bfs_pre.fill_sown,
-- --                  info_bfs_pre.fill_deleted,
-- --                  info_bfs_pre.fill_sold,
-- --                  info_bfs_pre.fill_defected,
-- --                  info_bfs_pre.fill_fed,
-- --                  info_bfs_pre.fill_reformed,
-- --                  info_bfs_pre.ru_fill_all,
-- --                  info_bfs_pre.ru_fill_localized,
-- --                  info_bfs_pre.ru_fill_purchase_en,
-- --                  info_bfs_pre.en_fill_all,
-- --                  info_bfs_pre.en_fill_localized,
-- --                  info_bfs_pre.en_fill_purchase_en,
-- --                  null, -- en_fill_purchase_en_processed
-- --                  info_bfs_pre.nr_fill_all,
-- --                  info_bfs_pre.nr_fill_purchase_en,
-- --                  info_bfs_pre.rp_nc,
-- --                  info_bfs_pre.rp_os,
-- --                  info_bfs_pre.rp_es,
-- --
-- --                  null, -- rp_rep_PP1
-- --                  null, -- rp_rep_SSE
-- --                  null, -- rp_rep_SE
-- --
-- --                  info_bfs_pre.rp_rs_1,
-- --                  info_bfs_pre.rp_rs_1_rst,
-- --                  info_bfs_pre.rp_rs_2,
-- --                  info_bfs_pre.rp_rs_2_rst,
-- --                  info_bfs_pre.rp_rs_3,
-- --                  info_bfs_pre.rp_rs_3_rst,
-- --                  info_bfs_pre.rp_rs_4,
-- --                  info_bfs_pre.rp_rs_4_rst,
-- --                  info_bfs_pre.rp_f1,
-- --                  info_bfs_pre.rp_rs_5b,
-- --                  info_bfs_pre.rp_rs_5b_rst,
-- --                  info_bfs_pre.rp_not,
-- --
-- --                  null, -- sa_all
-- --
-- --                  null, -- sa_nc
-- --                  null, -- sa_os
-- --                  null, -- sa_es
-- --
-- --                  null, -- sa_rep_PP1
-- --                  null, -- sa_rep_SSE
-- --                  null, -- sa_rep_SE
-- --
-- --                  null, -- sa_rs_1
-- --                  null, -- sa_rs_1_rst
-- --                  null, -- sa_rs_2
-- --                  null, -- sa_rs_2_rst
-- --                  null, -- sa_rs_3
-- --                  null, -- sa_rs_3_rst
-- --                  null, -- sa_rs_4
-- --                  null, -- sa_rs_4_rst
-- --                  null, -- sa_f1
-- --                  null, -- sa_rs_5b
-- --                  null, -- sa_rs_5b_rst
-- --                  null, -- sa_not
-- --
-- --                  info_bfs_pre.fill_purchase_en,
-- --                  null -- fill_purchase_en_processed
-- --                  ) as seeds_info_wrapper)) ELSE cast(null AS jsonb) END AS infos_data,
--              cast(null AS jsonb) AS infos_data,
--
--
--              ids_backfills, ids_infos,
--
--              spr.ter_region_id, spr.ter_region_name, spr.ter_federal_district_id, spr.ter_federal_district_name, spr.culture_name, spr.culture_id, spr.sort_type, spr.sort_code, spr.sort_name, spr.sort_region, spr.sort_year, spr.sort_originator_main, spr.sort_originator_main_name, spr.sort_originator_main_country_code_iso, spr.sort_originator_main_country_name, spr.sort_country_code_iso, spr.sort_country_name, spr.seedprod_seeds_kind, spr.fill_all, spr.fill_seed_fund, spr.fill_seed_for_sale, spr.fill_removed_seed_fund, spr.fill_AVALIABLE, spr.fill_SOWN, spr.fill_DELETED, spr.fill_SOLD, spr.fill_DEFECTED, spr.fill_FED, spr.fill_REFORMED, spr.ru_fill_all, spr.ru_fill_localized, spr.ru_fill_purchase_en, spr.en_fill_all, spr.en_fill_localized, spr.en_fill_purchase_en, spr.nr_fill_all, spr.nr_fill_purchase_en, spr.rp_NC, spr.rp_OS, spr.rp_ES, spr.rp_RS_1, spr.rp_RS_1_RST, spr.rp_RS_2, spr.rp_RS_2_RST, spr.rp_RS_3, spr.rp_RS_3_RST, spr.rp_RS_4, spr.rp_RS_4_RST, spr.rp_F1, spr.rp_RS_5B, spr.rp_RS_5B_RST, spr.rp_NOT, spr.fill_purchase_en
--          FROM sorts_w_culcats_pre_infos AS spr
--                   LEFT JOIN info_bfs_pre ON false
--          --                       (CASE WHEN :details AND (:data_layer = 'FULL' OR :data_layer = 'RSC') THEN
-- --                           (info_bfs_pre.ter_region_id = spr.ter_region_id
-- --                  AND info_bfs_pre.culture_id = spr.culture_id
-- --                  AND info_bfs_pre.seedprod_seeds_kind = spr.seedprod_seeds_kind
-- --                  AND (CASE WHEN spr.sort_code IS NOT NULL THEN info_bfs_pre.sort_code = spr.sort_code ELSE info_bfs_pre.sort_code ISNULL END)
-- --                  AND (CASE WHEN spr.flag_01 IS NOT NULL THEN info_bfs_pre.sort_sign_2 = spr.flag_01 ELSE true END)
-- --                  AND (CASE WHEN spr.flag_01_anti IS NOT NULL THEN info_bfs_pre.sort_sign_2 NOT IN (SELECT unnest(string_to_array(spr.flag_01_anti,','))) ELSE true END)
-- --                  AND (CASE WHEN spr.culture_season_id ISNULL THEN true ELSE spr.culture_season_id = coalesce(info_bfs_pre.culture_mix_season_id, info_bfs_pre.culture_season_id) END)
-- --                  AND (CASE WHEN spr.kind_plant_material ISNULL THEN true ELSE spr.kind_plant_material = info_bfs_pre.culture_kind_plant_material END))
-- --                           ELSE false END
-- --                       )
--          GROUP BY spr.id, spr.type, spr.tree_parent_id, spr.stc_object_id, spr.stc_parent_id, spr.group_level, spr.turn_all, spr.turn_in, spr.ter_federal_district_turn, spr.ter_region_turn, spr.view_name, spr.name, spr.cnt_groups, spr.cnt_cultures,
--                   spr.count_backfills, spr.backfills_data, spr.count_infos,
--                   ids_backfills, ids_infos,
--                   spr.ter_region_id, spr.ter_region_name, spr.ter_federal_district_id, spr.ter_federal_district_name, spr.culture_name, spr.culture_id, spr.sort_type, spr.sort_code, spr.sort_name, spr.sort_region, spr.sort_year, spr.sort_originator_main, spr.sort_originator_main_name, spr.sort_originator_main_country_code_iso, spr.sort_originator_main_country_name, spr.sort_country_code_iso, spr.sort_country_name, spr.seedprod_seeds_kind, spr.fill_all, spr.fill_seed_fund, spr.fill_seed_for_sale, spr.fill_removed_seed_fund, spr.fill_AVALIABLE, spr.fill_SOWN, spr.fill_DELETED, spr.fill_SOLD, spr.fill_DEFECTED, spr.fill_FED, spr.fill_REFORMED, spr.ru_fill_all, spr.ru_fill_localized, spr.ru_fill_purchase_en, spr.en_fill_all, spr.en_fill_localized, spr.en_fill_purchase_en, spr.nr_fill_all, spr.nr_fill_purchase_en, spr.rp_NC, spr.rp_OS, spr.rp_ES, spr.rp_RS_1, spr.rp_RS_1_RST, spr.rp_RS_2, spr.rp_RS_2_RST, spr.rp_RS_3, spr.rp_RS_3_RST, spr.rp_RS_4, spr.rp_RS_4_RST, spr.rp_F1, spr.rp_RS_5B, spr.rp_RS_5B_RST, spr.rp_NOT, spr.fill_purchase_en
--      ),


     grp AS (
         SELECT
             (array_agg(DISTINCT com.id) || array_agg(DISTINCT com_01.id) || array_agg(DISTINCT com_02.id) || array_agg(DISTINCT com_03.id) || array_agg(DISTINCT com_04.id) || array_agg(DISTINCT com_05.id) || array_agg(DISTINCT com_06.id) || array_agg(DISTINCT com_07.id)) AS com_ids

         FROM ase.seed_production_cultures_groups_com AS com
                  LEFT JOIN ase.seed_production_cultures_groups_com AS com_01 ON com_01.tree_parent_id = com.id
                  LEFT JOIN ase.seed_production_cultures_groups_com AS com_02 ON com_02.tree_parent_id = com_01.id
                  LEFT JOIN ase.seed_production_cultures_groups_com AS com_03 ON com_03.tree_parent_id = com_02.id
                  LEFT JOIN ase.seed_production_cultures_groups_com AS com_04 ON com_04.tree_parent_id = com_03.id
                  LEFT JOIN ase.seed_production_cultures_groups_com AS com_05 ON com_05.tree_parent_id = com_04.id
                  LEFT JOIN ase.seed_production_cultures_groups_com AS com_06 ON com_06.tree_parent_id = com_05.id
                  LEFT JOIN ase.seed_production_cultures_groups_com AS com_07 ON com_07.tree_parent_id = com_06.id

         WHERE com.id = :need_id
         GROUP BY com.id
     ),

     grpn AS (
         SELECT uid
         FROM (SELECT DISTINCT unnest(grp.com_ids) AS uid FROM grp) AS uids
         WHERE uid IS NOT NULL
     ),

     cats_raw_ter AS (
         SELECT
             gcom.id,
             gcom.type,
             gcom.tree_parent_id,
             gcom.stc_object_id,
             gcom.stc_parent_id,

             gcom.group_level,

             gcom.turn_all,
             gcom.turn_in,
             join_federal_districts.turn AS ter_federal_district_turn,
             join_regions.turn AS ter_region_turn,

             gcom.view_name,
             gcom.name,

             gcom.cnt_groups,
             gcom.cnt_cultures,

             SUM(swc.count_backfills) AS count_backfills,
             cast(null AS jsonb) AS backfills_data,

             SUM(swc.count_infos) AS count_infos,
             cast(null AS jsonb) AS infos_data,

             cast(null AS bigint[]) AS ids_backfills,
             cast(null AS bigint[]) AS ids_infos,

             join_regions.id AS ter_region_id,
             join_regions.name AS ter_region_name,
             join_federal_districts.id AS ter_federal_district_id,
             join_federal_districts.name AS ter_federal_district_name,


             join_cultures.name AS culture_name,
             gcom.culture_id,
             cast(null AS varchar) AS sort_code,
             cast(null AS varchar) AS sort_name,

             cast(null AS varchar) AS sort_type,
             cast(null AS varchar) AS sort_region,
             cast(null AS int) AS sort_year,

             cast(null AS bigint) sort_originator_main,
             cast(null AS varchar) sort_originator_main_name,
             cast(null AS varchar) sort_originator_main_country_code_iso,
             cast(null AS varchar) sort_originator_main_country_name,
             cast(null AS varchar) sort_country_code_iso,
             cast(null AS varchar) sort_country_name,

             cast(null AS varchar) AS seedprod_seeds_kind,

             SUM(swc.fill_all) AS fill_all,

             SUM(swc.fill_seed_fund) AS fill_seed_fund,
             SUM(swc.fill_seed_for_sale) AS fill_seed_for_sale,
             SUM(swc.fill_removed_seed_fund) AS fill_removed_seed_fund,

             SUM(swc.fill_AVALIABLE) AS fill_AVALIABLE,

             SUM(swc.fill_SOWN) AS fill_SOWN,
             SUM(swc.fill_DELETED) AS fill_DELETED,
             SUM(swc.fill_SOLD) AS fill_SOLD,
             SUM(swc.fill_DEFECTED) AS fill_DEFECTED,
             SUM(swc.fill_FED) AS fill_FED,
             SUM(swc.fill_REFORMED) AS fill_REFORMED,

             SUM(swc.ru_fill_all) AS ru_fill_all,
             SUM(swc.ru_fill_localized) AS ru_fill_localized,
             SUM(swc.ru_fill_purchase_en) AS ru_fill_purchase_en,

             SUM(swc.en_fill_all) AS en_fill_all,
             SUM(swc.en_fill_localized) AS en_fill_localized,
             SUM(swc.en_fill_purchase_en) AS en_fill_purchase_en,

             SUM(swc.nr_fill_all) AS nr_fill_all,
             SUM(swc.nr_fill_purchase_en) AS nr_fill_purchase_en,

             SUM(swc.rp_NC) AS rp_NC,
             SUM(swc.rp_OS) AS rp_OS,
             SUM(swc.rp_ES) AS rp_ES,
             SUM(swc.rp_RS_1) AS rp_RS_1,
             SUM(swc.rp_RS_1_RST) AS rp_RS_1_RST,
             SUM(swc.rp_RS_2) AS rp_RS_2,
             SUM(swc.rp_RS_2_RST) AS rp_RS_2_RST,
             SUM(swc.rp_RS_3) AS rp_RS_3,
             SUM(swc.rp_RS_3_RST) AS rp_RS_3_RST,
             SUM(swc.rp_RS_4) AS rp_RS_4,
             SUM(swc.rp_RS_4_RST) AS rp_RS_4_RST,
             SUM(swc.rp_F1) AS rp_F1,
             SUM(swc.rp_RS_5B) AS rp_RS_5B,
             SUM(swc.rp_RS_5B_RST) AS rp_RS_5B_RST,
             SUM(swc.rp_NOT) AS rp_NOT,
             SUM(swc.fill_purchase_en) AS fill_purchase_en

         FROM ase.seed_production_cultures_groups_com AS gcom
                  JOIN common.ter_regions AS join_regions ON true
                  LEFT JOIN common.cultures AS join_cultures ON join_cultures.id = gcom.culture_id
                  LEFT JOIN common.ter_federal_districts AS join_federal_districts ON join_federal_districts.id = join_regions.federal_district_id
                  LEFT JOIN sorts_w_culcats AS swc ON swc.tree_parent_id = gcom.id AND gcom.type = 'culture' AND swc.type = 'sort' AND swc.ter_region_id = join_regions.id

         GROUP BY gcom.id, gcom.type, gcom.tree_parent_id, gcom.stc_object_id, gcom.stc_parent_id, gcom.view_name, gcom.name, gcom.cnt_groups, gcom.cnt_cultures, gcom.group_level, gcom.culture_id, gcom.turn_all, gcom.turn_in,
                  join_cultures.name,
                  join_regions.id, join_regions.name, join_federal_districts.id, join_federal_districts.name, join_federal_districts.turn, join_regions.turn
     ),

     cats_raw_wo_ter AS (
         SELECT
             gcom.id,
             gcom.type,
             gcom.tree_parent_id,
             gcom.stc_object_id,
             gcom.stc_parent_id,

             gcom.group_level,

             gcom.turn_all,
             gcom.turn_in,
             0 AS ter_federal_district_turn,
             0 AS ter_region_turn,


             gcom.view_name,
             gcom.name,

             gcom.cnt_groups,
             gcom.cnt_cultures,

             SUM(swc.count_backfills) AS count_backfills,
             cast(null AS jsonb) AS backfills_data,

             SUM(swc.count_infos) AS count_infos,
             cast(null AS jsonb) AS infos_data,

             cast(null AS bigint[]) AS ids_backfills,
             cast(null AS bigint[]) AS ids_infos,

             0 AS ter_region_id,
             '' AS ter_region_name,
             0 AS ter_federal_district_id,
             '' AS ter_federal_district_name,


             join_cultures.name AS culture_name,
             gcom.culture_id,
             cast(null AS varchar) AS sort_code,
             cast(null AS varchar) AS sort_name,

             cast(null AS varchar) AS sort_type,
             cast(null AS varchar) AS sort_region,
             cast(null AS int) AS sort_year,

             cast(null AS bigint) sort_originator_main,
             cast(null AS varchar) sort_originator_main_name,
             cast(null AS varchar) sort_originator_main_country_code_iso,
             cast(null AS varchar) sort_originator_main_country_name,
             cast(null AS varchar) sort_country_code_iso,
             cast(null AS varchar) sort_country_name,

             cast(null AS varchar) AS seedprod_seeds_kind,

             SUM(swc.fill_all) AS fill_all,

             SUM(swc.fill_seed_fund) AS fill_seed_fund,
             SUM(swc.fill_seed_for_sale) AS fill_seed_for_sale,
             SUM(swc.fill_removed_seed_fund) AS fill_removed_seed_fund,

             SUM(swc.fill_AVALIABLE) AS fill_AVALIABLE,

             SUM(swc.fill_SOWN) AS fill_SOWN,
             SUM(swc.fill_DELETED) AS fill_DELETED,
             SUM(swc.fill_SOLD) AS fill_SOLD,
             SUM(swc.fill_DEFECTED) AS fill_DEFECTED,
             SUM(swc.fill_FED) AS fill_FED,
             SUM(swc.fill_REFORMED) AS fill_REFORMED,

             SUM(swc.ru_fill_all) AS ru_fill_all,
             SUM(swc.ru_fill_localized) AS ru_fill_localized,
             SUM(swc.ru_fill_purchase_en) AS ru_fill_purchase_en,

             SUM(swc.en_fill_all) AS en_fill_all,
             SUM(swc.en_fill_localized) AS en_fill_localized,
             SUM(swc.en_fill_purchase_en) AS en_fill_purchase_en,

             SUM(swc.nr_fill_all) AS nr_fill_all,
             SUM(swc.nr_fill_purchase_en) AS nr_fill_purchase_en,

             SUM(swc.rp_NC) AS rp_NC,
             SUM(swc.rp_OS) AS rp_OS,
             SUM(swc.rp_ES) AS rp_ES,
             SUM(swc.rp_RS_1) AS rp_RS_1,
             SUM(swc.rp_RS_1_RST) AS rp_RS_1_RST,
             SUM(swc.rp_RS_2) AS rp_RS_2,
             SUM(swc.rp_RS_2_RST) AS rp_RS_2_RST,
             SUM(swc.rp_RS_3) AS rp_RS_3,
             SUM(swc.rp_RS_3_RST) AS rp_RS_3_RST,
             SUM(swc.rp_RS_4) AS rp_RS_4,
             SUM(swc.rp_RS_4_RST) AS rp_RS_4_RST,
             SUM(swc.rp_F1) AS rp_F1,
             SUM(swc.rp_RS_5B) AS rp_RS_5B,
             SUM(swc.rp_RS_5B_RST) AS rp_RS_5B_RST,
             SUM(swc.rp_NOT) AS rp_NOT,
             SUM(swc.fill_purchase_en) AS fill_purchase_en

         FROM ase.seed_production_cultures_groups_com AS gcom
                  LEFT JOIN sorts_w_culcats AS swc ON swc.tree_parent_id = gcom.id AND gcom.type = 'culture' AND swc.type = 'sort'
                  LEFT JOIN common.cultures AS join_cultures ON join_cultures.id = gcom.culture_id

         GROUP BY gcom.id, gcom.type, gcom.tree_parent_id, gcom.stc_object_id, gcom.stc_parent_id, gcom.view_name, gcom.name, gcom.cnt_groups, gcom.cnt_cultures, gcom.group_level, gcom.culture_id, gcom.turn_all, gcom.turn_in,
                  join_cultures.name
     ),


     cats_raw AS (
         SELECT * FROM cats_raw_ter WHERE (CASE WHEN :sel_type = 'KIND_REGIONS' THEN true ELSE false END)
         UNION
         SELECT * FROM cats_raw_wo_ter WHERE (CASE WHEN :sel_type = 'KIND_CULTURES_GROUPS' THEN true ELSE false END)

     ),

     cats_sorts AS (
         SELECT *
         FROM sorts_w_culcats
         WHERE (CASE WHEN :need_id ISNULL THEN true ELSE sorts_w_culcats.id IN (SELECT * FROM grpn) OR sorts_w_culcats.tree_parent_id IN (SELECT * FROM grpn) END)

         UNION

         SELECT
             craw.id,
             craw.type,
             (CASE WHEN :sel_type = 'KIND_REGIONS' AND (:need_id = craw.id) AND craw.type = 'group' THEN craw.ter_region_id ELSE craw.tree_parent_id END) AS tree_parent_id,
             craw.stc_object_id,
             craw.stc_parent_id,

             craw.group_level,

             craw.turn_all,
             craw.turn_in,
             craw.ter_federal_district_turn, craw.ter_region_turn,

             craw.view_name AS view_name,
             craw.name,

             craw.cnt_groups,
             craw.cnt_cultures,

             coalesce(SUM(craw.count_backfills), 0.0) + coalesce(SUM(craw_01.count_backfills), 0.0) + coalesce(SUM(craw_02.count_backfills), 0.0) + coalesce(SUM(craw_03.count_backfills), 0.0) + coalesce(SUM(craw_04.count_backfills), 0.0) + coalesce(SUM(craw_05.count_backfills), 0.0) + coalesce(SUM(craw_06.count_backfills), 0.0)  / 10.0 AS count_backfills,
             cast(null AS jsonb) AS backfills_data,

             coalesce(SUM(craw.count_infos), 0.0) + coalesce(SUM(craw_01.count_infos), 0.0) + coalesce(SUM(craw_02.count_infos), 0.0) + coalesce(SUM(craw_03.count_infos), 0.0) + coalesce(SUM(craw_04.count_infos), 0.0) + coalesce(SUM(craw_05.count_infos), 0.0) + coalesce(SUM(craw_06.count_infos), 0.0) AS count_infos,
             cast(null AS jsonb) AS infos_data,

             cast(null AS bigint[]) AS ids_backfills,
             cast(null AS bigint[]) AS ids_infos,

             craw.ter_region_id, craw.ter_region_name, craw.ter_federal_district_id, craw.ter_federal_district_name,

             craw.culture_name,
             craw.culture_id,
             cast(null AS varchar) AS sort_code,
             cast(null AS varchar) AS sort_name,

             cast(null AS varchar) AS sort_type,
             cast(null AS varchar) AS sort_region,
             cast(null AS int) AS sort_year,

             cast(null AS bigint) sort_originator_main,
             cast(null AS varchar) sort_originator_main_name,
             cast(null AS varchar) sort_originator_main_country_code_iso,
             cast(null AS varchar) sort_originator_main_country_name,
             cast(null AS varchar) sort_country_code_iso,
             cast(null AS varchar) sort_country_name,

             cast(null AS varchar) AS seedprod_seeds_kind,

             coalesce(SUM(craw.fill_all), 0.0) + coalesce(SUM(craw_01.fill_all), 0.0) + coalesce(SUM(craw_02.fill_all), 0.0) + coalesce(SUM(craw_03.fill_all), 0.0) + coalesce(SUM(craw_04.fill_all), 0.0) + coalesce(SUM(craw_05.fill_all), 0.0) + coalesce(SUM(craw_06.fill_all), 0.0) AS fill_all,


--              coalesce(SUM(craw.fill_seed_fund), 0.0) + coalesce(SUM(craw_01.fill_seed_fund), 0.0) + coalesce(SUM(craw_02.fill_seed_fund), 0.0) + coalesce(SUM(craw_03.fill_seed_fund), 0.0) + coalesce(SUM(craw_04.fill_seed_fund), 0.0) + coalesce(SUM(craw_05.fill_seed_fund), 0.0) + coalesce(SUM(craw_06.fill_seed_fund), 0.0)AS fill_seed_fund,
             cast(0 AS double precision) AS fill_seed_fund,
             coalesce(SUM(craw.fill_seed_for_sale), 0.0) + coalesce(SUM(craw_01.fill_seed_for_sale), 0.0) + coalesce(SUM(craw_02.fill_seed_for_sale), 0.0) + coalesce(SUM(craw_03.fill_seed_for_sale), 0.0) + coalesce(SUM(craw_04.fill_seed_for_sale), 0.0) + coalesce(SUM(craw_05.fill_seed_for_sale), 0.0) + coalesce(SUM(craw_06.fill_seed_for_sale), 0.0) AS fill_seed_for_sale,
--              coalesce(SUM(craw.fill_removed_seed_fund), 0.0) + coalesce(SUM(craw_01.fill_removed_seed_fund), 0.0) + coalesce(SUM(craw_02.fill_removed_seed_fund), 0.0) + coalesce(SUM(craw_03.fill_removed_seed_fund), 0.0) + coalesce(SUM(craw_04.fill_removed_seed_fund), 0.0) + coalesce(SUM(craw_05.fill_removed_seed_fund), 0.0) + coalesce(SUM(craw_06.fill_removed_seed_fund), 0.0) AS fill_removed_seed_fund,
             cast(0 AS double precision) AS fill_removed_seed_fund,

             coalesce(SUM(craw.fill_AVALIABLE), 0.0) + coalesce(SUM(craw_01.fill_AVALIABLE), 0.0) + coalesce(SUM(craw_02.fill_AVALIABLE), 0.0) + coalesce(SUM(craw_03.fill_AVALIABLE), 0.0) + coalesce(SUM(craw_04.fill_AVALIABLE), 0.0) + coalesce(SUM(craw_05.fill_AVALIABLE), 0.0) + coalesce(SUM(craw_06.fill_AVALIABLE), 0.0)AS fill_AVALIABLE,

--              coalesce(SUM(craw.fill_SOWN), 0.0) + coalesce(SUM(craw_01.fill_SOWN), 0.0) + coalesce(SUM(craw_02.fill_SOWN), 0.0) + coalesce(SUM(craw_03.fill_SOWN), 0.0) + coalesce(SUM(craw_04.fill_SOWN), 0.0) + coalesce(SUM(craw_05.fill_SOWN), 0.0) + coalesce(SUM(craw_06.fill_SOWN), 0.0) AS fill_SOWN,
--              coalesce(SUM(craw.fill_DELETED), 0.0) + coalesce(SUM(craw_01.fill_DELETED), 0.0) + coalesce(SUM(craw_02.fill_DELETED), 0.0) + coalesce(SUM(craw_03.fill_DELETED), 0.0) + coalesce(SUM(craw_04.fill_DELETED), 0.0) + coalesce(SUM(craw_05.fill_DELETED), 0.0) + coalesce(SUM(craw_06.fill_DELETED), 0.0) AS fill_DELETED,
--              coalesce(SUM(craw.fill_SOLD), 0.0) + coalesce(SUM(craw_01.fill_SOLD), 0.0) + coalesce(SUM(craw_02.fill_SOLD), 0.0) + coalesce(SUM(craw_03.fill_SOLD), 0.0) + coalesce(SUM(craw_04.fill_SOLD), 0.0) + coalesce(SUM(craw_05.fill_SOLD), 0.0) + coalesce(SUM(craw_06.fill_SOLD), 0.0) AS fill_SOLD,
--              coalesce(SUM(craw.fill_DEFECTED), 0.0) + coalesce(SUM(craw_01.fill_DEFECTED), 0.0) + coalesce(SUM(craw_02.fill_DEFECTED), 0.0) + coalesce(SUM(craw_03.fill_DEFECTED), 0.0) + coalesce(SUM(craw_04.fill_DEFECTED), 0.0) + coalesce(SUM(craw_05.fill_DEFECTED), 0.0) + coalesce(SUM(craw_06.fill_DEFECTED), 0.0) AS fill_DEFECTED,
--              coalesce(SUM(craw.fill_FED), 0.0) + coalesce(SUM(craw_01.fill_FED), 0.0) + coalesce(SUM(craw_02.fill_FED), 0.0) + coalesce(SUM(craw_03.fill_FED), 0.0) + coalesce(SUM(craw_04.fill_FED), 0.0) + coalesce(SUM(craw_05.fill_FED), 0.0) + coalesce(SUM(craw_06.fill_FED), 0.0) AS fill_FED,
--              coalesce(SUM(craw.fill_REFORMED), 0.0) + coalesce(SUM(craw_01.fill_REFORMED), 0.0) + coalesce(SUM(craw_02.fill_REFORMED), 0.0) + coalesce(SUM(craw_03.fill_REFORMED), 0.0) + coalesce(SUM(craw_04.fill_REFORMED), 0.0) + coalesce(SUM(craw_05.fill_REFORMED), 0.0) + coalesce(SUM(craw_06.fill_REFORMED), 0.0) AS fill_REFORMED,

--              coalesce(SUM(craw.ru_fill_all), 0.0) + coalesce(SUM(craw_01.ru_fill_all), 0.0) + coalesce(SUM(craw_02.ru_fill_all), 0.0) + coalesce(SUM(craw_03.ru_fill_all), 0.0) + coalesce(SUM(craw_04.ru_fill_all), 0.0) + coalesce(SUM(craw_05.ru_fill_all), 0.0) + coalesce(SUM(craw_06.ru_fill_all), 0.0) AS ru_fill_all,
--              coalesce(SUM(craw.ru_fill_localized), 0.0) + coalesce(SUM(craw_01.ru_fill_localized), 0.0) + coalesce(SUM(craw_02.ru_fill_localized), 0.0) + coalesce(SUM(craw_03.ru_fill_localized), 0.0) + coalesce(SUM(craw_04.ru_fill_localized), 0.0) + coalesce(SUM(craw_05.ru_fill_localized), 0.0) + coalesce(SUM(craw_06.ru_fill_localized), 0.0) AS ru_fill_localized,
--              coalesce(SUM(craw.ru_fill_purchase_en), 0.0) + coalesce(SUM(craw_01.ru_fill_purchase_en), 0.0) + coalesce(SUM(craw_02.ru_fill_purchase_en), 0.0) + coalesce(SUM(craw_03.ru_fill_purchase_en), 0.0) + coalesce(SUM(craw_04.ru_fill_purchase_en), 0.0) + coalesce(SUM(craw_05.ru_fill_purchase_en), 0.0) + coalesce(SUM(craw_06.ru_fill_purchase_en), 0.0)AS ru_fill_purchase_en,

--              coalesce(SUM(craw.en_fill_all), 0.0) + coalesce(SUM(craw_01.en_fill_all), 0.0) + coalesce(SUM(craw_02.en_fill_all), 0.0) + coalesce(SUM(craw_03.en_fill_all), 0.0) + coalesce(SUM(craw_04.en_fill_all), 0.0) + coalesce(SUM(craw_05.en_fill_all), 0.0) + coalesce(SUM(craw_06.en_fill_all), 0.0) AS en_fill_all,
--              coalesce(SUM(craw.en_fill_localized), 0.0) + coalesce(SUM(craw_01.en_fill_localized), 0.0) + coalesce(SUM(craw_02.en_fill_localized), 0.0) + coalesce(SUM(craw_03.en_fill_localized), 0.0) + coalesce(SUM(craw_04.en_fill_localized), 0.0) + coalesce(SUM(craw_05.en_fill_localized), 0.0) + coalesce(SUM(craw_06.en_fill_localized), 0.0) AS en_fill_localized,
--              coalesce(SUM(craw.en_fill_purchase_en), 0.0) + coalesce(SUM(craw_01.en_fill_purchase_en), 0.0) + coalesce(SUM(craw_02.en_fill_purchase_en), 0.0) + coalesce(SUM(craw_03.en_fill_purchase_en), 0.0) + coalesce(SUM(craw_04.en_fill_purchase_en), 0.0) + coalesce(SUM(craw_05.en_fill_purchase_en), 0.0) + coalesce(SUM(craw_06.en_fill_purchase_en), 0.0) AS en_fill_purchase_en,

--              coalesce(SUM(craw.nr_fill_all), 0.0) + coalesce(SUM(craw_01.nr_fill_all), 0.0) + coalesce(SUM(craw_02.nr_fill_all), 0.0) + coalesce(SUM(craw_03.nr_fill_all), 0.0) + coalesce(SUM(craw_04.nr_fill_all), 0.0) + coalesce(SUM(craw_05.nr_fill_all), 0.0) + coalesce(SUM(craw_06.nr_fill_all), 0.0) AS nr_fill_all,
--              coalesce(SUM(craw.nr_fill_purchase_en), 0.0) + coalesce(SUM(craw_01.nr_fill_purchase_en), 0.0) + coalesce(SUM(craw_02.nr_fill_purchase_en), 0.0) + coalesce(SUM(craw_03.nr_fill_purchase_en), 0.0) + coalesce(SUM(craw_04.nr_fill_purchase_en), 0.0) + coalesce(SUM(craw_05.nr_fill_purchase_en), 0.0) + coalesce(SUM(craw_06.nr_fill_purchase_en), 0.0) AS nr_fill_purchase_en,
             cast(0 AS double precision) AS fill_SOWN,
             cast(0 AS double precision) AS fill_DELETED,
             cast(0 AS double precision) AS fill_SOLD,
             cast(0 AS double precision) AS fill_DEFECTED,
             cast(0 AS double precision) AS fill_FED,
             cast(0 AS double precision) AS fill_REFORMED,

             cast(0 AS double precision) AS ru_fill_all,
             cast(0 AS double precision) AS ru_fill_localized,
             cast(0 AS double precision) AS ru_fill_purchase_en,

             cast(0 AS double precision) AS en_fill_all,
             cast(0 AS double precision) AS en_fill_localized,
             cast(0 AS double precision) AS en_fill_purchase_en,


             cast(0 AS double precision) AS nr_fill_all,
             cast(0 AS double precision) ASnr_fill_purchase_en,

             coalesce(SUM(craw.rp_NC), 0.0) + coalesce(SUM(craw_01.rp_NC), 0.0) + coalesce(SUM(craw_02.rp_NC), 0.0) + coalesce(SUM(craw_03.rp_NC), 0.0) + coalesce(SUM(craw_04.rp_NC), 0.0) + coalesce(SUM(craw_05.rp_NC), 0.0) + coalesce(SUM(craw_06.rp_NC), 0.0) AS rp_NC,
             coalesce(SUM(craw.rp_OS), 0.0) + coalesce(SUM(craw_01.rp_OS), 0.0) + coalesce(SUM(craw_02.rp_OS), 0.0) + coalesce(SUM(craw_03.rp_OS), 0.0) + coalesce(SUM(craw_04.rp_OS), 0.0) + coalesce(SUM(craw_05.rp_OS), 0.0) + coalesce(SUM(craw_06.rp_OS), 0.0) AS rp_OS,
             coalesce(SUM(craw.rp_ES), 0.0) + coalesce(SUM(craw_01.rp_ES), 0.0) + coalesce(SUM(craw_02.rp_ES), 0.0) + coalesce(SUM(craw_03.rp_ES), 0.0) + coalesce(SUM(craw_04.rp_ES), 0.0) + coalesce(SUM(craw_05.rp_ES), 0.0) + coalesce(SUM(craw_06.rp_ES), 0.0) AS rp_ES,
             coalesce(SUM(craw.rp_RS_1), 0.0) + coalesce(SUM(craw_01.rp_RS_1), 0.0) + coalesce(SUM(craw_02.rp_RS_1), 0.0) + coalesce(SUM(craw_03.rp_RS_1), 0.0) + coalesce(SUM(craw_04.rp_RS_1), 0.0) + coalesce(SUM(craw_05.rp_RS_1), 0.0) + coalesce(SUM(craw_06.rp_RS_1), 0.0) AS rp_RS_1,
             coalesce(SUM(craw.rp_RS_1_RST), 0.0) + coalesce(SUM(craw_01.rp_RS_1_RST), 0.0) + coalesce(SUM(craw_02.rp_RS_1_RST), 0.0) + coalesce(SUM(craw_03.rp_RS_1_RST), 0.0) + coalesce(SUM(craw_04.rp_RS_1_RST), 0.0) + coalesce(SUM(craw_05.rp_RS_1_RST), 0.0) + coalesce(SUM(craw_06.rp_RS_1_RST), 0.0) AS rp_RS_1_RST,
             coalesce(SUM(craw.rp_RS_2), 0.0) + coalesce(SUM(craw_01.rp_RS_2), 0.0) + coalesce(SUM(craw_02.rp_RS_2), 0.0) + coalesce(SUM(craw_03.rp_RS_2), 0.0) + coalesce(SUM(craw_04.rp_RS_2), 0.0) + coalesce(SUM(craw_05.rp_RS_2), 0.0) + coalesce(SUM(craw_06.rp_RS_2), 0.0) AS rp_RS_2,
             coalesce(SUM(craw.rp_RS_2_RST), 0.0) + coalesce(SUM(craw_01.rp_RS_2_RST), 0.0) + coalesce(SUM(craw_02.rp_RS_2_RST), 0.0) + coalesce(SUM(craw_03.rp_RS_2_RST), 0.0) + coalesce(SUM(craw_04.rp_RS_2_RST), 0.0) + coalesce(SUM(craw_05.rp_RS_2_RST), 0.0) + coalesce(SUM(craw_06.rp_RS_2_RST), 0.0) AS rp_RS_2_RST,
             coalesce(SUM(craw.rp_RS_3), 0.0) + coalesce(SUM(craw_01.rp_RS_3), 0.0) + coalesce(SUM(craw_02.rp_RS_3), 0.0) + coalesce(SUM(craw_03.rp_RS_3), 0.0) + coalesce(SUM(craw_04.rp_RS_3), 0.0) + coalesce(SUM(craw_05.rp_RS_3), 0.0) + coalesce(SUM(craw_06.rp_RS_3), 0.0) AS rp_RS_3,
             coalesce(SUM(craw.rp_RS_3_RST), 0.0) + coalesce(SUM(craw_01.rp_RS_3_RST), 0.0) + coalesce(SUM(craw_02.rp_RS_3_RST), 0.0) + coalesce(SUM(craw_03.rp_RS_3_RST), 0.0) + coalesce(SUM(craw_04.rp_RS_3_RST), 0.0) + coalesce(SUM(craw_05.rp_RS_3_RST), 0.0) + coalesce(SUM(craw_06.rp_RS_3_RST), 0.0) AS rp_RS_3_RST,
             coalesce(SUM(craw.rp_RS_4), 0.0) + coalesce(SUM(craw_01.rp_RS_4), 0.0) + coalesce(SUM(craw_02.rp_RS_4), 0.0) + coalesce(SUM(craw_03.rp_RS_4), 0.0) + coalesce(SUM(craw_04.rp_RS_4), 0.0) + coalesce(SUM(craw_05.rp_RS_4), 0.0) + coalesce(SUM(craw_06.rp_RS_4), 0.0) AS rp_RS_4,
             coalesce(SUM(craw.rp_RS_4_RST), 0.0) + coalesce(SUM(craw_01.rp_RS_4_RST), 0.0) + coalesce(SUM(craw_02.rp_RS_4_RST), 0.0) + coalesce(SUM(craw_03.rp_RS_4_RST), 0.0) + coalesce(SUM(craw_04.rp_RS_4_RST), 0.0) + coalesce(SUM(craw_05.rp_RS_4_RST), 0.0) + coalesce(SUM(craw_06.rp_RS_4_RST), 0.0) AS rp_RS_4_RST,
             coalesce(SUM(craw.rp_F1), 0.0) + coalesce(SUM(craw_01.rp_F1), 0.0) + coalesce(SUM(craw_02.rp_F1), 0.0) + coalesce(SUM(craw_03.rp_F1), 0.0) + coalesce(SUM(craw_04.rp_F1), 0.0) + coalesce(SUM(craw_05.rp_F1), 0.0) + coalesce(SUM(craw_06.rp_F1), 0.0) AS rp_F1,
             coalesce(SUM(craw.rp_RS_5B), 0.0) + coalesce(SUM(craw_01.rp_RS_5B), 0.0) + coalesce(SUM(craw_02.rp_RS_5B), 0.0) + coalesce(SUM(craw_03.rp_RS_5B), 0.0) + coalesce(SUM(craw_04.rp_RS_5B), 0.0) + coalesce(SUM(craw_05.rp_RS_5B), 0.0) + coalesce(SUM(craw_06.rp_RS_5B), 0.0) AS rp_RS_5B,
             coalesce(SUM(craw.rp_RS_5B_RST), 0.0) + coalesce(SUM(craw_01.rp_RS_5B_RST), 0.0) + coalesce(SUM(craw_02.rp_RS_5B_RST), 0.0) + coalesce(SUM(craw_03.rp_RS_5B_RST), 0.0) + coalesce(SUM(craw_04.rp_RS_5B_RST), 0.0) + coalesce(SUM(craw_05.rp_RS_5B_RST), 0.0) + coalesce(SUM(craw_06.rp_RS_5B_RST), 0.0) AS rp_RS_5B_RST,
             coalesce(SUM(craw.rp_NOT), 0.0) + coalesce(SUM(craw_01.rp_NOT), 0.0) + coalesce(SUM(craw_02.rp_NOT), 0.0) + coalesce(SUM(craw_03.rp_NOT), 0.0) + coalesce(SUM(craw_04.rp_NOT), 0.0) + coalesce(SUM(craw_05.rp_NOT), 0.0) + + coalesce(SUM(craw_06.rp_NOT), 0.0) AS rp_NOT,
--              coalesce(SUM(craw.fill_purchase_en), 0.0) + coalesce(SUM(craw_01.fill_purchase_en), 0.0) + coalesce(SUM(craw_02.fill_purchase_en), 0.0) + coalesce(SUM(craw_03.fill_purchase_en), 0.0) + coalesce(SUM(craw_04.fill_purchase_en), 0.0) + coalesce(SUM(craw_05.fill_purchase_en), 0.0) + coalesce(SUM(craw_06.fill_purchase_en), 0.0) AS fill_purchase_en
             cast(0 AS double precision) AS fill_purchase_en

         FROM cats_raw AS craw
                  LEFT JOIN cats_raw AS craw_01 ON craw_01.tree_parent_id = craw.id AND craw_01.ter_region_id = craw.ter_region_id
                  LEFT JOIN cats_raw AS craw_02 ON craw_02.tree_parent_id = craw_01.id AND craw_02.ter_region_id = craw_01.ter_region_id
                  LEFT JOIN cats_raw AS craw_03 ON craw_03.tree_parent_id = craw_02.id AND craw_03.ter_region_id = craw_02.ter_region_id
                  LEFT JOIN cats_raw AS craw_04 ON craw_04.tree_parent_id = craw_03.id AND craw_04.ter_region_id = craw_03.ter_region_id
                  LEFT JOIN cats_raw AS craw_05 ON craw_05.tree_parent_id = craw_04.id AND craw_05.ter_region_id = craw_04.ter_region_id
                  LEFT JOIN cats_raw AS craw_06 ON craw_06.tree_parent_id = craw_05.id AND craw_06.ter_region_id = craw_05.ter_region_id

         WHERE (CASE WHEN :need_id ISNULL THEN true ELSE craw.id IN (SELECT * FROM grpn) OR craw.tree_parent_id IN (SELECT * FROM grpn) END)

         GROUP BY craw.id, craw.type, craw.tree_parent_id, craw.stc_object_id, craw.stc_parent_id, craw.view_name, craw.name, craw.cnt_groups, craw.cnt_cultures, craw.group_level, craw.culture_name, craw.culture_id, craw.turn_all, craw.turn_in,
                  craw.ter_region_id, craw.ter_region_name, craw.ter_federal_district_id, craw.ter_federal_district_name, craw.ter_federal_district_turn, craw.ter_region_turn
     ),

     regions_solv AS (
         SELECT join_ter_regions.id,
                'region' AS type,
                join_ter_regions.federal_district_id AS tree_parent_id,
                stc_object_id, stc_parent_id, 2 AS group_level,

                0 AS turn_all, -1 AS turn_in,
                join_ter_federal_districts.turn AS ter_federal_district_turn, join_ter_regions.turn AS ter_region_turn,

                join_ter_regions.name_short || ' - ' || gps.name AS view_name,
                join_ter_regions.name_short || ' - ' || gps.name AS name,

                cnt_groups, cnt_cultures, count_backfills, cast(null AS jsonb) AS backfills_data, count_infos, cast(null AS jsonb) AS infos_data,

                cast(null AS bigint[]) AS ids_backfills,
                cast(null AS bigint[]) AS ids_infos,

                ter_region_id, ter_region_name, ter_federal_district_id, ter_federal_district_name,

                culture_name, culture_id, sort_type, sort_code, sort_name, sort_region, sort_year,

                sort_originator_main,
                sort_originator_main_name,
                sort_originator_main_country_code_iso,
                sort_originator_main_country_name,
                sort_country_code_iso,
                sort_country_name,

                seedprod_seeds_kind, fill_all, fill_seed_fund, fill_seed_for_sale, fill_removed_seed_fund, fill_AVALIABLE, fill_SOWN, fill_DELETED, fill_SOLD, fill_DEFECTED, fill_FED, fill_REFORMED, ru_fill_all, ru_fill_localized, ru_fill_purchase_en, en_fill_all, en_fill_localized, en_fill_purchase_en, nr_fill_all, nr_fill_purchase_en, rp_NC, rp_OS, rp_ES, rp_RS_1, rp_RS_1_RST, rp_RS_2, rp_RS_2_RST, rp_RS_3, rp_RS_3_RST, rp_RS_4, rp_RS_4_RST, rp_F1, rp_RS_5B, rp_RS_5B_RST, rp_NOT, fill_purchase_en
         FROM cats_sorts AS gps
                  LEFT JOIN common.ter_regions AS join_ter_regions ON gps.ter_region_id = join_ter_regions.id
                  LEFT JOIN common.ter_federal_districts AS join_ter_federal_districts ON join_ter_federal_districts.id = join_ter_regions.federal_district_id

         WHERE gps.id = (CASE WHEN :need_id ISNULL THEN 0 ELSE :need_id END) AND (join_ter_regions.exclude_monitoring ISNULL OR join_ter_regions.exclude_monitoring = false)
         ORDER BY join_ter_federal_districts.turn, join_ter_regions.turn
     ),

     districts_solv AS (
         SELECT
             dts.id AS id,
             'district' AS type,
             0 AS tree_parent_id, 0 AS stc_object_id, 0 AS stc_parent_id, 1 AS group_level,

             0 AS turn_all, dts.turn AS turn_in,
             dts.turn AS ter_federal_district_turn, 0 AS ter_region_turn,

             dts.name_short AS view_name,
             dts.name_short AS name,

             COUNT(rgs.id) AS cnt_groups, 0 AS cnt_cultures,
             SUM(count_backfills) AS count_backfills,
             cast(null AS jsonb) AS backfills_data,

             SUM(count_infos) AS count_infos,
             cast(null AS jsonb) AS infos_data,

             cast(null AS bigint[]) AS ids_backfills,
             cast(null AS bigint[]) AS ids_infos,

             cast(null AS bigint) AS ter_region_id, cast(null AS varchar) AS ter_region_name, dts.id AS ter_federal_district_id, dts.name AS ter_federal_district_name,

             cast(null AS varchar) AS culture_name,
             cast(null AS bigint) AS culture_id, cast(null AS varchar) AS sort_type, cast(null AS varchar) AS sort_code, cast(null AS varchar) AS sort_name, cast(null AS varchar) AS sort_region, cast(null AS int) AS sort_year,

             cast(null AS bigint) sort_originator_main,
             cast(null AS varchar) sort_originator_main_name,
             cast(null AS varchar) sort_originator_main_country_code_iso,
             cast(null AS varchar) sort_originator_main_country_name,
             cast(null AS varchar) sort_country_code_iso,
             cast(null AS varchar) sort_country_name,

             cast(null AS varchar) AS seedprod_seeds_kind,

             SUM(fill_all) AS fill_all,
             SUM(fill_seed_fund) AS fill_seed_fund,
             SUM(fill_seed_for_sale) AS fill_seed_for_sale,
             SUM(fill_removed_seed_fund) AS fill_removed_seed_fund,
             SUM(fill_AVALIABLE) AS fill_AVALIABLE,
             SUM(fill_SOWN) AS fill_SOWN,
             SUM(fill_DELETED) AS fill_DELETED,
             SUM(fill_SOLD) AS fill_SOLD,
             SUM(fill_DEFECTED) AS fill_DEFECTED,
             SUM(fill_FED) AS fill_FED,
             SUM(fill_REFORMED) AS fill_REFORMED,
             SUM(ru_fill_all) AS ru_fill_all,
             SUM(ru_fill_localized) AS ru_fill_localized,
             SUM(ru_fill_purchase_en) AS ru_fill_purchase_en,
             SUM(en_fill_all) AS en_fill_all,
             SUM(en_fill_localized) AS en_fill_localized,
             SUM(en_fill_purchase_en) AS en_fill_purchase_en,
             SUM(nr_fill_all) AS nr_fill_all,
             SUM(nr_fill_purchase_en) AS nr_fill_purchase_en,
             SUM(rp_NC) AS rp_NC,
             SUM(rp_OS) AS rp_OS,
             SUM(rp_ES) AS rp_ES,
             SUM(rp_RS_1) AS rp_RS_1,
             SUM(rp_RS_1_RST) AS rp_RS_1_RST,
             SUM(rp_RS_2) AS rp_RS_2,
             SUM(rp_RS_2_RST) AS rp_RS_2_RST,
             SUM(rp_RS_3) AS rp_RS_3,
             SUM(rp_RS_3_RST) AS rp_RS_3_RST,
             SUM(rp_RS_4) AS rp_RS_4,
             SUM(rp_RS_4_RST) AS rp_RS_4_RST,
             SUM(rp_F1) AS rp_F1,
             SUM(rp_RS_5B) AS rp_RS_5B,
             SUM(rp_RS_5B_RST) AS rp_RS_5B_RST,
             SUM(rp_NOT) AS rp_NOT,
             SUM(fill_purchase_en) AS fill_purchase_en
         FROM common.ter_federal_districts AS dts
                  LEFT JOIN regions_solv AS rgs ON rgs.ter_federal_district_id = dts.id
         GROUP BY dts.id, dts.turn, dts.name_short
         ORDER BY dts.turn
     ),

     country_solv AS (
         SELECT
             0 AS id,
             'country' AS type,
             cast(null AS bigint) AS tree_parent_id, cast(null AS bigint) AS stc_object_id, cast(null AS bigint) AS stc_parent_id, 0 AS group_level,
             0 AS turn_all, 0 AS turn_in,
             0 AS ter_federal_district_turn, 0 AS ter_region_turn,
             'РФ' AS view_name,
             'РФ' AS name,

             COUNT(rgs.id) AS cnt_groups, 0 AS cnt_cultures,
             SUM(count_backfills) AS count_backfills,
             cast(null AS jsonb) AS backfills_data,

             SUM(count_infos) AS count_infos,
             cast(null AS jsonb) AS infos_data,

             cast(null AS bigint[]) AS ids_backfills,
             cast(null AS bigint[]) AS ids_infos,

             cast(null AS bigint) AS ter_region_id, null AS ter_region_name, cast(null AS bigint) AS ter_federal_district_id, null AS ter_federal_district_name,

             cast(null AS varchar) AS culture_name,
             cast(null AS bigint) AS culture_id, cast(null AS varchar) AS sort_type, cast(null AS varchar) AS sort_code, cast(null AS varchar) AS sort_name, cast(null AS varchar) AS sort_region, cast(null AS int) AS sort_year,

             cast(null AS bigint) sort_originator_main,
             cast(null AS varchar) sort_originator_main_name,
             cast(null AS varchar) sort_originator_main_country_code_iso,
             cast(null AS varchar) sort_originator_main_country_name,
             cast(null AS varchar) sort_country_code_iso,
             cast(null AS varchar) sort_country_name,

             cast(null AS varchar) AS seedprod_seeds_kind,

             SUM(fill_all) AS fill_all,
             SUM(fill_seed_fund) AS fill_seed_fund,
             SUM(fill_seed_for_sale) AS fill_seed_for_sale,
             SUM(fill_removed_seed_fund) AS fill_removed_seed_fund,
             SUM(fill_AVALIABLE) AS fill_AVALIABLE,
             SUM(fill_SOWN) AS fill_SOWN,
             SUM(fill_DELETED) AS fill_DELETED,
             SUM(fill_SOLD) AS fill_SOLD,
             SUM(fill_DEFECTED) AS fill_DEFECTED,
             SUM(fill_FED) AS fill_FED,
             SUM(fill_REFORMED) AS fill_REFORMED,
             SUM(ru_fill_all) AS ru_fill_all,
             SUM(ru_fill_localized) AS ru_fill_localized,
             SUM(ru_fill_purchase_en) AS ru_fill_purchase_en,
             SUM(en_fill_all) AS en_fill_all,
             SUM(en_fill_localized) AS en_fill_localized,
             SUM(en_fill_purchase_en) AS en_fill_purchase_en,
             SUM(nr_fill_all) AS nr_fill_all,
             SUM(nr_fill_purchase_en) AS nr_fill_purchase_en,
             SUM(rp_NC) AS rp_NC,
             SUM(rp_OS) AS rp_OS,
             SUM(rp_ES) AS rp_ES,
             SUM(rp_RS_1) AS rp_RS_1,
             SUM(rp_RS_1_RST) AS rp_RS_1_RST,
             SUM(rp_RS_2) AS rp_RS_2,
             SUM(rp_RS_2_RST) AS rp_RS_2_RST,
             SUM(rp_RS_3) AS rp_RS_3,
             SUM(rp_RS_3_RST) AS rp_RS_3_RST,
             SUM(rp_RS_4) AS rp_RS_4,
             SUM(rp_RS_4_RST) AS rp_RS_4_RST,
             SUM(rp_F1) AS rp_F1,
             SUM(rp_RS_5B) AS rp_RS_5B,
             SUM(rp_RS_5B_RST) AS rp_RS_5B_RST,
             SUM(rp_NOT) AS rp_NOT,
             SUM(fill_purchase_en) AS fill_purchase_en
         FROM regions_solv AS rgs
     ),

     all_solv AS (
         SELECT * FROM cats_sorts
         UNION ALL
         SELECT * FROM country_solv
         UNION ALL
         SELECT * FROM districts_solv
         UNION ALL
         SELECT * FROM regions_solv
     ),

     result_query AS (
         SELECT * FROM all_solv WHERE (CASE WHEN :sel_type = 'KIND_REGIONS' THEN true ELSE false END)
         UNION
         SELECT * FROM cats_sorts WHERE (CASE WHEN :sel_type = 'KIND_CULTURES_GROUPS' THEN true ELSE false END)
     )


SELECT row_number() over () AS
    rid,

    null AS qual_checked,
    null AS qual_conditioned,
    null AS qual_ncon_all,
    null AS qual_ncon_pests,
    null AS qual_ncon_debris,
    null AS qual_ncon_hum,
    null AS qual_ncon_germ,
    null AS qual_ncon_germ_lt_10,
    null AS qual_ncon_germ_10_20,

    null AS en_fill_purchase_en_processed,

    null AS rp_rep_PP1,
    null AS rp_rep_SSE,
    null AS rp_rep_SE,

    null AS sa_all,

    null AS sa_NC,
    null AS sa_OS,
    null AS sa_ES,

    null AS sa_rep_PP1,
    null AS sa_rep_SSE,
    null AS sa_rep_SE,

    null AS sa_RS_1,
    null AS sa_RS_1_RST,
    null AS sa_RS_2,
    null AS sa_RS_2_RST,
    null AS sa_RS_3,
    null AS sa_RS_3_RST,
    null AS sa_RS_4,
    null AS sa_RS_4_RST,
    null AS sa_F1,
    null AS sa_RS_5B,
    null AS sa_RS_5B_RST,
    null AS sa_NOT,
    null AS fill_purchase_en_processed,

--        CASE WHEN result_query.ids_backfills = array[cast(0 AS bigint)] THEN 0 ELSE array_length(result_query.ids_backfills, 1) END AS count_ids_backfills,
--        CASE WHEN result_query.ids_infos = array[cast(0 AS bigint)] THEN 0 ELSE array_length(result_query.ids_infos, 1) END AS count_ids_infos,

    result_query.*
FROM result_query
ORDER BY ter_federal_district_turn, ter_region_turn, turn_all, turn_in, sort_type DESC, sort_name;