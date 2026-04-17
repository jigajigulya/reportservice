
WITH ass AS (
    SELECT
        id,

        normal_is,
        culture_id = 143 AS potato_is,
        im_is,

        date_assay,

        seedprod_crop_kind,

        source_customer,
        spec_rsc,

        source_gov,
        source_com,
        source_oth,

        type_common,
        type_advance,
        type_other,

        type_sds,

        other_region,

        contractor_id,

        area_seeded,
        area_service,

        dep_township_id,
        dep_region_id,
        dep_ter_region_id,
        dep_ter_federal_district_id,

        app_is,
--         reg_is,

        (COALESCE(TOT_OS, 0) + COALESCE(TOT_ES, 0) + COALESCE(TOT_RS_1, 0) + COALESCE(TOT_RS_1_RST, 0) + COALESCE(TOT_RS_2, 0) + COALESCE(TOT_RS_2_RST, 0) + COALESCE(TOT_RS_3, 0) + COALESCE(TOT_RS_3_RST, 0) + COALESCE(TOT_RS_4, 0) + COALESCE(TOT_RS_4_RST, 0) + COALESCE(TOT_F1, 0) + COALESCE(TOT_RS_5B, 0) + COALESCE(TOT_RS_5B_RST, 0) + COALESCE(TOT_NC, 0) + COALESCE(TOT_REJECT_ALL, 0) + COALESCE(TOT_TRADE, 0))            AS TOT_sum_all,
        (COALESCE(TOT_OS, 0) + COALESCE(TOT_ES, 0) + COALESCE(TOT_RS_1, 0) + COALESCE(TOT_RS_1_RST, 0) + COALESCE(TOT_RS_2, 0) + COALESCE(TOT_RS_2_RST, 0) + COALESCE(TOT_RS_3, 0) + COALESCE(TOT_RS_3_RST, 0) + COALESCE(TOT_RS_4, 0) + COALESCE(TOT_RS_4_RST, 0) + COALESCE(TOT_F1, 0) + COALESCE(TOT_RS_5B, 0) + COALESCE(TOT_RS_5B_RST, 0) + COALESCE(TOT_NC, 0) + COALESCE(TOT_REJECT_ALL, 0))                                     AS TOT_sum_seeds,

        TOT_OS,
        TOT_ES,
        TOT_RS_1,
        TOT_RS_1_RST,
        TOT_RS_2,
        TOT_RS_2_RST,
        TOT_RS_3,
        TOT_RS_3_RST,
        TOT_RS_4,
        TOT_RS_4_RST,
        TOT_F1,
        TOT_RS_5B,
        TOT_RS_5B_RST,
        TOT_NC,
        TOT_PP1,
        PP1_is,
        TOT_REJECT_ALL,
--         TOT_REJECT_BY_LITTERED,
--         TOT_REJECT_BY_DISEASES,
--         TOT_REJECT_BY_WEEDS,
--         TOT_REJECT_OTHER,

        TOT_REJECT_REAS_1,
        TOT_REJECT_REAS_2,
        TOT_REJECT_REAS_3,
        TOT_REJECT_REAS_4,
        TOT_REJECT_REAS_5,
        TOT_REJECT_REAS_6,
        TOT_REJECT_REAS_7,
        TOT_REJECT_REAS_8,
        TOT_REJECT_REAS_9,
        TOT_REJECT_REAS_10,
        TOT_REJECT_REAS_OTHER,

        TOT_TRADE,

        IM_CNT,
        IM_REJECT_ALL_CNT,

        STATUS_BEFORE_APP_DEATH_is,
        STATUS_AFTER_APP_DEATH_is,
        STATUS_BEFORE_APP_OUT_SEEDS_is,
        STATUS_AFTER_APP_OUT_SEEDS_is,
        STATUS_NO_LIC_AGR_is,
        STATUS_NO_LIC_AGR_FLAG_643_is

    FROM (SELECT
              assays.id,

--               join_rep_taked_categories.type IS NULL - Категория не указана
--               join_rep_taked_categories.type != 'IM' - Устеревший ИМ категория убрана в 2024 году
              (join_rep_taked_categories.type = 'IM' OR join_rep_taked_types.type IN ('CLONES','IN_VITRO','MICRO_TUBER','MINI_TUBER')) AS im_is,
--               (join_rep_taked_categories.type != 'IM' OR join_rep_taked_categories.type IS NULL OR join_rep_taked_types.type NOT IN ('CLONES','IN_VITRO','MICRO_TUBER','MINI_TUBER')) AS normal_is,

              ((join_rep_taked_categories.type IS NULL OR join_rep_taked_categories.type NOT LIKE 'IM') AND (join_rep_taked_types.type ISNULL OR join_rep_taked_types.type NOT IN ('CLONES','IN_VITRO','MICRO_TUBER','MINI_TUBER'))) AS normal_is,

              assays.seedprod_crop_kind,
              date_assay,

              assays.source_customer,

              (CASE WHEN seedprod_service_provider_type = 'RSC' THEN true ELSE false END) AS spec_rsc,

              (CASE WHEN assays.source_customer = 'GOV_TASK' AND seedprod_service_provider_type = 'RSC' THEN true ELSE false END) AS source_gov,
              (CASE WHEN assays.source_customer = 'OUT_BUDGET' AND seedprod_service_provider_type = 'RSC' THEN true ELSE false END) AS source_com,
              (CASE WHEN assays.source_customer ISNULL OR seedprod_service_provider_type ISNULL OR assays.source_customer NOT IN ('GOV_TASK', 'OUT_BUDGET') OR seedprod_service_provider_type NOT IN ('RSC') THEN true ELSE false END) AS source_oth,

              (CASE WHEN join_contractors.seedprod_type IS NOT NULL AND join_contractors.seedprod_type IN ('COMMON') THEN true ELSE false END) AS type_common,
              (CASE WHEN join_contractors.seedprod_type IS NOT NULL AND join_contractors.seedprod_type IN ('NIU','STUDY','OPH') THEN true ELSE false END) AS type_advance,
              (CASE WHEN join_contractors.seedprod_type IS NULL OR join_contractors.seedprod_type IN ('OTHER','NONE') THEN true ELSE false END) AS type_other,

              (CASE WHEN join_contractors.seedprod_sds_number IS NOT NULL AND trim(join_contractors.seedprod_sds_number) != '' THEN true ELSE false END) AS type_sds,

              (CASE WHEN join_dep_region.ter_region_id != join_ter_township.region_id THEN true ELSE false END) AS other_region,

              assays.contractor_id,

              join_sorts.culture_id AS culture_id,

              (cropfield_area * :coef)                        AS area_seeded,
              (cropfield_service_area * :coef)                AS area_service,

              assays.department_id                            AS dep_township_id,

              join_dep_township.parent_id                     AS dep_region_id,

              join_dep_region.ter_region_id                   AS dep_ter_region_id,
              join_dep_region_ter_region.federal_district_id  AS dep_ter_federal_district_id,

              seedprod_service_date                           AS service_date,


              (CASE WHEN (seedprod_service_type = 'APPROBATION' OR seedprod_service_type = 'SURVEY_RM') AND seedprod_service_date BETWEEN cast(concat(cast(:harvest_year AS text),'-01-01') AS date) AND cast(concat(cast(:harvest_year AS text),'-12-31') AS date) THEN true ELSE false END)               AS app_is,
--               (CASE WHEN seedprod_service_type = 'REGISTRATION' AND seedprod_service_date BETWEEN cast(concat(cast(:harvest_year AS text),'-01-01') AS date) AND cast(concat(cast(:harvest_year AS text),'-12-31') AS date) THEN true ELSE false END)                                                        AS reg_is,

              (CASE WHEN seedprod_crop_kind = 'SEEDS' AND seedprod_service_result_type IS DISTINCT FROM 'REJECT' AND join_rep_taked_categories.type = 'OS' THEN cropfield_service_area * :coef ELSE 0 END)                                                                                                   AS TOT_OS,
              (CASE WHEN seedprod_crop_kind = 'SEEDS' AND seedprod_service_result_type IS DISTINCT FROM 'REJECT' AND join_rep_taked_categories.type = 'ES' THEN cropfield_service_area * :coef ELSE 0 END)                                                                                                   AS TOT_ES,
              (CASE WHEN seedprod_crop_kind = 'SEEDS' AND seedprod_service_result_type IS DISTINCT FROM 'REJECT' AND join_rep_taked_types.type IN ('RS', 'RS1') AND seedprod_taked_reproduction_trade != TRUE THEN cropfield_service_area * :coef ELSE 0 END)                                                AS TOT_RS_1,
              (CASE WHEN seedprod_crop_kind = 'SEEDS' AND seedprod_service_result_type IS DISTINCT FROM 'REJECT' AND join_rep_taked_types.type IN ('RS', 'RS1') AND seedprod_taked_reproduction_trade = TRUE THEN cropfield_service_area * :coef ELSE 0 END)                                                 AS TOT_RS_1_RST,
              (CASE WHEN seedprod_crop_kind = 'SEEDS' AND seedprod_service_result_type IS DISTINCT FROM 'REJECT' AND join_rep_taked_types.type IN ('RS2') AND seedprod_taked_reproduction_trade != TRUE THEN cropfield_service_area * :coef ELSE 0 END)                                                      AS TOT_RS_2,
              (CASE WHEN seedprod_crop_kind = 'SEEDS' AND seedprod_service_result_type IS DISTINCT FROM 'REJECT' AND join_rep_taked_types.type IN ('RS2') AND seedprod_taked_reproduction_trade = TRUE THEN cropfield_service_area * :coef ELSE 0 END)                                                       AS TOT_RS_2_RST,
              (CASE WHEN seedprod_crop_kind = 'SEEDS' AND seedprod_service_result_type IS DISTINCT FROM 'REJECT' AND join_rep_taked_types.type IN ('RS3') AND seedprod_taked_reproduction_trade != TRUE THEN cropfield_service_area * :coef ELSE 0 END)                                                      AS TOT_RS_3,
              (CASE WHEN seedprod_crop_kind = 'SEEDS' AND seedprod_service_result_type IS DISTINCT FROM 'REJECT' AND join_rep_taked_types.type IN ('RS3') AND seedprod_taked_reproduction_trade = TRUE THEN cropfield_service_area * :coef ELSE 0 END)                                                       AS TOT_RS_3_RST,
              (CASE WHEN seedprod_crop_kind = 'SEEDS' AND seedprod_service_result_type IS DISTINCT FROM 'REJECT' AND join_rep_taked_types.type IN ('RS4') AND seedprod_taked_reproduction_trade != TRUE THEN cropfield_service_area * :coef ELSE 0 END)                                                      AS TOT_RS_4,
              (CASE WHEN seedprod_crop_kind = 'SEEDS' AND seedprod_service_result_type IS DISTINCT FROM 'REJECT' AND join_rep_taked_types.type IN ('RS4') AND seedprod_taked_reproduction_trade = TRUE THEN cropfield_service_area * :coef ELSE 0 END)                                                       AS TOT_RS_4_RST,
              (CASE WHEN seedprod_crop_kind = 'SEEDS' AND seedprod_service_result_type IS DISTINCT FROM 'REJECT' AND join_rep_taked_types.type = 'F1' THEN cropfield_service_area * :coef ELSE 0 END)                                                                                                        AS TOT_F1,
              (CASE WHEN seedprod_crop_kind = 'SEEDS' AND seedprod_service_result_type IS DISTINCT FROM 'REJECT' AND join_rep_taked_types.type IN ('RS5', 'RS6', 'RS7', 'RS8', 'RS9', 'RS10') AND seedprod_taked_reproduction_trade != TRUE THEN cropfield_service_area * :coef ELSE 0 END)                  AS TOT_RS_5B,
              (CASE WHEN seedprod_crop_kind = 'SEEDS' AND seedprod_service_result_type IS DISTINCT FROM 'REJECT' AND join_rep_taked_types.type IN ('RS5', 'RS6', 'RS7', 'RS8', 'RS9', 'RS10') AND seedprod_taked_reproduction_trade = TRUE THEN cropfield_service_area * :coef ELSE 0 END)                   AS TOT_RS_5B_RST,
              (CASE WHEN seedprod_crop_kind = 'SEEDS' AND seedprod_service_result_type IS DISTINCT FROM 'REJECT' AND join_rep_taked_categories.type = 'NC' THEN cropfield_service_area * :coef ELSE 0 END)                                                                                                   AS TOT_NC,

              (CASE WHEN seedprod_crop_kind = 'SEEDS' AND seedprod_service_result_type IS DISTINCT FROM 'REJECT' AND join_rep_taked_types.type = 'PP1' THEN cropfield_service_area * :coef ELSE 0 END)                                                                                                       AS TOT_PP1,
              (CASE WHEN seedprod_crop_kind = 'SEEDS' AND join_rep_taked_types.type = 'PP1' THEN true ELSE false END)                                                                                                                                                                                        AS PP1_is,

              (CASE WHEN seedprod_crop_kind = 'SEEDS' AND seedprod_service_result_type = 'REJECT' THEN cropfield_service_area * :coef ELSE 0 END)                                                                                                                                                            AS TOT_REJECT_ALL,
--               (CASE WHEN seedprod_crop_kind = 'SEEDS' AND seedprod_service_result_type = 'REJECT' AND seedprod_service_result_reject_type = 'BY_LITTERED' THEN cropfield_service_area * :coef ELSE 0 END)                                                                                                    AS TOT_REJECT_BY_LITTERED,
--               (CASE WHEN seedprod_crop_kind = 'SEEDS' AND seedprod_service_result_type = 'REJECT' AND seedprod_service_result_reject_type = 'BY_DISEASES' THEN cropfield_service_area * :coef ELSE 0 END)                                                                                                    AS TOT_REJECT_BY_DISEASES,
--               (CASE WHEN seedprod_crop_kind = 'SEEDS' AND seedprod_service_result_type = 'REJECT' AND seedprod_service_result_reject_type = 'BY_WEEDS' THEN cropfield_service_area * :coef ELSE 0 END)                                                                                                       AS TOT_REJECT_BY_WEEDS,
--               (CASE WHEN seedprod_crop_kind = 'SEEDS' AND seedprod_service_result_type = 'REJECT' AND seedprod_service_result_reject_type IS NULL THEN cropfield_service_area * :coef ELSE 0 END)                                                                                                            AS TOT_REJECT_OTHER,
              (CASE WHEN seedprod_crop_kind = 'TRADE' THEN cropfield_service_area * :coef ELSE 0 END)                                                                                                                                                                                                        AS TOT_TRADE,


              (CASE WHEN seedprod_crop_kind = 'SEEDS' AND seedprod_service_result_type = 'REJECT' AND seed_prod_akt_defect_reason = 1 THEN cropfield_service_area * :coef ELSE 0 END)                                                                                                    AS TOT_REJECT_REAS_1,
              (CASE WHEN seedprod_crop_kind = 'SEEDS' AND seedprod_service_result_type = 'REJECT' AND seed_prod_akt_defect_reason = 2 THEN cropfield_service_area * :coef ELSE 0 END)                                                                                                    AS TOT_REJECT_REAS_2,
              (CASE WHEN seedprod_crop_kind = 'SEEDS' AND seedprod_service_result_type = 'REJECT' AND seed_prod_akt_defect_reason = 3 THEN cropfield_service_area * :coef ELSE 0 END)                                                                                                    AS TOT_REJECT_REAS_3,
              (CASE WHEN seedprod_crop_kind = 'SEEDS' AND seedprod_service_result_type = 'REJECT' AND seed_prod_akt_defect_reason = 4 THEN cropfield_service_area * :coef ELSE 0 END)                                                                                                    AS TOT_REJECT_REAS_4,
              (CASE WHEN seedprod_crop_kind = 'SEEDS' AND seedprod_service_result_type = 'REJECT' AND seed_prod_akt_defect_reason = 5 THEN cropfield_service_area * :coef ELSE 0 END)                                                                                                    AS TOT_REJECT_REAS_5,
              (CASE WHEN seedprod_crop_kind = 'SEEDS' AND seedprod_service_result_type = 'REJECT' AND seed_prod_akt_defect_reason = 6 THEN cropfield_service_area * :coef ELSE 0 END)                                                                                                    AS TOT_REJECT_REAS_6,
              (CASE WHEN seedprod_crop_kind = 'SEEDS' AND seedprod_service_result_type = 'REJECT' AND seed_prod_akt_defect_reason = 7 THEN cropfield_service_area * :coef ELSE 0 END)                                                                                                    AS TOT_REJECT_REAS_7,
              (CASE WHEN seedprod_crop_kind = 'SEEDS' AND seedprod_service_result_type = 'REJECT' AND seed_prod_akt_defect_reason = 8 THEN cropfield_service_area * :coef ELSE 0 END)                                                                                                    AS TOT_REJECT_REAS_8,
              (CASE WHEN seedprod_crop_kind = 'SEEDS' AND seedprod_service_result_type = 'REJECT' AND seed_prod_akt_defect_reason = 9 THEN cropfield_service_area * :coef ELSE 0 END)                                                                                                    AS TOT_REJECT_REAS_9,
              (CASE WHEN seedprod_crop_kind = 'SEEDS' AND seedprod_service_result_type = 'REJECT' AND seed_prod_akt_defect_reason = 10 THEN cropfield_service_area * :coef ELSE 0 END)                                                                                                    AS TOT_REJECT_REAS_10,
              (CASE WHEN seedprod_crop_kind = 'SEEDS' AND seedprod_service_result_type = 'REJECT' AND seed_prod_akt_defect_reason IS NULL OR seed_prod_akt_defect_reason > 10 THEN cropfield_service_area * :coef ELSE 0 END)                                                             AS TOT_REJECT_REAS_OTHER,



              (CASE WHEN seedprod_crop_kind = 'SEEDS' AND seedprod_service_result_type IS DISTINCT FROM 'REJECT' THEN assays.seedprod_service_plants_count ELSE 0 END)                                                                                                                                       AS IM_CNT,
              (CASE WHEN seedprod_crop_kind = 'SEEDS' AND seedprod_service_result_type = 'REJECT' THEN assays.seedprod_service_plants_count ELSE 0 END)                                                                                                                                                      AS IM_REJECT_ALL_CNT,

              (CASE WHEN seedprod_service_before_state_type = 'DEATH' THEN true ELSE false END)                                                                                                                                                 AS STATUS_BEFORE_APP_DEATH_is,
              (CASE WHEN seedprod_service_after_state_type = 'DEATH' THEN true ELSE false END)                                                                                                                                                  AS STATUS_AFTER_APP_DEATH_is,
              (CASE WHEN seedprod_service_before_state_type = 'OUT_SEEDS' THEN true ELSE false END)                                                                                                                                             AS STATUS_BEFORE_APP_OUT_SEEDS_is,
              (CASE WHEN seedprod_service_after_state_type = 'OUT' THEN true ELSE false END)                                                                                                                                                    AS STATUS_AFTER_APP_OUT_SEEDS_is,
              (CASE WHEN seedprod_service_before_state_type = 'NO_LIC_AGR' THEN true ELSE false END)                                                                                                                                                                                            AS STATUS_NO_LIC_AGR_is,
              (CASE WHEN seedprod_service_before_state_type = 'NO_LIC_AGR' AND seed_prod_akt_flag_643 = true THEN true ELSE false END)                                                                                                                                                          AS STATUS_NO_LIC_AGR_FLAG_643_is



          FROM public.assays
                   LEFT JOIN common.contractors AS join_contractors ON join_contractors.id = assays.contractor_id
                   LEFT JOIN common.ter_townships AS join_ter_township ON join_ter_township.id = assays.township_id
                   LEFT JOIN common.department_structure AS join_dep_township ON join_dep_township.id = assays.department_id
                   LEFT JOIN common.department_structure AS join_dep_region ON join_dep_region.id = join_dep_township.parent_id
                   LEFT JOIN common.ter_regions AS join_dep_region_ter_region ON join_dep_region_ter_region.id = join_dep_region.ter_region_id
                   LEFT JOIN common.culture_sorts AS join_sorts ON join_sorts.code = assays.crop_current_culturesort_code

                   LEFT JOIN common.reproduction_types AS join_rep_taked_types ON join_rep_taked_types.id = seedprod_taked_reproduction_type_id
                   LEFT JOIN common.reproduction_categories AS join_rep_taked_categories ON join_rep_taked_categories.id = join_rep_taked_types.category_id
          WHERE
              is_seedprod
            AND (seedprod_crop_kind = 'SEEDS' OR seedprod_crop_kind = 'TRADE')
            AND assays.township_id IS NOT null
            AND seedprod_for_harvest_year = :harvest_year
            AND crop_current_culturesort_code IS NOT NULL
            AND join_sorts.mark_01 IS DISTINCT FROM 'ns'
            AND culture_id IN (SELECT spc.culture_id FROM ase.seed_production_cultures_groups_cultures AS spc)

         ) AS assraw

    WHERE :dep_ter_structure
),

 specs AS (
     SELECT
         id,
         REGEXP_REPLACE(name_short, E'[[:space:]]', '', 'g') AS name_short,
         REGEXP_REPLACE(name_full, E'[[:space:]]', '', 'g') AS name_full,

         dep_township_id,
         dep_region_id,
         dep_ter_region_id,
         dep_ter_federal_district_id

     FROM (SELECT
               dss.id,
               dss.name_short,
               dss.name_full,

               dss.department_id                               AS dep_township_id,
               dss.department_region_id                        AS dep_region_id,
               join_dep_region.ter_region_id                   AS dep_ter_region_id,
               join_dep_region_ter_region.federal_district_id  AS dep_ter_federal_district_id

           FROM common.department_specialists AS dss

                    LEFT JOIN common.department_structure AS join_dep_township ON join_dep_township.id = dss.department_id
                    LEFT JOIN common.department_structure AS join_dep_region ON join_dep_region.id = dss.department_region_id

                    LEFT JOIN common.ter_townships AS join_ter_township ON join_ter_township.id = join_dep_township.ter_township_id
                    LEFT JOIN common.ter_regions AS join_dep_region_ter_region ON join_dep_region_ter_region.id = join_dep_region.ter_region_id

           WHERE join_dep_township.type = 'TOWNSHIP'
             AND dss.type = 'APPROBATOR'
             AND dss.doc_date_finish > cast(concat(cast(:harvest_year AS text),'-01-01') AS date)
           
          ) AS assraw

     WHERE :dep_ter_structure
 ),


un_all AS (

SELECT
  10 AS id,
  'Количество геоточек семенных посевов' AS title,
  'Всего геоточек занесенных в систему только сем.посевов, без ИМ репродукций (ИМ только у картофеля)' AS description,
  0 AS level,
  'normal' AS type,
  'bigint' AS val_type,
  count(ass.id) AS val_all_all,
  count(CASE WHEN source_gov THEN true END) AS val_all_gov,
  count(CASE WHEN source_com THEN true END) AS val_all_com,
  count(CASE WHEN source_oth THEN true END) AS val_all_oth,

  count(CASE WHEN other_region THEN true END) AS val_other_region,

  count(CASE WHEN type_common THEN true END) AS val_common_all,
  count(CASE WHEN type_common AND source_gov THEN true END) AS val_common_gov,
  count(CASE WHEN type_common AND source_com THEN true END) AS val_common_com,
  count(CASE WHEN type_common AND source_oth THEN true END) AS val_common_oth,

  count(CASE WHEN type_advance THEN true END) AS val_advance_all,
  count(CASE WHEN type_advance AND source_gov THEN true END) AS val_advance_gov,
  count(CASE WHEN type_advance AND source_com THEN true END) AS val_advance_com,
  count(CASE WHEN type_advance AND source_oth THEN true END) AS val_advance_oth,

  count(CASE WHEN type_other THEN true END) AS val_other_all,
  count(CASE WHEN type_other AND source_gov THEN true END) AS val_other_gov,
  count(CASE WHEN type_other AND source_com THEN true END) AS val_other_com,
  count(CASE WHEN type_other AND source_oth THEN true END) AS val_other_oth,

  count(CASE WHEN type_sds THEN true END) AS val_sds_all,
  count(CASE WHEN type_sds AND source_gov THEN true END) AS val_sds_gov,
  count(CASE WHEN type_sds AND source_com THEN true END) AS val_sds_com,
  count(CASE WHEN type_sds AND source_oth THEN true END) AS val_sds_oth,

  (CASE WHEN :details THEN array_agg(cast(ass.id AS bigint)) ELSE null END) AS assays_ids

FROM ass WHERE ass.seedprod_crop_kind = 'SEEDS' AND normal_is

UNION

SELECT
  20 AS id,
  'Площадь высеянных семенных посевов, тыс.га' AS title,
  'Всего высеянных занесенных в систему только сем.посевов, без ИМ репродукций (ИМ только у картофеля)' AS description,
  0 AS level,
  'normal' AS type,
  'double' AS val_type,
  SUM(area_seeded) AS val_all_all,
  SUM(CASE WHEN source_gov THEN area_seeded END) AS val_all_gov,
  SUM(CASE WHEN source_com THEN area_seeded END) AS val_all_com,
  SUM(CASE WHEN source_oth THEN area_seeded END) AS val_all_oth,

  SUM(CASE WHEN other_region THEN area_seeded END) AS val_other_region,

  SUM(CASE WHEN type_common THEN area_seeded END) AS val_common_all,
  SUM(CASE WHEN type_common AND source_gov THEN area_seeded END) AS val_common_gov,
  SUM(CASE WHEN type_common AND source_com THEN area_seeded END) AS val_common_com,
  SUM(CASE WHEN type_common AND source_oth THEN area_seeded END) AS val_common_oth,

  SUM(CASE WHEN type_advance THEN area_seeded END) AS val_advance_all,
  SUM(CASE WHEN type_advance AND source_gov THEN area_seeded END) AS val_advance_gov,
  SUM(CASE WHEN type_advance AND source_com THEN area_seeded END) AS val_advance_com,
  SUM(CASE WHEN type_advance AND source_oth THEN area_seeded END) AS val_advance_oth,

  SUM(CASE WHEN type_other THEN area_seeded END) AS val_other_all,
  SUM(CASE WHEN type_other AND source_gov THEN area_seeded END) AS val_other_gov,
  SUM(CASE WHEN type_other AND source_com THEN area_seeded END) AS val_other_com,
  SUM(CASE WHEN type_other AND source_oth THEN area_seeded END) AS val_other_oth,

  SUM(CASE WHEN type_sds THEN area_seeded END) AS val_sds_all,
  SUM(CASE WHEN type_sds AND source_gov THEN area_seeded END) AS val_sds_gov,
  SUM(CASE WHEN type_sds AND source_com THEN area_seeded END) AS val_sds_com,
  SUM(CASE WHEN type_sds AND source_oth THEN area_seeded END) AS val_sds_oth,

  null AS assays_ids

FROM ass WHERE ass.seedprod_crop_kind = 'SEEDS' AND normal_is

UNION

SELECT
  30 AS id,
  'Количество хозяйств, где проведена апробация, шт.' AS title,
  '' AS description,
  0 AS level,
  'normal' AS type,
  'bigint' AS val_type,
  count(DISTINCT ass.contractor_id) AS val_all_all,
  count(DISTINCT CASE WHEN source_gov THEN ass.contractor_id END) AS val_all_gov,
  count(DISTINCT CASE WHEN source_com THEN ass.contractor_id END) AS val_all_com,
  count(DISTINCT CASE WHEN source_oth THEN ass.contractor_id END) AS val_all_oth,

  count(DISTINCT CASE WHEN other_region THEN ass.contractor_id END) AS val_other_region,

  count(DISTINCT CASE WHEN type_common THEN ass.contractor_id END) AS val_common_all,
  count(DISTINCT CASE WHEN type_common AND source_gov THEN ass.contractor_id END) AS val_common_gov,
  count(DISTINCT CASE WHEN type_common AND source_com THEN ass.contractor_id END) AS val_common_com,
  count(DISTINCT CASE WHEN type_common AND source_oth THEN ass.contractor_id END) AS val_common_oth,

  count(DISTINCT CASE WHEN type_advance THEN ass.contractor_id END) AS val_advance_all,
  count(DISTINCT CASE WHEN type_advance AND source_gov THEN ass.contractor_id END) AS val_advance_gov,
  count(DISTINCT CASE WHEN type_advance AND source_com THEN ass.contractor_id END) AS val_advance_com,
  count(DISTINCT CASE WHEN type_advance AND source_oth THEN ass.contractor_id END) AS val_advance_oth,

  count(DISTINCT CASE WHEN type_other THEN ass.contractor_id END) AS val_other_all,
  count(DISTINCT CASE WHEN type_other AND source_gov THEN ass.contractor_id END) AS val_other_gov,
  count(DISTINCT CASE WHEN type_other AND source_com THEN ass.contractor_id END) AS val_other_com,
  count(DISTINCT CASE WHEN type_other AND source_oth THEN ass.contractor_id END) AS val_other_oth,

  count(DISTINCT CASE WHEN type_sds THEN ass.contractor_id END) AS val_sds_all,
  count(DISTINCT CASE WHEN type_sds AND source_gov THEN ass.contractor_id END) AS val_sds_gov,
  count(DISTINCT CASE WHEN type_sds AND source_com THEN ass.contractor_id END) AS val_sds_com,
  count(DISTINCT CASE WHEN type_sds AND source_oth THEN ass.contractor_id END) AS val_sds_oth,

  null AS assays_ids

FROM ass WHERE ass.seedprod_crop_kind = 'SEEDS' AND normal_is AND app_is
-- FROM ass WHERE ass.seedprod_crop_kind = 'SEEDS' AND normal_is AND (reg_is OR app_is)

UNION

SELECT
  40 AS id,
  '  из них к-во хозяйств, где проведена апробация специалистами филиала, шт.' AS title,
  '' AS description,
  1 AS level,
  'normal' AS type,
  'bigint' AS val_type,
  count(DISTINCT ass.contractor_id) AS val_all_all,
  count(DISTINCT CASE WHEN source_gov THEN ass.contractor_id END) AS val_all_gov,
  count(DISTINCT CASE WHEN source_com THEN ass.contractor_id END) AS val_all_com,
  count(DISTINCT CASE WHEN source_oth THEN ass.contractor_id END) AS val_all_oth,

  count(DISTINCT CASE WHEN other_region THEN ass.contractor_id END) AS val_other_region,

  count(DISTINCT CASE WHEN type_common THEN ass.contractor_id END) AS val_common_all,
  count(DISTINCT CASE WHEN type_common AND source_gov THEN ass.contractor_id END) AS val_common_gov,
  count(DISTINCT CASE WHEN type_common AND source_com THEN ass.contractor_id END) AS val_common_com,
  count(DISTINCT CASE WHEN type_common AND source_oth THEN ass.contractor_id END) AS val_common_oth,

  count(DISTINCT CASE WHEN type_advance THEN ass.contractor_id END) AS val_advance_all,
  count(DISTINCT CASE WHEN type_advance AND source_gov THEN ass.contractor_id END) AS val_advance_gov,
  count(DISTINCT CASE WHEN type_advance AND source_com THEN ass.contractor_id END) AS val_advance_com,
  count(DISTINCT CASE WHEN type_advance AND source_oth THEN ass.contractor_id END) AS val_advance_oth,

  count(DISTINCT CASE WHEN type_other THEN ass.contractor_id END) AS val_other_all,
  count(DISTINCT CASE WHEN type_other AND source_gov THEN ass.contractor_id END) AS val_other_gov,
  count(DISTINCT CASE WHEN type_other AND source_com THEN ass.contractor_id END) AS val_other_com,
  count(DISTINCT CASE WHEN type_other AND source_oth THEN ass.contractor_id END) AS val_other_oth,

  count(DISTINCT CASE WHEN type_sds THEN ass.contractor_id END) AS val_sds_all,
  count(DISTINCT CASE WHEN type_sds AND source_gov THEN ass.contractor_id END) AS val_sds_gov,
  count(DISTINCT CASE WHEN type_sds AND source_com THEN ass.contractor_id END) AS val_sds_com,
  count(DISTINCT CASE WHEN type_sds AND source_oth THEN ass.contractor_id END) AS val_sds_oth,

  null AS assays_ids

FROM ass WHERE ass.seedprod_crop_kind = 'SEEDS' AND normal_is AND app_is AND spec_rsc
-- FROM ass WHERE ass.seedprod_crop_kind = 'SEEDS' AND normal_is AND (reg_is OR app_is) AND spec_rsc

UNION

SELECT
  50 AS id,
  'Число специалистов-апробаторов филиала' AS title,
  '' AS description,
  0 AS level,
  'normal' AS type,
  'bigint' AS val_type,

  count(DISTINCT specs.name_full) AS val_all_all,
  null AS val_all_gov,
  null AS val_all_com,
  null AS val_all_oth,

  null AS val_other_region,

  null AS val_common_all,
  null AS val_common_gov,
  null AS val_common_com,
  null AS val_common_oth,

  null AS val_advance_all,
  null AS val_advance_gov,
  null AS val_advance_com,
  null AS val_advance_oth,

  null AS val_other_all,
  null AS val_other_gov,
  null AS val_other_com,
  null AS val_other_oth,

  null AS val_sds_all,
  null AS val_sds_gov,
  null AS val_sds_com,
  null AS val_sds_oth,

  null AS assays_ids

FROM specs

UNION

SELECT
  70 AS id,
  'Количество геоточек апробировано/обследовано ИМ семенных посевов' AS title,
  '' AS description,
  0 AS level,
  'normal' AS type,
  'bigint' AS val_type,

  count(ass.id) AS val_all_all,
  count(CASE WHEN source_gov THEN true END) AS val_all_gov,
  count(CASE WHEN source_com THEN true END) AS val_all_com,
  count(CASE WHEN source_oth THEN true END) AS val_all_oth,

  count(CASE WHEN other_region THEN true END) AS val_other_region,

  count(CASE WHEN type_common THEN true END) AS val_common_all,
  count(CASE WHEN type_common AND source_gov THEN true END) AS val_common_gov,
  count(CASE WHEN type_common AND source_com THEN true END) AS val_common_com,
  count(CASE WHEN type_common AND source_oth THEN true END) AS val_common_oth,

  count(CASE WHEN type_advance THEN true END) AS val_advance_all,
  count(CASE WHEN type_advance AND source_gov THEN true END) AS val_advance_gov,
  count(CASE WHEN type_advance AND source_com THEN true END) AS val_advance_com,
  count(CASE WHEN type_advance AND source_oth THEN true END) AS val_advance_oth,

  count(CASE WHEN type_other THEN true END) AS val_other_all,
  count(CASE WHEN type_other AND source_gov THEN true END) AS val_other_gov,
  count(CASE WHEN type_other AND source_com THEN true END) AS val_other_com,
  count(CASE WHEN type_other AND source_oth THEN true END) AS val_other_oth,

  count(CASE WHEN type_sds THEN true END) AS val_sds_all,
  count(CASE WHEN type_sds AND source_gov THEN true END) AS val_sds_gov,
  count(CASE WHEN type_sds AND source_com THEN true END) AS val_sds_com,
  count(CASE WHEN type_sds AND source_oth THEN true END) AS val_sds_oth,

  null AS assays_ids

FROM ass WHERE normal_is AND app_is AND ass.seedprod_crop_kind = 'SEEDS'


UNION

SELECT
    71 AS id,
    'Погибло всего семенных посевов, тыс.га' AS title,
    '' AS description,
    0 AS level,
    'normal' AS type,
    'double' AS val_type,
    SUM(area_seeded) AS val_all_all,
    SUM(CASE WHEN source_gov THEN area_seeded END) AS val_all_gov,
    SUM(CASE WHEN source_com THEN area_seeded END) AS val_all_com,
    SUM(CASE WHEN source_oth THEN area_seeded END) AS val_all_oth,

    SUM(CASE WHEN other_region THEN area_seeded END) AS val_other_region,

    SUM(CASE WHEN type_common THEN area_seeded END) AS val_common_all,
    SUM(CASE WHEN type_common AND source_gov THEN area_seeded END) AS val_common_gov,
    SUM(CASE WHEN type_common AND source_com THEN area_seeded END) AS val_common_com,
    SUM(CASE WHEN type_common AND source_oth THEN area_seeded END) AS val_common_oth,

    SUM(CASE WHEN type_advance THEN area_seeded END) AS val_advance_all,
    SUM(CASE WHEN type_advance AND source_gov THEN area_seeded END) AS val_advance_gov,
    SUM(CASE WHEN type_advance AND source_com THEN area_seeded END) AS val_advance_com,
    SUM(CASE WHEN type_advance AND source_oth THEN area_seeded END) AS val_advance_oth,

    SUM(CASE WHEN type_other THEN area_seeded END) AS val_other_all,
    SUM(CASE WHEN type_other AND source_gov THEN area_seeded END) AS val_other_gov,
    SUM(CASE WHEN type_other AND source_com THEN area_seeded END) AS val_other_com,
    SUM(CASE WHEN type_other AND source_oth THEN area_seeded END) AS val_other_oth,

    SUM(CASE WHEN type_sds THEN area_seeded END) AS val_sds_all,
    SUM(CASE WHEN type_sds AND source_gov THEN area_seeded END) AS val_sds_gov,
    SUM(CASE WHEN type_sds AND source_com THEN area_seeded END) AS val_sds_com,
    SUM(CASE WHEN type_sds AND source_oth THEN area_seeded END) AS val_sds_oth,

    null AS assays_ids

FROM ass WHERE ass.seedprod_crop_kind = 'SEEDS' AND normal_is AND (STATUS_BEFORE_APP_DEATH_is OR STATUS_AFTER_APP_DEATH_is)

UNION

SELECT
    72 AS id,
    'Погибло до "периода апробации" семенных посевов, тыс.га' AS title,
    '' AS description,
    1 AS level,
    'normal' AS type,
    'double' AS val_type,
    SUM(area_seeded) AS val_all_all,
    SUM(CASE WHEN source_gov THEN area_seeded END) AS val_all_gov,
    SUM(CASE WHEN source_com THEN area_seeded END) AS val_all_com,
    SUM(CASE WHEN source_oth THEN area_seeded END) AS val_all_oth,

    SUM(CASE WHEN other_region THEN area_seeded END) AS val_other_region,

    SUM(CASE WHEN type_common THEN area_seeded END) AS val_common_all,
    SUM(CASE WHEN type_common AND source_gov THEN area_seeded END) AS val_common_gov,
    SUM(CASE WHEN type_common AND source_com THEN area_seeded END) AS val_common_com,
    SUM(CASE WHEN type_common AND source_oth THEN area_seeded END) AS val_common_oth,

    SUM(CASE WHEN type_advance THEN area_seeded END) AS val_advance_all,
    SUM(CASE WHEN type_advance AND source_gov THEN area_seeded END) AS val_advance_gov,
    SUM(CASE WHEN type_advance AND source_com THEN area_seeded END) AS val_advance_com,
    SUM(CASE WHEN type_advance AND source_oth THEN area_seeded END) AS val_advance_oth,

    SUM(CASE WHEN type_other THEN area_seeded END) AS val_other_all,
    SUM(CASE WHEN type_other AND source_gov THEN area_seeded END) AS val_other_gov,
    SUM(CASE WHEN type_other AND source_com THEN area_seeded END) AS val_other_com,
    SUM(CASE WHEN type_other AND source_oth THEN area_seeded END) AS val_other_oth,

    SUM(CASE WHEN type_sds THEN area_seeded END) AS val_sds_all,
    SUM(CASE WHEN type_sds AND source_gov THEN area_seeded END) AS val_sds_gov,
    SUM(CASE WHEN type_sds AND source_com THEN area_seeded END) AS val_sds_com,
    SUM(CASE WHEN type_sds AND source_oth THEN area_seeded END) AS val_sds_oth,

    null AS assays_ids

FROM ass WHERE ass.seedprod_crop_kind = 'SEEDS' AND normal_is AND STATUS_BEFORE_APP_DEATH_is


UNION

SELECT
    73 AS id,
    'Погибло после "периода апробации" семенных посевов, тыс.га' AS title,
    '' AS description,
    1 AS level,
    'normal' AS type,
    'double' AS val_type,
    SUM(area_seeded) AS val_all_all,
    SUM(CASE WHEN source_gov THEN area_seeded END) AS val_all_gov,
    SUM(CASE WHEN source_com THEN area_seeded END) AS val_all_com,
    SUM(CASE WHEN source_oth THEN area_seeded END) AS val_all_oth,

    SUM(CASE WHEN other_region THEN area_seeded END) AS val_other_region,

    SUM(CASE WHEN type_common THEN area_seeded END) AS val_common_all,
    SUM(CASE WHEN type_common AND source_gov THEN area_seeded END) AS val_common_gov,
    SUM(CASE WHEN type_common AND source_com THEN area_seeded END) AS val_common_com,
    SUM(CASE WHEN type_common AND source_oth THEN area_seeded END) AS val_common_oth,

    SUM(CASE WHEN type_advance THEN area_seeded END) AS val_advance_all,
    SUM(CASE WHEN type_advance AND source_gov THEN area_seeded END) AS val_advance_gov,
    SUM(CASE WHEN type_advance AND source_com THEN area_seeded END) AS val_advance_com,
    SUM(CASE WHEN type_advance AND source_oth THEN area_seeded END) AS val_advance_oth,

    SUM(CASE WHEN type_other THEN area_seeded END) AS val_other_all,
    SUM(CASE WHEN type_other AND source_gov THEN area_seeded END) AS val_other_gov,
    SUM(CASE WHEN type_other AND source_com THEN area_seeded END) AS val_other_com,
    SUM(CASE WHEN type_other AND source_oth THEN area_seeded END) AS val_other_oth,

    SUM(CASE WHEN type_sds THEN area_seeded END) AS val_sds_all,
    SUM(CASE WHEN type_sds AND source_gov THEN area_seeded END) AS val_sds_gov,
    SUM(CASE WHEN type_sds AND source_com THEN area_seeded END) AS val_sds_com,
    SUM(CASE WHEN type_sds AND source_oth THEN area_seeded END) AS val_sds_oth,

    null AS assays_ids

FROM ass WHERE ass.seedprod_crop_kind = 'SEEDS' AND normal_is AND STATUS_AFTER_APP_DEATH_is AND NOT STATUS_BEFORE_APP_DEATH_is

UNION

SELECT
    74 AS id,
    'Выведено всего из семенного баланса, тыс.га' AS title,
    '' AS description,
    0 AS level,
    'normal' AS type,
    'double' AS val_type,
    SUM(area_seeded) AS val_all_all,
    SUM(CASE WHEN source_gov THEN area_seeded END) AS val_all_gov,
    SUM(CASE WHEN source_com THEN area_seeded END) AS val_all_com,
    SUM(CASE WHEN source_oth THEN area_seeded END) AS val_all_oth,

    SUM(CASE WHEN other_region THEN area_seeded END) AS val_other_region,

    SUM(CASE WHEN type_common THEN area_seeded END) AS val_common_all,
    SUM(CASE WHEN type_common AND source_gov THEN area_seeded END) AS val_common_gov,
    SUM(CASE WHEN type_common AND source_com THEN area_seeded END) AS val_common_com,
    SUM(CASE WHEN type_common AND source_oth THEN area_seeded END) AS val_common_oth,

    SUM(CASE WHEN type_advance THEN area_seeded END) AS val_advance_all,
    SUM(CASE WHEN type_advance AND source_gov THEN area_seeded END) AS val_advance_gov,
    SUM(CASE WHEN type_advance AND source_com THEN area_seeded END) AS val_advance_com,
    SUM(CASE WHEN type_advance AND source_oth THEN area_seeded END) AS val_advance_oth,

    SUM(CASE WHEN type_other THEN area_seeded END) AS val_other_all,
    SUM(CASE WHEN type_other AND source_gov THEN area_seeded END) AS val_other_gov,
    SUM(CASE WHEN type_other AND source_com THEN area_seeded END) AS val_other_com,
    SUM(CASE WHEN type_other AND source_oth THEN area_seeded END) AS val_other_oth,

    SUM(CASE WHEN type_sds THEN area_seeded END) AS val_sds_all,
    SUM(CASE WHEN type_sds AND source_gov THEN area_seeded END) AS val_sds_gov,
    SUM(CASE WHEN type_sds AND source_com THEN area_seeded END) AS val_sds_com,
    SUM(CASE WHEN type_sds AND source_oth THEN area_seeded END) AS val_sds_oth,

    null AS assays_ids

FROM ass WHERE ass.seedprod_crop_kind = 'SEEDS' AND normal_is AND (STATUS_BEFORE_APP_OUT_SEEDS_is OR STATUS_AFTER_APP_OUT_SEEDS_is)

UNION

SELECT
    75 AS id,
    'Выведено до "периода апробации" из семенного баланса, тыс.га' AS title,
    '' AS description,
    1 AS level,
    'normal' AS type,
    'double' AS val_type,
    SUM(area_seeded) AS val_all_all,
    SUM(CASE WHEN source_gov THEN area_seeded END) AS val_all_gov,
    SUM(CASE WHEN source_com THEN area_seeded END) AS val_all_com,
    SUM(CASE WHEN source_oth THEN area_seeded END) AS val_all_oth,

    SUM(CASE WHEN other_region THEN area_seeded END) AS val_other_region,

    SUM(CASE WHEN type_common THEN area_seeded END) AS val_common_all,
    SUM(CASE WHEN type_common AND source_gov THEN area_seeded END) AS val_common_gov,
    SUM(CASE WHEN type_common AND source_com THEN area_seeded END) AS val_common_com,
    SUM(CASE WHEN type_common AND source_oth THEN area_seeded END) AS val_common_oth,

    SUM(CASE WHEN type_advance THEN area_seeded END) AS val_advance_all,
    SUM(CASE WHEN type_advance AND source_gov THEN area_seeded END) AS val_advance_gov,
    SUM(CASE WHEN type_advance AND source_com THEN area_seeded END) AS val_advance_com,
    SUM(CASE WHEN type_advance AND source_oth THEN area_seeded END) AS val_advance_oth,

    SUM(CASE WHEN type_other THEN area_seeded END) AS val_other_all,
    SUM(CASE WHEN type_other AND source_gov THEN area_seeded END) AS val_other_gov,
    SUM(CASE WHEN type_other AND source_com THEN area_seeded END) AS val_other_com,
    SUM(CASE WHEN type_other AND source_oth THEN area_seeded END) AS val_other_oth,

    SUM(CASE WHEN type_sds THEN area_seeded END) AS val_sds_all,
    SUM(CASE WHEN type_sds AND source_gov THEN area_seeded END) AS val_sds_gov,
    SUM(CASE WHEN type_sds AND source_com THEN area_seeded END) AS val_sds_com,
    SUM(CASE WHEN type_sds AND source_oth THEN area_seeded END) AS val_sds_oth,

    null AS assays_ids

FROM ass WHERE ass.seedprod_crop_kind = 'SEEDS' AND normal_is AND STATUS_BEFORE_APP_OUT_SEEDS_is

UNION

SELECT
    76 AS id,
    'Выведено после "периода апробации" из семенного баланса, тыс.га' AS title,
    '' AS description,
    1 AS level,
    'normal' AS type,
    'double' AS val_type,
    SUM(area_seeded) AS val_all_all,
    SUM(CASE WHEN source_gov THEN area_seeded END) AS val_all_gov,
    SUM(CASE WHEN source_com THEN area_seeded END) AS val_all_com,
    SUM(CASE WHEN source_oth THEN area_seeded END) AS val_all_oth,

    SUM(CASE WHEN other_region THEN area_seeded END) AS val_other_region,

    SUM(CASE WHEN type_common THEN area_seeded END) AS val_common_all,
    SUM(CASE WHEN type_common AND source_gov THEN area_seeded END) AS val_common_gov,
    SUM(CASE WHEN type_common AND source_com THEN area_seeded END) AS val_common_com,
    SUM(CASE WHEN type_common AND source_oth THEN area_seeded END) AS val_common_oth,

    SUM(CASE WHEN type_advance THEN area_seeded END) AS val_advance_all,
    SUM(CASE WHEN type_advance AND source_gov THEN area_seeded END) AS val_advance_gov,
    SUM(CASE WHEN type_advance AND source_com THEN area_seeded END) AS val_advance_com,
    SUM(CASE WHEN type_advance AND source_oth THEN area_seeded END) AS val_advance_oth,

    SUM(CASE WHEN type_other THEN area_seeded END) AS val_other_all,
    SUM(CASE WHEN type_other AND source_gov THEN area_seeded END) AS val_other_gov,
    SUM(CASE WHEN type_other AND source_com THEN area_seeded END) AS val_other_com,
    SUM(CASE WHEN type_other AND source_oth THEN area_seeded END) AS val_other_oth,

    SUM(CASE WHEN type_sds THEN area_seeded END) AS val_sds_all,
    SUM(CASE WHEN type_sds AND source_gov THEN area_seeded END) AS val_sds_gov,
    SUM(CASE WHEN type_sds AND source_com THEN area_seeded END) AS val_sds_com,
    SUM(CASE WHEN type_sds AND source_oth THEN area_seeded END) AS val_sds_oth,

    null AS assays_ids

FROM ass WHERE ass.seedprod_crop_kind = 'SEEDS' AND normal_is AND STATUS_AFTER_APP_OUT_SEEDS_is AND NOT STATUS_BEFORE_APP_OUT_SEEDS_is

UNION

SELECT
    78 AS id,
    'Не предоставлен лицензионный договор, тыс.га' AS title,
    '' AS description,
    0 AS level,
    'normal' AS type,
    'double' AS val_type,
    SUM(area_seeded) AS val_all_all,
    SUM(CASE WHEN source_gov THEN area_seeded END) AS val_all_gov,
    SUM(CASE WHEN source_com THEN area_seeded END) AS val_all_com,
    SUM(CASE WHEN source_oth THEN area_seeded END) AS val_all_oth,

    SUM(CASE WHEN other_region THEN area_seeded END) AS val_other_region,

    SUM(CASE WHEN type_common THEN area_seeded END) AS val_common_all,
    SUM(CASE WHEN type_common AND source_gov THEN area_seeded END) AS val_common_gov,
    SUM(CASE WHEN type_common AND source_com THEN area_seeded END) AS val_common_com,
    SUM(CASE WHEN type_common AND source_oth THEN area_seeded END) AS val_common_oth,

    SUM(CASE WHEN type_advance THEN area_seeded END) AS val_advance_all,
    SUM(CASE WHEN type_advance AND source_gov THEN area_seeded END) AS val_advance_gov,
    SUM(CASE WHEN type_advance AND source_com THEN area_seeded END) AS val_advance_com,
    SUM(CASE WHEN type_advance AND source_oth THEN area_seeded END) AS val_advance_oth,

    SUM(CASE WHEN type_other THEN area_seeded END) AS val_other_all,
    SUM(CASE WHEN type_other AND source_gov THEN area_seeded END) AS val_other_gov,
    SUM(CASE WHEN type_other AND source_com THEN area_seeded END) AS val_other_com,
    SUM(CASE WHEN type_other AND source_oth THEN area_seeded END) AS val_other_oth,

    SUM(CASE WHEN type_sds THEN area_seeded END) AS val_sds_all,
    SUM(CASE WHEN type_sds AND source_gov THEN area_seeded END) AS val_sds_gov,
    SUM(CASE WHEN type_sds AND source_com THEN area_seeded END) AS val_sds_com,
    SUM(CASE WHEN type_sds AND source_oth THEN area_seeded END) AS val_sds_oth,

    null AS assays_ids

FROM ass WHERE ass.seedprod_crop_kind = 'SEEDS' AND normal_is AND STATUS_NO_LIC_AGR_is

--     OUT_SEEDS ("Выведен из семенного баланса"),
--     NO_LIC_AGR ("Не предоставлен лицензионный договор");


UNION

SELECT
  80 AS id,
  'Апробировано всего, тыс.га' AS title,
  '' AS description,
  0 AS level,
  'normal' AS type,
  'double' AS val_type,

  SUM(TOT_sum_all) AS val_all_all,
  SUM(CASE WHEN source_gov THEN TOT_sum_all END) AS val_all_gov,
  SUM(CASE WHEN source_com THEN TOT_sum_all END) AS val_all_com,
  SUM(CASE WHEN source_oth THEN TOT_sum_all END) AS val_all_oth,

  SUM(CASE WHEN other_region THEN TOT_sum_all END) AS val_other_region,

  SUM(CASE WHEN type_common THEN TOT_sum_all END) AS val_common_all,
  SUM(CASE WHEN type_common AND source_gov THEN TOT_sum_all END) AS val_common_gov,
  SUM(CASE WHEN type_common AND source_com THEN TOT_sum_all END) AS val_common_com,
  SUM(CASE WHEN type_common AND source_oth THEN TOT_sum_all END) AS val_common_oth,

  SUM(CASE WHEN type_advance THEN TOT_sum_all END) AS val_advance_all,
  SUM(CASE WHEN type_advance AND source_gov THEN TOT_sum_all END) AS val_advance_gov,
  SUM(CASE WHEN type_advance AND source_com THEN TOT_sum_all END) AS val_advance_com,
  SUM(CASE WHEN type_advance AND source_oth THEN TOT_sum_all END) AS val_advance_oth,

  SUM(CASE WHEN type_other THEN TOT_sum_all END) AS val_other_all,
  SUM(CASE WHEN type_other AND source_gov THEN TOT_sum_all END) AS val_other_gov,
  SUM(CASE WHEN type_other AND source_com THEN TOT_sum_all END) AS val_other_com,
  SUM(CASE WHEN type_other AND source_oth THEN TOT_sum_all END) AS val_other_oth,

  SUM(CASE WHEN type_sds THEN TOT_sum_all END) AS val_sds_all,
  SUM(CASE WHEN type_sds AND source_gov THEN TOT_sum_all END) AS val_sds_gov,
  SUM(CASE WHEN type_sds AND source_com THEN TOT_sum_all END) AS val_sds_com,
  SUM(CASE WHEN type_sds AND source_oth THEN TOT_sum_all END) AS val_sds_oth,

  null AS assays_ids

FROM ass WHERE normal_is AND app_is

UNION

SELECT
  85 AS id,
  '  из них: семенных посевов, тыс.га' AS title,
  '' AS description,
  1 AS level,
  'normal' AS type,
  'double' AS val_type,

  SUM(TOT_sum_seeds) AS val_all_all,
  SUM(CASE WHEN source_gov THEN TOT_sum_seeds END) AS val_all_gov,
  SUM(CASE WHEN source_com THEN TOT_sum_seeds END) AS val_all_com,
  SUM(CASE WHEN source_oth THEN TOT_sum_seeds END) AS val_all_oth,

  SUM(CASE WHEN other_region THEN TOT_sum_seeds END) AS val_other_region,

  SUM(CASE WHEN type_common THEN TOT_sum_seeds END) AS val_common_all,
  SUM(CASE WHEN type_common AND source_gov THEN TOT_sum_seeds END) AS val_common_gov,
  SUM(CASE WHEN type_common AND source_com THEN TOT_sum_seeds END) AS val_common_com,
  SUM(CASE WHEN type_common AND source_oth THEN TOT_sum_seeds END) AS val_common_oth,

  SUM(CASE WHEN type_advance THEN TOT_sum_seeds END) AS val_advance_all,
  SUM(CASE WHEN type_advance AND source_gov THEN TOT_sum_seeds END) AS val_advance_gov,
  SUM(CASE WHEN type_advance AND source_com THEN TOT_sum_seeds END) AS val_advance_com,
  SUM(CASE WHEN type_advance AND source_oth THEN TOT_sum_seeds END) AS val_advance_oth,

  SUM(CASE WHEN type_other THEN TOT_sum_seeds END) AS val_other_all,
  SUM(CASE WHEN type_other AND source_gov THEN TOT_sum_seeds END) AS val_other_gov,
  SUM(CASE WHEN type_other AND source_com THEN TOT_sum_seeds END) AS val_other_com,
  SUM(CASE WHEN type_other AND source_oth THEN TOT_sum_seeds END) AS val_other_oth,

  SUM(CASE WHEN type_sds THEN TOT_sum_seeds END) AS val_sds_all,
  SUM(CASE WHEN type_sds AND source_gov THEN TOT_sum_seeds END) AS val_sds_gov,
  SUM(CASE WHEN type_sds AND source_com THEN TOT_sum_seeds END) AS val_sds_com,
  SUM(CASE WHEN type_sds AND source_oth THEN TOT_sum_seeds END) AS val_sds_oth,

  null AS assays_ids

FROM ass WHERE normal_is AND app_is

UNION

SELECT
  100 AS id,
  '    ОС' AS title,
  '' AS description,
  2 AS level,
  'normal' AS type,
  'double' AS val_type,

  SUM(TOT_OS) AS val_all_all,
  SUM(CASE WHEN source_gov THEN TOT_OS END) AS val_all_gov,
  SUM(CASE WHEN source_com THEN TOT_OS END) AS val_all_com,
  SUM(CASE WHEN source_oth THEN TOT_OS END) AS val_all_oth,

  SUM(CASE WHEN other_region THEN TOT_OS END) AS val_other_region,

  SUM(CASE WHEN type_common THEN TOT_OS END) AS val_common_all,
  SUM(CASE WHEN type_common AND source_gov THEN TOT_OS END) AS val_common_gov,
  SUM(CASE WHEN type_common AND source_com THEN TOT_OS END) AS val_common_com,
  SUM(CASE WHEN type_common AND source_oth THEN TOT_OS END) AS val_common_oth,

  SUM(CASE WHEN type_advance THEN TOT_OS END) AS val_advance_all,
  SUM(CASE WHEN type_advance AND source_gov THEN TOT_OS END) AS val_advance_gov,
  SUM(CASE WHEN type_advance AND source_com THEN TOT_OS END) AS val_advance_com,
  SUM(CASE WHEN type_advance AND source_oth THEN TOT_OS END) AS val_advance_oth,

  SUM(CASE WHEN type_other THEN TOT_OS END) AS val_other_all,
  SUM(CASE WHEN type_other AND source_gov THEN TOT_OS END) AS val_other_gov,
  SUM(CASE WHEN type_other AND source_com THEN TOT_OS END) AS val_other_com,
  SUM(CASE WHEN type_other AND source_oth THEN TOT_OS END) AS val_other_oth,

  SUM(CASE WHEN type_sds THEN TOT_OS END) AS val_sds_all,
  SUM(CASE WHEN type_sds AND source_gov THEN TOT_OS END) AS val_sds_gov,
  SUM(CASE WHEN type_sds AND source_com THEN TOT_OS END) AS val_sds_com,
  SUM(CASE WHEN type_sds AND source_oth THEN TOT_OS END) AS val_sds_oth,

  null AS assays_ids

FROM ass WHERE normal_is AND app_is

UNION

SELECT
  110 AS id,
  '    ЭС' AS title,
  '' AS description,
  2 AS level,
  'normal' AS type,
  'double' AS val_type,

  SUM(TOT_ES) AS val_all_all,
  SUM(CASE WHEN source_gov THEN TOT_ES END) AS val_all_gov,
  SUM(CASE WHEN source_com THEN TOT_ES END) AS val_all_com,
  SUM(CASE WHEN source_oth THEN TOT_ES END) AS val_all_oth,

  SUM(CASE WHEN other_region THEN TOT_ES END) AS val_other_region,

  SUM(CASE WHEN type_common THEN TOT_ES END) AS val_common_all,
  SUM(CASE WHEN type_common AND source_gov THEN TOT_ES END) AS val_common_gov,
  SUM(CASE WHEN type_common AND source_com THEN TOT_ES END) AS val_common_com,
  SUM(CASE WHEN type_common AND source_oth THEN TOT_ES END) AS val_common_oth,

  SUM(CASE WHEN type_advance THEN TOT_ES END) AS val_advance_all,
  SUM(CASE WHEN type_advance AND source_gov THEN TOT_ES END) AS val_advance_gov,
  SUM(CASE WHEN type_advance AND source_com THEN TOT_ES END) AS val_advance_com,
  SUM(CASE WHEN type_advance AND source_oth THEN TOT_ES END) AS val_advance_oth,

  SUM(CASE WHEN type_other THEN TOT_ES END) AS val_other_all,
  SUM(CASE WHEN type_other AND source_gov THEN TOT_ES END) AS val_other_gov,
  SUM(CASE WHEN type_other AND source_com THEN TOT_ES END) AS val_other_com,
  SUM(CASE WHEN type_other AND source_oth THEN TOT_ES END) AS val_other_oth,

  SUM(CASE WHEN type_sds THEN TOT_ES END) AS val_sds_all,
  SUM(CASE WHEN type_sds AND source_gov THEN TOT_ES END) AS val_sds_gov,
  SUM(CASE WHEN type_sds AND source_com THEN TOT_ES END) AS val_sds_com,
  SUM(CASE WHEN type_sds AND source_oth THEN TOT_ES END) AS val_sds_oth,

  null AS assays_ids

FROM ass WHERE normal_is AND app_is

UNION

SELECT
  120 AS id,
  '    РС1' AS title,
  '' AS description,
  2 AS level,
  'normal' AS type,
  'double' AS val_type,

  SUM(TOT_RS_1) AS val_all_all,
  SUM(CASE WHEN source_gov THEN TOT_RS_1 END) AS val_all_gov,
  SUM(CASE WHEN source_com THEN TOT_RS_1 END) AS val_all_com,
  SUM(CASE WHEN source_oth THEN TOT_RS_1 END) AS val_all_oth,

  SUM(CASE WHEN other_region THEN TOT_RS_1 END) AS val_other_region,

  SUM(CASE WHEN type_common THEN TOT_RS_1 END) AS val_common_all,
  SUM(CASE WHEN type_common AND source_gov THEN TOT_RS_1 END) AS val_common_gov,
  SUM(CASE WHEN type_common AND source_com THEN TOT_RS_1 END) AS val_common_com,
  SUM(CASE WHEN type_common AND source_oth THEN TOT_RS_1 END) AS val_common_oth,

  SUM(CASE WHEN type_advance THEN TOT_RS_1 END) AS val_advance_all,
  SUM(CASE WHEN type_advance AND source_gov THEN TOT_RS_1 END) AS val_advance_gov,
  SUM(CASE WHEN type_advance AND source_com THEN TOT_RS_1 END) AS val_advance_com,
  SUM(CASE WHEN type_advance AND source_oth THEN TOT_RS_1 END) AS val_advance_oth,

  SUM(CASE WHEN type_other THEN TOT_RS_1 END) AS val_other_all,
  SUM(CASE WHEN type_other AND source_gov THEN TOT_RS_1 END) AS val_other_gov,
  SUM(CASE WHEN type_other AND source_com THEN TOT_RS_1 END) AS val_other_com,
  SUM(CASE WHEN type_other AND source_oth THEN TOT_RS_1 END) AS val_other_oth,

  SUM(CASE WHEN type_sds THEN TOT_RS_1 END) AS val_sds_all,
  SUM(CASE WHEN type_sds AND source_gov THEN TOT_RS_1 END) AS val_sds_gov,
  SUM(CASE WHEN type_sds AND source_com THEN TOT_RS_1 END) AS val_sds_com,
  SUM(CASE WHEN type_sds AND source_oth THEN TOT_RS_1 END) AS val_sds_oth,

  null AS assays_ids

FROM ass WHERE normal_is AND app_is

UNION

SELECT
  130 AS id,
  '      РС1 - РСт' AS title,
  '' AS description,
  3 AS level,
  'normal' AS type,
  'double' AS val_type,

  SUM(TOT_RS_1_RST) AS val_all_all,
  SUM(CASE WHEN source_gov THEN TOT_RS_1_RST END) AS val_all_gov,
  SUM(CASE WHEN source_com THEN TOT_RS_1_RST END) AS val_all_com,
  SUM(CASE WHEN source_oth THEN TOT_RS_1_RST END) AS val_all_oth,

  SUM(CASE WHEN other_region THEN TOT_RS_1_RST END) AS val_other_region,

  SUM(CASE WHEN type_common THEN TOT_RS_1_RST END) AS val_common_all,
  SUM(CASE WHEN type_common AND source_gov THEN TOT_RS_1_RST END) AS val_common_gov,
  SUM(CASE WHEN type_common AND source_com THEN TOT_RS_1_RST END) AS val_common_com,
  SUM(CASE WHEN type_common AND source_oth THEN TOT_RS_1_RST END) AS val_common_oth,

  SUM(CASE WHEN type_advance THEN TOT_RS_1_RST END) AS val_advance_all,
  SUM(CASE WHEN type_advance AND source_gov THEN TOT_RS_1_RST END) AS val_advance_gov,
  SUM(CASE WHEN type_advance AND source_com THEN TOT_RS_1_RST END) AS val_advance_com,
  SUM(CASE WHEN type_advance AND source_oth THEN TOT_RS_1_RST END) AS val_advance_oth,

  SUM(CASE WHEN type_other THEN TOT_RS_1_RST END) AS val_other_all,
  SUM(CASE WHEN type_other AND source_gov THEN TOT_RS_1_RST END) AS val_other_gov,
  SUM(CASE WHEN type_other AND source_com THEN TOT_RS_1_RST END) AS val_other_com,
  SUM(CASE WHEN type_other AND source_oth THEN TOT_RS_1_RST END) AS val_other_oth,

  SUM(CASE WHEN type_sds THEN TOT_RS_1_RST END) AS val_sds_all,
  SUM(CASE WHEN type_sds AND source_gov THEN TOT_RS_1_RST END) AS val_sds_gov,
  SUM(CASE WHEN type_sds AND source_com THEN TOT_RS_1_RST END) AS val_sds_com,
  SUM(CASE WHEN type_sds AND source_oth THEN TOT_RS_1_RST END) AS val_sds_oth,

  null AS assays_ids

FROM ass WHERE normal_is AND app_is

UNION

SELECT
  140 AS id,
  '    РС2' AS title,
  '' AS description,
  2 AS level,
  'normal' AS type,
  'double' AS val_type,

  SUM(TOT_RS_2) AS val_all_all,
  SUM(CASE WHEN source_gov THEN TOT_RS_2 END) AS val_all_gov,
  SUM(CASE WHEN source_com THEN TOT_RS_2 END) AS val_all_com,
  SUM(CASE WHEN source_oth THEN TOT_RS_2 END) AS val_all_oth,

  SUM(CASE WHEN other_region THEN TOT_RS_2 END) AS val_other_region,

  SUM(CASE WHEN type_common THEN TOT_RS_2 END) AS val_common_all,
  SUM(CASE WHEN type_common AND source_gov THEN TOT_RS_2 END) AS val_common_gov,
  SUM(CASE WHEN type_common AND source_com THEN TOT_RS_2 END) AS val_common_com,
  SUM(CASE WHEN type_common AND source_oth THEN TOT_RS_2 END) AS val_common_oth,

  SUM(CASE WHEN type_advance THEN TOT_RS_2 END) AS val_advance_all,
  SUM(CASE WHEN type_advance AND source_gov THEN TOT_RS_2 END) AS val_advance_gov,
  SUM(CASE WHEN type_advance AND source_com THEN TOT_RS_2 END) AS val_advance_com,
  SUM(CASE WHEN type_advance AND source_oth THEN TOT_RS_2 END) AS val_advance_oth,

  SUM(CASE WHEN type_other THEN TOT_RS_2 END) AS val_other_all,
  SUM(CASE WHEN type_other AND source_gov THEN TOT_RS_2 END) AS val_other_gov,
  SUM(CASE WHEN type_other AND source_com THEN TOT_RS_2 END) AS val_other_com,
  SUM(CASE WHEN type_other AND source_oth THEN TOT_RS_2 END) AS val_other_oth,

  SUM(CASE WHEN type_sds THEN TOT_RS_2 END) AS val_sds_all,
  SUM(CASE WHEN type_sds AND source_gov THEN TOT_RS_2 END) AS val_sds_gov,
  SUM(CASE WHEN type_sds AND source_com THEN TOT_RS_2 END) AS val_sds_com,
  SUM(CASE WHEN type_sds AND source_oth THEN TOT_RS_2 END) AS val_sds_oth,

  null AS assays_ids

FROM ass WHERE normal_is AND app_is

UNION

SELECT
  150 AS id,
  '      РС2 - РСт' AS title,
  '' AS description,
  3 AS level,
  'normal' AS type,
  'double' AS val_type,

  SUM(TOT_RS_2_RST) AS val_all_all,
  SUM(CASE WHEN source_gov THEN TOT_RS_2_RST END) AS val_all_gov,
  SUM(CASE WHEN source_com THEN TOT_RS_2_RST END) AS val_all_com,
  SUM(CASE WHEN source_oth THEN TOT_RS_2_RST END) AS val_all_oth,

  SUM(CASE WHEN other_region THEN TOT_RS_2_RST END) AS val_other_region,

  SUM(CASE WHEN type_common THEN TOT_RS_2_RST END) AS val_common_all,
  SUM(CASE WHEN type_common AND source_gov THEN TOT_RS_2_RST END) AS val_common_gov,
  SUM(CASE WHEN type_common AND source_com THEN TOT_RS_2_RST END) AS val_common_com,
  SUM(CASE WHEN type_common AND source_oth THEN TOT_RS_2_RST END) AS val_common_oth,

  SUM(CASE WHEN type_advance THEN TOT_RS_2_RST END) AS val_advance_all,
  SUM(CASE WHEN type_advance AND source_gov THEN TOT_RS_2_RST END) AS val_advance_gov,
  SUM(CASE WHEN type_advance AND source_com THEN TOT_RS_2_RST END) AS val_advance_com,
  SUM(CASE WHEN type_advance AND source_oth THEN TOT_RS_2_RST END) AS val_advance_oth,

  SUM(CASE WHEN type_other THEN TOT_RS_2_RST END) AS val_other_all,
  SUM(CASE WHEN type_other AND source_gov THEN TOT_RS_2_RST END) AS val_other_gov,
  SUM(CASE WHEN type_other AND source_com THEN TOT_RS_2_RST END) AS val_other_com,
  SUM(CASE WHEN type_other AND source_oth THEN TOT_RS_2_RST END) AS val_other_oth,

  SUM(CASE WHEN type_sds THEN TOT_RS_2_RST END) AS val_sds_all,
  SUM(CASE WHEN type_sds AND source_gov THEN TOT_RS_2_RST END) AS val_sds_gov,
  SUM(CASE WHEN type_sds AND source_com THEN TOT_RS_2_RST END) AS val_sds_com,
  SUM(CASE WHEN type_sds AND source_oth THEN TOT_RS_2_RST END) AS val_sds_oth,

  null AS assays_ids

FROM ass WHERE normal_is AND app_is

UNION

SELECT
  160 AS id,
  '    РС3' AS title,
  '' AS description,
  2 AS level,
  'normal' AS type,
  'double' AS val_type,

  SUM(TOT_RS_3) AS val_all_all,
  SUM(CASE WHEN source_gov THEN TOT_RS_3 END) AS val_all_gov,
  SUM(CASE WHEN source_com THEN TOT_RS_3 END) AS val_all_com,
  SUM(CASE WHEN source_oth THEN TOT_RS_3 END) AS val_all_oth,

  SUM(CASE WHEN other_region THEN TOT_RS_3 END) AS val_other_region,

  SUM(CASE WHEN type_common THEN TOT_RS_3 END) AS val_common_all,
  SUM(CASE WHEN type_common AND source_gov THEN TOT_RS_3 END) AS val_common_gov,
  SUM(CASE WHEN type_common AND source_com THEN TOT_RS_3 END) AS val_common_com,
  SUM(CASE WHEN type_common AND source_oth THEN TOT_RS_3 END) AS val_common_oth,

  SUM(CASE WHEN type_advance THEN TOT_RS_3 END) AS val_advance_all,
  SUM(CASE WHEN type_advance AND source_gov THEN TOT_RS_3 END) AS val_advance_gov,
  SUM(CASE WHEN type_advance AND source_com THEN TOT_RS_3 END) AS val_advance_com,
  SUM(CASE WHEN type_advance AND source_oth THEN TOT_RS_3 END) AS val_advance_oth,

  SUM(CASE WHEN type_other THEN TOT_RS_3 END) AS val_other_all,
  SUM(CASE WHEN type_other AND source_gov THEN TOT_RS_3 END) AS val_other_gov,
  SUM(CASE WHEN type_other AND source_com THEN TOT_RS_3 END) AS val_other_com,
  SUM(CASE WHEN type_other AND source_oth THEN TOT_RS_3 END) AS val_other_oth,

  SUM(CASE WHEN type_sds THEN TOT_RS_3 END) AS val_sds_all,
  SUM(CASE WHEN type_sds AND source_gov THEN TOT_RS_3 END) AS val_sds_gov,
  SUM(CASE WHEN type_sds AND source_com THEN TOT_RS_3 END) AS val_sds_com,
  SUM(CASE WHEN type_sds AND source_oth THEN TOT_RS_3 END) AS val_sds_oth,

  null AS assays_ids

FROM ass WHERE normal_is AND app_is

UNION

SELECT
  170 AS id,
  '      РС3 - РСт' AS title,
  '' AS description,
  3 AS level,
  'normal' AS type,
  'double' AS val_type,

  SUM(TOT_RS_3_RST) AS val_all_all,
  SUM(CASE WHEN source_gov THEN TOT_RS_3_RST END) AS val_all_gov,
  SUM(CASE WHEN source_com THEN TOT_RS_3_RST END) AS val_all_com,
  SUM(CASE WHEN source_oth THEN TOT_RS_3_RST END) AS val_all_oth,

  SUM(CASE WHEN other_region THEN TOT_RS_3_RST END) AS val_other_region,

  SUM(CASE WHEN type_common THEN TOT_RS_3_RST END) AS val_common_all,
  SUM(CASE WHEN type_common AND source_gov THEN TOT_RS_3_RST END) AS val_common_gov,
  SUM(CASE WHEN type_common AND source_com THEN TOT_RS_3_RST END) AS val_common_com,
  SUM(CASE WHEN type_common AND source_oth THEN TOT_RS_3_RST END) AS val_common_oth,

  SUM(CASE WHEN type_advance THEN TOT_RS_3_RST END) AS val_advance_all,
  SUM(CASE WHEN type_advance AND source_gov THEN TOT_RS_3_RST END) AS val_advance_gov,
  SUM(CASE WHEN type_advance AND source_com THEN TOT_RS_3_RST END) AS val_advance_com,
  SUM(CASE WHEN type_advance AND source_oth THEN TOT_RS_3_RST END) AS val_advance_oth,

  SUM(CASE WHEN type_other THEN TOT_RS_3_RST END) AS val_other_all,
  SUM(CASE WHEN type_other AND source_gov THEN TOT_RS_3_RST END) AS val_other_gov,
  SUM(CASE WHEN type_other AND source_com THEN TOT_RS_3_RST END) AS val_other_com,
  SUM(CASE WHEN type_other AND source_oth THEN TOT_RS_3_RST END) AS val_other_oth,

  SUM(CASE WHEN type_sds THEN TOT_RS_3_RST END) AS val_sds_all,
  SUM(CASE WHEN type_sds AND source_gov THEN TOT_RS_3_RST END) AS val_sds_gov,
  SUM(CASE WHEN type_sds AND source_com THEN TOT_RS_3_RST END) AS val_sds_com,
  SUM(CASE WHEN type_sds AND source_oth THEN TOT_RS_3_RST END) AS val_sds_oth,

  null AS assays_ids

FROM ass WHERE normal_is AND app_is

UNION

SELECT
  180 AS id,
  '    РС4' AS title,
  '' AS description,
  2 AS level,
  'normal' AS type,
  'double' AS val_type,

  SUM(TOT_RS_4) AS val_all_all,
  SUM(CASE WHEN source_gov THEN TOT_RS_4 END) AS val_all_gov,
  SUM(CASE WHEN source_com THEN TOT_RS_4 END) AS val_all_com,
  SUM(CASE WHEN source_oth THEN TOT_RS_4 END) AS val_all_oth,

  SUM(CASE WHEN other_region THEN TOT_RS_4 END) AS val_other_region,

  SUM(CASE WHEN type_common THEN TOT_RS_4 END) AS val_common_all,
  SUM(CASE WHEN type_common AND source_gov THEN TOT_RS_4 END) AS val_common_gov,
  SUM(CASE WHEN type_common AND source_com THEN TOT_RS_4 END) AS val_common_com,
  SUM(CASE WHEN type_common AND source_oth THEN TOT_RS_4 END) AS val_common_oth,

  SUM(CASE WHEN type_advance THEN TOT_RS_4 END) AS val_advance_all,
  SUM(CASE WHEN type_advance AND source_gov THEN TOT_RS_4 END) AS val_advance_gov,
  SUM(CASE WHEN type_advance AND source_com THEN TOT_RS_4 END) AS val_advance_com,
  SUM(CASE WHEN type_advance AND source_oth THEN TOT_RS_4 END) AS val_advance_oth,

  SUM(CASE WHEN type_other THEN TOT_RS_4 END) AS val_other_all,
  SUM(CASE WHEN type_other AND source_gov THEN TOT_RS_4 END) AS val_other_gov,
  SUM(CASE WHEN type_other AND source_com THEN TOT_RS_4 END) AS val_other_com,
  SUM(CASE WHEN type_other AND source_oth THEN TOT_RS_4 END) AS val_other_oth,

  SUM(CASE WHEN type_sds THEN TOT_RS_4 END) AS val_sds_all,
  SUM(CASE WHEN type_sds AND source_gov THEN TOT_RS_4 END) AS val_sds_gov,
  SUM(CASE WHEN type_sds AND source_com THEN TOT_RS_4 END) AS val_sds_com,
  SUM(CASE WHEN type_sds AND source_oth THEN TOT_RS_4 END) AS val_sds_oth,

  null AS assays_ids

FROM ass WHERE normal_is AND app_is

UNION

SELECT
  190 AS id,
  '      РС4 - РСт' AS title,
  '' AS description,
  3 AS level,
  'normal' AS type,
  'double' AS val_type,

  SUM(TOT_RS_4_RST) AS val_all_all,
  SUM(CASE WHEN source_gov THEN TOT_RS_4_RST END) AS val_all_gov,
  SUM(CASE WHEN source_com THEN TOT_RS_4_RST END) AS val_all_com,
  SUM(CASE WHEN source_oth THEN TOT_RS_4_RST END) AS val_all_oth,

  SUM(CASE WHEN other_region THEN TOT_RS_4_RST END) AS val_other_region,

  SUM(CASE WHEN type_common THEN TOT_RS_4_RST END) AS val_common_all,
  SUM(CASE WHEN type_common AND source_gov THEN TOT_RS_4_RST END) AS val_common_gov,
  SUM(CASE WHEN type_common AND source_com THEN TOT_RS_4_RST END) AS val_common_com,
  SUM(CASE WHEN type_common AND source_oth THEN TOT_RS_4_RST END) AS val_common_oth,

  SUM(CASE WHEN type_advance THEN TOT_RS_4_RST END) AS val_advance_all,
  SUM(CASE WHEN type_advance AND source_gov THEN TOT_RS_4_RST END) AS val_advance_gov,
  SUM(CASE WHEN type_advance AND source_com THEN TOT_RS_4_RST END) AS val_advance_com,
  SUM(CASE WHEN type_advance AND source_oth THEN TOT_RS_4_RST END) AS val_advance_oth,

  SUM(CASE WHEN type_other THEN TOT_RS_4_RST END) AS val_other_all,
  SUM(CASE WHEN type_other AND source_gov THEN TOT_RS_4_RST END) AS val_other_gov,
  SUM(CASE WHEN type_other AND source_com THEN TOT_RS_4_RST END) AS val_other_com,
  SUM(CASE WHEN type_other AND source_oth THEN TOT_RS_4_RST END) AS val_other_oth,

  SUM(CASE WHEN type_sds THEN TOT_RS_4_RST END) AS val_sds_all,
  SUM(CASE WHEN type_sds AND source_gov THEN TOT_RS_4_RST END) AS val_sds_gov,
  SUM(CASE WHEN type_sds AND source_com THEN TOT_RS_4_RST END) AS val_sds_com,
  SUM(CASE WHEN type_sds AND source_oth THEN TOT_RS_4_RST END) AS val_sds_oth,

  null AS assays_ids

FROM ass WHERE normal_is AND app_is

UNION

SELECT
  200 AS id,
  '    Гибриды F1' AS title,
  '' AS description,
  2 AS level,
  'normal' AS type,
  'double' AS val_type,

  SUM(TOT_F1) AS val_all_all,
  SUM(CASE WHEN source_gov THEN TOT_F1 END) AS val_all_gov,
  SUM(CASE WHEN source_com THEN TOT_F1 END) AS val_all_com,
  SUM(CASE WHEN source_oth THEN TOT_F1 END) AS val_all_oth,

  SUM(CASE WHEN other_region THEN TOT_F1 END) AS val_other_region,

  SUM(CASE WHEN type_common THEN TOT_F1 END) AS val_common_all,
  SUM(CASE WHEN type_common AND source_gov THEN TOT_F1 END) AS val_common_gov,
  SUM(CASE WHEN type_common AND source_com THEN TOT_F1 END) AS val_common_com,
  SUM(CASE WHEN type_common AND source_oth THEN TOT_F1 END) AS val_common_oth,

  SUM(CASE WHEN type_advance THEN TOT_F1 END) AS val_advance_all,
  SUM(CASE WHEN type_advance AND source_gov THEN TOT_F1 END) AS val_advance_gov,
  SUM(CASE WHEN type_advance AND source_com THEN TOT_F1 END) AS val_advance_com,
  SUM(CASE WHEN type_advance AND source_oth THEN TOT_F1 END) AS val_advance_oth,

  SUM(CASE WHEN type_other THEN TOT_F1 END) AS val_other_all,
  SUM(CASE WHEN type_other AND source_gov THEN TOT_F1 END) AS val_other_gov,
  SUM(CASE WHEN type_other AND source_com THEN TOT_F1 END) AS val_other_com,
  SUM(CASE WHEN type_other AND source_oth THEN TOT_F1 END) AS val_other_oth,

  SUM(CASE WHEN type_sds THEN TOT_F1 END) AS val_sds_all,
  SUM(CASE WHEN type_sds AND source_gov THEN TOT_F1 END) AS val_sds_gov,
  SUM(CASE WHEN type_sds AND source_com THEN TOT_F1 END) AS val_sds_com,
  SUM(CASE WHEN type_sds AND source_oth THEN TOT_F1 END) AS val_sds_oth,

  null AS assays_ids

FROM ass WHERE normal_is AND app_is

UNION

SELECT
  210 AS id,
  '    РС5-РСn' AS title,
  '' AS description,
  2 AS level,
  'normal' AS type,
  'double' AS val_type,

  SUM(TOT_RS_5B) AS val_all_all,
  SUM(CASE WHEN source_gov THEN TOT_RS_5B END) AS val_all_gov,
  SUM(CASE WHEN source_com THEN TOT_RS_5B END) AS val_all_com,
  SUM(CASE WHEN source_oth THEN TOT_RS_5B END) AS val_all_oth,

  SUM(CASE WHEN other_region THEN TOT_RS_5B END) AS val_other_region,

  SUM(CASE WHEN type_common THEN TOT_RS_5B END) AS val_common_all,
  SUM(CASE WHEN type_common AND source_gov THEN TOT_RS_5B END) AS val_common_gov,
  SUM(CASE WHEN type_common AND source_com THEN TOT_RS_5B END) AS val_common_com,
  SUM(CASE WHEN type_common AND source_oth THEN TOT_RS_5B END) AS val_common_oth,

  SUM(CASE WHEN type_advance THEN TOT_RS_5B END) AS val_advance_all,
  SUM(CASE WHEN type_advance AND source_gov THEN TOT_RS_5B END) AS val_advance_gov,
  SUM(CASE WHEN type_advance AND source_com THEN TOT_RS_5B END) AS val_advance_com,
  SUM(CASE WHEN type_advance AND source_oth THEN TOT_RS_5B END) AS val_advance_oth,

  SUM(CASE WHEN type_other THEN TOT_RS_5B END) AS val_other_all,
  SUM(CASE WHEN type_other AND source_gov THEN TOT_RS_5B END) AS val_other_gov,
  SUM(CASE WHEN type_other AND source_com THEN TOT_RS_5B END) AS val_other_com,
  SUM(CASE WHEN type_other AND source_oth THEN TOT_RS_5B END) AS val_other_oth,

  SUM(CASE WHEN type_sds THEN TOT_RS_5B END) AS val_sds_all,
  SUM(CASE WHEN type_sds AND source_gov THEN TOT_RS_5B END) AS val_sds_gov,
  SUM(CASE WHEN type_sds AND source_com THEN TOT_RS_5B END) AS val_sds_com,
  SUM(CASE WHEN type_sds AND source_oth THEN TOT_RS_5B END) AS val_sds_oth,

  null AS assays_ids

FROM ass WHERE normal_is AND app_is

UNION

SELECT
  220 AS id,
  '      РС5-РСn - РСт' AS title,
  '' AS description,
  3 AS level,
  'normal' AS type,
  'double' AS val_type,

  SUM(TOT_RS_5B_RST) AS val_all_all,
  SUM(CASE WHEN source_gov THEN TOT_RS_5B_RST END) AS val_all_gov,
  SUM(CASE WHEN source_com THEN TOT_RS_5B_RST END) AS val_all_com,
  SUM(CASE WHEN source_oth THEN TOT_RS_5B_RST END) AS val_all_oth,

  SUM(CASE WHEN other_region THEN TOT_RS_5B_RST END) AS val_other_region,

  SUM(CASE WHEN type_common THEN TOT_RS_5B_RST END) AS val_common_all,
  SUM(CASE WHEN type_common AND source_gov THEN TOT_RS_5B_RST END) AS val_common_gov,
  SUM(CASE WHEN type_common AND source_com THEN TOT_RS_5B_RST END) AS val_common_com,
  SUM(CASE WHEN type_common AND source_oth THEN TOT_RS_5B_RST END) AS val_common_oth,

  SUM(CASE WHEN type_advance THEN TOT_RS_5B_RST END) AS val_advance_all,
  SUM(CASE WHEN type_advance AND source_gov THEN TOT_RS_5B_RST END) AS val_advance_gov,
  SUM(CASE WHEN type_advance AND source_com THEN TOT_RS_5B_RST END) AS val_advance_com,
  SUM(CASE WHEN type_advance AND source_oth THEN TOT_RS_5B_RST END) AS val_advance_oth,

  SUM(CASE WHEN type_other THEN TOT_RS_5B_RST END) AS val_other_all,
  SUM(CASE WHEN type_other AND source_gov THEN TOT_RS_5B_RST END) AS val_other_gov,
  SUM(CASE WHEN type_other AND source_com THEN TOT_RS_5B_RST END) AS val_other_com,
  SUM(CASE WHEN type_other AND source_oth THEN TOT_RS_5B_RST END) AS val_other_oth,

  SUM(CASE WHEN type_sds THEN TOT_RS_5B_RST END) AS val_sds_all,
  SUM(CASE WHEN type_sds AND source_gov THEN TOT_RS_5B_RST END) AS val_sds_gov,
  SUM(CASE WHEN type_sds AND source_com THEN TOT_RS_5B_RST END) AS val_sds_com,
  SUM(CASE WHEN type_sds AND source_oth THEN TOT_RS_5B_RST END) AS val_sds_oth,

  null AS assays_ids

FROM ass WHERE normal_is AND app_is

UNION

SELECT
  230 AS id,
  '    без категории' AS title,
  '' AS description,
  2 AS level,
  'normal' AS type,
  'double' AS val_type,

  SUM(TOT_NC) AS val_all_all,
  SUM(CASE WHEN source_gov THEN TOT_NC END) AS val_all_gov,
  SUM(CASE WHEN source_com THEN TOT_NC END) AS val_all_com,
  SUM(CASE WHEN source_oth THEN TOT_NC END) AS val_all_oth,

  SUM(CASE WHEN other_region THEN TOT_NC END) AS val_other_region,

  SUM(CASE WHEN type_common THEN TOT_NC END) AS val_common_all,
  SUM(CASE WHEN type_common AND source_gov THEN TOT_NC END) AS val_common_gov,
  SUM(CASE WHEN type_common AND source_com THEN TOT_NC END) AS val_common_com,
  SUM(CASE WHEN type_common AND source_oth THEN TOT_NC END) AS val_common_oth,

  SUM(CASE WHEN type_advance THEN TOT_NC END) AS val_advance_all,
  SUM(CASE WHEN type_advance AND source_gov THEN TOT_NC END) AS val_advance_gov,
  SUM(CASE WHEN type_advance AND source_com THEN TOT_NC END) AS val_advance_com,
  SUM(CASE WHEN type_advance AND source_oth THEN TOT_NC END) AS val_advance_oth,

  SUM(CASE WHEN type_other THEN TOT_NC END) AS val_other_all,
  SUM(CASE WHEN type_other AND source_gov THEN TOT_NC END) AS val_other_gov,
  SUM(CASE WHEN type_other AND source_com THEN TOT_NC END) AS val_other_com,
  SUM(CASE WHEN type_other AND source_oth THEN TOT_NC END) AS val_other_oth,

  SUM(CASE WHEN type_sds THEN TOT_NC END) AS val_sds_all,
  SUM(CASE WHEN type_sds AND source_gov THEN TOT_NC END) AS val_sds_gov,
  SUM(CASE WHEN type_sds AND source_com THEN TOT_NC END) AS val_sds_com,
  SUM(CASE WHEN type_sds AND source_oth THEN TOT_NC END) AS val_sds_oth,

  null AS assays_ids

FROM ass WHERE normal_is AND app_is

UNION

SELECT
  240 AS id,
  '  из них: товарных посевов, тыс.га' AS title,
  '' AS description,
  1 AS level,
  'normal' AS type,
  'double' AS val_type,

  SUM(TOT_TRADE) AS val_all_all,
  SUM(CASE WHEN source_gov THEN TOT_TRADE END) AS val_all_gov,
  SUM(CASE WHEN source_com THEN TOT_TRADE END) AS val_all_com,
  SUM(CASE WHEN source_oth THEN TOT_TRADE END) AS val_all_oth,

  SUM(CASE WHEN other_region THEN TOT_TRADE END) AS val_other_region,

  SUM(CASE WHEN type_common THEN TOT_TRADE END) AS val_common_all,
  SUM(CASE WHEN type_common AND source_gov THEN TOT_TRADE END) AS val_common_gov,
  SUM(CASE WHEN type_common AND source_com THEN TOT_TRADE END) AS val_common_com,
  SUM(CASE WHEN type_common AND source_oth THEN TOT_TRADE END) AS val_common_oth,

  SUM(CASE WHEN type_advance THEN TOT_TRADE END) AS val_advance_all,
  SUM(CASE WHEN type_advance AND source_gov THEN TOT_TRADE END) AS val_advance_gov,
  SUM(CASE WHEN type_advance AND source_com THEN TOT_TRADE END) AS val_advance_com,
  SUM(CASE WHEN type_advance AND source_oth THEN TOT_TRADE END) AS val_advance_oth,

  SUM(CASE WHEN type_other THEN TOT_TRADE END) AS val_other_all,
  SUM(CASE WHEN type_other AND source_gov THEN TOT_TRADE END) AS val_other_gov,
  SUM(CASE WHEN type_other AND source_com THEN TOT_TRADE END) AS val_other_com,
  SUM(CASE WHEN type_other AND source_oth THEN TOT_TRADE END) AS val_other_oth,

  SUM(CASE WHEN type_sds THEN TOT_TRADE END) AS val_sds_all,
  SUM(CASE WHEN type_sds AND source_gov THEN TOT_TRADE END) AS val_sds_gov,
  SUM(CASE WHEN type_sds AND source_com THEN TOT_TRADE END) AS val_sds_com,
  SUM(CASE WHEN type_sds AND source_oth THEN TOT_TRADE END) AS val_sds_oth,

  null AS assays_ids

FROM ass WHERE normal_is AND app_is


UNION

SELECT
  250 AS id,
  '  из них: выбраковано апробированных посевов, тыс.га' AS title,
  '' AS description,
  1 AS level,
  'normal' AS type,
  'double' AS val_type,

  SUM(TOT_REJECT_ALL) AS val_all_all,
  SUM(CASE WHEN source_gov THEN TOT_REJECT_ALL END) AS val_all_gov,
  SUM(CASE WHEN source_com THEN TOT_REJECT_ALL END) AS val_all_com,
  SUM(CASE WHEN source_oth THEN TOT_REJECT_ALL END) AS val_all_oth,

  SUM(CASE WHEN other_region THEN TOT_REJECT_ALL END) AS val_other_region,

  SUM(CASE WHEN type_common THEN TOT_REJECT_ALL END) AS val_common_all,
  SUM(CASE WHEN type_common AND source_gov THEN TOT_REJECT_ALL END) AS val_common_gov,
  SUM(CASE WHEN type_common AND source_com THEN TOT_REJECT_ALL END) AS val_common_com,
  SUM(CASE WHEN type_common AND source_oth THEN TOT_REJECT_ALL END) AS val_common_oth,

  SUM(CASE WHEN type_advance THEN TOT_REJECT_ALL END) AS val_advance_all,
  SUM(CASE WHEN type_advance AND source_gov THEN TOT_REJECT_ALL END) AS val_advance_gov,
  SUM(CASE WHEN type_advance AND source_com THEN TOT_REJECT_ALL END) AS val_advance_com,
  SUM(CASE WHEN type_advance AND source_oth THEN TOT_REJECT_ALL END) AS val_advance_oth,

  SUM(CASE WHEN type_other THEN TOT_REJECT_ALL END) AS val_other_all,
  SUM(CASE WHEN type_other AND source_gov THEN TOT_REJECT_ALL END) AS val_other_gov,
  SUM(CASE WHEN type_other AND source_com THEN TOT_REJECT_ALL END) AS val_other_com,
  SUM(CASE WHEN type_other AND source_oth THEN TOT_REJECT_ALL END) AS val_other_oth,

  SUM(CASE WHEN type_sds THEN TOT_REJECT_ALL END) AS val_sds_all,
  SUM(CASE WHEN type_sds AND source_gov THEN TOT_REJECT_ALL END) AS val_sds_gov,
  SUM(CASE WHEN type_sds AND source_com THEN TOT_REJECT_ALL END) AS val_sds_com,
  SUM(CASE WHEN type_sds AND source_oth THEN TOT_REJECT_ALL END) AS val_sds_oth,

  null AS assays_ids

FROM ass WHERE normal_is AND app_is

UNION

SELECT
  255 AS id,
  '    Пространственная изоляция не соответствует минимальным нормам' AS title,
  '' AS description,
  2 AS level,
  'normal' AS type,
  'double' AS val_type,

  SUM(TOT_REJECT_REAS_1) AS val_all_all,
  SUM(CASE WHEN source_gov THEN TOT_REJECT_REAS_1 END) AS val_all_gov,
  SUM(CASE WHEN source_com THEN TOT_REJECT_REAS_1 END) AS val_all_com,
  SUM(CASE WHEN source_oth THEN TOT_REJECT_REAS_1 END) AS val_all_oth,

  SUM(CASE WHEN other_region THEN TOT_REJECT_REAS_1 END) AS val_other_region,

  SUM(CASE WHEN type_common THEN TOT_REJECT_REAS_1 END) AS val_common_all,
  SUM(CASE WHEN type_common AND source_gov THEN TOT_REJECT_REAS_1 END) AS val_common_gov,
  SUM(CASE WHEN type_common AND source_com THEN TOT_REJECT_REAS_1 END) AS val_common_com,
  SUM(CASE WHEN type_common AND source_oth THEN TOT_REJECT_REAS_1 END) AS val_common_oth,

  SUM(CASE WHEN type_advance THEN TOT_REJECT_REAS_1 END) AS val_advance_all,
  SUM(CASE WHEN type_advance AND source_gov THEN TOT_REJECT_REAS_1 END) AS val_advance_gov,
  SUM(CASE WHEN type_advance AND source_com THEN TOT_REJECT_REAS_1 END) AS val_advance_com,
  SUM(CASE WHEN type_advance AND source_oth THEN TOT_REJECT_REAS_1 END) AS val_advance_oth,

  SUM(CASE WHEN type_other THEN TOT_REJECT_REAS_1 END) AS val_other_all,
  SUM(CASE WHEN type_other AND source_gov THEN TOT_REJECT_REAS_1 END) AS val_other_gov,
  SUM(CASE WHEN type_other AND source_com THEN TOT_REJECT_REAS_1 END) AS val_other_com,
  SUM(CASE WHEN type_other AND source_oth THEN TOT_REJECT_REAS_1 END) AS val_other_oth,

  SUM(CASE WHEN type_sds THEN TOT_REJECT_REAS_1 END) AS val_sds_all,
  SUM(CASE WHEN type_sds AND source_gov THEN TOT_REJECT_REAS_1 END) AS val_sds_gov,
  SUM(CASE WHEN type_sds AND source_com THEN TOT_REJECT_REAS_1 END) AS val_sds_com,
  SUM(CASE WHEN type_sds AND source_oth THEN TOT_REJECT_REAS_1 END) AS val_sds_oth,

  null AS assays_ids

FROM ass WHERE normal_is AND app_is

UNION

SELECT
    256 AS id,
    '    Отсутствие предусмотренных изолирующих устройств и (или) разделительной полосы' AS title,
    '' AS description,
    2 AS level,
    'normal' AS type,
    'double' AS val_type,

    SUM(TOT_REJECT_REAS_2) AS val_all_all,
    SUM(CASE WHEN source_gov THEN TOT_REJECT_REAS_2 END) AS val_all_gov,
    SUM(CASE WHEN source_com THEN TOT_REJECT_REAS_2 END) AS val_all_com,
    SUM(CASE WHEN source_oth THEN TOT_REJECT_REAS_2 END) AS val_all_oth,

    SUM(CASE WHEN other_region THEN TOT_REJECT_REAS_2 END) AS val_other_region,

    SUM(CASE WHEN type_common THEN TOT_REJECT_REAS_2 END) AS val_common_all,
    SUM(CASE WHEN type_common AND source_gov THEN TOT_REJECT_REAS_2 END) AS val_common_gov,
    SUM(CASE WHEN type_common AND source_com THEN TOT_REJECT_REAS_2 END) AS val_common_com,
    SUM(CASE WHEN type_common AND source_oth THEN TOT_REJECT_REAS_2 END) AS val_common_oth,

    SUM(CASE WHEN type_advance THEN TOT_REJECT_REAS_2 END) AS val_advance_all,
    SUM(CASE WHEN type_advance AND source_gov THEN TOT_REJECT_REAS_2 END) AS val_advance_gov,
    SUM(CASE WHEN type_advance AND source_com THEN TOT_REJECT_REAS_2 END) AS val_advance_com,
    SUM(CASE WHEN type_advance AND source_oth THEN TOT_REJECT_REAS_2 END) AS val_advance_oth,

    SUM(CASE WHEN type_other THEN TOT_REJECT_REAS_2 END) AS val_other_all,
    SUM(CASE WHEN type_other AND source_gov THEN TOT_REJECT_REAS_2 END) AS val_other_gov,
    SUM(CASE WHEN type_other AND source_com THEN TOT_REJECT_REAS_2 END) AS val_other_com,
    SUM(CASE WHEN type_other AND source_oth THEN TOT_REJECT_REAS_2 END) AS val_other_oth,

    SUM(CASE WHEN type_sds THEN TOT_REJECT_REAS_2 END) AS val_sds_all,
    SUM(CASE WHEN type_sds AND source_gov THEN TOT_REJECT_REAS_2 END) AS val_sds_gov,
    SUM(CASE WHEN type_sds AND source_com THEN TOT_REJECT_REAS_2 END) AS val_sds_com,
    SUM(CASE WHEN type_sds AND source_oth THEN TOT_REJECT_REAS_2 END) AS val_sds_oth,

    null AS assays_ids

FROM ass WHERE normal_is AND app_is

UNION

SELECT
    257 AS id,
    '    Не соответствие посевов требованиям к сортовой чистоте' AS title,
    '' AS description,
    2 AS level,
    'normal' AS type,
    'double' AS val_type,

    SUM(TOT_REJECT_REAS_3) AS val_all_all,
    SUM(CASE WHEN source_gov THEN TOT_REJECT_REAS_3 END) AS val_all_gov,
    SUM(CASE WHEN source_com THEN TOT_REJECT_REAS_3 END) AS val_all_com,
    SUM(CASE WHEN source_oth THEN TOT_REJECT_REAS_3 END) AS val_all_oth,

    SUM(CASE WHEN other_region THEN TOT_REJECT_REAS_3 END) AS val_other_region,

    SUM(CASE WHEN type_common THEN TOT_REJECT_REAS_3 END) AS val_common_all,
    SUM(CASE WHEN type_common AND source_gov THEN TOT_REJECT_REAS_3 END) AS val_common_gov,
    SUM(CASE WHEN type_common AND source_com THEN TOT_REJECT_REAS_3 END) AS val_common_com,
    SUM(CASE WHEN type_common AND source_oth THEN TOT_REJECT_REAS_3 END) AS val_common_oth,

    SUM(CASE WHEN type_advance THEN TOT_REJECT_REAS_3 END) AS val_advance_all,
    SUM(CASE WHEN type_advance AND source_gov THEN TOT_REJECT_REAS_3 END) AS val_advance_gov,
    SUM(CASE WHEN type_advance AND source_com THEN TOT_REJECT_REAS_3 END) AS val_advance_com,
    SUM(CASE WHEN type_advance AND source_oth THEN TOT_REJECT_REAS_3 END) AS val_advance_oth,

    SUM(CASE WHEN type_other THEN TOT_REJECT_REAS_3 END) AS val_other_all,
    SUM(CASE WHEN type_other AND source_gov THEN TOT_REJECT_REAS_3 END) AS val_other_gov,
    SUM(CASE WHEN type_other AND source_com THEN TOT_REJECT_REAS_3 END) AS val_other_com,
    SUM(CASE WHEN type_other AND source_oth THEN TOT_REJECT_REAS_3 END) AS val_other_oth,

    SUM(CASE WHEN type_sds THEN TOT_REJECT_REAS_3 END) AS val_sds_all,
    SUM(CASE WHEN type_sds AND source_gov THEN TOT_REJECT_REAS_3 END) AS val_sds_gov,
    SUM(CASE WHEN type_sds AND source_com THEN TOT_REJECT_REAS_3 END) AS val_sds_com,
    SUM(CASE WHEN type_sds AND source_oth THEN TOT_REJECT_REAS_3 END) AS val_sds_oth,

    null AS assays_ids

FROM ass WHERE normal_is AND app_is

UNION

SELECT
    258 AS id,
    '    Обнаружены в посевах карантинные объекты и (или) ядовитые сорные растения' AS title,
    '' AS description,
    2 AS level,
    'normal' AS type,
    'double' AS val_type,

    SUM(TOT_REJECT_REAS_4) AS val_all_all,
    SUM(CASE WHEN source_gov THEN TOT_REJECT_REAS_4 END) AS val_all_gov,
    SUM(CASE WHEN source_com THEN TOT_REJECT_REAS_4 END) AS val_all_com,
    SUM(CASE WHEN source_oth THEN TOT_REJECT_REAS_4 END) AS val_all_oth,

    SUM(CASE WHEN other_region THEN TOT_REJECT_REAS_4 END) AS val_other_region,

    SUM(CASE WHEN type_common THEN TOT_REJECT_REAS_4 END) AS val_common_all,
    SUM(CASE WHEN type_common AND source_gov THEN TOT_REJECT_REAS_4 END) AS val_common_gov,
    SUM(CASE WHEN type_common AND source_com THEN TOT_REJECT_REAS_4 END) AS val_common_com,
    SUM(CASE WHEN type_common AND source_oth THEN TOT_REJECT_REAS_4 END) AS val_common_oth,

    SUM(CASE WHEN type_advance THEN TOT_REJECT_REAS_4 END) AS val_advance_all,
    SUM(CASE WHEN type_advance AND source_gov THEN TOT_REJECT_REAS_4 END) AS val_advance_gov,
    SUM(CASE WHEN type_advance AND source_com THEN TOT_REJECT_REAS_4 END) AS val_advance_com,
    SUM(CASE WHEN type_advance AND source_oth THEN TOT_REJECT_REAS_4 END) AS val_advance_oth,

    SUM(CASE WHEN type_other THEN TOT_REJECT_REAS_4 END) AS val_other_all,
    SUM(CASE WHEN type_other AND source_gov THEN TOT_REJECT_REAS_4 END) AS val_other_gov,
    SUM(CASE WHEN type_other AND source_com THEN TOT_REJECT_REAS_4 END) AS val_other_com,
    SUM(CASE WHEN type_other AND source_oth THEN TOT_REJECT_REAS_4 END) AS val_other_oth,

    SUM(CASE WHEN type_sds THEN TOT_REJECT_REAS_4 END) AS val_sds_all,
    SUM(CASE WHEN type_sds AND source_gov THEN TOT_REJECT_REAS_4 END) AS val_sds_gov,
    SUM(CASE WHEN type_sds AND source_com THEN TOT_REJECT_REAS_4 END) AS val_sds_com,
    SUM(CASE WHEN type_sds AND source_oth THEN TOT_REJECT_REAS_4 END) AS val_sds_oth,

    null AS assays_ids

FROM ass WHERE normal_is AND app_is

UNION

SELECT
    259 AS id,
    '    Превышены нормы засорённости трудноотделимыми сорными растениями' AS title,
    '' AS description,
    2 AS level,
    'normal' AS type,
    'double' AS val_type,

    SUM(TOT_REJECT_REAS_5) AS val_all_all,
    SUM(CASE WHEN source_gov THEN TOT_REJECT_REAS_5 END) AS val_all_gov,
    SUM(CASE WHEN source_com THEN TOT_REJECT_REAS_5 END) AS val_all_com,
    SUM(CASE WHEN source_oth THEN TOT_REJECT_REAS_5 END) AS val_all_oth,

    SUM(CASE WHEN other_region THEN TOT_REJECT_REAS_5 END) AS val_other_region,

    SUM(CASE WHEN type_common THEN TOT_REJECT_REAS_5 END) AS val_common_all,
    SUM(CASE WHEN type_common AND source_gov THEN TOT_REJECT_REAS_5 END) AS val_common_gov,
    SUM(CASE WHEN type_common AND source_com THEN TOT_REJECT_REAS_5 END) AS val_common_com,
    SUM(CASE WHEN type_common AND source_oth THEN TOT_REJECT_REAS_5 END) AS val_common_oth,

    SUM(CASE WHEN type_advance THEN TOT_REJECT_REAS_5 END) AS val_advance_all,
    SUM(CASE WHEN type_advance AND source_gov THEN TOT_REJECT_REAS_5 END) AS val_advance_gov,
    SUM(CASE WHEN type_advance AND source_com THEN TOT_REJECT_REAS_5 END) AS val_advance_com,
    SUM(CASE WHEN type_advance AND source_oth THEN TOT_REJECT_REAS_5 END) AS val_advance_oth,

    SUM(CASE WHEN type_other THEN TOT_REJECT_REAS_5 END) AS val_other_all,
    SUM(CASE WHEN type_other AND source_gov THEN TOT_REJECT_REAS_5 END) AS val_other_gov,
    SUM(CASE WHEN type_other AND source_com THEN TOT_REJECT_REAS_5 END) AS val_other_com,
    SUM(CASE WHEN type_other AND source_oth THEN TOT_REJECT_REAS_5 END) AS val_other_oth,

    SUM(CASE WHEN type_sds THEN TOT_REJECT_REAS_5 END) AS val_sds_all,
    SUM(CASE WHEN type_sds AND source_gov THEN TOT_REJECT_REAS_5 END) AS val_sds_gov,
    SUM(CASE WHEN type_sds AND source_com THEN TOT_REJECT_REAS_5 END) AS val_sds_com,
    SUM(CASE WHEN type_sds AND source_oth THEN TOT_REJECT_REAS_5 END) AS val_sds_oth,

    null AS assays_ids

FROM ass WHERE normal_is AND app_is

UNION

SELECT
    260 AS id,
    '    Превышены нормы засоренности трудноотделимыми сельскохозяйственными растениями' AS title,
    '' AS description,
    2 AS level,
    'normal' AS type,
    'double' AS val_type,

    SUM(TOT_REJECT_REAS_6) AS val_all_all,
    SUM(CASE WHEN source_gov THEN TOT_REJECT_REAS_6 END) AS val_all_gov,
    SUM(CASE WHEN source_com THEN TOT_REJECT_REAS_6 END) AS val_all_com,
    SUM(CASE WHEN source_oth THEN TOT_REJECT_REAS_6 END) AS val_all_oth,

    SUM(CASE WHEN other_region THEN TOT_REJECT_REAS_6 END) AS val_other_region,

    SUM(CASE WHEN type_common THEN TOT_REJECT_REAS_6 END) AS val_common_all,
    SUM(CASE WHEN type_common AND source_gov THEN TOT_REJECT_REAS_6 END) AS val_common_gov,
    SUM(CASE WHEN type_common AND source_com THEN TOT_REJECT_REAS_6 END) AS val_common_com,
    SUM(CASE WHEN type_common AND source_oth THEN TOT_REJECT_REAS_6 END) AS val_common_oth,

    SUM(CASE WHEN type_advance THEN TOT_REJECT_REAS_6 END) AS val_advance_all,
    SUM(CASE WHEN type_advance AND source_gov THEN TOT_REJECT_REAS_6 END) AS val_advance_gov,
    SUM(CASE WHEN type_advance AND source_com THEN TOT_REJECT_REAS_6 END) AS val_advance_com,
    SUM(CASE WHEN type_advance AND source_oth THEN TOT_REJECT_REAS_6 END) AS val_advance_oth,

    SUM(CASE WHEN type_other THEN TOT_REJECT_REAS_6 END) AS val_other_all,
    SUM(CASE WHEN type_other AND source_gov THEN TOT_REJECT_REAS_6 END) AS val_other_gov,
    SUM(CASE WHEN type_other AND source_com THEN TOT_REJECT_REAS_6 END) AS val_other_com,
    SUM(CASE WHEN type_other AND source_oth THEN TOT_REJECT_REAS_6 END) AS val_other_oth,

    SUM(CASE WHEN type_sds THEN TOT_REJECT_REAS_6 END) AS val_sds_all,
    SUM(CASE WHEN type_sds AND source_gov THEN TOT_REJECT_REAS_6 END) AS val_sds_gov,
    SUM(CASE WHEN type_sds AND source_com THEN TOT_REJECT_REAS_6 END) AS val_sds_com,
    SUM(CASE WHEN type_sds AND source_oth THEN TOT_REJECT_REAS_6 END) AS val_sds_oth,

    null AS assays_ids

FROM ass WHERE normal_is AND app_is

UNION

SELECT
    261 AS id,
    '    Превышение нормы поражения болезнями' AS title,
    '' AS description,
    2 AS level,
    'normal' AS type,
    'double' AS val_type,

    SUM(TOT_REJECT_REAS_7) AS val_all_all,
    SUM(CASE WHEN source_gov THEN TOT_REJECT_REAS_7 END) AS val_all_gov,
    SUM(CASE WHEN source_com THEN TOT_REJECT_REAS_7 END) AS val_all_com,
    SUM(CASE WHEN source_oth THEN TOT_REJECT_REAS_7 END) AS val_all_oth,

    SUM(CASE WHEN other_region THEN TOT_REJECT_REAS_7 END) AS val_other_region,

    SUM(CASE WHEN type_common THEN TOT_REJECT_REAS_7 END) AS val_common_all,
    SUM(CASE WHEN type_common AND source_gov THEN TOT_REJECT_REAS_7 END) AS val_common_gov,
    SUM(CASE WHEN type_common AND source_com THEN TOT_REJECT_REAS_7 END) AS val_common_com,
    SUM(CASE WHEN type_common AND source_oth THEN TOT_REJECT_REAS_7 END) AS val_common_oth,

    SUM(CASE WHEN type_advance THEN TOT_REJECT_REAS_7 END) AS val_advance_all,
    SUM(CASE WHEN type_advance AND source_gov THEN TOT_REJECT_REAS_7 END) AS val_advance_gov,
    SUM(CASE WHEN type_advance AND source_com THEN TOT_REJECT_REAS_7 END) AS val_advance_com,
    SUM(CASE WHEN type_advance AND source_oth THEN TOT_REJECT_REAS_7 END) AS val_advance_oth,

    SUM(CASE WHEN type_other THEN TOT_REJECT_REAS_7 END) AS val_other_all,
    SUM(CASE WHEN type_other AND source_gov THEN TOT_REJECT_REAS_7 END) AS val_other_gov,
    SUM(CASE WHEN type_other AND source_com THEN TOT_REJECT_REAS_7 END) AS val_other_com,
    SUM(CASE WHEN type_other AND source_oth THEN TOT_REJECT_REAS_7 END) AS val_other_oth,

    SUM(CASE WHEN type_sds THEN TOT_REJECT_REAS_7 END) AS val_sds_all,
    SUM(CASE WHEN type_sds AND source_gov THEN TOT_REJECT_REAS_7 END) AS val_sds_gov,
    SUM(CASE WHEN type_sds AND source_com THEN TOT_REJECT_REAS_7 END) AS val_sds_com,
    SUM(CASE WHEN type_sds AND source_oth THEN TOT_REJECT_REAS_7 END) AS val_sds_oth,

    null AS assays_ids

FROM ass WHERE normal_is AND app_is

UNION

SELECT
    262 AS id,
    '    Не выполнение или неполное выполнение выданных Заявителю рекомендаций по улучшению состояния сортовых посевов (посадок)' AS title,
    '' AS description,
    2 AS level,
    'normal' AS type,
    'double' AS val_type,

    SUM(TOT_REJECT_REAS_8) AS val_all_all,
    SUM(CASE WHEN source_gov THEN TOT_REJECT_REAS_8 END) AS val_all_gov,
    SUM(CASE WHEN source_com THEN TOT_REJECT_REAS_8 END) AS val_all_com,
    SUM(CASE WHEN source_oth THEN TOT_REJECT_REAS_8 END) AS val_all_oth,

    SUM(CASE WHEN other_region THEN TOT_REJECT_REAS_8 END) AS val_other_region,

    SUM(CASE WHEN type_common THEN TOT_REJECT_REAS_8 END) AS val_common_all,
    SUM(CASE WHEN type_common AND source_gov THEN TOT_REJECT_REAS_8 END) AS val_common_gov,
    SUM(CASE WHEN type_common AND source_com THEN TOT_REJECT_REAS_8 END) AS val_common_com,
    SUM(CASE WHEN type_common AND source_oth THEN TOT_REJECT_REAS_8 END) AS val_common_oth,

    SUM(CASE WHEN type_advance THEN TOT_REJECT_REAS_8 END) AS val_advance_all,
    SUM(CASE WHEN type_advance AND source_gov THEN TOT_REJECT_REAS_8 END) AS val_advance_gov,
    SUM(CASE WHEN type_advance AND source_com THEN TOT_REJECT_REAS_8 END) AS val_advance_com,
    SUM(CASE WHEN type_advance AND source_oth THEN TOT_REJECT_REAS_8 END) AS val_advance_oth,

    SUM(CASE WHEN type_other THEN TOT_REJECT_REAS_8 END) AS val_other_all,
    SUM(CASE WHEN type_other AND source_gov THEN TOT_REJECT_REAS_8 END) AS val_other_gov,
    SUM(CASE WHEN type_other AND source_com THEN TOT_REJECT_REAS_8 END) AS val_other_com,
    SUM(CASE WHEN type_other AND source_oth THEN TOT_REJECT_REAS_8 END) AS val_other_oth,

    SUM(CASE WHEN type_sds THEN TOT_REJECT_REAS_8 END) AS val_sds_all,
    SUM(CASE WHEN type_sds AND source_gov THEN TOT_REJECT_REAS_8 END) AS val_sds_gov,
    SUM(CASE WHEN type_sds AND source_com THEN TOT_REJECT_REAS_8 END) AS val_sds_com,
    SUM(CASE WHEN type_sds AND source_oth THEN TOT_REJECT_REAS_8 END) AS val_sds_oth,

    null AS assays_ids

FROM ass WHERE normal_is AND app_is

UNION

SELECT
    263 AS id,
    '    Не соблюдение схемы производства гибридных семян' AS title,
    '' AS description,
    2 AS level,
    'normal' AS type,
    'double' AS val_type,

    SUM(TOT_REJECT_REAS_9) AS val_all_all,
    SUM(CASE WHEN source_gov THEN TOT_REJECT_REAS_9 END) AS val_all_gov,
    SUM(CASE WHEN source_com THEN TOT_REJECT_REAS_9 END) AS val_all_com,
    SUM(CASE WHEN source_oth THEN TOT_REJECT_REAS_9 END) AS val_all_oth,

    SUM(CASE WHEN other_region THEN TOT_REJECT_REAS_9 END) AS val_other_region,

    SUM(CASE WHEN type_common THEN TOT_REJECT_REAS_9 END) AS val_common_all,
    SUM(CASE WHEN type_common AND source_gov THEN TOT_REJECT_REAS_9 END) AS val_common_gov,
    SUM(CASE WHEN type_common AND source_com THEN TOT_REJECT_REAS_9 END) AS val_common_com,
    SUM(CASE WHEN type_common AND source_oth THEN TOT_REJECT_REAS_9 END) AS val_common_oth,

    SUM(CASE WHEN type_advance THEN TOT_REJECT_REAS_9 END) AS val_advance_all,
    SUM(CASE WHEN type_advance AND source_gov THEN TOT_REJECT_REAS_9 END) AS val_advance_gov,
    SUM(CASE WHEN type_advance AND source_com THEN TOT_REJECT_REAS_9 END) AS val_advance_com,
    SUM(CASE WHEN type_advance AND source_oth THEN TOT_REJECT_REAS_9 END) AS val_advance_oth,

    SUM(CASE WHEN type_other THEN TOT_REJECT_REAS_9 END) AS val_other_all,
    SUM(CASE WHEN type_other AND source_gov THEN TOT_REJECT_REAS_9 END) AS val_other_gov,
    SUM(CASE WHEN type_other AND source_com THEN TOT_REJECT_REAS_9 END) AS val_other_com,
    SUM(CASE WHEN type_other AND source_oth THEN TOT_REJECT_REAS_9 END) AS val_other_oth,

    SUM(CASE WHEN type_sds THEN TOT_REJECT_REAS_9 END) AS val_sds_all,
    SUM(CASE WHEN type_sds AND source_gov THEN TOT_REJECT_REAS_9 END) AS val_sds_gov,
    SUM(CASE WHEN type_sds AND source_com THEN TOT_REJECT_REAS_9 END) AS val_sds_com,
    SUM(CASE WHEN type_sds AND source_oth THEN TOT_REJECT_REAS_9 END) AS val_sds_oth,

    null AS assays_ids

FROM ass WHERE normal_is AND app_is

UNION

SELECT
    264 AS id,
    '    По причине отсутствия лицензионного договора посев перевести в несортовой' AS title,
    '' AS description,
    2 AS level,
    'normal' AS type,
    'double' AS val_type,

    SUM(TOT_REJECT_REAS_10) AS val_all_all,
    SUM(CASE WHEN source_gov THEN TOT_REJECT_REAS_10 END) AS val_all_gov,
    SUM(CASE WHEN source_com THEN TOT_REJECT_REAS_10 END) AS val_all_com,
    SUM(CASE WHEN source_oth THEN TOT_REJECT_REAS_10 END) AS val_all_oth,

    SUM(CASE WHEN other_region THEN TOT_REJECT_REAS_10 END) AS val_other_region,

    SUM(CASE WHEN type_common THEN TOT_REJECT_REAS_10 END) AS val_common_all,
    SUM(CASE WHEN type_common AND source_gov THEN TOT_REJECT_REAS_10 END) AS val_common_gov,
    SUM(CASE WHEN type_common AND source_com THEN TOT_REJECT_REAS_10 END) AS val_common_com,
    SUM(CASE WHEN type_common AND source_oth THEN TOT_REJECT_REAS_10 END) AS val_common_oth,

    SUM(CASE WHEN type_advance THEN TOT_REJECT_REAS_10 END) AS val_advance_all,
    SUM(CASE WHEN type_advance AND source_gov THEN TOT_REJECT_REAS_10 END) AS val_advance_gov,
    SUM(CASE WHEN type_advance AND source_com THEN TOT_REJECT_REAS_10 END) AS val_advance_com,
    SUM(CASE WHEN type_advance AND source_oth THEN TOT_REJECT_REAS_10 END) AS val_advance_oth,

    SUM(CASE WHEN type_other THEN TOT_REJECT_REAS_10 END) AS val_other_all,
    SUM(CASE WHEN type_other AND source_gov THEN TOT_REJECT_REAS_10 END) AS val_other_gov,
    SUM(CASE WHEN type_other AND source_com THEN TOT_REJECT_REAS_10 END) AS val_other_com,
    SUM(CASE WHEN type_other AND source_oth THEN TOT_REJECT_REAS_10 END) AS val_other_oth,

    SUM(CASE WHEN type_sds THEN TOT_REJECT_REAS_10 END) AS val_sds_all,
    SUM(CASE WHEN type_sds AND source_gov THEN TOT_REJECT_REAS_10 END) AS val_sds_gov,
    SUM(CASE WHEN type_sds AND source_com THEN TOT_REJECT_REAS_10 END) AS val_sds_com,
    SUM(CASE WHEN type_sds AND source_oth THEN TOT_REJECT_REAS_10 END) AS val_sds_oth,

    null AS assays_ids

FROM ass WHERE normal_is AND app_is

UNION

SELECT
    268 AS id,
    '    другие причины' AS title,
    '' AS description,
    2 AS level,
    'normal' AS type,
    'double' AS val_type,

    SUM(TOT_REJECT_REAS_OTHER) AS val_all_all,
    SUM(CASE WHEN source_gov THEN TOT_REJECT_REAS_OTHER END) AS val_all_gov,
    SUM(CASE WHEN source_com THEN TOT_REJECT_REAS_OTHER END) AS val_all_com,
    SUM(CASE WHEN source_oth THEN TOT_REJECT_REAS_OTHER END) AS val_all_oth,

    SUM(CASE WHEN other_region THEN TOT_REJECT_REAS_OTHER END) AS val_other_region,

    SUM(CASE WHEN type_common THEN TOT_REJECT_REAS_OTHER END) AS val_common_all,
    SUM(CASE WHEN type_common AND source_gov THEN TOT_REJECT_REAS_OTHER END) AS val_common_gov,
    SUM(CASE WHEN type_common AND source_com THEN TOT_REJECT_REAS_OTHER END) AS val_common_com,
    SUM(CASE WHEN type_common AND source_oth THEN TOT_REJECT_REAS_OTHER END) AS val_common_oth,

    SUM(CASE WHEN type_advance THEN TOT_REJECT_REAS_OTHER END) AS val_advance_all,
    SUM(CASE WHEN type_advance AND source_gov THEN TOT_REJECT_REAS_OTHER END) AS val_advance_gov,
    SUM(CASE WHEN type_advance AND source_com THEN TOT_REJECT_REAS_OTHER END) AS val_advance_com,
    SUM(CASE WHEN type_advance AND source_oth THEN TOT_REJECT_REAS_OTHER END) AS val_advance_oth,

    SUM(CASE WHEN type_other THEN TOT_REJECT_REAS_OTHER END) AS val_other_all,
    SUM(CASE WHEN type_other AND source_gov THEN TOT_REJECT_REAS_OTHER END) AS val_other_gov,
    SUM(CASE WHEN type_other AND source_com THEN TOT_REJECT_REAS_OTHER END) AS val_other_com,
    SUM(CASE WHEN type_other AND source_oth THEN TOT_REJECT_REAS_OTHER END) AS val_other_oth,

    SUM(CASE WHEN type_sds THEN TOT_REJECT_REAS_OTHER END) AS val_sds_all,
    SUM(CASE WHEN type_sds AND source_gov THEN TOT_REJECT_REAS_OTHER END) AS val_sds_gov,
    SUM(CASE WHEN type_sds AND source_com THEN TOT_REJECT_REAS_OTHER END) AS val_sds_com,
    SUM(CASE WHEN type_sds AND source_oth THEN TOT_REJECT_REAS_OTHER END) AS val_sds_oth,

    null AS assays_ids

FROM ass WHERE normal_is AND app_is

UNION

SELECT
    270 AS id,
    '  из них: По ферм. льготе (не предоставлен лицензионный договор), тыс.га' AS title,
    '' AS description,
    1 AS level,
    'normal' AS type,
    'double' AS val_type,
    SUM(TOT_sum_all) AS val_all_all,
    SUM(CASE WHEN source_gov THEN TOT_sum_all END) AS val_all_gov,
    SUM(CASE WHEN source_com THEN TOT_sum_all END) AS val_all_com,
    SUM(CASE WHEN source_oth THEN TOT_sum_all END) AS val_all_oth,

    SUM(CASE WHEN other_region THEN TOT_sum_all END) AS val_other_region,

    SUM(CASE WHEN type_common THEN TOT_sum_all END) AS val_common_all,
    SUM(CASE WHEN type_common AND source_gov THEN TOT_sum_all END) AS val_common_gov,
    SUM(CASE WHEN type_common AND source_com THEN TOT_sum_all END) AS val_common_com,
    SUM(CASE WHEN type_common AND source_oth THEN TOT_sum_all END) AS val_common_oth,

    SUM(CASE WHEN type_advance THEN TOT_sum_all END) AS val_advance_all,
    SUM(CASE WHEN type_advance AND source_gov THEN TOT_sum_all END) AS val_advance_gov,
    SUM(CASE WHEN type_advance AND source_com THEN TOT_sum_all END) AS val_advance_com,
    SUM(CASE WHEN type_advance AND source_oth THEN TOT_sum_all END) AS val_advance_oth,

    SUM(CASE WHEN type_other THEN TOT_sum_all END) AS val_other_all,
    SUM(CASE WHEN type_other AND source_gov THEN TOT_sum_all END) AS val_other_gov,
    SUM(CASE WHEN type_other AND source_com THEN TOT_sum_all END) AS val_other_com,
    SUM(CASE WHEN type_other AND source_oth THEN TOT_sum_all END) AS val_other_oth,

    SUM(CASE WHEN type_sds THEN TOT_sum_all END) AS val_sds_all,
    SUM(CASE WHEN type_sds AND source_gov THEN TOT_sum_all END) AS val_sds_gov,
    SUM(CASE WHEN type_sds AND source_com THEN TOT_sum_all END) AS val_sds_com,
    SUM(CASE WHEN type_sds AND source_oth THEN TOT_sum_all END) AS val_sds_oth,

    null AS assays_ids

FROM ass WHERE normal_is AND app_is AND STATUS_NO_LIC_AGR_FLAG_643_is

UNION

SELECT
    271 AS id,
    '  из них: Погибло семенных посевов, тыс.га' AS title,
    '' AS description,
    1 AS level,
    'normal' AS type,
    'double' AS val_type,
    SUM(TOT_sum_all) AS val_all_all,
    SUM(CASE WHEN source_gov THEN TOT_sum_all END) AS val_all_gov,
    SUM(CASE WHEN source_com THEN TOT_sum_all END) AS val_all_com,
    SUM(CASE WHEN source_oth THEN TOT_sum_all END) AS val_all_oth,

    SUM(CASE WHEN other_region THEN TOT_sum_all END) AS val_other_region,

    SUM(CASE WHEN type_common THEN TOT_sum_all END) AS val_common_all,
    SUM(CASE WHEN type_common AND source_gov THEN TOT_sum_all END) AS val_common_gov,
    SUM(CASE WHEN type_common AND source_com THEN TOT_sum_all END) AS val_common_com,
    SUM(CASE WHEN type_common AND source_oth THEN TOT_sum_all END) AS val_common_oth,

    SUM(CASE WHEN type_advance THEN TOT_sum_all END) AS val_advance_all,
    SUM(CASE WHEN type_advance AND source_gov THEN TOT_sum_all END) AS val_advance_gov,
    SUM(CASE WHEN type_advance AND source_com THEN TOT_sum_all END) AS val_advance_com,
    SUM(CASE WHEN type_advance AND source_oth THEN TOT_sum_all END) AS val_advance_oth,

    SUM(CASE WHEN type_other THEN TOT_sum_all END) AS val_other_all,
    SUM(CASE WHEN type_other AND source_gov THEN TOT_sum_all END) AS val_other_gov,
    SUM(CASE WHEN type_other AND source_com THEN TOT_sum_all END) AS val_other_com,
    SUM(CASE WHEN type_other AND source_oth THEN TOT_sum_all END) AS val_other_oth,

    SUM(CASE WHEN type_sds THEN TOT_sum_all END) AS val_sds_all,
    SUM(CASE WHEN type_sds AND source_gov THEN TOT_sum_all END) AS val_sds_gov,
    SUM(CASE WHEN type_sds AND source_com THEN TOT_sum_all END) AS val_sds_com,
    SUM(CASE WHEN type_sds AND source_oth THEN TOT_sum_all END) AS val_sds_oth,

    null AS assays_ids

FROM ass WHERE normal_is AND app_is AND STATUS_AFTER_APP_DEATH_is

UNION

SELECT
    272 AS id,
    '  из них: Выведено из семенного баланса, тыс.га' AS title,
    '' AS description,
    1 AS level,
    'normal' AS type,
    'double' AS val_type,
    SUM(TOT_sum_all) AS val_all_all,
    SUM(CASE WHEN source_gov THEN TOT_sum_all END) AS val_all_gov,
    SUM(CASE WHEN source_com THEN TOT_sum_all END) AS val_all_com,
    SUM(CASE WHEN source_oth THEN TOT_sum_all END) AS val_all_oth,

    SUM(CASE WHEN other_region THEN TOT_sum_all END) AS val_other_region,

    SUM(CASE WHEN type_common THEN TOT_sum_all END) AS val_common_all,
    SUM(CASE WHEN type_common AND source_gov THEN TOT_sum_all END) AS val_common_gov,
    SUM(CASE WHEN type_common AND source_com THEN TOT_sum_all END) AS val_common_com,
    SUM(CASE WHEN type_common AND source_oth THEN TOT_sum_all END) AS val_common_oth,

    SUM(CASE WHEN type_advance THEN TOT_sum_all END) AS val_advance_all,
    SUM(CASE WHEN type_advance AND source_gov THEN TOT_sum_all END) AS val_advance_gov,
    SUM(CASE WHEN type_advance AND source_com THEN TOT_sum_all END) AS val_advance_com,
    SUM(CASE WHEN type_advance AND source_oth THEN TOT_sum_all END) AS val_advance_oth,

    SUM(CASE WHEN type_other THEN TOT_sum_all END) AS val_other_all,
    SUM(CASE WHEN type_other AND source_gov THEN TOT_sum_all END) AS val_other_gov,
    SUM(CASE WHEN type_other AND source_com THEN TOT_sum_all END) AS val_other_com,
    SUM(CASE WHEN type_other AND source_oth THEN TOT_sum_all END) AS val_other_oth,

    SUM(CASE WHEN type_sds THEN TOT_sum_all END) AS val_sds_all,
    SUM(CASE WHEN type_sds AND source_gov THEN TOT_sum_all END) AS val_sds_gov,
    SUM(CASE WHEN type_sds AND source_com THEN TOT_sum_all END) AS val_sds_com,
    SUM(CASE WHEN type_sds AND source_oth THEN TOT_sum_all END) AS val_sds_oth,

    null AS assays_ids

FROM ass WHERE normal_is AND app_is AND STATUS_AFTER_APP_OUT_SEEDS_is AND NOT STATUS_BEFORE_APP_OUT_SEEDS_is

UNION

SELECT
  300 AS id,
  'Апробировано специалистами филиала всего, тыс.га' AS title,
  '' AS description,
  0 AS level,
  'normal' AS type,
  'double' AS val_type,

  SUM(TOT_sum_all) AS val_all_all,
  SUM(CASE WHEN source_gov THEN TOT_sum_all END) AS val_all_gov,
  SUM(CASE WHEN source_com THEN TOT_sum_all END) AS val_all_com,
  SUM(CASE WHEN source_oth THEN TOT_sum_all END) AS val_all_oth,

  SUM(CASE WHEN other_region THEN TOT_sum_all END) AS val_other_region,

  SUM(CASE WHEN type_common THEN TOT_sum_all END) AS val_common_all,
  SUM(CASE WHEN type_common AND source_gov THEN TOT_sum_all END) AS val_common_gov,
  SUM(CASE WHEN type_common AND source_com THEN TOT_sum_all END) AS val_common_com,
  SUM(CASE WHEN type_common AND source_oth THEN TOT_sum_all END) AS val_common_oth,

  SUM(CASE WHEN type_advance THEN TOT_sum_all END) AS val_advance_all,
  SUM(CASE WHEN type_advance AND source_gov THEN TOT_sum_all END) AS val_advance_gov,
  SUM(CASE WHEN type_advance AND source_com THEN TOT_sum_all END) AS val_advance_com,
  SUM(CASE WHEN type_advance AND source_oth THEN TOT_sum_all END) AS val_advance_oth,

  SUM(CASE WHEN type_other THEN TOT_sum_all END) AS val_other_all,
  SUM(CASE WHEN type_other AND source_gov THEN TOT_sum_all END) AS val_other_gov,
  SUM(CASE WHEN type_other AND source_com THEN TOT_sum_all END) AS val_other_com,
  SUM(CASE WHEN type_other AND source_oth THEN TOT_sum_all END) AS val_other_oth,

  SUM(CASE WHEN type_sds THEN TOT_sum_all END) AS val_sds_all,
  SUM(CASE WHEN type_sds AND source_gov THEN TOT_sum_all END) AS val_sds_gov,
  SUM(CASE WHEN type_sds AND source_com THEN TOT_sum_all END) AS val_sds_com,
  SUM(CASE WHEN type_sds AND source_oth THEN TOT_sum_all END) AS val_sds_oth,

  null AS assays_ids

FROM ass WHERE normal_is AND app_is AND spec_rsc



UNION

SELECT
  310 AS id,
  '  из них: семенных посевов, тыс.га' AS title,
  '' AS description,
  1 AS level,
  'normal' AS type,
  'double' AS val_type,

  SUM(TOT_sum_seeds) AS val_all_all,
  SUM(CASE WHEN source_gov THEN TOT_sum_seeds END) AS val_all_gov,
  SUM(CASE WHEN source_com THEN TOT_sum_seeds END) AS val_all_com,
  SUM(CASE WHEN source_oth THEN TOT_sum_seeds END) AS val_all_oth,

  SUM(CASE WHEN other_region THEN TOT_sum_seeds END) AS val_other_region,

  SUM(CASE WHEN type_common THEN TOT_sum_seeds END) AS val_common_all,
  SUM(CASE WHEN type_common AND source_gov THEN TOT_sum_seeds END) AS val_common_gov,
  SUM(CASE WHEN type_common AND source_com THEN TOT_sum_seeds END) AS val_common_com,
  SUM(CASE WHEN type_common AND source_oth THEN TOT_sum_seeds END) AS val_common_oth,

  SUM(CASE WHEN type_advance THEN TOT_sum_seeds END) AS val_advance_all,
  SUM(CASE WHEN type_advance AND source_gov THEN TOT_sum_seeds END) AS val_advance_gov,
  SUM(CASE WHEN type_advance AND source_com THEN TOT_sum_seeds END) AS val_advance_com,
  SUM(CASE WHEN type_advance AND source_oth THEN TOT_sum_seeds END) AS val_advance_oth,

  SUM(CASE WHEN type_other THEN TOT_sum_seeds END) AS val_other_all,
  SUM(CASE WHEN type_other AND source_gov THEN TOT_sum_seeds END) AS val_other_gov,
  SUM(CASE WHEN type_other AND source_com THEN TOT_sum_seeds END) AS val_other_com,
  SUM(CASE WHEN type_other AND source_oth THEN TOT_sum_seeds END) AS val_other_oth,

  SUM(CASE WHEN type_sds THEN TOT_sum_seeds END) AS val_sds_all,
  SUM(CASE WHEN type_sds AND source_gov THEN TOT_sum_seeds END) AS val_sds_gov,
  SUM(CASE WHEN type_sds AND source_com THEN TOT_sum_seeds END) AS val_sds_com,
  SUM(CASE WHEN type_sds AND source_oth THEN TOT_sum_seeds END) AS val_sds_oth,

  null AS assays_ids

FROM ass WHERE normal_is AND app_is AND spec_rsc

UNION

SELECT
  320 AS id,
  '    ОС' AS title,
  '' AS description,
  2 AS level,
  'normal' AS type,
  'double' AS val_type,

  SUM(TOT_OS) AS val_all_all,
  SUM(CASE WHEN source_gov THEN TOT_OS END) AS val_all_gov,
  SUM(CASE WHEN source_com THEN TOT_OS END) AS val_all_com,
  SUM(CASE WHEN source_oth THEN TOT_OS END) AS val_all_oth,

  SUM(CASE WHEN other_region THEN TOT_OS END) AS val_other_region,

  SUM(CASE WHEN type_common THEN TOT_OS END) AS val_common_all,
  SUM(CASE WHEN type_common AND source_gov THEN TOT_OS END) AS val_common_gov,
  SUM(CASE WHEN type_common AND source_com THEN TOT_OS END) AS val_common_com,
  SUM(CASE WHEN type_common AND source_oth THEN TOT_OS END) AS val_common_oth,

  SUM(CASE WHEN type_advance THEN TOT_OS END) AS val_advance_all,
  SUM(CASE WHEN type_advance AND source_gov THEN TOT_OS END) AS val_advance_gov,
  SUM(CASE WHEN type_advance AND source_com THEN TOT_OS END) AS val_advance_com,
  SUM(CASE WHEN type_advance AND source_oth THEN TOT_OS END) AS val_advance_oth,

  SUM(CASE WHEN type_other THEN TOT_OS END) AS val_other_all,
  SUM(CASE WHEN type_other AND source_gov THEN TOT_OS END) AS val_other_gov,
  SUM(CASE WHEN type_other AND source_com THEN TOT_OS END) AS val_other_com,
  SUM(CASE WHEN type_other AND source_oth THEN TOT_OS END) AS val_other_oth,

  SUM(CASE WHEN type_sds THEN TOT_OS END) AS val_sds_all,
  SUM(CASE WHEN type_sds AND source_gov THEN TOT_OS END) AS val_sds_gov,
  SUM(CASE WHEN type_sds AND source_com THEN TOT_OS END) AS val_sds_com,
  SUM(CASE WHEN type_sds AND source_oth THEN TOT_OS END) AS val_sds_oth,

  null AS assays_ids

FROM ass WHERE normal_is AND app_is AND spec_rsc

UNION

SELECT
  330 AS id,
  '    ЭС' AS title,
  '' AS description,
  2 AS level,
  'normal' AS type,
  'double' AS val_type,

  SUM(TOT_ES) AS val_all_all,
  SUM(CASE WHEN source_gov THEN TOT_ES END) AS val_all_gov,
  SUM(CASE WHEN source_com THEN TOT_ES END) AS val_all_com,
  SUM(CASE WHEN source_oth THEN TOT_ES END) AS val_all_oth,

  SUM(CASE WHEN other_region THEN TOT_ES END) AS val_other_region,

  SUM(CASE WHEN type_common THEN TOT_ES END) AS val_common_all,
  SUM(CASE WHEN type_common AND source_gov THEN TOT_ES END) AS val_common_gov,
  SUM(CASE WHEN type_common AND source_com THEN TOT_ES END) AS val_common_com,
  SUM(CASE WHEN type_common AND source_oth THEN TOT_ES END) AS val_common_oth,

  SUM(CASE WHEN type_advance THEN TOT_ES END) AS val_advance_all,
  SUM(CASE WHEN type_advance AND source_gov THEN TOT_ES END) AS val_advance_gov,
  SUM(CASE WHEN type_advance AND source_com THEN TOT_ES END) AS val_advance_com,
  SUM(CASE WHEN type_advance AND source_oth THEN TOT_ES END) AS val_advance_oth,

  SUM(CASE WHEN type_other THEN TOT_ES END) AS val_other_all,
  SUM(CASE WHEN type_other AND source_gov THEN TOT_ES END) AS val_other_gov,
  SUM(CASE WHEN type_other AND source_com THEN TOT_ES END) AS val_other_com,
  SUM(CASE WHEN type_other AND source_oth THEN TOT_ES END) AS val_other_oth,

  SUM(CASE WHEN type_sds THEN TOT_ES END) AS val_sds_all,
  SUM(CASE WHEN type_sds AND source_gov THEN TOT_ES END) AS val_sds_gov,
  SUM(CASE WHEN type_sds AND source_com THEN TOT_ES END) AS val_sds_com,
  SUM(CASE WHEN type_sds AND source_oth THEN TOT_ES END) AS val_sds_oth,

  null AS assays_ids


FROM ass WHERE normal_is AND app_is AND spec_rsc

UNION

SELECT
  340 AS id,
  '    РС1' AS title,
  '' AS description,
  2 AS level,
  'normal' AS type,
  'double' AS val_type,

  SUM(TOT_RS_1) AS val_all_all,
  SUM(CASE WHEN source_gov THEN TOT_RS_1 END) AS val_all_gov,
  SUM(CASE WHEN source_com THEN TOT_RS_1 END) AS val_all_com,
  SUM(CASE WHEN source_oth THEN TOT_RS_1 END) AS val_all_oth,

  SUM(CASE WHEN other_region THEN TOT_RS_1 END) AS val_other_region,

  SUM(CASE WHEN type_common THEN TOT_RS_1 END) AS val_common_all,
  SUM(CASE WHEN type_common AND source_gov THEN TOT_RS_1 END) AS val_common_gov,
  SUM(CASE WHEN type_common AND source_com THEN TOT_RS_1 END) AS val_common_com,
  SUM(CASE WHEN type_common AND source_oth THEN TOT_RS_1 END) AS val_common_oth,

  SUM(CASE WHEN type_advance THEN TOT_RS_1 END) AS val_advance_all,
  SUM(CASE WHEN type_advance AND source_gov THEN TOT_RS_1 END) AS val_advance_gov,
  SUM(CASE WHEN type_advance AND source_com THEN TOT_RS_1 END) AS val_advance_com,
  SUM(CASE WHEN type_advance AND source_oth THEN TOT_RS_1 END) AS val_advance_oth,

  SUM(CASE WHEN type_other THEN TOT_RS_1 END) AS val_other_all,
  SUM(CASE WHEN type_other AND source_gov THEN TOT_RS_1 END) AS val_other_gov,
  SUM(CASE WHEN type_other AND source_com THEN TOT_RS_1 END) AS val_other_com,
  SUM(CASE WHEN type_other AND source_oth THEN TOT_RS_1 END) AS val_other_oth,

  SUM(CASE WHEN type_sds THEN TOT_RS_1 END) AS val_sds_all,
  SUM(CASE WHEN type_sds AND source_gov THEN TOT_RS_1 END) AS val_sds_gov,
  SUM(CASE WHEN type_sds AND source_com THEN TOT_RS_1 END) AS val_sds_com,
  SUM(CASE WHEN type_sds AND source_oth THEN TOT_RS_1 END) AS val_sds_oth,

  null AS assays_ids

FROM ass WHERE normal_is AND app_is AND spec_rsc

UNION

SELECT
  350 AS id,
  '      РС1 - РСт' AS title,
  '' AS description,
  3 AS level,
  'normal' AS type,
  'double' AS val_type,

  SUM(TOT_RS_1_RST) AS val_all_all,
  SUM(CASE WHEN source_gov THEN TOT_RS_1_RST END) AS val_all_gov,
  SUM(CASE WHEN source_com THEN TOT_RS_1_RST END) AS val_all_com,
  SUM(CASE WHEN source_oth THEN TOT_RS_1_RST END) AS val_all_oth,

  SUM(CASE WHEN other_region THEN TOT_RS_1_RST END) AS val_other_region,

  SUM(CASE WHEN type_common THEN TOT_RS_1_RST END) AS val_common_all,
  SUM(CASE WHEN type_common AND source_gov THEN TOT_RS_1_RST END) AS val_common_gov,
  SUM(CASE WHEN type_common AND source_com THEN TOT_RS_1_RST END) AS val_common_com,
  SUM(CASE WHEN type_common AND source_oth THEN TOT_RS_1_RST END) AS val_common_oth,

  SUM(CASE WHEN type_advance THEN TOT_RS_1_RST END) AS val_advance_all,
  SUM(CASE WHEN type_advance AND source_gov THEN TOT_RS_1_RST END) AS val_advance_gov,
  SUM(CASE WHEN type_advance AND source_com THEN TOT_RS_1_RST END) AS val_advance_com,
  SUM(CASE WHEN type_advance AND source_oth THEN TOT_RS_1_RST END) AS val_advance_oth,

  SUM(CASE WHEN type_other THEN TOT_RS_1_RST END) AS val_other_all,
  SUM(CASE WHEN type_other AND source_gov THEN TOT_RS_1_RST END) AS val_other_gov,
  SUM(CASE WHEN type_other AND source_com THEN TOT_RS_1_RST END) AS val_other_com,
  SUM(CASE WHEN type_other AND source_oth THEN TOT_RS_1_RST END) AS val_other_oth,

  SUM(CASE WHEN type_sds THEN TOT_RS_1_RST END) AS val_sds_all,
  SUM(CASE WHEN type_sds AND source_gov THEN TOT_RS_1_RST END) AS val_sds_gov,
  SUM(CASE WHEN type_sds AND source_com THEN TOT_RS_1_RST END) AS val_sds_com,
  SUM(CASE WHEN type_sds AND source_oth THEN TOT_RS_1_RST END) AS val_sds_oth,

  null AS assays_ids

FROM ass WHERE normal_is AND app_is AND spec_rsc

UNION

SELECT
  360 AS id,
  '    РС2' AS title,
  '' AS description,
  2 AS level,
  'normal' AS type,
  'double' AS val_type,

  SUM(TOT_RS_2) AS val_all_all,
  SUM(CASE WHEN source_gov THEN TOT_RS_2 END) AS val_all_gov,
  SUM(CASE WHEN source_com THEN TOT_RS_2 END) AS val_all_com,
  SUM(CASE WHEN source_oth THEN TOT_RS_2 END) AS val_all_oth,

  SUM(CASE WHEN other_region THEN TOT_RS_2 END) AS val_other_region,

  SUM(CASE WHEN type_common THEN TOT_RS_2 END) AS val_common_all,
  SUM(CASE WHEN type_common AND source_gov THEN TOT_RS_2 END) AS val_common_gov,
  SUM(CASE WHEN type_common AND source_com THEN TOT_RS_2 END) AS val_common_com,
  SUM(CASE WHEN type_common AND source_oth THEN TOT_RS_2 END) AS val_common_oth,

  SUM(CASE WHEN type_advance THEN TOT_RS_2 END) AS val_advance_all,
  SUM(CASE WHEN type_advance AND source_gov THEN TOT_RS_2 END) AS val_advance_gov,
  SUM(CASE WHEN type_advance AND source_com THEN TOT_RS_2 END) AS val_advance_com,
  SUM(CASE WHEN type_advance AND source_oth THEN TOT_RS_2 END) AS val_advance_oth,

  SUM(CASE WHEN type_other THEN TOT_RS_2 END) AS val_other_all,
  SUM(CASE WHEN type_other AND source_gov THEN TOT_RS_2 END) AS val_other_gov,
  SUM(CASE WHEN type_other AND source_com THEN TOT_RS_2 END) AS val_other_com,
  SUM(CASE WHEN type_other AND source_oth THEN TOT_RS_2 END) AS val_other_oth,

  SUM(CASE WHEN type_sds THEN TOT_RS_2 END) AS val_sds_all,
  SUM(CASE WHEN type_sds AND source_gov THEN TOT_RS_2 END) AS val_sds_gov,
  SUM(CASE WHEN type_sds AND source_com THEN TOT_RS_2 END) AS val_sds_com,
  SUM(CASE WHEN type_sds AND source_oth THEN TOT_RS_2 END) AS val_sds_oth,

  null AS assays_ids

FROM ass WHERE normal_is AND app_is AND spec_rsc

UNION

SELECT
  370 AS id,
  '      РС2 - РСт' AS title,
  '' AS description,
  3 AS level,
  'normal' AS type,
  'double' AS val_type,

  SUM(TOT_RS_2_RST) AS val_all_all,
  SUM(CASE WHEN source_gov THEN TOT_RS_2_RST END) AS val_all_gov,
  SUM(CASE WHEN source_com THEN TOT_RS_2_RST END) AS val_all_com,
  SUM(CASE WHEN source_oth THEN TOT_RS_2_RST END) AS val_all_oth,

  SUM(CASE WHEN other_region THEN TOT_RS_2_RST END) AS val_other_region,

  SUM(CASE WHEN type_common THEN TOT_RS_2_RST END) AS val_common_all,
  SUM(CASE WHEN type_common AND source_gov THEN TOT_RS_2_RST END) AS val_common_gov,
  SUM(CASE WHEN type_common AND source_com THEN TOT_RS_2_RST END) AS val_common_com,
  SUM(CASE WHEN type_common AND source_oth THEN TOT_RS_2_RST END) AS val_common_oth,

  SUM(CASE WHEN type_advance THEN TOT_RS_2_RST END) AS val_advance_all,
  SUM(CASE WHEN type_advance AND source_gov THEN TOT_RS_2_RST END) AS val_advance_gov,
  SUM(CASE WHEN type_advance AND source_com THEN TOT_RS_2_RST END) AS val_advance_com,
  SUM(CASE WHEN type_advance AND source_oth THEN TOT_RS_2_RST END) AS val_advance_oth,

  SUM(CASE WHEN type_other THEN TOT_RS_2_RST END) AS val_other_all,
  SUM(CASE WHEN type_other AND source_gov THEN TOT_RS_2_RST END) AS val_other_gov,
  SUM(CASE WHEN type_other AND source_com THEN TOT_RS_2_RST END) AS val_other_com,
  SUM(CASE WHEN type_other AND source_oth THEN TOT_RS_2_RST END) AS val_other_oth,

  SUM(CASE WHEN type_sds THEN TOT_RS_2_RST END) AS val_sds_all,
  SUM(CASE WHEN type_sds AND source_gov THEN TOT_RS_2_RST END) AS val_sds_gov,
  SUM(CASE WHEN type_sds AND source_com THEN TOT_RS_2_RST END) AS val_sds_com,
  SUM(CASE WHEN type_sds AND source_oth THEN TOT_RS_2_RST END) AS val_sds_oth,

  null AS assays_ids

FROM ass WHERE normal_is AND app_is AND spec_rsc

UNION

SELECT
  380 AS id,
  '    РС3' AS title,
  '' AS description,
  2 AS level,
  'normal' AS type,
  'double' AS val_type,

  SUM(TOT_RS_3) AS val_all_all,
  SUM(CASE WHEN source_gov THEN TOT_RS_3 END) AS val_all_gov,
  SUM(CASE WHEN source_com THEN TOT_RS_3 END) AS val_all_com,
  SUM(CASE WHEN source_oth THEN TOT_RS_3 END) AS val_all_oth,

  SUM(CASE WHEN other_region THEN TOT_RS_3 END) AS val_other_region,

  SUM(CASE WHEN type_common THEN TOT_RS_3 END) AS val_common_all,
  SUM(CASE WHEN type_common AND source_gov THEN TOT_RS_3 END) AS val_common_gov,
  SUM(CASE WHEN type_common AND source_com THEN TOT_RS_3 END) AS val_common_com,
  SUM(CASE WHEN type_common AND source_oth THEN TOT_RS_3 END) AS val_common_oth,

  SUM(CASE WHEN type_advance THEN TOT_RS_3 END) AS val_advance_all,
  SUM(CASE WHEN type_advance AND source_gov THEN TOT_RS_3 END) AS val_advance_gov,
  SUM(CASE WHEN type_advance AND source_com THEN TOT_RS_3 END) AS val_advance_com,
  SUM(CASE WHEN type_advance AND source_oth THEN TOT_RS_3 END) AS val_advance_oth,

  SUM(CASE WHEN type_other THEN TOT_RS_3 END) AS val_other_all,
  SUM(CASE WHEN type_other AND source_gov THEN TOT_RS_3 END) AS val_other_gov,
  SUM(CASE WHEN type_other AND source_com THEN TOT_RS_3 END) AS val_other_com,
  SUM(CASE WHEN type_other AND source_oth THEN TOT_RS_3 END) AS val_other_oth,

  SUM(CASE WHEN type_sds THEN TOT_RS_3 END) AS val_sds_all,
  SUM(CASE WHEN type_sds AND source_gov THEN TOT_RS_3 END) AS val_sds_gov,
  SUM(CASE WHEN type_sds AND source_com THEN TOT_RS_3 END) AS val_sds_com,
  SUM(CASE WHEN type_sds AND source_oth THEN TOT_RS_3 END) AS val_sds_oth,

  null AS assays_ids

FROM ass WHERE normal_is AND app_is AND spec_rsc

UNION

SELECT
  390 AS id,
  '      РС3 - РСт' AS title,
  '' AS description,
  3 AS level,
  'normal' AS type,
  'double' AS val_type,

  SUM(TOT_RS_3_RST) AS val_all_all,
  SUM(CASE WHEN source_gov THEN TOT_RS_3_RST END) AS val_all_gov,
  SUM(CASE WHEN source_com THEN TOT_RS_3_RST END) AS val_all_com,
  SUM(CASE WHEN source_oth THEN TOT_RS_3_RST END) AS val_all_oth,

  SUM(CASE WHEN other_region THEN TOT_RS_3_RST END) AS val_other_region,

  SUM(CASE WHEN type_common THEN TOT_RS_3_RST END) AS val_common_all,
  SUM(CASE WHEN type_common AND source_gov THEN TOT_RS_3_RST END) AS val_common_gov,
  SUM(CASE WHEN type_common AND source_com THEN TOT_RS_3_RST END) AS val_common_com,
  SUM(CASE WHEN type_common AND source_oth THEN TOT_RS_3_RST END) AS val_common_oth,

  SUM(CASE WHEN type_advance THEN TOT_RS_3_RST END) AS val_advance_all,
  SUM(CASE WHEN type_advance AND source_gov THEN TOT_RS_3_RST END) AS val_advance_gov,
  SUM(CASE WHEN type_advance AND source_com THEN TOT_RS_3_RST END) AS val_advance_com,
  SUM(CASE WHEN type_advance AND source_oth THEN TOT_RS_3_RST END) AS val_advance_oth,

  SUM(CASE WHEN type_other THEN TOT_RS_3_RST END) AS val_other_all,
  SUM(CASE WHEN type_other AND source_gov THEN TOT_RS_3_RST END) AS val_other_gov,
  SUM(CASE WHEN type_other AND source_com THEN TOT_RS_3_RST END) AS val_other_com,
  SUM(CASE WHEN type_other AND source_oth THEN TOT_RS_3_RST END) AS val_other_oth,

  SUM(CASE WHEN type_sds THEN TOT_RS_3_RST END) AS val_sds_all,
  SUM(CASE WHEN type_sds AND source_gov THEN TOT_RS_3_RST END) AS val_sds_gov,
  SUM(CASE WHEN type_sds AND source_com THEN TOT_RS_3_RST END) AS val_sds_com,
  SUM(CASE WHEN type_sds AND source_oth THEN TOT_RS_3_RST END) AS val_sds_oth,

  null AS assays_ids

FROM ass WHERE normal_is AND app_is AND spec_rsc

UNION

SELECT
  400 AS id,
  '    РС4' AS title,
  '' AS description,
  2 AS level,
  'normal' AS type,
  'double' AS val_type,

  SUM(TOT_RS_4) AS val_all_all,
  SUM(CASE WHEN source_gov THEN TOT_RS_4 END) AS val_all_gov,
  SUM(CASE WHEN source_com THEN TOT_RS_4 END) AS val_all_com,
  SUM(CASE WHEN source_oth THEN TOT_RS_4 END) AS val_all_oth,

  SUM(CASE WHEN other_region THEN TOT_RS_4 END) AS val_other_region,

  SUM(CASE WHEN type_common THEN TOT_RS_4 END) AS val_common_all,
  SUM(CASE WHEN type_common AND source_gov THEN TOT_RS_4 END) AS val_common_gov,
  SUM(CASE WHEN type_common AND source_com THEN TOT_RS_4 END) AS val_common_com,
  SUM(CASE WHEN type_common AND source_oth THEN TOT_RS_4 END) AS val_common_oth,

  SUM(CASE WHEN type_advance THEN TOT_RS_4 END) AS val_advance_all,
  SUM(CASE WHEN type_advance AND source_gov THEN TOT_RS_4 END) AS val_advance_gov,
  SUM(CASE WHEN type_advance AND source_com THEN TOT_RS_4 END) AS val_advance_com,
  SUM(CASE WHEN type_advance AND source_oth THEN TOT_RS_4 END) AS val_advance_oth,

  SUM(CASE WHEN type_other THEN TOT_RS_4 END) AS val_other_all,
  SUM(CASE WHEN type_other AND source_gov THEN TOT_RS_4 END) AS val_other_gov,
  SUM(CASE WHEN type_other AND source_com THEN TOT_RS_4 END) AS val_other_com,
  SUM(CASE WHEN type_other AND source_oth THEN TOT_RS_4 END) AS val_other_oth,

  SUM(CASE WHEN type_sds THEN TOT_RS_4 END) AS val_sds_all,
  SUM(CASE WHEN type_sds AND source_gov THEN TOT_RS_4 END) AS val_sds_gov,
  SUM(CASE WHEN type_sds AND source_com THEN TOT_RS_4 END) AS val_sds_com,
  SUM(CASE WHEN type_sds AND source_oth THEN TOT_RS_4 END) AS val_sds_oth,

  null AS assays_ids

FROM ass WHERE normal_is AND app_is AND spec_rsc

UNION

SELECT
  410 AS id,
  '      РС4 - РСт' AS title,
  '' AS description,
  3 AS level,
  'normal' AS type,
  'double' AS val_type,

  SUM(TOT_RS_4_RST) AS val_all_all,
  SUM(CASE WHEN source_gov THEN TOT_RS_4_RST END) AS val_all_gov,
  SUM(CASE WHEN source_com THEN TOT_RS_4_RST END) AS val_all_com,
  SUM(CASE WHEN source_oth THEN TOT_RS_4_RST END) AS val_all_oth,

  SUM(CASE WHEN other_region THEN TOT_RS_4_RST END) AS val_other_region,

  SUM(CASE WHEN type_common THEN TOT_RS_4_RST END) AS val_common_all,
  SUM(CASE WHEN type_common AND source_gov THEN TOT_RS_4_RST END) AS val_common_gov,
  SUM(CASE WHEN type_common AND source_com THEN TOT_RS_4_RST END) AS val_common_com,
  SUM(CASE WHEN type_common AND source_oth THEN TOT_RS_4_RST END) AS val_common_oth,

  SUM(CASE WHEN type_advance THEN TOT_RS_4_RST END) AS val_advance_all,
  SUM(CASE WHEN type_advance AND source_gov THEN TOT_RS_4_RST END) AS val_advance_gov,
  SUM(CASE WHEN type_advance AND source_com THEN TOT_RS_4_RST END) AS val_advance_com,
  SUM(CASE WHEN type_advance AND source_oth THEN TOT_RS_4_RST END) AS val_advance_oth,

  SUM(CASE WHEN type_other THEN TOT_RS_4_RST END) AS val_other_all,
  SUM(CASE WHEN type_other AND source_gov THEN TOT_RS_4_RST END) AS val_other_gov,
  SUM(CASE WHEN type_other AND source_com THEN TOT_RS_4_RST END) AS val_other_com,
  SUM(CASE WHEN type_other AND source_oth THEN TOT_RS_4_RST END) AS val_other_oth,

  SUM(CASE WHEN type_sds THEN TOT_RS_4_RST END) AS val_sds_all,
  SUM(CASE WHEN type_sds AND source_gov THEN TOT_RS_4_RST END) AS val_sds_gov,
  SUM(CASE WHEN type_sds AND source_com THEN TOT_RS_4_RST END) AS val_sds_com,
  SUM(CASE WHEN type_sds AND source_oth THEN TOT_RS_4_RST END) AS val_sds_oth,

  null AS assays_ids

FROM ass WHERE normal_is AND app_is AND spec_rsc

UNION

SELECT
  420 AS id,
  '    Гибриды F1' AS title,
  '' AS description,
  2 AS level,
  'normal' AS type,
  'double' AS val_type,

  SUM(TOT_F1) AS val_all_all,
  SUM(CASE WHEN source_gov THEN TOT_F1 END) AS val_all_gov,
  SUM(CASE WHEN source_com THEN TOT_F1 END) AS val_all_com,
  SUM(CASE WHEN source_oth THEN TOT_F1 END) AS val_all_oth,

  SUM(CASE WHEN other_region THEN TOT_F1 END) AS val_other_region,

  SUM(CASE WHEN type_common THEN TOT_F1 END) AS val_common_all,
  SUM(CASE WHEN type_common AND source_gov THEN TOT_F1 END) AS val_common_gov,
  SUM(CASE WHEN type_common AND source_com THEN TOT_F1 END) AS val_common_com,
  SUM(CASE WHEN type_common AND source_oth THEN TOT_F1 END) AS val_common_oth,

  SUM(CASE WHEN type_advance THEN TOT_F1 END) AS val_advance_all,
  SUM(CASE WHEN type_advance AND source_gov THEN TOT_F1 END) AS val_advance_gov,
  SUM(CASE WHEN type_advance AND source_com THEN TOT_F1 END) AS val_advance_com,
  SUM(CASE WHEN type_advance AND source_oth THEN TOT_F1 END) AS val_advance_oth,

  SUM(CASE WHEN type_other THEN TOT_F1 END) AS val_other_all,
  SUM(CASE WHEN type_other AND source_gov THEN TOT_F1 END) AS val_other_gov,
  SUM(CASE WHEN type_other AND source_com THEN TOT_F1 END) AS val_other_com,
  SUM(CASE WHEN type_other AND source_oth THEN TOT_F1 END) AS val_other_oth,

  SUM(CASE WHEN type_sds THEN TOT_F1 END) AS val_sds_all,
  SUM(CASE WHEN type_sds AND source_gov THEN TOT_F1 END) AS val_sds_gov,
  SUM(CASE WHEN type_sds AND source_com THEN TOT_F1 END) AS val_sds_com,
  SUM(CASE WHEN type_sds AND source_oth THEN TOT_F1 END) AS val_sds_oth,

  null AS assays_ids

FROM ass WHERE normal_is AND app_is AND spec_rsc

UNION

SELECT
  430 AS id,
  '    РС5-РСn' AS title,
  '' AS description,
  2 AS level,
  'normal' AS type,
  'double' AS val_type,

  SUM(TOT_RS_5B) AS val_all_all,
  SUM(CASE WHEN source_gov THEN TOT_RS_5B END) AS val_all_gov,
  SUM(CASE WHEN source_com THEN TOT_RS_5B END) AS val_all_com,
  SUM(CASE WHEN source_oth THEN TOT_RS_5B END) AS val_all_oth,

  SUM(CASE WHEN other_region THEN TOT_RS_5B END) AS val_other_region,

  SUM(CASE WHEN type_common THEN TOT_RS_5B END) AS val_common_all,
  SUM(CASE WHEN type_common AND source_gov THEN TOT_RS_5B END) AS val_common_gov,
  SUM(CASE WHEN type_common AND source_com THEN TOT_RS_5B END) AS val_common_com,
  SUM(CASE WHEN type_common AND source_oth THEN TOT_RS_5B END) AS val_common_oth,

  SUM(CASE WHEN type_advance THEN TOT_RS_5B END) AS val_advance_all,
  SUM(CASE WHEN type_advance AND source_gov THEN TOT_RS_5B END) AS val_advance_gov,
  SUM(CASE WHEN type_advance AND source_com THEN TOT_RS_5B END) AS val_advance_com,
  SUM(CASE WHEN type_advance AND source_oth THEN TOT_RS_5B END) AS val_advance_oth,

  SUM(CASE WHEN type_other THEN TOT_RS_5B END) AS val_other_all,
  SUM(CASE WHEN type_other AND source_gov THEN TOT_RS_5B END) AS val_other_gov,
  SUM(CASE WHEN type_other AND source_com THEN TOT_RS_5B END) AS val_other_com,
  SUM(CASE WHEN type_other AND source_oth THEN TOT_RS_5B END) AS val_other_oth,

  SUM(CASE WHEN type_sds THEN TOT_RS_5B END) AS val_sds_all,
  SUM(CASE WHEN type_sds AND source_gov THEN TOT_RS_5B END) AS val_sds_gov,
  SUM(CASE WHEN type_sds AND source_com THEN TOT_RS_5B END) AS val_sds_com,
  SUM(CASE WHEN type_sds AND source_oth THEN TOT_RS_5B END) AS val_sds_oth,

  null AS assays_ids

FROM ass WHERE normal_is AND app_is AND spec_rsc

UNION

SELECT
  440 AS id,
  '      РС5-РСn - РСт' AS title,
  '' AS description,
  3 AS level,
  'normal' AS type,
  'double' AS val_type,

  SUM(TOT_RS_5B_RST) AS val_all_all,
  SUM(CASE WHEN source_gov THEN TOT_RS_5B_RST END) AS val_all_gov,
  SUM(CASE WHEN source_com THEN TOT_RS_5B_RST END) AS val_all_com,
  SUM(CASE WHEN source_oth THEN TOT_RS_5B_RST END) AS val_all_oth,

  SUM(CASE WHEN other_region THEN TOT_RS_5B_RST END) AS val_other_region,

  SUM(CASE WHEN type_common THEN TOT_RS_5B_RST END) AS val_common_all,
  SUM(CASE WHEN type_common AND source_gov THEN TOT_RS_5B_RST END) AS val_common_gov,
  SUM(CASE WHEN type_common AND source_com THEN TOT_RS_5B_RST END) AS val_common_com,
  SUM(CASE WHEN type_common AND source_oth THEN TOT_RS_5B_RST END) AS val_common_oth,

  SUM(CASE WHEN type_advance THEN TOT_RS_5B_RST END) AS val_advance_all,
  SUM(CASE WHEN type_advance AND source_gov THEN TOT_RS_5B_RST END) AS val_advance_gov,
  SUM(CASE WHEN type_advance AND source_com THEN TOT_RS_5B_RST END) AS val_advance_com,
  SUM(CASE WHEN type_advance AND source_oth THEN TOT_RS_5B_RST END) AS val_advance_oth,

  SUM(CASE WHEN type_other THEN TOT_RS_5B_RST END) AS val_other_all,
  SUM(CASE WHEN type_other AND source_gov THEN TOT_RS_5B_RST END) AS val_other_gov,
  SUM(CASE WHEN type_other AND source_com THEN TOT_RS_5B_RST END) AS val_other_com,
  SUM(CASE WHEN type_other AND source_oth THEN TOT_RS_5B_RST END) AS val_other_oth,

  SUM(CASE WHEN type_sds THEN TOT_RS_5B_RST END) AS val_sds_all,
  SUM(CASE WHEN type_sds AND source_gov THEN TOT_RS_5B_RST END) AS val_sds_gov,
  SUM(CASE WHEN type_sds AND source_com THEN TOT_RS_5B_RST END) AS val_sds_com,
  SUM(CASE WHEN type_sds AND source_oth THEN TOT_RS_5B_RST END) AS val_sds_oth,

  null AS assays_ids

FROM ass WHERE normal_is AND app_is AND spec_rsc

UNION

SELECT
  450 AS id,
  '    без категории' AS title,
  '' AS description,
  2 AS level,
  'normal' AS type,
  'double' AS val_type,

  SUM(TOT_NC) AS val_all_all,
  SUM(CASE WHEN source_gov THEN TOT_NC END) AS val_all_gov,
  SUM(CASE WHEN source_com THEN TOT_NC END) AS val_all_com,
  SUM(CASE WHEN source_oth THEN TOT_NC END) AS val_all_oth,

  SUM(CASE WHEN other_region THEN TOT_NC END) AS val_other_region,

  SUM(CASE WHEN type_common THEN TOT_NC END) AS val_common_all,
  SUM(CASE WHEN type_common AND source_gov THEN TOT_NC END) AS val_common_gov,
  SUM(CASE WHEN type_common AND source_com THEN TOT_NC END) AS val_common_com,
  SUM(CASE WHEN type_common AND source_oth THEN TOT_NC END) AS val_common_oth,

  SUM(CASE WHEN type_advance THEN TOT_NC END) AS val_advance_all,
  SUM(CASE WHEN type_advance AND source_gov THEN TOT_NC END) AS val_advance_gov,
  SUM(CASE WHEN type_advance AND source_com THEN TOT_NC END) AS val_advance_com,
  SUM(CASE WHEN type_advance AND source_oth THEN TOT_NC END) AS val_advance_oth,

  SUM(CASE WHEN type_other THEN TOT_NC END) AS val_other_all,
  SUM(CASE WHEN type_other AND source_gov THEN TOT_NC END) AS val_other_gov,
  SUM(CASE WHEN type_other AND source_com THEN TOT_NC END) AS val_other_com,
  SUM(CASE WHEN type_other AND source_oth THEN TOT_NC END) AS val_other_oth,

  SUM(CASE WHEN type_sds THEN TOT_NC END) AS val_sds_all,
  SUM(CASE WHEN type_sds AND source_gov THEN TOT_NC END) AS val_sds_gov,
  SUM(CASE WHEN type_sds AND source_com THEN TOT_NC END) AS val_sds_com,
  SUM(CASE WHEN type_sds AND source_oth THEN TOT_NC END) AS val_sds_oth,

  null AS assays_ids

FROM ass WHERE normal_is AND app_is AND spec_rsc

UNION

SELECT
  460 AS id,
  '  из них: товарных посевов, тыс.га' AS title,
  '' AS description,
  1 AS level,
  'normal' AS type,
  'double' AS val_type,

  SUM(TOT_TRADE) AS val_all_all,
  SUM(CASE WHEN source_gov THEN TOT_TRADE END) AS val_all_gov,
  SUM(CASE WHEN source_com THEN TOT_TRADE END) AS val_all_com,
  SUM(CASE WHEN source_oth THEN TOT_TRADE END) AS val_all_oth,

  SUM(CASE WHEN other_region THEN TOT_TRADE END) AS val_other_region,

  SUM(CASE WHEN type_common THEN TOT_TRADE END) AS val_common_all,
  SUM(CASE WHEN type_common AND source_gov THEN TOT_TRADE END) AS val_common_gov,
  SUM(CASE WHEN type_common AND source_com THEN TOT_TRADE END) AS val_common_com,
  SUM(CASE WHEN type_common AND source_oth THEN TOT_TRADE END) AS val_common_oth,

  SUM(CASE WHEN type_advance THEN TOT_TRADE END) AS val_advance_all,
  SUM(CASE WHEN type_advance AND source_gov THEN TOT_TRADE END) AS val_advance_gov,
  SUM(CASE WHEN type_advance AND source_com THEN TOT_TRADE END) AS val_advance_com,
  SUM(CASE WHEN type_advance AND source_oth THEN TOT_TRADE END) AS val_advance_oth,

  SUM(CASE WHEN type_other THEN TOT_TRADE END) AS val_other_all,
  SUM(CASE WHEN type_other AND source_gov THEN TOT_TRADE END) AS val_other_gov,
  SUM(CASE WHEN type_other AND source_com THEN TOT_TRADE END) AS val_other_com,
  SUM(CASE WHEN type_other AND source_oth THEN TOT_TRADE END) AS val_other_oth,

  SUM(CASE WHEN type_sds THEN TOT_TRADE END) AS val_sds_all,
  SUM(CASE WHEN type_sds AND source_gov THEN TOT_TRADE END) AS val_sds_gov,
  SUM(CASE WHEN type_sds AND source_com THEN TOT_TRADE END) AS val_sds_com,
  SUM(CASE WHEN type_sds AND source_oth THEN TOT_TRADE END) AS val_sds_oth,

  null AS assays_ids

FROM ass WHERE normal_is AND app_is AND spec_rsc

UNION

SELECT
  600 AS id,
  '  из них: выбраковано апробированных посевов, тыс.га' AS title,
  '' AS description,
  1 AS level,
  'normal' AS type,
  'double' AS val_type,

  SUM(TOT_REJECT_ALL) AS val_all_all,
  SUM(CASE WHEN source_gov THEN TOT_REJECT_ALL END) AS val_all_gov,
  SUM(CASE WHEN source_com THEN TOT_REJECT_ALL END) AS val_all_com,
  SUM(CASE WHEN source_oth THEN TOT_REJECT_ALL END) AS val_all_oth,

  SUM(CASE WHEN other_region THEN TOT_REJECT_ALL END) AS val_other_region,

  SUM(CASE WHEN type_common THEN TOT_REJECT_ALL END) AS val_common_all,
  SUM(CASE WHEN type_common AND source_gov THEN TOT_REJECT_ALL END) AS val_common_gov,
  SUM(CASE WHEN type_common AND source_com THEN TOT_REJECT_ALL END) AS val_common_com,
  SUM(CASE WHEN type_common AND source_oth THEN TOT_REJECT_ALL END) AS val_common_oth,

  SUM(CASE WHEN type_advance THEN TOT_REJECT_ALL END) AS val_advance_all,
  SUM(CASE WHEN type_advance AND source_gov THEN TOT_REJECT_ALL END) AS val_advance_gov,
  SUM(CASE WHEN type_advance AND source_com THEN TOT_REJECT_ALL END) AS val_advance_com,
  SUM(CASE WHEN type_advance AND source_oth THEN TOT_REJECT_ALL END) AS val_advance_oth,

  SUM(CASE WHEN type_other THEN TOT_REJECT_ALL END) AS val_other_all,
  SUM(CASE WHEN type_other AND source_gov THEN TOT_REJECT_ALL END) AS val_other_gov,
  SUM(CASE WHEN type_other AND source_com THEN TOT_REJECT_ALL END) AS val_other_com,
  SUM(CASE WHEN type_other AND source_oth THEN TOT_REJECT_ALL END) AS val_other_oth,

  SUM(CASE WHEN type_sds THEN TOT_REJECT_ALL END) AS val_sds_all,
  SUM(CASE WHEN type_sds AND source_gov THEN TOT_REJECT_ALL END) AS val_sds_gov,
  SUM(CASE WHEN type_sds AND source_com THEN TOT_REJECT_ALL END) AS val_sds_com,
  SUM(CASE WHEN type_sds AND source_oth THEN TOT_REJECT_ALL END) AS val_sds_oth,

  null AS assays_ids

FROM ass WHERE normal_is AND app_is AND spec_rsc


UNION

SELECT
    645 AS id,
    '    Пространственная изоляция не соответствует минимальным нормам' AS title,
    '' AS description,
    2 AS level,
    'normal' AS type,
    'double' AS val_type,

    SUM(TOT_REJECT_REAS_1) AS val_all_all,
    SUM(CASE WHEN source_gov THEN TOT_REJECT_REAS_1 END) AS val_all_gov,
    SUM(CASE WHEN source_com THEN TOT_REJECT_REAS_1 END) AS val_all_com,
    SUM(CASE WHEN source_oth THEN TOT_REJECT_REAS_1 END) AS val_all_oth,

    SUM(CASE WHEN other_region THEN TOT_REJECT_REAS_1 END) AS val_other_region,

    SUM(CASE WHEN type_common THEN TOT_REJECT_REAS_1 END) AS val_common_all,
    SUM(CASE WHEN type_common AND source_gov THEN TOT_REJECT_REAS_1 END) AS val_common_gov,
    SUM(CASE WHEN type_common AND source_com THEN TOT_REJECT_REAS_1 END) AS val_common_com,
    SUM(CASE WHEN type_common AND source_oth THEN TOT_REJECT_REAS_1 END) AS val_common_oth,

    SUM(CASE WHEN type_advance THEN TOT_REJECT_REAS_1 END) AS val_advance_all,
    SUM(CASE WHEN type_advance AND source_gov THEN TOT_REJECT_REAS_1 END) AS val_advance_gov,
    SUM(CASE WHEN type_advance AND source_com THEN TOT_REJECT_REAS_1 END) AS val_advance_com,
    SUM(CASE WHEN type_advance AND source_oth THEN TOT_REJECT_REAS_1 END) AS val_advance_oth,

    SUM(CASE WHEN type_other THEN TOT_REJECT_REAS_1 END) AS val_other_all,
    SUM(CASE WHEN type_other AND source_gov THEN TOT_REJECT_REAS_1 END) AS val_other_gov,
    SUM(CASE WHEN type_other AND source_com THEN TOT_REJECT_REAS_1 END) AS val_other_com,
    SUM(CASE WHEN type_other AND source_oth THEN TOT_REJECT_REAS_1 END) AS val_other_oth,

    SUM(CASE WHEN type_sds THEN TOT_REJECT_REAS_1 END) AS val_sds_all,
    SUM(CASE WHEN type_sds AND source_gov THEN TOT_REJECT_REAS_1 END) AS val_sds_gov,
    SUM(CASE WHEN type_sds AND source_com THEN TOT_REJECT_REAS_1 END) AS val_sds_com,
    SUM(CASE WHEN type_sds AND source_oth THEN TOT_REJECT_REAS_1 END) AS val_sds_oth,

    null AS assays_ids

FROM ass WHERE normal_is AND app_is AND spec_rsc

UNION

SELECT
    646 AS id,
    '    Отсутствие предусмотренных изолирующих устройств и (или) разделительной полосы' AS title,
    '' AS description,
    2 AS level,
    'normal' AS type,
    'double' AS val_type,

    SUM(TOT_REJECT_REAS_2) AS val_all_all,
    SUM(CASE WHEN source_gov THEN TOT_REJECT_REAS_2 END) AS val_all_gov,
    SUM(CASE WHEN source_com THEN TOT_REJECT_REAS_2 END) AS val_all_com,
    SUM(CASE WHEN source_oth THEN TOT_REJECT_REAS_2 END) AS val_all_oth,

    SUM(CASE WHEN other_region THEN TOT_REJECT_REAS_2 END) AS val_other_region,

    SUM(CASE WHEN type_common THEN TOT_REJECT_REAS_2 END) AS val_common_all,
    SUM(CASE WHEN type_common AND source_gov THEN TOT_REJECT_REAS_2 END) AS val_common_gov,
    SUM(CASE WHEN type_common AND source_com THEN TOT_REJECT_REAS_2 END) AS val_common_com,
    SUM(CASE WHEN type_common AND source_oth THEN TOT_REJECT_REAS_2 END) AS val_common_oth,

    SUM(CASE WHEN type_advance THEN TOT_REJECT_REAS_2 END) AS val_advance_all,
    SUM(CASE WHEN type_advance AND source_gov THEN TOT_REJECT_REAS_2 END) AS val_advance_gov,
    SUM(CASE WHEN type_advance AND source_com THEN TOT_REJECT_REAS_2 END) AS val_advance_com,
    SUM(CASE WHEN type_advance AND source_oth THEN TOT_REJECT_REAS_2 END) AS val_advance_oth,

    SUM(CASE WHEN type_other THEN TOT_REJECT_REAS_2 END) AS val_other_all,
    SUM(CASE WHEN type_other AND source_gov THEN TOT_REJECT_REAS_2 END) AS val_other_gov,
    SUM(CASE WHEN type_other AND source_com THEN TOT_REJECT_REAS_2 END) AS val_other_com,
    SUM(CASE WHEN type_other AND source_oth THEN TOT_REJECT_REAS_2 END) AS val_other_oth,

    SUM(CASE WHEN type_sds THEN TOT_REJECT_REAS_2 END) AS val_sds_all,
    SUM(CASE WHEN type_sds AND source_gov THEN TOT_REJECT_REAS_2 END) AS val_sds_gov,
    SUM(CASE WHEN type_sds AND source_com THEN TOT_REJECT_REAS_2 END) AS val_sds_com,
    SUM(CASE WHEN type_sds AND source_oth THEN TOT_REJECT_REAS_2 END) AS val_sds_oth,

    null AS assays_ids

FROM ass WHERE normal_is AND app_is AND spec_rsc

UNION

SELECT
    647 AS id,
    '    Не соответствие посевов требованиям к сортовой чистоте' AS title,
    '' AS description,
    2 AS level,
    'normal' AS type,
    'double' AS val_type,

    SUM(TOT_REJECT_REAS_3) AS val_all_all,
    SUM(CASE WHEN source_gov THEN TOT_REJECT_REAS_3 END) AS val_all_gov,
    SUM(CASE WHEN source_com THEN TOT_REJECT_REAS_3 END) AS val_all_com,
    SUM(CASE WHEN source_oth THEN TOT_REJECT_REAS_3 END) AS val_all_oth,

    SUM(CASE WHEN other_region THEN TOT_REJECT_REAS_3 END) AS val_other_region,

    SUM(CASE WHEN type_common THEN TOT_REJECT_REAS_3 END) AS val_common_all,
    SUM(CASE WHEN type_common AND source_gov THEN TOT_REJECT_REAS_3 END) AS val_common_gov,
    SUM(CASE WHEN type_common AND source_com THEN TOT_REJECT_REAS_3 END) AS val_common_com,
    SUM(CASE WHEN type_common AND source_oth THEN TOT_REJECT_REAS_3 END) AS val_common_oth,

    SUM(CASE WHEN type_advance THEN TOT_REJECT_REAS_3 END) AS val_advance_all,
    SUM(CASE WHEN type_advance AND source_gov THEN TOT_REJECT_REAS_3 END) AS val_advance_gov,
    SUM(CASE WHEN type_advance AND source_com THEN TOT_REJECT_REAS_3 END) AS val_advance_com,
    SUM(CASE WHEN type_advance AND source_oth THEN TOT_REJECT_REAS_3 END) AS val_advance_oth,

    SUM(CASE WHEN type_other THEN TOT_REJECT_REAS_3 END) AS val_other_all,
    SUM(CASE WHEN type_other AND source_gov THEN TOT_REJECT_REAS_3 END) AS val_other_gov,
    SUM(CASE WHEN type_other AND source_com THEN TOT_REJECT_REAS_3 END) AS val_other_com,
    SUM(CASE WHEN type_other AND source_oth THEN TOT_REJECT_REAS_3 END) AS val_other_oth,

    SUM(CASE WHEN type_sds THEN TOT_REJECT_REAS_3 END) AS val_sds_all,
    SUM(CASE WHEN type_sds AND source_gov THEN TOT_REJECT_REAS_3 END) AS val_sds_gov,
    SUM(CASE WHEN type_sds AND source_com THEN TOT_REJECT_REAS_3 END) AS val_sds_com,
    SUM(CASE WHEN type_sds AND source_oth THEN TOT_REJECT_REAS_3 END) AS val_sds_oth,

    null AS assays_ids

FROM ass WHERE normal_is AND app_is AND spec_rsc

UNION

SELECT
    648 AS id,
    '    Обнаружены в посевах карантинные объекты и (или) ядовитые сорные растения' AS title,
    '' AS description,
    2 AS level,
    'normal' AS type,
    'double' AS val_type,

    SUM(TOT_REJECT_REAS_4) AS val_all_all,
    SUM(CASE WHEN source_gov THEN TOT_REJECT_REAS_4 END) AS val_all_gov,
    SUM(CASE WHEN source_com THEN TOT_REJECT_REAS_4 END) AS val_all_com,
    SUM(CASE WHEN source_oth THEN TOT_REJECT_REAS_4 END) AS val_all_oth,

    SUM(CASE WHEN other_region THEN TOT_REJECT_REAS_4 END) AS val_other_region,

    SUM(CASE WHEN type_common THEN TOT_REJECT_REAS_4 END) AS val_common_all,
    SUM(CASE WHEN type_common AND source_gov THEN TOT_REJECT_REAS_4 END) AS val_common_gov,
    SUM(CASE WHEN type_common AND source_com THEN TOT_REJECT_REAS_4 END) AS val_common_com,
    SUM(CASE WHEN type_common AND source_oth THEN TOT_REJECT_REAS_4 END) AS val_common_oth,

    SUM(CASE WHEN type_advance THEN TOT_REJECT_REAS_4 END) AS val_advance_all,
    SUM(CASE WHEN type_advance AND source_gov THEN TOT_REJECT_REAS_4 END) AS val_advance_gov,
    SUM(CASE WHEN type_advance AND source_com THEN TOT_REJECT_REAS_4 END) AS val_advance_com,
    SUM(CASE WHEN type_advance AND source_oth THEN TOT_REJECT_REAS_4 END) AS val_advance_oth,

    SUM(CASE WHEN type_other THEN TOT_REJECT_REAS_4 END) AS val_other_all,
    SUM(CASE WHEN type_other AND source_gov THEN TOT_REJECT_REAS_4 END) AS val_other_gov,
    SUM(CASE WHEN type_other AND source_com THEN TOT_REJECT_REAS_4 END) AS val_other_com,
    SUM(CASE WHEN type_other AND source_oth THEN TOT_REJECT_REAS_4 END) AS val_other_oth,

    SUM(CASE WHEN type_sds THEN TOT_REJECT_REAS_4 END) AS val_sds_all,
    SUM(CASE WHEN type_sds AND source_gov THEN TOT_REJECT_REAS_4 END) AS val_sds_gov,
    SUM(CASE WHEN type_sds AND source_com THEN TOT_REJECT_REAS_4 END) AS val_sds_com,
    SUM(CASE WHEN type_sds AND source_oth THEN TOT_REJECT_REAS_4 END) AS val_sds_oth,

    null AS assays_ids

FROM ass WHERE normal_is AND app_is AND spec_rsc

UNION

SELECT
    649 AS id,
    '    Превышены нормы засорённости трудноотделимыми сорными растениями' AS title,
    '' AS description,
    2 AS level,
    'normal' AS type,
    'double' AS val_type,

    SUM(TOT_REJECT_REAS_5) AS val_all_all,
    SUM(CASE WHEN source_gov THEN TOT_REJECT_REAS_5 END) AS val_all_gov,
    SUM(CASE WHEN source_com THEN TOT_REJECT_REAS_5 END) AS val_all_com,
    SUM(CASE WHEN source_oth THEN TOT_REJECT_REAS_5 END) AS val_all_oth,

    SUM(CASE WHEN other_region THEN TOT_REJECT_REAS_5 END) AS val_other_region,

    SUM(CASE WHEN type_common THEN TOT_REJECT_REAS_5 END) AS val_common_all,
    SUM(CASE WHEN type_common AND source_gov THEN TOT_REJECT_REAS_5 END) AS val_common_gov,
    SUM(CASE WHEN type_common AND source_com THEN TOT_REJECT_REAS_5 END) AS val_common_com,
    SUM(CASE WHEN type_common AND source_oth THEN TOT_REJECT_REAS_5 END) AS val_common_oth,

    SUM(CASE WHEN type_advance THEN TOT_REJECT_REAS_5 END) AS val_advance_all,
    SUM(CASE WHEN type_advance AND source_gov THEN TOT_REJECT_REAS_5 END) AS val_advance_gov,
    SUM(CASE WHEN type_advance AND source_com THEN TOT_REJECT_REAS_5 END) AS val_advance_com,
    SUM(CASE WHEN type_advance AND source_oth THEN TOT_REJECT_REAS_5 END) AS val_advance_oth,

    SUM(CASE WHEN type_other THEN TOT_REJECT_REAS_5 END) AS val_other_all,
    SUM(CASE WHEN type_other AND source_gov THEN TOT_REJECT_REAS_5 END) AS val_other_gov,
    SUM(CASE WHEN type_other AND source_com THEN TOT_REJECT_REAS_5 END) AS val_other_com,
    SUM(CASE WHEN type_other AND source_oth THEN TOT_REJECT_REAS_5 END) AS val_other_oth,

    SUM(CASE WHEN type_sds THEN TOT_REJECT_REAS_5 END) AS val_sds_all,
    SUM(CASE WHEN type_sds AND source_gov THEN TOT_REJECT_REAS_5 END) AS val_sds_gov,
    SUM(CASE WHEN type_sds AND source_com THEN TOT_REJECT_REAS_5 END) AS val_sds_com,
    SUM(CASE WHEN type_sds AND source_oth THEN TOT_REJECT_REAS_5 END) AS val_sds_oth,

    null AS assays_ids

FROM ass WHERE normal_is AND app_is AND spec_rsc

UNION

SELECT
    650 AS id,
    '    Превышены нормы засоренности трудноотделимыми сельскохозяйственными растениями' AS title,
    '' AS description,
    2 AS level,
    'normal' AS type,
    'double' AS val_type,

    SUM(TOT_REJECT_REAS_6) AS val_all_all,
    SUM(CASE WHEN source_gov THEN TOT_REJECT_REAS_6 END) AS val_all_gov,
    SUM(CASE WHEN source_com THEN TOT_REJECT_REAS_6 END) AS val_all_com,
    SUM(CASE WHEN source_oth THEN TOT_REJECT_REAS_6 END) AS val_all_oth,

    SUM(CASE WHEN other_region THEN TOT_REJECT_REAS_6 END) AS val_other_region,

    SUM(CASE WHEN type_common THEN TOT_REJECT_REAS_6 END) AS val_common_all,
    SUM(CASE WHEN type_common AND source_gov THEN TOT_REJECT_REAS_6 END) AS val_common_gov,
    SUM(CASE WHEN type_common AND source_com THEN TOT_REJECT_REAS_6 END) AS val_common_com,
    SUM(CASE WHEN type_common AND source_oth THEN TOT_REJECT_REAS_6 END) AS val_common_oth,

    SUM(CASE WHEN type_advance THEN TOT_REJECT_REAS_6 END) AS val_advance_all,
    SUM(CASE WHEN type_advance AND source_gov THEN TOT_REJECT_REAS_6 END) AS val_advance_gov,
    SUM(CASE WHEN type_advance AND source_com THEN TOT_REJECT_REAS_6 END) AS val_advance_com,
    SUM(CASE WHEN type_advance AND source_oth THEN TOT_REJECT_REAS_6 END) AS val_advance_oth,

    SUM(CASE WHEN type_other THEN TOT_REJECT_REAS_6 END) AS val_other_all,
    SUM(CASE WHEN type_other AND source_gov THEN TOT_REJECT_REAS_6 END) AS val_other_gov,
    SUM(CASE WHEN type_other AND source_com THEN TOT_REJECT_REAS_6 END) AS val_other_com,
    SUM(CASE WHEN type_other AND source_oth THEN TOT_REJECT_REAS_6 END) AS val_other_oth,

    SUM(CASE WHEN type_sds THEN TOT_REJECT_REAS_6 END) AS val_sds_all,
    SUM(CASE WHEN type_sds AND source_gov THEN TOT_REJECT_REAS_6 END) AS val_sds_gov,
    SUM(CASE WHEN type_sds AND source_com THEN TOT_REJECT_REAS_6 END) AS val_sds_com,
    SUM(CASE WHEN type_sds AND source_oth THEN TOT_REJECT_REAS_6 END) AS val_sds_oth,

    null AS assays_ids

FROM ass WHERE normal_is AND app_is AND spec_rsc

UNION

SELECT
    651 AS id,
    '    Превышение нормы поражения болезнями' AS title,
    '' AS description,
    2 AS level,
    'normal' AS type,
    'double' AS val_type,

    SUM(TOT_REJECT_REAS_7) AS val_all_all,
    SUM(CASE WHEN source_gov THEN TOT_REJECT_REAS_7 END) AS val_all_gov,
    SUM(CASE WHEN source_com THEN TOT_REJECT_REAS_7 END) AS val_all_com,
    SUM(CASE WHEN source_oth THEN TOT_REJECT_REAS_7 END) AS val_all_oth,

    SUM(CASE WHEN other_region THEN TOT_REJECT_REAS_7 END) AS val_other_region,

    SUM(CASE WHEN type_common THEN TOT_REJECT_REAS_7 END) AS val_common_all,
    SUM(CASE WHEN type_common AND source_gov THEN TOT_REJECT_REAS_7 END) AS val_common_gov,
    SUM(CASE WHEN type_common AND source_com THEN TOT_REJECT_REAS_7 END) AS val_common_com,
    SUM(CASE WHEN type_common AND source_oth THEN TOT_REJECT_REAS_7 END) AS val_common_oth,

    SUM(CASE WHEN type_advance THEN TOT_REJECT_REAS_7 END) AS val_advance_all,
    SUM(CASE WHEN type_advance AND source_gov THEN TOT_REJECT_REAS_7 END) AS val_advance_gov,
    SUM(CASE WHEN type_advance AND source_com THEN TOT_REJECT_REAS_7 END) AS val_advance_com,
    SUM(CASE WHEN type_advance AND source_oth THEN TOT_REJECT_REAS_7 END) AS val_advance_oth,

    SUM(CASE WHEN type_other THEN TOT_REJECT_REAS_7 END) AS val_other_all,
    SUM(CASE WHEN type_other AND source_gov THEN TOT_REJECT_REAS_7 END) AS val_other_gov,
    SUM(CASE WHEN type_other AND source_com THEN TOT_REJECT_REAS_7 END) AS val_other_com,
    SUM(CASE WHEN type_other AND source_oth THEN TOT_REJECT_REAS_7 END) AS val_other_oth,

    SUM(CASE WHEN type_sds THEN TOT_REJECT_REAS_7 END) AS val_sds_all,
    SUM(CASE WHEN type_sds AND source_gov THEN TOT_REJECT_REAS_7 END) AS val_sds_gov,
    SUM(CASE WHEN type_sds AND source_com THEN TOT_REJECT_REAS_7 END) AS val_sds_com,
    SUM(CASE WHEN type_sds AND source_oth THEN TOT_REJECT_REAS_7 END) AS val_sds_oth,

    null AS assays_ids

FROM ass WHERE normal_is AND app_is AND spec_rsc

UNION

SELECT
    652 AS id,
    '    Не выполнение или неполное выполнение выданных Заявителю рекомендаций по улучшению состояния сортовых посевов (посадок)' AS title,
    '' AS description,
    2 AS level,
    'normal' AS type,
    'double' AS val_type,

    SUM(TOT_REJECT_REAS_8) AS val_all_all,
    SUM(CASE WHEN source_gov THEN TOT_REJECT_REAS_8 END) AS val_all_gov,
    SUM(CASE WHEN source_com THEN TOT_REJECT_REAS_8 END) AS val_all_com,
    SUM(CASE WHEN source_oth THEN TOT_REJECT_REAS_8 END) AS val_all_oth,

    SUM(CASE WHEN other_region THEN TOT_REJECT_REAS_8 END) AS val_other_region,

    SUM(CASE WHEN type_common THEN TOT_REJECT_REAS_8 END) AS val_common_all,
    SUM(CASE WHEN type_common AND source_gov THEN TOT_REJECT_REAS_8 END) AS val_common_gov,
    SUM(CASE WHEN type_common AND source_com THEN TOT_REJECT_REAS_8 END) AS val_common_com,
    SUM(CASE WHEN type_common AND source_oth THEN TOT_REJECT_REAS_8 END) AS val_common_oth,

    SUM(CASE WHEN type_advance THEN TOT_REJECT_REAS_8 END) AS val_advance_all,
    SUM(CASE WHEN type_advance AND source_gov THEN TOT_REJECT_REAS_8 END) AS val_advance_gov,
    SUM(CASE WHEN type_advance AND source_com THEN TOT_REJECT_REAS_8 END) AS val_advance_com,
    SUM(CASE WHEN type_advance AND source_oth THEN TOT_REJECT_REAS_8 END) AS val_advance_oth,

    SUM(CASE WHEN type_other THEN TOT_REJECT_REAS_8 END) AS val_other_all,
    SUM(CASE WHEN type_other AND source_gov THEN TOT_REJECT_REAS_8 END) AS val_other_gov,
    SUM(CASE WHEN type_other AND source_com THEN TOT_REJECT_REAS_8 END) AS val_other_com,
    SUM(CASE WHEN type_other AND source_oth THEN TOT_REJECT_REAS_8 END) AS val_other_oth,

    SUM(CASE WHEN type_sds THEN TOT_REJECT_REAS_8 END) AS val_sds_all,
    SUM(CASE WHEN type_sds AND source_gov THEN TOT_REJECT_REAS_8 END) AS val_sds_gov,
    SUM(CASE WHEN type_sds AND source_com THEN TOT_REJECT_REAS_8 END) AS val_sds_com,
    SUM(CASE WHEN type_sds AND source_oth THEN TOT_REJECT_REAS_8 END) AS val_sds_oth,

    null AS assays_ids

FROM ass WHERE normal_is AND app_is AND spec_rsc

UNION

SELECT
    653 AS id,
    '    Не соблюдение схемы производства гибридных семян' AS title,
    '' AS description,
    2 AS level,
    'normal' AS type,
    'double' AS val_type,

    SUM(TOT_REJECT_REAS_9) AS val_all_all,
    SUM(CASE WHEN source_gov THEN TOT_REJECT_REAS_9 END) AS val_all_gov,
    SUM(CASE WHEN source_com THEN TOT_REJECT_REAS_9 END) AS val_all_com,
    SUM(CASE WHEN source_oth THEN TOT_REJECT_REAS_9 END) AS val_all_oth,

    SUM(CASE WHEN other_region THEN TOT_REJECT_REAS_9 END) AS val_other_region,

    SUM(CASE WHEN type_common THEN TOT_REJECT_REAS_9 END) AS val_common_all,
    SUM(CASE WHEN type_common AND source_gov THEN TOT_REJECT_REAS_9 END) AS val_common_gov,
    SUM(CASE WHEN type_common AND source_com THEN TOT_REJECT_REAS_9 END) AS val_common_com,
    SUM(CASE WHEN type_common AND source_oth THEN TOT_REJECT_REAS_9 END) AS val_common_oth,

    SUM(CASE WHEN type_advance THEN TOT_REJECT_REAS_9 END) AS val_advance_all,
    SUM(CASE WHEN type_advance AND source_gov THEN TOT_REJECT_REAS_9 END) AS val_advance_gov,
    SUM(CASE WHEN type_advance AND source_com THEN TOT_REJECT_REAS_9 END) AS val_advance_com,
    SUM(CASE WHEN type_advance AND source_oth THEN TOT_REJECT_REAS_9 END) AS val_advance_oth,

    SUM(CASE WHEN type_other THEN TOT_REJECT_REAS_9 END) AS val_other_all,
    SUM(CASE WHEN type_other AND source_gov THEN TOT_REJECT_REAS_9 END) AS val_other_gov,
    SUM(CASE WHEN type_other AND source_com THEN TOT_REJECT_REAS_9 END) AS val_other_com,
    SUM(CASE WHEN type_other AND source_oth THEN TOT_REJECT_REAS_9 END) AS val_other_oth,

    SUM(CASE WHEN type_sds THEN TOT_REJECT_REAS_9 END) AS val_sds_all,
    SUM(CASE WHEN type_sds AND source_gov THEN TOT_REJECT_REAS_9 END) AS val_sds_gov,
    SUM(CASE WHEN type_sds AND source_com THEN TOT_REJECT_REAS_9 END) AS val_sds_com,
    SUM(CASE WHEN type_sds AND source_oth THEN TOT_REJECT_REAS_9 END) AS val_sds_oth,

    null AS assays_ids

FROM ass WHERE normal_is AND app_is AND spec_rsc

UNION

SELECT
    654 AS id,
    '    По причине отсутствия лицензионного договора посев перевести в несортовой' AS title,
    '' AS description,
    2 AS level,
    'normal' AS type,
    'double' AS val_type,

    SUM(TOT_REJECT_REAS_10) AS val_all_all,
    SUM(CASE WHEN source_gov THEN TOT_REJECT_REAS_10 END) AS val_all_gov,
    SUM(CASE WHEN source_com THEN TOT_REJECT_REAS_10 END) AS val_all_com,
    SUM(CASE WHEN source_oth THEN TOT_REJECT_REAS_10 END) AS val_all_oth,

    SUM(CASE WHEN other_region THEN TOT_REJECT_REAS_10 END) AS val_other_region,

    SUM(CASE WHEN type_common THEN TOT_REJECT_REAS_10 END) AS val_common_all,
    SUM(CASE WHEN type_common AND source_gov THEN TOT_REJECT_REAS_10 END) AS val_common_gov,
    SUM(CASE WHEN type_common AND source_com THEN TOT_REJECT_REAS_10 END) AS val_common_com,
    SUM(CASE WHEN type_common AND source_oth THEN TOT_REJECT_REAS_10 END) AS val_common_oth,

    SUM(CASE WHEN type_advance THEN TOT_REJECT_REAS_10 END) AS val_advance_all,
    SUM(CASE WHEN type_advance AND source_gov THEN TOT_REJECT_REAS_10 END) AS val_advance_gov,
    SUM(CASE WHEN type_advance AND source_com THEN TOT_REJECT_REAS_10 END) AS val_advance_com,
    SUM(CASE WHEN type_advance AND source_oth THEN TOT_REJECT_REAS_10 END) AS val_advance_oth,

    SUM(CASE WHEN type_other THEN TOT_REJECT_REAS_10 END) AS val_other_all,
    SUM(CASE WHEN type_other AND source_gov THEN TOT_REJECT_REAS_10 END) AS val_other_gov,
    SUM(CASE WHEN type_other AND source_com THEN TOT_REJECT_REAS_10 END) AS val_other_com,
    SUM(CASE WHEN type_other AND source_oth THEN TOT_REJECT_REAS_10 END) AS val_other_oth,

    SUM(CASE WHEN type_sds THEN TOT_REJECT_REAS_10 END) AS val_sds_all,
    SUM(CASE WHEN type_sds AND source_gov THEN TOT_REJECT_REAS_10 END) AS val_sds_gov,
    SUM(CASE WHEN type_sds AND source_com THEN TOT_REJECT_REAS_10 END) AS val_sds_com,
    SUM(CASE WHEN type_sds AND source_oth THEN TOT_REJECT_REAS_10 END) AS val_sds_oth,

    null AS assays_ids

FROM ass WHERE normal_is AND app_is AND spec_rsc

UNION

SELECT
    658 AS id,
    '    другие причины' AS title,
    '' AS description,
    2 AS level,
    'normal' AS type,
    'double' AS val_type,

    SUM(TOT_REJECT_REAS_OTHER) AS val_all_all,
    SUM(CASE WHEN source_gov THEN TOT_REJECT_REAS_OTHER END) AS val_all_gov,
    SUM(CASE WHEN source_com THEN TOT_REJECT_REAS_OTHER END) AS val_all_com,
    SUM(CASE WHEN source_oth THEN TOT_REJECT_REAS_OTHER END) AS val_all_oth,

    SUM(CASE WHEN other_region THEN TOT_REJECT_REAS_OTHER END) AS val_other_region,

    SUM(CASE WHEN type_common THEN TOT_REJECT_REAS_OTHER END) AS val_common_all,
    SUM(CASE WHEN type_common AND source_gov THEN TOT_REJECT_REAS_OTHER END) AS val_common_gov,
    SUM(CASE WHEN type_common AND source_com THEN TOT_REJECT_REAS_OTHER END) AS val_common_com,
    SUM(CASE WHEN type_common AND source_oth THEN TOT_REJECT_REAS_OTHER END) AS val_common_oth,

    SUM(CASE WHEN type_advance THEN TOT_REJECT_REAS_OTHER END) AS val_advance_all,
    SUM(CASE WHEN type_advance AND source_gov THEN TOT_REJECT_REAS_OTHER END) AS val_advance_gov,
    SUM(CASE WHEN type_advance AND source_com THEN TOT_REJECT_REAS_OTHER END) AS val_advance_com,
    SUM(CASE WHEN type_advance AND source_oth THEN TOT_REJECT_REAS_OTHER END) AS val_advance_oth,

    SUM(CASE WHEN type_other THEN TOT_REJECT_REAS_OTHER END) AS val_other_all,
    SUM(CASE WHEN type_other AND source_gov THEN TOT_REJECT_REAS_OTHER END) AS val_other_gov,
    SUM(CASE WHEN type_other AND source_com THEN TOT_REJECT_REAS_OTHER END) AS val_other_com,
    SUM(CASE WHEN type_other AND source_oth THEN TOT_REJECT_REAS_OTHER END) AS val_other_oth,

    SUM(CASE WHEN type_sds THEN TOT_REJECT_REAS_OTHER END) AS val_sds_all,
    SUM(CASE WHEN type_sds AND source_gov THEN TOT_REJECT_REAS_OTHER END) AS val_sds_gov,
    SUM(CASE WHEN type_sds AND source_com THEN TOT_REJECT_REAS_OTHER END) AS val_sds_com,
    SUM(CASE WHEN type_sds AND source_oth THEN TOT_REJECT_REAS_OTHER END) AS val_sds_oth,

    null AS assays_ids

FROM ass WHERE normal_is AND app_is AND spec_rsc

UNION

SELECT
    680 AS id,
    '  из них: По ферм. льготе (не предоставлен лицензионный договор), тыс.га' AS title,
    '' AS description,
    1 AS level,
    'normal' AS type,
    'double' AS val_type,
    SUM(TOT_sum_all) AS val_all_all,
    SUM(CASE WHEN source_gov THEN TOT_sum_all END) AS val_all_gov,
    SUM(CASE WHEN source_com THEN TOT_sum_all END) AS val_all_com,
    SUM(CASE WHEN source_oth THEN TOT_sum_all END) AS val_all_oth,

    SUM(CASE WHEN other_region THEN TOT_sum_all END) AS val_other_region,

    SUM(CASE WHEN type_common THEN TOT_sum_all END) AS val_common_all,
    SUM(CASE WHEN type_common AND source_gov THEN TOT_sum_all END) AS val_common_gov,
    SUM(CASE WHEN type_common AND source_com THEN TOT_sum_all END) AS val_common_com,
    SUM(CASE WHEN type_common AND source_oth THEN TOT_sum_all END) AS val_common_oth,

    SUM(CASE WHEN type_advance THEN TOT_sum_all END) AS val_advance_all,
    SUM(CASE WHEN type_advance AND source_gov THEN TOT_sum_all END) AS val_advance_gov,
    SUM(CASE WHEN type_advance AND source_com THEN TOT_sum_all END) AS val_advance_com,
    SUM(CASE WHEN type_advance AND source_oth THEN TOT_sum_all END) AS val_advance_oth,

    SUM(CASE WHEN type_other THEN TOT_sum_all END) AS val_other_all,
    SUM(CASE WHEN type_other AND source_gov THEN TOT_sum_all END) AS val_other_gov,
    SUM(CASE WHEN type_other AND source_com THEN TOT_sum_all END) AS val_other_com,
    SUM(CASE WHEN type_other AND source_oth THEN TOT_sum_all END) AS val_other_oth,

    SUM(CASE WHEN type_sds THEN TOT_sum_all END) AS val_sds_all,
    SUM(CASE WHEN type_sds AND source_gov THEN TOT_sum_all END) AS val_sds_gov,
    SUM(CASE WHEN type_sds AND source_com THEN TOT_sum_all END) AS val_sds_com,
    SUM(CASE WHEN type_sds AND source_oth THEN TOT_sum_all END) AS val_sds_oth,

    null AS assays_ids

FROM ass WHERE normal_is AND app_is AND spec_rsc AND STATUS_NO_LIC_AGR_FLAG_643_is

UNION

SELECT
    681 AS id,
    '  из них: Погибло семенных посевов, тыс.га' AS title,
    '' AS description,
    1 AS level,
    'normal' AS type,
    'double' AS val_type,
    SUM(TOT_sum_all) AS val_all_all,
    SUM(CASE WHEN source_gov THEN TOT_sum_all END) AS val_all_gov,
    SUM(CASE WHEN source_com THEN TOT_sum_all END) AS val_all_com,
    SUM(CASE WHEN source_oth THEN TOT_sum_all END) AS val_all_oth,

    SUM(CASE WHEN other_region THEN TOT_sum_all END) AS val_other_region,

    SUM(CASE WHEN type_common THEN TOT_sum_all END) AS val_common_all,
    SUM(CASE WHEN type_common AND source_gov THEN TOT_sum_all END) AS val_common_gov,
    SUM(CASE WHEN type_common AND source_com THEN TOT_sum_all END) AS val_common_com,
    SUM(CASE WHEN type_common AND source_oth THEN TOT_sum_all END) AS val_common_oth,

    SUM(CASE WHEN type_advance THEN TOT_sum_all END) AS val_advance_all,
    SUM(CASE WHEN type_advance AND source_gov THEN TOT_sum_all END) AS val_advance_gov,
    SUM(CASE WHEN type_advance AND source_com THEN TOT_sum_all END) AS val_advance_com,
    SUM(CASE WHEN type_advance AND source_oth THEN TOT_sum_all END) AS val_advance_oth,

    SUM(CASE WHEN type_other THEN TOT_sum_all END) AS val_other_all,
    SUM(CASE WHEN type_other AND source_gov THEN TOT_sum_all END) AS val_other_gov,
    SUM(CASE WHEN type_other AND source_com THEN TOT_sum_all END) AS val_other_com,
    SUM(CASE WHEN type_other AND source_oth THEN TOT_sum_all END) AS val_other_oth,

    SUM(CASE WHEN type_sds THEN TOT_sum_all END) AS val_sds_all,
    SUM(CASE WHEN type_sds AND source_gov THEN TOT_sum_all END) AS val_sds_gov,
    SUM(CASE WHEN type_sds AND source_com THEN TOT_sum_all END) AS val_sds_com,
    SUM(CASE WHEN type_sds AND source_oth THEN TOT_sum_all END) AS val_sds_oth,

    null AS assays_ids

FROM ass WHERE normal_is AND app_is AND spec_rsc AND STATUS_AFTER_APP_DEATH_is

UNION

SELECT
    682 AS id,
    '  из них: Выведено из семенного баланса, тыс.га' AS title,
    '' AS description,
    1 AS level,
    'normal' AS type,
    'double' AS val_type,
    SUM(TOT_sum_all) AS val_all_all,
    SUM(CASE WHEN source_gov THEN TOT_sum_all END) AS val_all_gov,
    SUM(CASE WHEN source_com THEN TOT_sum_all END) AS val_all_com,
    SUM(CASE WHEN source_oth THEN TOT_sum_all END) AS val_all_oth,

    SUM(CASE WHEN other_region THEN TOT_sum_all END) AS val_other_region,

    SUM(CASE WHEN type_common THEN TOT_sum_all END) AS val_common_all,
    SUM(CASE WHEN type_common AND source_gov THEN TOT_sum_all END) AS val_common_gov,
    SUM(CASE WHEN type_common AND source_com THEN TOT_sum_all END) AS val_common_com,
    SUM(CASE WHEN type_common AND source_oth THEN TOT_sum_all END) AS val_common_oth,

    SUM(CASE WHEN type_advance THEN TOT_sum_all END) AS val_advance_all,
    SUM(CASE WHEN type_advance AND source_gov THEN TOT_sum_all END) AS val_advance_gov,
    SUM(CASE WHEN type_advance AND source_com THEN TOT_sum_all END) AS val_advance_com,
    SUM(CASE WHEN type_advance AND source_oth THEN TOT_sum_all END) AS val_advance_oth,

    SUM(CASE WHEN type_other THEN TOT_sum_all END) AS val_other_all,
    SUM(CASE WHEN type_other AND source_gov THEN TOT_sum_all END) AS val_other_gov,
    SUM(CASE WHEN type_other AND source_com THEN TOT_sum_all END) AS val_other_com,
    SUM(CASE WHEN type_other AND source_oth THEN TOT_sum_all END) AS val_other_oth,

    SUM(CASE WHEN type_sds THEN TOT_sum_all END) AS val_sds_all,
    SUM(CASE WHEN type_sds AND source_gov THEN TOT_sum_all END) AS val_sds_gov,
    SUM(CASE WHEN type_sds AND source_com THEN TOT_sum_all END) AS val_sds_com,
    SUM(CASE WHEN type_sds AND source_oth THEN TOT_sum_all END) AS val_sds_oth,

    null AS assays_ids

FROM ass WHERE normal_is AND app_is AND spec_rsc AND STATUS_AFTER_APP_OUT_SEEDS_is AND NOT STATUS_BEFORE_APP_OUT_SEEDS_is

UNION

SELECT
  1000 AS id,
  'Количество геоточек ИМ репродукций картофеля (получены микро-клубни, мини-клубни)' AS title,
  '' AS description,
  0 AS level,
  'normal' AS type,
  'bigint' AS val_type,

  count(ass.id) AS val_all_all,
  count(CASE WHEN source_gov THEN true END) AS val_all_gov,
  count(CASE WHEN source_com THEN true END) AS val_all_com,
  count(CASE WHEN source_oth THEN true END) AS val_all_oth,

  count(CASE WHEN other_region THEN true END) AS val_other_region,

  count(CASE WHEN type_common THEN true END) AS val_common_all,
  count(CASE WHEN type_common AND source_gov THEN true END) AS val_common_gov,
  count(CASE WHEN type_common AND source_com THEN true END) AS val_common_com,
  count(CASE WHEN type_common AND source_oth THEN true END) AS val_common_oth,

  count(CASE WHEN type_advance THEN true END) AS val_advance_all,
  count(CASE WHEN type_advance AND source_gov THEN true END) AS val_advance_gov,
  count(CASE WHEN type_advance AND source_com THEN true END) AS val_advance_com,
  count(CASE WHEN type_advance AND source_oth THEN true END) AS val_advance_oth,

  count(CASE WHEN type_other THEN true END) AS val_other_all,
  count(CASE WHEN type_other AND source_gov THEN true END) AS val_other_gov,
  count(CASE WHEN type_other AND source_com THEN true END) AS val_other_com,
  count(CASE WHEN type_other AND source_oth THEN true END) AS val_other_oth,

  count(CASE WHEN type_sds THEN true END) AS val_sds_all,
  count(CASE WHEN type_sds AND source_gov THEN true END) AS val_sds_gov,
  count(CASE WHEN type_sds AND source_com THEN true END) AS val_sds_com,
  count(CASE WHEN type_sds AND source_oth THEN true END) AS val_sds_oth,

  null AS assays_ids

FROM ass WHERE potato_is AND im_is AND ass.seedprod_crop_kind = 'SEEDS'

UNION

SELECT
  1010 AS id,
  'Обследовано ИМ, апробировано картофеля (получены микро-клубни, мини-клубни), шт.' AS title,
  '' AS description,
  0 AS level,
  'normal' AS type,
  'bigint' AS val_type,

  SUM(IM_CNT) AS val_all_all,
  SUM(CASE WHEN source_gov THEN IM_CNT END) AS val_all_gov,
  SUM(CASE WHEN source_com THEN IM_CNT END) AS val_all_com,
  SUM(CASE WHEN source_oth THEN IM_CNT END) AS val_all_oth,

  SUM(CASE WHEN other_region THEN IM_CNT END) AS val_other_region,

  SUM(CASE WHEN type_common THEN IM_CNT END) AS val_common_all,
  SUM(CASE WHEN type_common AND source_gov THEN IM_CNT END) AS val_common_gov,
  SUM(CASE WHEN type_common AND source_com THEN IM_CNT END) AS val_common_com,
  SUM(CASE WHEN type_common AND source_oth THEN IM_CNT END) AS val_common_oth,

  SUM(CASE WHEN type_advance THEN IM_CNT END) AS val_advance_all,
  SUM(CASE WHEN type_advance AND source_gov THEN IM_CNT END) AS val_advance_gov,
  SUM(CASE WHEN type_advance AND source_com THEN IM_CNT END) AS val_advance_com,
  SUM(CASE WHEN type_advance AND source_oth THEN IM_CNT END) AS val_advance_oth,

  SUM(CASE WHEN type_other THEN IM_CNT END) AS val_other_all,
  SUM(CASE WHEN type_other AND source_gov THEN IM_CNT END) AS val_other_gov,
  SUM(CASE WHEN type_other AND source_com THEN IM_CNT END) AS val_other_com,
  SUM(CASE WHEN type_other AND source_oth THEN IM_CNT END) AS val_other_oth,

  SUM(CASE WHEN type_sds THEN IM_CNT END) AS val_sds_all,
  SUM(CASE WHEN type_sds AND source_gov THEN IM_CNT END) AS val_sds_gov,
  SUM(CASE WHEN type_sds AND source_com THEN IM_CNT END) AS val_sds_com,
  SUM(CASE WHEN type_sds AND source_oth THEN IM_CNT END) AS val_sds_oth,

  null AS assays_ids

FROM ass WHERE potato_is AND im_is AND app_is AND ass.seedprod_crop_kind = 'SEEDS'

UNION

SELECT
  1020 AS id,
  '  в т.ч. специалистами филиала, шт.' AS title,
  '' AS description,
  1 AS level,
  'normal' AS type,
  'bigint' AS val_type,

  SUM(IM_CNT) AS val_all_all,
  SUM(CASE WHEN source_gov THEN IM_CNT END) AS val_all_gov,
  SUM(CASE WHEN source_com THEN IM_CNT END) AS val_all_com,
  SUM(CASE WHEN source_oth THEN IM_CNT END) AS val_all_oth,

  SUM(CASE WHEN other_region THEN IM_CNT END) AS val_other_region,

  SUM(CASE WHEN type_common THEN IM_CNT END) AS val_common_all,
  SUM(CASE WHEN type_common AND source_gov THEN IM_CNT END) AS val_common_gov,
  SUM(CASE WHEN type_common AND source_com THEN IM_CNT END) AS val_common_com,
  SUM(CASE WHEN type_common AND source_oth THEN IM_CNT END) AS val_common_oth,

  SUM(CASE WHEN type_advance THEN IM_CNT END) AS val_advance_all,
  SUM(CASE WHEN type_advance AND source_gov THEN IM_CNT END) AS val_advance_gov,
  SUM(CASE WHEN type_advance AND source_com THEN IM_CNT END) AS val_advance_com,
  SUM(CASE WHEN type_advance AND source_oth THEN IM_CNT END) AS val_advance_oth,

  SUM(CASE WHEN type_other THEN IM_CNT END) AS val_other_all,
  SUM(CASE WHEN type_other AND source_gov THEN IM_CNT END) AS val_other_gov,
  SUM(CASE WHEN type_other AND source_com THEN IM_CNT END) AS val_other_com,
  SUM(CASE WHEN type_other AND source_oth THEN IM_CNT END) AS val_other_oth,

  SUM(CASE WHEN type_sds THEN IM_CNT END) AS val_sds_all,
  SUM(CASE WHEN type_sds AND source_gov THEN IM_CNT END) AS val_sds_gov,
  SUM(CASE WHEN type_sds AND source_com THEN IM_CNT END) AS val_sds_com,
  SUM(CASE WHEN type_sds AND source_oth THEN IM_CNT END) AS val_sds_oth,

  null AS assays_ids

FROM ass WHERE potato_is AND im_is AND app_is AND ass.seedprod_crop_kind = 'SEEDS' AND spec_rsc

UNION

SELECT
  1030 AS id,
  '  из них: Выбраковано, шт.' AS title,
  '' AS description,
  1 AS level,
  'normal' AS type,
  'bigint' AS val_type,

  SUM(IM_REJECT_ALL_CNT) AS val_all_all,
  SUM(CASE WHEN source_gov THEN IM_REJECT_ALL_CNT END) AS val_all_gov,
  SUM(CASE WHEN source_com THEN IM_REJECT_ALL_CNT END) AS val_all_com,
  SUM(CASE WHEN source_oth THEN IM_REJECT_ALL_CNT END) AS val_all_oth,

  SUM(CASE WHEN other_region THEN IM_REJECT_ALL_CNT END) AS val_other_region,

  SUM(CASE WHEN type_common THEN IM_REJECT_ALL_CNT END) AS val_common_all,
  SUM(CASE WHEN type_common AND source_gov THEN IM_REJECT_ALL_CNT END) AS val_common_gov,
  SUM(CASE WHEN type_common AND source_com THEN IM_REJECT_ALL_CNT END) AS val_common_com,
  SUM(CASE WHEN type_common AND source_oth THEN IM_REJECT_ALL_CNT END) AS val_common_oth,

  SUM(CASE WHEN type_advance THEN IM_REJECT_ALL_CNT END) AS val_advance_all,
  SUM(CASE WHEN type_advance AND source_gov THEN IM_REJECT_ALL_CNT END) AS val_advance_gov,
  SUM(CASE WHEN type_advance AND source_com THEN IM_REJECT_ALL_CNT END) AS val_advance_com,
  SUM(CASE WHEN type_advance AND source_oth THEN IM_REJECT_ALL_CNT END) AS val_advance_oth,

  SUM(CASE WHEN type_other THEN IM_REJECT_ALL_CNT END) AS val_other_all,
  SUM(CASE WHEN type_other AND source_gov THEN IM_REJECT_ALL_CNT END) AS val_other_gov,
  SUM(CASE WHEN type_other AND source_com THEN IM_REJECT_ALL_CNT END) AS val_other_com,
  SUM(CASE WHEN type_other AND source_oth THEN IM_REJECT_ALL_CNT END) AS val_other_oth,

  SUM(CASE WHEN type_sds THEN IM_REJECT_ALL_CNT END) AS val_sds_all,
  SUM(CASE WHEN type_sds AND source_gov THEN IM_REJECT_ALL_CNT END) AS val_sds_gov,
  SUM(CASE WHEN type_sds AND source_com THEN IM_REJECT_ALL_CNT END) AS val_sds_com,
  SUM(CASE WHEN type_sds AND source_oth THEN IM_REJECT_ALL_CNT END) AS val_sds_oth,

  null AS assays_ids

FROM ass WHERE potato_is AND im_is AND app_is AND ass.seedprod_crop_kind = 'SEEDS'



UNION

SELECT
    1034 AS id,
    '  из них: По ферм. льготе (не предоставлен лицензионный договор), шт.' AS title,
    '' AS description,
    1 AS level,
    'normal' AS type,
    'double' AS val_type,
    SUM(IM_CNT) AS val_all_all,
    SUM(CASE WHEN source_gov THEN IM_CNT END) AS val_all_gov,
    SUM(CASE WHEN source_com THEN IM_CNT END) AS val_all_com,
    SUM(CASE WHEN source_oth THEN IM_CNT END) AS val_all_oth,

    SUM(CASE WHEN other_region THEN IM_CNT END) AS val_other_region,

    SUM(CASE WHEN type_common THEN IM_CNT END) AS val_common_all,
    SUM(CASE WHEN type_common AND source_gov THEN IM_CNT END) AS val_common_gov,
    SUM(CASE WHEN type_common AND source_com THEN IM_CNT END) AS val_common_com,
    SUM(CASE WHEN type_common AND source_oth THEN IM_CNT END) AS val_common_oth,

    SUM(CASE WHEN type_advance THEN IM_CNT END) AS val_advance_all,
    SUM(CASE WHEN type_advance AND source_gov THEN IM_CNT END) AS val_advance_gov,
    SUM(CASE WHEN type_advance AND source_com THEN IM_CNT END) AS val_advance_com,
    SUM(CASE WHEN type_advance AND source_oth THEN IM_CNT END) AS val_advance_oth,

    SUM(CASE WHEN type_other THEN IM_CNT END) AS val_other_all,
    SUM(CASE WHEN type_other AND source_gov THEN IM_CNT END) AS val_other_gov,
    SUM(CASE WHEN type_other AND source_com THEN IM_CNT END) AS val_other_com,
    SUM(CASE WHEN type_other AND source_oth THEN IM_CNT END) AS val_other_oth,

    SUM(CASE WHEN type_sds THEN IM_CNT END) AS val_sds_all,
    SUM(CASE WHEN type_sds AND source_gov THEN IM_CNT END) AS val_sds_gov,
    SUM(CASE WHEN type_sds AND source_com THEN IM_CNT END) AS val_sds_com,
    SUM(CASE WHEN type_sds AND source_oth THEN IM_CNT END) AS val_sds_oth,

    null AS assays_ids

FROM ass WHERE potato_is AND im_is AND app_is AND STATUS_NO_LIC_AGR_FLAG_643_is

UNION

SELECT
    1035 AS id,
    '  из них: Погибло семенных посевов, шт.' AS title,
    '' AS description,
    1 AS level,
    'normal' AS type,
    'double' AS val_type,
    SUM(IM_CNT) AS val_all_all,
    SUM(CASE WHEN source_gov THEN IM_CNT END) AS val_all_gov,
    SUM(CASE WHEN source_com THEN IM_CNT END) AS val_all_com,
    SUM(CASE WHEN source_oth THEN IM_CNT END) AS val_all_oth,

    SUM(CASE WHEN other_region THEN IM_CNT END) AS val_other_region,

    SUM(CASE WHEN type_common THEN IM_CNT END) AS val_common_all,
    SUM(CASE WHEN type_common AND source_gov THEN IM_CNT END) AS val_common_gov,
    SUM(CASE WHEN type_common AND source_com THEN IM_CNT END) AS val_common_com,
    SUM(CASE WHEN type_common AND source_oth THEN IM_CNT END) AS val_common_oth,

    SUM(CASE WHEN type_advance THEN IM_CNT END) AS val_advance_all,
    SUM(CASE WHEN type_advance AND source_gov THEN IM_CNT END) AS val_advance_gov,
    SUM(CASE WHEN type_advance AND source_com THEN IM_CNT END) AS val_advance_com,
    SUM(CASE WHEN type_advance AND source_oth THEN IM_CNT END) AS val_advance_oth,

    SUM(CASE WHEN type_other THEN IM_CNT END) AS val_other_all,
    SUM(CASE WHEN type_other AND source_gov THEN IM_CNT END) AS val_other_gov,
    SUM(CASE WHEN type_other AND source_com THEN IM_CNT END) AS val_other_com,
    SUM(CASE WHEN type_other AND source_oth THEN IM_CNT END) AS val_other_oth,

    SUM(CASE WHEN type_sds THEN IM_CNT END) AS val_sds_all,
    SUM(CASE WHEN type_sds AND source_gov THEN IM_CNT END) AS val_sds_gov,
    SUM(CASE WHEN type_sds AND source_com THEN IM_CNT END) AS val_sds_com,
    SUM(CASE WHEN type_sds AND source_oth THEN IM_CNT END) AS val_sds_oth,

    null AS assays_ids

FROM ass WHERE potato_is AND im_is AND app_is AND STATUS_AFTER_APP_DEATH_is

UNION

SELECT
    1036 AS id,
    '  из них: Выведено из семенного баланса, шт.' AS title,
    '' AS description,
    1 AS level,
    'normal' AS type,
    'double' AS val_type,
    SUM(IM_CNT) AS val_all_all,
    SUM(CASE WHEN source_gov THEN IM_CNT END) AS val_all_gov,
    SUM(CASE WHEN source_com THEN IM_CNT END) AS val_all_com,
    SUM(CASE WHEN source_oth THEN IM_CNT END) AS val_all_oth,

    SUM(CASE WHEN other_region THEN IM_CNT END) AS val_other_region,

    SUM(CASE WHEN type_common THEN IM_CNT END) AS val_common_all,
    SUM(CASE WHEN type_common AND source_gov THEN IM_CNT END) AS val_common_gov,
    SUM(CASE WHEN type_common AND source_com THEN IM_CNT END) AS val_common_com,
    SUM(CASE WHEN type_common AND source_oth THEN IM_CNT END) AS val_common_oth,

    SUM(CASE WHEN type_advance THEN IM_CNT END) AS val_advance_all,
    SUM(CASE WHEN type_advance AND source_gov THEN IM_CNT END) AS val_advance_gov,
    SUM(CASE WHEN type_advance AND source_com THEN IM_CNT END) AS val_advance_com,
    SUM(CASE WHEN type_advance AND source_oth THEN IM_CNT END) AS val_advance_oth,

    SUM(CASE WHEN type_other THEN IM_CNT END) AS val_other_all,
    SUM(CASE WHEN type_other AND source_gov THEN IM_CNT END) AS val_other_gov,
    SUM(CASE WHEN type_other AND source_com THEN IM_CNT END) AS val_other_com,
    SUM(CASE WHEN type_other AND source_oth THEN IM_CNT END) AS val_other_oth,

    SUM(CASE WHEN type_sds THEN IM_CNT END) AS val_sds_all,
    SUM(CASE WHEN type_sds AND source_gov THEN IM_CNT END) AS val_sds_gov,
    SUM(CASE WHEN type_sds AND source_com THEN IM_CNT END) AS val_sds_com,
    SUM(CASE WHEN type_sds AND source_oth THEN IM_CNT END) AS val_sds_oth,

    null AS assays_ids

FROM ass WHERE potato_is AND im_is AND app_is AND STATUS_AFTER_APP_OUT_SEEDS_is AND NOT STATUS_BEFORE_APP_OUT_SEEDS_is


UNION

SELECT
  1040 AS id,
  'Количество геоточек обследовано ИМ, апробировано картофеля (получено ПП-1), шт.' AS title,
  '' AS description,
  0 AS level,
  'normal' AS type,
  'bigint' AS val_type,

  count(ass.id) AS val_all_all,
  count(CASE WHEN source_gov THEN true END) AS val_all_gov,
  count(CASE WHEN source_com THEN true END) AS val_all_com,
  count(CASE WHEN source_oth THEN true END) AS val_all_oth,

  count(CASE WHEN other_region THEN true END) AS val_other_region,

  count(CASE WHEN type_common THEN true END) AS val_common_all,
  count(CASE WHEN type_common AND source_gov THEN true END) AS val_common_gov,
  count(CASE WHEN type_common AND source_com THEN true END) AS val_common_com,
  count(CASE WHEN type_common AND source_oth THEN true END) AS val_common_oth,

  count(CASE WHEN type_advance THEN true END) AS val_advance_all,
  count(CASE WHEN type_advance AND source_gov THEN true END) AS val_advance_gov,
  count(CASE WHEN type_advance AND source_com THEN true END) AS val_advance_com,
  count(CASE WHEN type_advance AND source_oth THEN true END) AS val_advance_oth,

  count(CASE WHEN type_other THEN true END) AS val_other_all,
  count(CASE WHEN type_other AND source_gov THEN true END) AS val_other_gov,
  count(CASE WHEN type_other AND source_com THEN true END) AS val_other_com,
  count(CASE WHEN type_other AND source_oth THEN true END) AS val_other_oth,

  count(CASE WHEN type_sds THEN true END) AS val_sds_all,
  count(CASE WHEN type_sds AND source_gov THEN true END) AS val_sds_gov,
  count(CASE WHEN type_sds AND source_com THEN true END) AS val_sds_com,
  count(CASE WHEN type_sds AND source_oth THEN true END) AS val_sds_oth,

  null AS assays_ids

FROM ass WHERE ass.seedprod_crop_kind = 'SEEDS' AND PP1_is AND app_is

UNION

SELECT
  1050 AS id,
  'Обследовано ИМ, апробировано картофеля (получено ПП-1), тыс. га' AS title,
  '' AS description,
  0 AS level,
  'normal' AS type,
  'double' AS val_type,

  SUM(TOT_PP1) AS val_all_all,
  SUM(CASE WHEN source_gov THEN TOT_PP1 END) AS val_all_gov,
  SUM(CASE WHEN source_com THEN TOT_PP1 END) AS val_all_com,
  SUM(CASE WHEN source_oth THEN TOT_PP1 END) AS val_all_oth,

  SUM(CASE WHEN other_region THEN TOT_PP1 END) AS val_other_region,

  SUM(CASE WHEN type_common THEN TOT_PP1 END) AS val_common_all,
  SUM(CASE WHEN type_common AND source_gov THEN TOT_PP1 END) AS val_common_gov,
  SUM(CASE WHEN type_common AND source_com THEN TOT_PP1 END) AS val_common_com,
  SUM(CASE WHEN type_common AND source_oth THEN TOT_PP1 END) AS val_common_oth,

  SUM(CASE WHEN type_advance THEN TOT_PP1 END) AS val_advance_all,
  SUM(CASE WHEN type_advance AND source_gov THEN TOT_PP1 END) AS val_advance_gov,
  SUM(CASE WHEN type_advance AND source_com THEN TOT_PP1 END) AS val_advance_com,
  SUM(CASE WHEN type_advance AND source_oth THEN TOT_PP1 END) AS val_advance_oth,

  SUM(CASE WHEN type_other THEN TOT_PP1 END) AS val_other_all,
  SUM(CASE WHEN type_other AND source_gov THEN TOT_PP1 END) AS val_other_gov,
  SUM(CASE WHEN type_other AND source_com THEN TOT_PP1 END) AS val_other_com,
  SUM(CASE WHEN type_other AND source_oth THEN TOT_PP1 END) AS val_other_oth,

  SUM(CASE WHEN type_sds THEN TOT_PP1 END) AS val_sds_all,
  SUM(CASE WHEN type_sds AND source_gov THEN TOT_PP1 END) AS val_sds_gov,
  SUM(CASE WHEN type_sds AND source_com THEN TOT_PP1 END) AS val_sds_com,
  SUM(CASE WHEN type_sds AND source_oth THEN TOT_PP1 END) AS val_sds_oth,

  null AS assays_ids

FROM ass WHERE app_is AND ass.seedprod_crop_kind = 'SEEDS' AND potato_is AND PP1_is

UNION

SELECT
  1060 AS id,
  '  в т.ч. специалистами филиала, тыс. га' AS title,
  '' AS description,
  1 AS level,
  'normal' AS type,
  'double' AS val_type,

  SUM(TOT_PP1) AS val_all_all,
  SUM(CASE WHEN source_gov THEN TOT_PP1 END) AS val_all_gov,
  SUM(CASE WHEN source_com THEN TOT_PP1 END) AS val_all_com,
  SUM(CASE WHEN source_oth THEN TOT_PP1 END) AS val_all_oth,

  SUM(CASE WHEN other_region THEN TOT_PP1 END) AS val_other_region,

  SUM(CASE WHEN type_common THEN TOT_PP1 END) AS val_common_all,
  SUM(CASE WHEN type_common AND source_gov THEN TOT_PP1 END) AS val_common_gov,
  SUM(CASE WHEN type_common AND source_com THEN TOT_PP1 END) AS val_common_com,
  SUM(CASE WHEN type_common AND source_oth THEN TOT_PP1 END) AS val_common_oth,

  SUM(CASE WHEN type_advance THEN TOT_PP1 END) AS val_advance_all,
  SUM(CASE WHEN type_advance AND source_gov THEN TOT_PP1 END) AS val_advance_gov,
  SUM(CASE WHEN type_advance AND source_com THEN TOT_PP1 END) AS val_advance_com,
  SUM(CASE WHEN type_advance AND source_oth THEN TOT_PP1 END) AS val_advance_oth,

  SUM(CASE WHEN type_other THEN TOT_PP1 END) AS val_other_all,
  SUM(CASE WHEN type_other AND source_gov THEN TOT_PP1 END) AS val_other_gov,
  SUM(CASE WHEN type_other AND source_com THEN TOT_PP1 END) AS val_other_com,
  SUM(CASE WHEN type_other AND source_oth THEN TOT_PP1 END) AS val_other_oth,

  SUM(CASE WHEN type_sds THEN TOT_PP1 END) AS val_sds_all,
  SUM(CASE WHEN type_sds AND source_gov THEN TOT_PP1 END) AS val_sds_gov,
  SUM(CASE WHEN type_sds AND source_com THEN TOT_PP1 END) AS val_sds_com,
  SUM(CASE WHEN type_sds AND source_oth THEN TOT_PP1 END) AS val_sds_oth,

  null AS assays_ids

FROM ass WHERE app_is AND ass.seedprod_crop_kind = 'SEEDS' AND spec_rsc AND potato_is AND PP1_is



UNION

SELECT
  1070 AS id,
  '  из них: Выбраковано, тыс. га' AS title,
  '' AS description,
  1 AS level,
  'normal' AS type,
  'double' AS val_type,

  SUM(TOT_REJECT_ALL) AS val_all_all,
  SUM(CASE WHEN source_gov THEN TOT_REJECT_ALL END) AS val_all_gov,
  SUM(CASE WHEN source_com THEN TOT_REJECT_ALL END) AS val_all_com,
  SUM(CASE WHEN source_oth THEN TOT_REJECT_ALL END) AS val_all_oth,

  SUM(CASE WHEN other_region THEN TOT_REJECT_ALL END) AS val_other_region,

  SUM(CASE WHEN type_common THEN TOT_REJECT_ALL END) AS val_common_all,
  SUM(CASE WHEN type_common AND source_gov THEN TOT_REJECT_ALL END) AS val_common_gov,
  SUM(CASE WHEN type_common AND source_com THEN TOT_REJECT_ALL END) AS val_common_com,
  SUM(CASE WHEN type_common AND source_oth THEN TOT_REJECT_ALL END) AS val_common_oth,

  SUM(CASE WHEN type_advance THEN TOT_REJECT_ALL END) AS val_advance_all,
  SUM(CASE WHEN type_advance AND source_gov THEN TOT_REJECT_ALL END) AS val_advance_gov,
  SUM(CASE WHEN type_advance AND source_com THEN TOT_REJECT_ALL END) AS val_advance_com,
  SUM(CASE WHEN type_advance AND source_oth THEN TOT_REJECT_ALL END) AS val_advance_oth,

  SUM(CASE WHEN type_other THEN TOT_REJECT_ALL END) AS val_other_all,
  SUM(CASE WHEN type_other AND source_gov THEN TOT_REJECT_ALL END) AS val_other_gov,
  SUM(CASE WHEN type_other AND source_com THEN TOT_REJECT_ALL END) AS val_other_com,
  SUM(CASE WHEN type_other AND source_oth THEN TOT_REJECT_ALL END) AS val_other_oth,

  SUM(CASE WHEN type_sds THEN TOT_REJECT_ALL END) AS val_sds_all,
  SUM(CASE WHEN type_sds AND source_gov THEN TOT_REJECT_ALL END) AS val_sds_gov,
  SUM(CASE WHEN type_sds AND source_com THEN TOT_REJECT_ALL END) AS val_sds_com,
  SUM(CASE WHEN type_sds AND source_oth THEN TOT_REJECT_ALL END) AS val_sds_oth,

  null AS assays_ids

FROM ass WHERE app_is AND ass.seedprod_crop_kind = 'SEEDS' AND PP1_is AND potato_is

UNION

SELECT
    1075 AS id,
    '  из них: По ферм. льготе (не предоставлен лицензионный договор), тыс. га' AS title,
    '' AS description,
    1 AS level,
    'normal' AS type,
    'double' AS val_type,
    SUM(TOT_PP1) AS val_all_all,
    SUM(CASE WHEN source_gov THEN TOT_PP1 END) AS val_all_gov,
    SUM(CASE WHEN source_com THEN TOT_PP1 END) AS val_all_com,
    SUM(CASE WHEN source_oth THEN TOT_PP1 END) AS val_all_oth,

    SUM(CASE WHEN other_region THEN TOT_PP1 END) AS val_other_region,

    SUM(CASE WHEN type_common THEN TOT_PP1 END) AS val_common_all,
    SUM(CASE WHEN type_common AND source_gov THEN TOT_PP1 END) AS val_common_gov,
    SUM(CASE WHEN type_common AND source_com THEN TOT_PP1 END) AS val_common_com,
    SUM(CASE WHEN type_common AND source_oth THEN TOT_PP1 END) AS val_common_oth,

    SUM(CASE WHEN type_advance THEN TOT_PP1 END) AS val_advance_all,
    SUM(CASE WHEN type_advance AND source_gov THEN TOT_PP1 END) AS val_advance_gov,
    SUM(CASE WHEN type_advance AND source_com THEN TOT_PP1 END) AS val_advance_com,
    SUM(CASE WHEN type_advance AND source_oth THEN TOT_PP1 END) AS val_advance_oth,

    SUM(CASE WHEN type_other THEN TOT_PP1 END) AS val_other_all,
    SUM(CASE WHEN type_other AND source_gov THEN TOT_PP1 END) AS val_other_gov,
    SUM(CASE WHEN type_other AND source_com THEN TOT_PP1 END) AS val_other_com,
    SUM(CASE WHEN type_other AND source_oth THEN TOT_PP1 END) AS val_other_oth,

    SUM(CASE WHEN type_sds THEN TOT_PP1 END) AS val_sds_all,
    SUM(CASE WHEN type_sds AND source_gov THEN TOT_PP1 END) AS val_sds_gov,
    SUM(CASE WHEN type_sds AND source_com THEN TOT_PP1 END) AS val_sds_com,
    SUM(CASE WHEN type_sds AND source_oth THEN TOT_PP1 END) AS val_sds_oth,

    null AS assays_ids

FROM ass WHERE app_is AND ass.seedprod_crop_kind = 'SEEDS' AND PP1_is AND potato_is AND STATUS_NO_LIC_AGR_FLAG_643_is

UNION

SELECT
    1076 AS id,
    '  из них: Погибло семенных посевов, тыс. га' AS title,
    '' AS description,
    1 AS level,
    'normal' AS type,
    'double' AS val_type,
    SUM(TOT_PP1) AS val_all_all,
    SUM(CASE WHEN source_gov THEN TOT_PP1 END) AS val_all_gov,
    SUM(CASE WHEN source_com THEN TOT_PP1 END) AS val_all_com,
    SUM(CASE WHEN source_oth THEN TOT_PP1 END) AS val_all_oth,

    SUM(CASE WHEN other_region THEN TOT_PP1 END) AS val_other_region,

    SUM(CASE WHEN type_common THEN TOT_PP1 END) AS val_common_all,
    SUM(CASE WHEN type_common AND source_gov THEN TOT_PP1 END) AS val_common_gov,
    SUM(CASE WHEN type_common AND source_com THEN TOT_PP1 END) AS val_common_com,
    SUM(CASE WHEN type_common AND source_oth THEN TOT_PP1 END) AS val_common_oth,

    SUM(CASE WHEN type_advance THEN TOT_PP1 END) AS val_advance_all,
    SUM(CASE WHEN type_advance AND source_gov THEN TOT_PP1 END) AS val_advance_gov,
    SUM(CASE WHEN type_advance AND source_com THEN TOT_PP1 END) AS val_advance_com,
    SUM(CASE WHEN type_advance AND source_oth THEN TOT_PP1 END) AS val_advance_oth,

    SUM(CASE WHEN type_other THEN TOT_PP1 END) AS val_other_all,
    SUM(CASE WHEN type_other AND source_gov THEN TOT_PP1 END) AS val_other_gov,
    SUM(CASE WHEN type_other AND source_com THEN TOT_PP1 END) AS val_other_com,
    SUM(CASE WHEN type_other AND source_oth THEN TOT_PP1 END) AS val_other_oth,

    SUM(CASE WHEN type_sds THEN TOT_PP1 END) AS val_sds_all,
    SUM(CASE WHEN type_sds AND source_gov THEN TOT_PP1 END) AS val_sds_gov,
    SUM(CASE WHEN type_sds AND source_com THEN TOT_PP1 END) AS val_sds_com,
    SUM(CASE WHEN type_sds AND source_oth THEN TOT_PP1 END) AS val_sds_oth,

    null AS assays_ids

FROM ass WHERE app_is AND ass.seedprod_crop_kind = 'SEEDS' AND PP1_is AND potato_is AND STATUS_AFTER_APP_DEATH_is

UNION

SELECT
    1077 AS id,
    '  из них: Выведено из семенного баланса, тыс. га' AS title,
    '' AS description,
    1 AS level,
    'normal' AS type,
    'double' AS val_type,
    SUM(TOT_PP1) AS val_all_all,
    SUM(CASE WHEN source_gov THEN TOT_PP1 END) AS val_all_gov,
    SUM(CASE WHEN source_com THEN TOT_PP1 END) AS val_all_com,
    SUM(CASE WHEN source_oth THEN TOT_PP1 END) AS val_all_oth,

    SUM(CASE WHEN other_region THEN TOT_PP1 END) AS val_other_region,

    SUM(CASE WHEN type_common THEN TOT_PP1 END) AS val_common_all,
    SUM(CASE WHEN type_common AND source_gov THEN TOT_PP1 END) AS val_common_gov,
    SUM(CASE WHEN type_common AND source_com THEN TOT_PP1 END) AS val_common_com,
    SUM(CASE WHEN type_common AND source_oth THEN TOT_PP1 END) AS val_common_oth,

    SUM(CASE WHEN type_advance THEN TOT_PP1 END) AS val_advance_all,
    SUM(CASE WHEN type_advance AND source_gov THEN TOT_PP1 END) AS val_advance_gov,
    SUM(CASE WHEN type_advance AND source_com THEN TOT_PP1 END) AS val_advance_com,
    SUM(CASE WHEN type_advance AND source_oth THEN TOT_PP1 END) AS val_advance_oth,

    SUM(CASE WHEN type_other THEN TOT_PP1 END) AS val_other_all,
    SUM(CASE WHEN type_other AND source_gov THEN TOT_PP1 END) AS val_other_gov,
    SUM(CASE WHEN type_other AND source_com THEN TOT_PP1 END) AS val_other_com,
    SUM(CASE WHEN type_other AND source_oth THEN TOT_PP1 END) AS val_other_oth,

    SUM(CASE WHEN type_sds THEN TOT_PP1 END) AS val_sds_all,
    SUM(CASE WHEN type_sds AND source_gov THEN TOT_PP1 END) AS val_sds_gov,
    SUM(CASE WHEN type_sds AND source_com THEN TOT_PP1 END) AS val_sds_com,
    SUM(CASE WHEN type_sds AND source_oth THEN TOT_PP1 END) AS val_sds_oth,

    null AS assays_ids

FROM ass WHERE app_is AND ass.seedprod_crop_kind = 'SEEDS' AND PP1_is AND potato_is AND STATUS_AFTER_APP_OUT_SEEDS_is AND NOT STATUS_BEFORE_APP_OUT_SEEDS_is
         
         
UNION

SELECT
  1100 AS id,
  'Количество геоточек "не учтённых" (не обследованных) семенных посевов, кроме выведенных, погибших, без договора (не по льготе), шт' AS title,
  '' AS description,
  0 AS level,
  'normal' AS type,
  'bigint' AS val_type,

  count(ass.id) AS val_all_all,
  count(CASE WHEN source_gov THEN true END) AS val_all_gov,
  count(CASE WHEN source_com THEN true END) AS val_all_com,
  count(CASE WHEN source_oth THEN true END) AS val_all_oth,

  count(CASE WHEN other_region THEN true END) AS val_other_region,

  count(CASE WHEN type_common THEN true END) AS val_common_all,
  count(CASE WHEN type_common AND source_gov THEN true END) AS val_common_gov,
  count(CASE WHEN type_common AND source_com THEN true END) AS val_common_com,
  count(CASE WHEN type_common AND source_oth THEN true END) AS val_common_oth,

  count(CASE WHEN type_advance THEN true END) AS val_advance_all,
  count(CASE WHEN type_advance AND source_gov THEN true END) AS val_advance_gov,
  count(CASE WHEN type_advance AND source_com THEN true END) AS val_advance_com,
  count(CASE WHEN type_advance AND source_oth THEN true END) AS val_advance_oth,

  count(CASE WHEN type_other THEN true END) AS val_other_all,
  count(CASE WHEN type_other AND source_gov THEN true END) AS val_other_gov,
  count(CASE WHEN type_other AND source_com THEN true END) AS val_other_com,
  count(CASE WHEN type_other AND source_oth THEN true END) AS val_other_oth,

  count(CASE WHEN type_sds THEN true END) AS val_sds_all,
  count(CASE WHEN type_sds AND source_gov THEN true END) AS val_sds_gov,
  count(CASE WHEN type_sds AND source_com THEN true END) AS val_sds_com,
  count(CASE WHEN type_sds AND source_oth THEN true END) AS val_sds_oth,

  null AS assays_ids

FROM ass WHERE normal_is AND (NOT app_is) AND ass.seedprod_crop_kind = 'SEEDS'
           AND NOT STATUS_BEFORE_APP_OUT_SEEDS_is AND NOT STATUS_AFTER_APP_OUT_SEEDS_is
           AND NOT STATUS_BEFORE_APP_DEATH_is AND NOT STATUS_AFTER_APP_DEATH_is
           AND NOT STATUS_NO_LIC_AGR_FLAG_643_is

-- FROM ass WHERE normal_is AND (NOT (reg_is OR app_is)) AND ass.seedprod_crop_kind = 'SEEDS'
)

SELECT
    row_number() over (ORDER BY id) AS rid,
    un_all.*
FROM un_all ORDER BY id