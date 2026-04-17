package org.genom.reportservice.repository;

import com.gnm.criteria.SeedProductionParameter;
import com.gnm.enums.ase.SeedProductionReportKindEnum;
import com.gnm.enums.ase.SeedProductionReportTypeEnum;
import lombok.extern.slf4j.Slf4j;
import org.genom.reportservice.interfaces.SeedProductionParameterInt;
import org.springframework.stereotype.Repository;

import static com.gnm.enums.ase.SeedProductionReportKindEnum.KIND_REGIONS;
import static org.genom.reportservice.utils.SeedProdSQLUtil.constructDepClause;


@Slf4j
public class SeedCropsQueryBuilder {

    public static String build(SeedProductionParameterInt seedProductionParameter) {



        /*language=SQL*/
        String detail_assays =
                "jsonb_agg(cast(ROW ( " +
                        " id,                       /* id                                 bigint */ " +
                        " 0,                        /* turn                               bigint */ " +
                        " 0,                        /* parent_id                          bigint */ " +
                        " status,                   /* status                             character varying */ " +
                        " mobile,                   /* mobile                             boolean */ " +
                        " date_assay,               /* date_assay                         timestamp without time zone */ " +
                        " service_date,             /* date_service                       timestamp without time zone */ " +
                        " " +
                        " dep_township_name,        /* department_township_name           character varying */ " +
                        " dep_township_id,          /* department_township_id             bigint */ " +
                        " dep_region_name,          /* department_region_name             character varying */ " +
                        " dep_region_id,            /* department_region_id               bigint */ " +
                        " " +
                        " '',                       /* township_name                      character varying */ " +
                        " null,                     /* township_id                        bigint */ " +
                        " '',                       /* region_name                        character varying */ " +
                        " null,                     /* region_id                          bigint */ " +
                        " " +
                        " service_provider_type,    /* service_provider_type              character varying */ " +
                        " culture_id,               /* culture_id                         bigint */ " +
                        " culture_name,             /* culture_name                       character varying */ " +
                        " sort_code,                /* sort_code                          character varying */ " +
                        " sort_name,                /* sort_name                          character varying */ " +
                        " " +
                        " rep_taked_name,           /* taked_reproduction_name            character varying */ " +
                        " rep_taked_type_id,        /* taked_reproduction_type_id         bigint */ " +
                        " rep_taked_type,           /* taked_reproduction_type            character varying */ " +
                        " rep_taked_trade,          /* taked_reproduction_trade           boolean */ " +
                        " rep_taked_category_id,    /* taked_reproduction_category_id     bigint */ " +
                        " rep_taked_category_type,  /* taked_reproduction_category_type   character varying */ " +
                        " rep_taked_category_name,  /* taked_reproduction_category_name   character varying */ " +

                        " reproduction_add_result_sign_01,  /* reproduction_add_result_sign_01   character varying */ " +
                        " reproduction_add_result_sign_02,  /* reproduction_add_result_sign_02   character varying */ " +
                        " reproduction_add_result_sign_03,  /* reproduction_add_result_sign_03   character varying */ " +
                        " reproduction_add_result_sign_04,  /* reproduction_add_result_sign_04   character varying */ " +
                        " reproduction_add_result_sign_05,  /* reproduction_add_result_sign_05   character varying */ " +

                        " " +
                        " '',                       /* name                               character varying */ " +
                        " 100,                      /* group_level                        integer */ " +
                        " 1,                        /* count_assays                       bigint */ " +
                        " 0,                        /* culs_cnt                           bigint */ " +
                        " 0,                        /* groups_cnt                         bigint */ " +
                        " " +
                        " area_seeded_all,          /* area_seeded_all */ " +
                        " area_seeded_harvest, " +
                        " area_harvest_all,         /* area_harvest_all */ " +
                        " area_harvest_app,         /* area_harvest_app */ " +
                        " area_harvest_reg,         /* area_harvest_reg */ " +
                        " area_forsale_all,         /* area_forsale_all */ " +
                        " area_forsale_app,         /* area_forsale_app */ " +
                        " area_forsale_reg,         /* area_forsale_reg */ " +
                        " check_ph_area_harvest,    /* check_ph_area_harvest */ " +
                        " check_ph_mass_from_area_harvest, /* check_ph_mass_from_area_harvest */ " +
                        " " +
                        " app_tot_sum_total,        /* app_tot_sum_total */ " +
                        " app_tot_sum_seeds,        /* app_tot_sum_seeds */ " +
                        " app_rsc_sum_total,        /* app_rsc_sum_total */ " +
                        " app_rsc_sum_seeds,        /* app_rsc_sum_seeds */ " +
                        " " +
                        " app_tot_os,               /* app_tot_os */ " +
                        " app_tot_es,               /* app_tot_es */ " +
                        " app_tot_rs_14,            /* app_tot_rs_14 */ " +
                        " app_tot_rs_14_rst,        /* app_tot_rs_14_rst */ " +
                        " app_tot_f1,               /* app_tot_f1 */ " +
                        " app_tot_rs_5b,            /* app_tot_rs_5b */ " +
                        " app_tot_rs_5b_rst,        /* app_tot_rs_5b_rst */ " +
                        " app_tot_nc,               /* app_tot_nc */ " +
                        " app_tot_reject,           /* app_tot_reject */ " +
                        " app_tot_trades, " +
                        " " +
                        " app_rsc_os,               /* app_rsc_os */ " +
                        " app_rsc_es,               /* app_rsc_es */ " +
                        " app_rsc_rs_14,            /* app_rsc_rs_14 */ " +
                        " app_rsc_rs_14_rst,        /* app_rsc_rs_14_rst */ " +
                        " app_rsc_f1,               /* app_rsc_f1 */ " +
                        " app_rsc_rs_5b,            /* app_rsc_rs_5b */ " +
                        " app_rsc_rs_5b_rst,        /* app_rsc_rs_5b_rst */ " +
                        " app_rsc_nc,               /* app_rsc_nc */ " +
                        " app_rsc_reject,           /* app_rsc_reject */ " +
                        " app_rsc_trades, " +
                        " " +
                        " reg_tot_sum_total,        /* reg_tot_sum_total */ " +
                        " reg_tot_sum_seeds,        /* reg_tot_sum_seeds */ " +
                        " reg_rsc_sum_total,        /* reg_rsc_sum_total */ " +
                        " reg_rsc_sum_seeds,        /* reg_rsc_sum_seeds */ " +
                        " " +
                        " reg_tot_os,               /* reg_tot_os */ " +
                        " reg_tot_es,               /* reg_tot_es */ " +
                        " reg_tot_rs_14,            /* reg_tot_rs_14 */ " +
                        " reg_tot_rs_14_rst,        /* reg_tot_rs_14_rst */ " +
                        " reg_tot_f1,               /* reg_tot_f1 */ " +
                        " reg_tot_rs_5b,            /* reg_tot_rs_5b */ " +
                        " reg_tot_rs_5b_rst,        /* reg_tot_rs_5b_rst */ " +
                        " reg_tot_nc,               /* reg_tot_nc */ " +
                        " reg_tot_reject,           /* reg_tot_reject */ " +
                        " reg_tot_trades, " +
                        " " +
                        " reg_rsc_os,               /* reg_rsc_os */ " +
                        " reg_rsc_es,               /* reg_rsc_es */ " +
                        " reg_rsc_rs_14,            /* reg_rsc_rs_14 */ " +
                        " reg_rsc_rs_14_rst,        /* reg_rsc_rs_14_rst */ " +
                        " reg_rsc_f1,               /* reg_rsc_f1 */ " +
                        " reg_rsc_rs_5b,            /* reg_rsc_rs_5b */ " +
                        " reg_rsc_rs_5b_rst,        /* reg_rsc_rs_5b_rst */ " +
                        " reg_rsc_nc,               /* reg_rsc_nc */ " +
                        " reg_rsc_reject,           /* reg_rsc_reject */ " +
                        " reg_rsc_trades " +
                        ") as assay_seedprod_full_wrapper)) ";

        String detail_assays_agg = " jsonb_agg(assay_ids) ";

        /*language=SQL*/
        String base =
                "WITH scrop_ret AS ( " +
                        " SELECT " +
                        (seedProductionParameter.isAssaysInclude() ? detail_assays : "null") + " AS assay_ids, " +
                        " SUM(count_assays)                      AS count_assays, " +
                        " 'culture'                              AS type, " +
                        " dep_ter_region_id, " +
                        " dep_ter_federal_district_id, " +
                        "    " +
                        " culture_id, " +
                        " seedprod_crop_category," +
                        " " +
                        " seedprod_culture_season_id, " +
                        " " +
                        " sort_sign_2, " +
                        " " +
                        " SUM(area_seeded_all)                   AS area_seeded_all, " +
                        " SUM(area_seeded_harvest)               AS area_seeded_harvest, " +
                        " SUM(area_harvest_all)                  AS area_harvest_all, " +
                        " SUM(area_harvest_app)                  AS area_harvest_app, " +
                        " SUM(area_harvest_reg)                  AS area_harvest_reg, " +
                        " SUM(area_forsale_all)                  AS area_forsale_all, " +
                        " SUM(area_forsale_app)                  AS area_forsale_app, " +
                        " SUM(area_forsale_reg)                  AS area_forsale_reg, " +
                        " SUM(check_ph_area_harvest)             AS check_ph_area_harvest, " +
                        " SUM(check_ph_mass_from_area_harvest)   AS check_ph_mass_from_area_harvest, " +
                        " " +
                        " SUM(app_TOT_sum_total)                 AS app_tot_sum_total, " +
                        " SUM(app_TOT_sum_seeds)                 AS app_tot_sum_seeds, " +
                        " SUM(app_RSC_sum_total)                 AS app_rsc_sum_total, " +
                        " SUM(app_RSC_sum_seeds)                 AS app_rsc_sum_seeds, " +
                        " " +
                        " SUM(app_TOT_OS)                        AS app_tot_os, " +
                        " SUM(app_TOT_ES)                        AS app_tot_es, " +
                        " SUM(app_TOT_RS_14)                     AS app_tot_rs_14, " +
                        " SUM(app_TOT_RS_14_RST)                 AS app_tot_rs_14_rst, " +
                        " SUM(app_TOT_F1)                        AS app_tot_f1, " +
                        " SUM(app_TOT_RS_5B)                     AS app_tot_rs_5b, " +
                        " SUM(app_TOT_RS_5B_RST)                 AS app_tot_rs_5b_rst, " +
                        " SUM(app_TOT_NC)                        AS app_tot_nc, " +
                        " SUM(app_TOT_reject)                    AS app_tot_reject, " +
                        " SUM(app_TOT_trades)                    AS app_tot_trades, " +
                        " " +
                        " SUM(app_RSC_OS)                        AS app_rsc_os, " +
                        " SUM(app_RSC_ES)                        AS app_rsc_es, " +
                        " SUM(app_RSC_RS_14)                     AS app_rsc_rs_14, " +
                        " SUM(app_RSC_RS_14_RST)                 AS app_rsc_rs_14_rst, " +
                        " SUM(app_RSC_F1)                        AS app_rsc_f1, " +
                        " SUM(app_RSC_RS_5B)                     AS app_rsc_rs_5b, " +
                        " SUM(app_RSC_RS_5B_RST)                 AS app_rsc_rs_5b_rst, " +
                        " SUM(app_RSC_NC)                        AS app_rsc_nc, " +
                        " SUM(app_RSC_reject)                    AS app_rsc_reject, " +
                        " SUM(app_RSC_trades)                    AS app_rsc_trades, " +
                        " " +
                        " SUM(reg_TOT_sum_total)                 AS reg_tot_sum_total, " +
                        " SUM(reg_TOT_sum_seeds)                 AS reg_tot_sum_seeds, " +
                        " SUM(reg_RSC_sum_total)                 AS reg_rsc_sum_total, " +
                        " SUM(reg_RSC_sum_seeds)                 AS reg_rsc_sum_seeds, " +
                        " " +
                        " SUM(reg_TOT_OS)                        AS reg_tot_os, " +
                        " SUM(reg_TOT_ES)                        AS reg_tot_es, " +
                        " SUM(reg_TOT_RS_14)                     AS reg_tot_rs_14, " +
                        " SUM(reg_TOT_RS_14_RST)                 AS reg_tot_rs_14_rst, " +
                        " SUM(reg_TOT_F1)                        AS reg_tot_f1, " +
                        " SUM(reg_TOT_RS_5B)                     AS reg_tot_rs_5b, " +
                        " SUM(reg_TOT_RS_5B_RST)                 AS reg_tot_rs_5b_rst, " +
                        " SUM(reg_TOT_NC)                        AS reg_tot_nc, " +
                        " SUM(reg_TOT_reject)                    AS reg_tot_reject, " +
                        " SUM(reg_TOT_trades)                    AS reg_tot_trades, " +
                        " " +
                        " SUM(reg_RSC_OS)                        AS reg_rsc_os, " +
                        " SUM(reg_RSC_ES)                        AS reg_rsc_es, " +
                        " SUM(reg_RSC_RS_14)                     AS reg_rsc_rs_14, " +
                        " SUM(reg_RSC_RS_14_RST)                 AS reg_rsc_rs_14_rst, " +
                        " SUM(reg_RSC_F1)                        AS reg_rsc_f1, " +
                        " SUM(reg_RSC_RS_5B)                     AS reg_rsc_rs_5b, " +
                        " SUM(reg_RSC_RS_5B_RST)                 AS reg_rsc_rs_5b_rst, " +
                        " SUM(reg_RSC_NC)                        AS reg_rsc_nc, " +
                        " SUM(reg_RSC_reject)                    AS reg_rsc_reject, " +
                        " SUM(reg_RSC_trades)                    AS reg_rsc_trades " +
                        " FROM ( " +
                        " SELECT " +
                        "     id, " +
                        "     date_assay, " +
//        "     1 AS count_assays, " +
                        "     (CASE WHEN seedprod_crop_kind = 'SEEDS' THEN 1 ELSE 0 END) AS count_assays, " +
                        "     status, " +
                        "     mobile, " +
                        " " +
                        "     dep_township_id, " +
                        "     dep_township_name, " +
                        " " +
                        "     dep_region_id, " +
                        "     dep_region_name, " +
                        " " +
                        "     dep_ter_region_id, " +
                        "     dep_ter_federal_district_id, " +
                        " " +
                        "     culture_id, " +
                        "     culture_name, " +
                        "     seedprod_crop_category, " +
                        " " +
                        "     seedprod_culture_season_id, " +
                        " " +
                        "     sort_code, " +
                        "     sort_name, " +
                        "     sort_sign_2, " +
                        " " +
                        "     area_seeded                                                           AS area_seeded_all, " +
                        "     (CASE WHEN area_harvest IS NOT NULL THEN area_seeded ELSE 0.0 END)    AS area_seeded_harvest, " +
                        "     area_service, " +
                        " " +
                        "     area_harvest                                             AS area_harvest_all, " +
                        "     (CASE WHEN app_is THEN area_harvest ELSE 0 END)          AS area_harvest_app, " +
                        "     (CASE WHEN reg_is THEN area_harvest ELSE 0 END)          AS area_harvest_reg, " +
                        " " +
                        "     area_forsale                                             AS area_forsale_all, " +
                        "     (CASE WHEN app_is THEN area_forsale ELSE 0 END)          AS area_forsale_app, " +
                        "     (CASE WHEN reg_is THEN area_forsale ELSE 0 END)          AS area_forsale_reg, " +
                        " " +
                        "     (CASE WHEN productivity_fact_seed > 0 AND area_harvest > 0 THEN (area_harvest / :coef) ELSE null END)                              AS check_ph_area_harvest, " +
                        "     (CASE WHEN productivity_fact_seed > 0 AND area_harvest > 0 THEN (area_harvest / :coef) * productivity_fact_seed * :massCoef ELSE null END)     AS check_ph_mass_from_area_harvest, " +
                        " " +
                        "     rep_taked_name, " +
                        "     rep_taked_type_id, " +
                        "     rep_taked_type, " +
                        "     rep_taked_trade, " +
                        "     rep_taked_category_id, " +
                        "     rep_taked_category_type, " +
                        "     rep_taked_category_name, " +

                        "     reproduction_add_result_sign_01, " +
                        "     reproduction_add_result_sign_02, " +
                        "     reproduction_add_result_sign_03, " +
                        "     reproduction_add_result_sign_04, " +
                        "     reproduction_add_result_sign_05, " +

                        " " +
                        "     service_date, " +
                        "     service_type, " +
                        "     service_before_state_type, " +
                        "     service_result_type, " +
                        "     service_provider_type, " +
                        "     service_after_state_type, " +
                        " " +
                        "     (CASE WHEN app_is THEN COALESCE(TOT_OS, 0) + COALESCE(TOT_ES, 0) + COALESCE(TOT_RS_14, 0) + COALESCE(TOT_RS_14_RST, 0) + COALESCE(TOT_F1, 0) + COALESCE(TOT_RS_5B, 0) + COALESCE(TOT_RS_5B_RST, 0) + COALESCE(TOT_NC, 0) + COALESCE(TOT_reject, 0) + COALESCE(TOT_TRADE, 0) ELSE 0 END)               AS app_TOT_sum_total, " +
                        "     (CASE WHEN app_is THEN COALESCE(TOT_OS, 0) + COALESCE(TOT_ES, 0) + COALESCE(TOT_RS_14, 0) + COALESCE(TOT_RS_14_RST, 0) + COALESCE(TOT_F1, 0) + COALESCE(TOT_RS_5B, 0) + COALESCE(TOT_RS_5B_RST, 0) + COALESCE(TOT_NC, 0) + COALESCE(TOT_reject, 0) ELSE 0 END)               AS app_TOT_sum_seeds, " +
                        "     (CASE WHEN app_is THEN COALESCE(RSC_OS, 0) + COALESCE(RSC_ES, 0) + COALESCE(RSC_RS_14, 0) + COALESCE(RSC_RS_14_RST, 0) + COALESCE(RSC_F1, 0) + COALESCE(RSC_RS_5B, 0) + COALESCE(RSC_RS_5B_RST, 0) + COALESCE(RSC_NC, 0) + COALESCE(RSC_reject, 0) + COALESCE(RSC_TRADE, 0) ELSE 0 END)               AS app_RSC_sum_total, " +
                        "     (CASE WHEN app_is THEN COALESCE(RSC_OS, 0) + COALESCE(RSC_ES, 0) + COALESCE(RSC_RS_14, 0) + COALESCE(RSC_RS_14_RST, 0) + COALESCE(RSC_F1, 0) + COALESCE(RSC_RS_5B, 0) + COALESCE(RSC_RS_5B_RST, 0) + COALESCE(RSC_NC, 0) + COALESCE(RSC_reject, 0) ELSE 0 END)               AS app_RSC_sum_seeds, " +
                        " " +
                        "     (CASE WHEN app_is THEN TOT_OS ELSE 0 END)               AS app_TOT_OS, " +
                        "     (CASE WHEN app_is THEN TOT_ES ELSE 0 END)               AS app_TOT_ES, " +
                        "     (CASE WHEN app_is THEN TOT_RS_14 ELSE 0 END)            AS app_TOT_RS_14, " +
                        "     (CASE WHEN app_is THEN TOT_RS_14_RST ELSE 0 END)        AS app_TOT_RS_14_RST, " +
                        "     (CASE WHEN app_is THEN TOT_F1 ELSE 0 END)               AS app_TOT_F1, " +
                        "     (CASE WHEN app_is THEN TOT_RS_5B ELSE 0 END)            AS app_TOT_RS_5B, " +
                        "     (CASE WHEN app_is THEN TOT_RS_5B_RST ELSE 0 END)        AS app_TOT_RS_5B_RST, " +
                        "     (CASE WHEN app_is THEN TOT_NC ELSE 0 END)               AS app_TOT_NC, " +
                        "     (CASE WHEN app_is THEN TOT_reject ELSE 0 END)           AS app_TOT_reject, " +
                        "     (CASE WHEN app_is THEN TOT_TRADE ELSE 0 END)            AS app_TOT_trades, " +
                        " " +
                        "     (CASE WHEN app_is THEN RSC_OS ELSE 0 END)               AS app_RSC_OS, " +
                        "     (CASE WHEN app_is THEN RSC_ES ELSE 0 END)               AS app_RSC_ES, " +
                        "     (CASE WHEN app_is THEN RSC_RS_14 ELSE 0 END)            AS app_RSC_RS_14, " +
                        "     (CASE WHEN app_is THEN RSC_RS_14_RST ELSE 0 END)        AS app_RSC_RS_14_RST, " +
                        "     (CASE WHEN app_is THEN RSC_F1 ELSE 0 END)               AS app_RSC_F1, " +
                        "     (CASE WHEN app_is THEN RSC_RS_5B ELSE 0 END)            AS app_RSC_RS_5B, " +
                        "     (CASE WHEN app_is THEN RSC_RS_5B_RST ELSE 0 END)        AS app_RSC_RS_5B_RST, " +
                        "     (CASE WHEN app_is THEN RSC_NC ELSE 0 END)               AS app_RSC_NC, " +
                        "     (CASE WHEN app_is THEN RSC_reject ELSE 0 END)           AS app_RSC_reject, " +
                        "     (CASE WHEN app_is THEN RSC_TRADE ELSE 0 END)            AS app_RSC_trades, " +
                        " " +
                        "     (CASE WHEN reg_is THEN COALESCE(TOT_OS, 0) + COALESCE(TOT_ES, 0) + COALESCE(TOT_RS_14, 0) + COALESCE(TOT_RS_14_RST, 0) + COALESCE(TOT_F1, 0) + COALESCE(TOT_RS_5B, 0) + COALESCE(TOT_RS_5B_RST, 0) + COALESCE(TOT_NC, 0) + COALESCE(TOT_reject, 0) + COALESCE(TOT_TRADE, 0) ELSE 0 END)               AS reg_TOT_sum_total, " +
                        "     (CASE WHEN reg_is THEN COALESCE(TOT_OS, 0) + COALESCE(TOT_ES, 0) + COALESCE(TOT_RS_14, 0) + COALESCE(TOT_RS_14_RST, 0) + COALESCE(TOT_F1, 0) + COALESCE(TOT_RS_5B, 0) + COALESCE(TOT_RS_5B_RST, 0) + COALESCE(TOT_NC, 0) + COALESCE(TOT_reject, 0) ELSE 0 END)               AS reg_TOT_sum_seeds, " +
                        "     (CASE WHEN reg_is THEN COALESCE(RSC_OS, 0) + COALESCE(RSC_ES, 0) + COALESCE(RSC_RS_14, 0) + COALESCE(RSC_RS_14_RST, 0) + COALESCE(RSC_F1, 0) + COALESCE(RSC_RS_5B, 0) + COALESCE(RSC_RS_5B_RST, 0) + COALESCE(RSC_NC, 0) + COALESCE(RSC_reject, 0) + COALESCE(RSC_TRADE, 0) ELSE 0 END)               AS reg_RSC_sum_total, " +
                        "     (CASE WHEN reg_is THEN COALESCE(RSC_OS, 0) + COALESCE(RSC_ES, 0) + COALESCE(RSC_RS_14, 0) + COALESCE(RSC_RS_14_RST, 0) + COALESCE(RSC_F1, 0) + COALESCE(RSC_RS_5B, 0) + COALESCE(RSC_RS_5B_RST, 0) + COALESCE(RSC_NC, 0) + COALESCE(RSC_reject, 0) ELSE 0 END)               AS reg_RSC_sum_seeds, " +
                        " " +
                        "     (CASE WHEN reg_is THEN TOT_OS ELSE 0 END)               AS reg_TOT_OS, " +
                        "     (CASE WHEN reg_is THEN TOT_ES ELSE 0 END)               AS reg_TOT_ES, " +
                        "     (CASE WHEN reg_is THEN TOT_RS_14 ELSE 0 END)            AS reg_TOT_RS_14, " +
                        "     (CASE WHEN reg_is THEN TOT_RS_14_RST ELSE 0 END)        AS reg_TOT_RS_14_RST, " +
                        "     (CASE WHEN reg_is THEN TOT_F1 ELSE 0 END)               AS reg_TOT_F1, " +
                        "     (CASE WHEN reg_is THEN TOT_RS_5B ELSE 0 END)            AS reg_TOT_RS_5B, " +
                        "     (CASE WHEN reg_is THEN TOT_RS_5B_RST ELSE 0 END)        AS reg_TOT_RS_5B_RST, " +
                        "     (CASE WHEN reg_is THEN TOT_NC ELSE 0 END)               AS reg_TOT_NC, " +
                        "     (CASE WHEN reg_is THEN TOT_reject ELSE 0 END)           AS reg_TOT_reject, " +
                        "     (CASE WHEN reg_is THEN TOT_TRADE ELSE 0 END)            AS reg_TOT_trades, " +
                        " " +
                        "     (CASE WHEN reg_is THEN RSC_OS ELSE 0 END)               AS reg_RSC_OS, " +
                        "     (CASE WHEN reg_is THEN RSC_ES ELSE 0 END)               AS reg_RSC_ES, " +
                        "     (CASE WHEN reg_is THEN RSC_RS_14 ELSE 0 END)            AS reg_RSC_RS_14, " +
                        "     (CASE WHEN reg_is THEN RSC_RS_14_RST ELSE 0 END)        AS reg_RSC_RS_14_RST, " +
                        "     (CASE WHEN reg_is THEN RSC_F1 ELSE 0 END)               AS reg_RSC_F1, " +
                        "     (CASE WHEN reg_is THEN RSC_RS_5B ELSE 0 END)            AS reg_RSC_RS_5B, " +
                        "     (CASE WHEN reg_is THEN RSC_RS_5B_RST ELSE 0 END)        AS reg_RSC_RS_5B_RST, " +
                        "     (CASE WHEN reg_is THEN RSC_NC ELSE 0 END)               AS reg_RSC_NC, " +
                        "     (CASE WHEN reg_is THEN RSC_reject ELSE 0 END)           AS reg_RSC_reject, " +
                        "     (CASE WHEN reg_is THEN RSC_TRADE ELSE 0 END)            AS reg_RSC_trades " +
                        " FROM ( " +
                        "    SELECT " +
                        "        assays.id," +
                        "        assays.seedprod_crop_kind, " +
                        "        date_assay, " +
                        "        assays.status, " +
                        "        assays.mobile, " +
                        "        is_seedprod, " +
                        " " +
                        "        assays.township_id                              AS ter_township_id, " +
                        "        join_ter_township.name_short                    AS ter_township_name, " +
                        "        join_ter_township.region_id                     AS ter_region_id, " +
                        "        join_ter_region.name_short                      AS ter_region_name, " +
                        "        join_ter_region.federal_district_id             AS ter_federal_district_id, " +
                        "        join_ter_federal_districts.name_short           AS ter_federal_district_name, " +
                        " " +
                        "        (CASE WHEN seedprod_crop_kind = 'SEEDS' THEN cropfield_area * :coef ELSE null END)                        AS area_seeded, " +
                        "        (CASE WHEN seedprod_crop_kind = 'SEEDS' THEN cropfield_service_area * :coef ELSE null END)                AS area_service, " +
                        "        (CASE WHEN seedprod_crop_kind = 'SEEDS' THEN cropfield_harvest_area * :coef ELSE null END)                AS area_harvest, " +
                        "        (CASE WHEN seedprod_crop_kind = 'SEEDS' THEN cropfield_forsale_area * :coef ELSE null END)                AS area_forsale, " +
                        " " +
                        "        (CASE WHEN seedprod_crop_kind = 'SEEDS' THEN cropfield_service_productivity ELSE null END)                AS productivity_service, " +
                        "        (CASE WHEN seedprod_crop_kind = 'SEEDS' THEN cropfield_plan_productivity ELSE null END)                   AS productivity_plan_seed, " +
                        "        (CASE WHEN seedprod_crop_kind = 'SEEDS' THEN cropfield_fact_productivity ELSE null END)                   AS productivity_fact_seed, " +
                        " " +
                        "        assays.department_id                            AS dep_township_id, " +
                        "        join_dep_township.name_short                    AS dep_township_name, " +
                        " " +
                        "        join_dep_township.parent_id                     AS dep_region_id, " +
                        "        join_dep_region.name_short                      AS dep_region_name, " +
                        " " +
                        "        join_dep_region.ter_region_id                   AS dep_ter_region_id, " +
                        "        join_dep_region_ter_region.federal_district_id  AS dep_ter_federal_district_id, " +
                        " " +
                        "        join_cultures.id                                AS culture_id, " +
                        "        join_cultures.name                              AS culture_name, " +
                        "        seedprod_crop_category                          AS seedprod_crop_category, " +

                        "        assays.seedprod_culture_season_id               AS seedprod_culture_season_id, " +
                        " " +
                        "        crop_current_culturesort_code                   AS sort_code, " +
                        "        join_sorts.name                                 AS sort_name, " +
                        "        join_sorts.sign_2                               AS sort_sign_2, " +
                        " " +
                        "        join_rep_taked_types.name_short                 AS rep_taked_name, " +
                        "        seedprod_taked_reproduction_type_id             AS rep_taked_type_id, " +
                        "        join_rep_taked_types.type                       AS rep_taked_type, " +
                        "        seedprod_taked_reproduction_trade               AS rep_taked_trade, " +
                        "        join_rep_taked_types.category_id                AS rep_taked_category_id, " +
                        "        join_rep_taked_categories.type                  AS rep_taked_category_type, " +
                        "        join_rep_taked_categories.name_short            AS rep_taked_category_name, " +

                        "        join_reproduction_add_result.sign_01            AS reproduction_add_result_sign_01, " +
                        "        join_reproduction_add_result.sign_02            AS reproduction_add_result_sign_02, " +
                        "        join_reproduction_add_result.sign_03            AS reproduction_add_result_sign_03, " +
                        "        join_reproduction_add_result.sign_04            AS reproduction_add_result_sign_04, " +
                        "        join_reproduction_add_result.sign_05            AS reproduction_add_result_sign_05, " +

                        "        join_rep_current_types.name_short               AS rep_current_name, " +
                        "        reproduction_type_id                            AS rep_current_type_id, " +
                        "        join_rep_current_types.type                     AS rep_current_type, " +
                        "        reproduction_trade                              AS rep_current_trade, " +
                        "        join_rep_current_types.category_id              AS rep_current_category_id, " +
                        "        join_current_taked_categories.type              AS rep_current_category_type, " +
                        "        join_current_taked_categories.name_short        AS rep_current_category_name, " +

                        " " +
                        "        seedprod_service_date                           AS service_date, " +
                        "        seedprod_service_type                           AS service_type, " +
                        "        seedprod_service_before_state_type              AS service_before_state_type, " +
                        "        seedprod_service_result_type                    AS service_result_type, " +
                        "        seedprod_service_provider_type                  AS service_provider_type, " +
                        "        seedprod_service_after_state_type               AS service_after_state_type, " +
                        " " +
                        "        (CASE WHEN seedprod_service_type = 'APPROBATION' OR seedprod_service_type = 'SURVEY_RM' THEN true ELSE false END)                                               AS app_is, " +
                        "        (CASE WHEN seedprod_service_type = 'REGISTRATION' THEN true ELSE false END)                                                                                   AS reg_is, " +

//        "        (CASE WHEN (seedprod_service_type = 'APPROBATION' OR seedprod_service_type = 'SURVEY_RM') AND assays.seedprod_service_date BETWEEN cast(concat(cast(:harvest_year AS text),'-01-01') AS date) AND cast(concat(cast(:harvest_year AS text),'-12-31') AS date) THEN true ELSE false END)                                               AS app_is, " +
//        "        (CASE WHEN seedprod_service_type = 'REGISTRATION' AND assays.seedprod_service_date BETWEEN cast(concat(cast(:harvest_year AS text),'-01-01') AS date) AND cast(concat(cast(:harvest_year AS text),'-12-31') AS date) THEN true ELSE false END)                                                                                   AS reg_is, " +
                        " " +

                        "        (CASE WHEN seedprod_crop_kind = 'SEEDS' AND seedprod_service_result_type IS DISTINCT FROM 'REJECT' AND join_rep_taked_categories.type = 'OS' THEN cropfield_service_area * :coef ELSE 0 END)                                                                                                                               AS TOT_OS, " +
                        "        (CASE WHEN seedprod_crop_kind = 'SEEDS' AND seedprod_service_result_type IS DISTINCT FROM 'REJECT' AND join_rep_taked_categories.type = 'ES' THEN cropfield_service_area * :coef ELSE 0 END)                                                                                                                               AS TOT_ES, " +
                        "        (CASE WHEN seedprod_crop_kind = 'SEEDS' AND seedprod_service_result_type IS DISTINCT FROM 'REJECT' AND join_rep_taked_types.type IN ('RS', 'RS1', 'RS2', 'RS3', 'RS4') AND seedprod_taked_reproduction_trade != TRUE THEN cropfield_service_area * :coef ELSE 0 END)                                                       AS TOT_RS_14, " +
                        "        (CASE WHEN seedprod_crop_kind = 'SEEDS' AND seedprod_service_result_type IS DISTINCT FROM 'REJECT' AND join_rep_taked_types.type IN ('RS', 'RS1', 'RS2', 'RS3', 'RS4') AND seedprod_taked_reproduction_trade = TRUE THEN cropfield_service_area * :coef ELSE 0 END)                                                        AS TOT_RS_14_RST, " +
                        "        (CASE WHEN seedprod_crop_kind = 'SEEDS' AND seedprod_service_result_type IS DISTINCT FROM 'REJECT' AND join_rep_taked_types.type = 'F1' THEN cropfield_service_area * :coef ELSE 0 END)                                                                                                                                    AS TOT_F1, " +
                        "        (CASE WHEN seedprod_crop_kind = 'SEEDS' AND seedprod_service_result_type IS DISTINCT FROM 'REJECT' AND join_rep_taked_types.type IN ('RS5', 'RS6', 'RS7', 'RS8', 'RS9', 'RS10') AND seedprod_taked_reproduction_trade != TRUE THEN cropfield_service_area * :coef ELSE 0 END)                                              AS TOT_RS_5B, " +
                        "        (CASE WHEN seedprod_crop_kind = 'SEEDS' AND seedprod_service_result_type IS DISTINCT FROM 'REJECT' AND join_rep_taked_types.type IN ('RS5', 'RS6', 'RS7', 'RS8', 'RS9', 'RS10') AND seedprod_taked_reproduction_trade = TRUE THEN cropfield_service_area * :coef ELSE 0 END)                                               AS TOT_RS_5B_RST, " +
                        "        (CASE WHEN seedprod_crop_kind = 'SEEDS' AND seedprod_service_result_type IS DISTINCT FROM 'REJECT' AND join_rep_taked_categories.type = 'NC' THEN cropfield_service_area * :coef ELSE 0 END)                                                                                                                               AS TOT_NC, " +
                        "        (CASE WHEN seedprod_crop_kind = 'SEEDS' AND seedprod_service_result_type = 'REJECT' THEN cropfield_service_area * :coef ELSE 0 END)                                                                                            AS TOT_reject, " +
                        "        (CASE WHEN seedprod_crop_kind = 'TRADE' THEN cropfield_service_area * :coef ELSE 0 END)                                                                                                                               AS TOT_TRADE, " +
                        " " +
                        "        (CASE WHEN seedprod_crop_kind = 'SEEDS' AND seedprod_service_result_type IS DISTINCT FROM 'REJECT' AND seedprod_service_provider_type = 'RSC' AND join_rep_taked_categories.type = 'OS' THEN cropfield_service_area * :coef ELSE 0 END)                                                                                    AS RSC_OS, " +
                        "        (CASE WHEN seedprod_crop_kind = 'SEEDS' AND seedprod_service_result_type IS DISTINCT FROM 'REJECT' AND seedprod_service_provider_type = 'RSC' AND join_rep_taked_categories.type = 'ES' THEN cropfield_service_area * :coef ELSE 0 END)                                                                                    AS RSC_ES, " +
                        "        (CASE WHEN seedprod_crop_kind = 'SEEDS' AND seedprod_service_result_type IS DISTINCT FROM 'REJECT' AND seedprod_service_provider_type = 'RSC' AND join_rep_taked_types.type IN ('RS', 'RS1', 'RS2', 'RS3', 'RS4') AND seedprod_taked_reproduction_trade != TRUE THEN cropfield_service_area * :coef ELSE 0 END)            AS RSC_RS_14, " +
                        "        (CASE WHEN seedprod_crop_kind = 'SEEDS' AND seedprod_service_result_type IS DISTINCT FROM 'REJECT' AND seedprod_service_provider_type = 'RSC' AND join_rep_taked_types.type IN ('RS', 'RS1', 'RS2', 'RS3', 'RS4') AND seedprod_taked_reproduction_trade = TRUE THEN cropfield_service_area * :coef ELSE 0 END)             AS RSC_RS_14_RST, " +
                        "        (CASE WHEN seedprod_crop_kind = 'SEEDS' AND seedprod_service_result_type IS DISTINCT FROM 'REJECT' AND seedprod_service_provider_type = 'RSC' AND join_rep_taked_types.type = 'F1' THEN cropfield_service_area * :coef ELSE 0 END)                                                                                         AS RSC_F1, " +
                        "        (CASE WHEN seedprod_crop_kind = 'SEEDS' AND seedprod_service_result_type IS DISTINCT FROM 'REJECT' AND seedprod_service_provider_type = 'RSC' AND join_rep_taked_types.type IN ('RS5', 'RS6', 'RS7', 'RS8', 'RS9', 'RS10') AND seedprod_taked_reproduction_trade != TRUE THEN cropfield_service_area * :coef ELSE 0 END)   AS RSC_RS_5B, " +
                        "        (CASE WHEN seedprod_crop_kind = 'SEEDS' AND seedprod_service_result_type IS DISTINCT FROM 'REJECT' AND seedprod_service_provider_type = 'RSC' AND join_rep_taked_types.type IN ('RS5', 'RS6', 'RS7', 'RS8', 'RS9', 'RS10') AND seedprod_taked_reproduction_trade = TRUE THEN cropfield_service_area * :coef ELSE 0 END)    AS RSC_RS_5B_RST, " +
                        "        (CASE WHEN seedprod_crop_kind = 'SEEDS' AND seedprod_service_result_type IS DISTINCT FROM 'REJECT' AND seedprod_service_provider_type = 'RSC' AND join_rep_taked_categories.type = 'NC' THEN cropfield_service_area * :coef ELSE 0 END)                                                                                    AS RSC_NC, " +
                        "        (CASE WHEN seedprod_crop_kind = 'SEEDS' AND seedprod_service_result_type = 'REJECT' AND seedprod_service_provider_type = 'RSC' THEN cropfield_service_area * :coef ELSE 0 END)                                                 AS RSC_reject, " +
                        "        (CASE WHEN seedprod_crop_kind = 'TRADE' AND seedprod_service_provider_type = 'RSC' THEN cropfield_service_area * :coef ELSE 0 END)                                                                                    AS RSC_TRADE " +

                        " " +
                        "    FROM public.assays " +
                        "        LEFT JOIN common.ter_townships AS join_ter_township ON join_ter_township.id = assays.township_id " +
                        "        LEFT JOIN common.ter_regions AS join_ter_region ON join_ter_region.id = join_ter_township.region_id " +
                        "        LEFT JOIN common.ter_federal_districts AS join_ter_federal_districts ON join_ter_federal_districts.id = join_ter_region.federal_district_id " +
                        "        LEFT JOIN common.department_structure AS join_dep_township ON join_dep_township.id = assays.department_id " +
                        "        LEFT JOIN common.department_structure AS join_dep_region ON join_dep_region.id = join_dep_township.parent_id " +
                        "        LEFT JOIN common.ter_regions AS join_dep_region_ter_region ON join_dep_region_ter_region.id = join_dep_region.ter_region_id " +
                        "        LEFT JOIN common.culture_sorts AS join_sorts ON join_sorts.code = assays.crop_current_culturesort_code " +
                        "        LEFT JOIN common.cultures AS join_cultures ON join_cultures.id = join_sorts.culture_id " +
                        "        LEFT JOIN common.reproduction_types AS join_rep_taked_types ON join_rep_taked_types.id = seedprod_taked_reproduction_type_id " +
                        "        LEFT JOIN common.reproduction_categories AS join_rep_taked_categories ON join_rep_taked_categories.id = join_rep_taked_types.category_id " +

                        "        LEFT JOIN common.reproduction_types AS join_rep_current_types ON join_rep_current_types.id = reproduction_type_id " +
                        "        LEFT JOIN common.reproduction_categories AS join_current_taked_categories ON join_current_taked_categories.id = join_rep_current_types.category_id " +

                        "        LEFT JOIN ase.assays_reproduction_add_result AS join_reproduction_add_result ON join_reproduction_add_result.assay_id = assays.id  " +
//                        "        LEFT JOIN ase.assays_reproduction_add_sowed AS join_reproduction_add_sowed ON join_reproduction_add_sowed.assay_id = assays.id  " +

                        "    WHERE is_seedprod AND (seedprod_crop_kind = 'SEEDS' OR seedprod_crop_kind = 'TRADE') AND township_id IS NOT null " +
                        "        AND seedprod_for_harvest_year = :harvest_year " +
                        "        AND assays.crop_current_culturesort_code IS NOT null AND join_sorts.mark_01 IS DISTINCT FROM 'ns' " +
                        "    ORDER BY join_sorts.name, rep_taked_type_id, assays.id " +
                        " ) AS scrop " +
                        "    WHERE scrop.sort_code IS NOT NULL " +
//                         rep_taked_category_type IS NULL - Категория не указана
//                         rep_taked_category_type != 'IM' - Устеревший ИМ категория убрана в 2024 году
//                        "        AND (rep_taked_category_type != 'IM' OR rep_taked_category_type IS NULL OR rep_taked_type NOT IN ('CLONES','IN_VITRO','MICRO_TUBER','MINI_TUBER')) " + // возможно должна быть разлчная логика для 10а/б и 10в т.к. полученная репродукция в 10в может быть и не введена а 10а/б должна быть
                        "        AND ((rep_taked_category_type IS NULL OR rep_taked_category_type NOT LIKE 'IM') AND (rep_taked_type ISNULL OR rep_taked_type NOT IN ('CLONES','IN_VITRO','MICRO_TUBER','MINI_TUBER'))) " + // возможно должна быть разлчная логика для 10а/б и 10в т.к. полученная репродукция в 10в может быть и не введена а 10а/б должна быть
//                        "        AND (rep_taked_category_type != 'IM' OR rep_taked_category_type IS NULL) " + // возможно должна быть разлчная логика для 10а/б и 10в т.к. полученная репродукция в 10в может быть и не введена а 10а/б должна быть

                        ((SeedProductionReportTypeEnum.TYPE_APPROBATION.equals(seedProductionParameter.getSeedProdReportType()) || SeedProductionReportTypeEnum.TYPE_REGISTRATION.equals(seedProductionParameter.getSeedProdReportType())) ? " AND scrop.service_date BETWEEN cast(concat(cast(:harvest_year AS text),'-01-01') AS date) AND cast(concat(cast(:harvest_year AS text),'-12-31') AS date) " : " ") +

                        (seedProductionParameter.isSetDataPeriod() ? " AND (scrop.service_date BETWEEN :dateBegin AND :dateEnd OR scrop.date_assay BETWEEN :dateBegin AND :dateEnd) " : " ") +

                        constructDepClause(seedProductionParameter, "scrop") +
                        " " +
                        " ) AS scrop_in " +
                        " GROUP BY dep_ter_federal_district_id, dep_ter_region_id, culture_id, seedprod_crop_category, seedprod_culture_season_id, sort_sign_2 " +
                        "), " +
                        " " +
                        "sp_cultures AS ( " +
                        " SELECT " +
                        " dep_ter_federal_district_id, " +
                        " dep_ter_region_id, " +
                        " " +
                        " sp_cultures.id, " +
                        " 'culture'                             AS type, " +
                        (seedProductionParameter.isAssaysInclude() ? detail_assays_agg : "null") + " AS assay_ids, " +
                        " turn                                  AS turn_in, " +
                        " sp_cultures.culture_id                AS object_id, " +
                        " join_cultures.name                    AS name, " +
                        " parent_group_id                       AS parent_id, " +
                        " 10                                    AS group_level, " +
                        " 0                                     AS turn_all, " +
                        " SUM(count_assays)                     AS count_assays, " +
                        " 0                                     AS culs_cnt, " +
                        " 0                                     AS groups_cnt, " +
                        " " +
                        " SUM(area_seeded_all)                  AS area_seeded_all, " +
                        " SUM(area_seeded_harvest)              AS area_seeded_harvest, " +
                        " SUM(area_harvest_all)                 AS area_harvest_all, " +
                        " SUM(area_harvest_app)                 AS area_harvest_app, " +
                        " SUM(area_harvest_reg)                 AS area_harvest_reg, " +
                        " SUM(area_forsale_all)                 AS area_forsale_all, " +
                        " SUM(area_forsale_app)                 AS area_forsale_app, " +
                        " SUM(area_forsale_reg)                 AS area_forsale_reg, " +
                        " SUM(check_ph_area_harvest)            AS check_ph_area_harvest, " +
                        " SUM(check_ph_mass_from_area_harvest)  AS check_ph_mass_from_area_harvest, " +
                        " " +
                        " SUM(app_tot_sum_total)                AS app_tot_sum_total, " +
                        " SUM(app_tot_sum_seeds)                AS app_tot_sum_seeds, " +
                        " SUM(app_rsc_sum_total)                AS app_rsc_sum_total, " +
                        " SUM(app_rsc_sum_seeds)                AS app_rsc_sum_seeds, " +
                        " " +
                        " SUM(app_tot_os)                       AS app_tot_os, " +
                        " SUM(app_tot_es)                       AS app_tot_es, " +
                        " SUM(app_tot_rs_14)                    AS app_tot_rs_14, " +
                        " SUM(app_tot_rs_14_rst)                AS app_tot_rs_14_rst, " +
                        " SUM(app_tot_f1)                       AS app_tot_f1, " +
                        " SUM(app_tot_rs_5b)                    AS app_tot_rs_5b, " +
                        " SUM(app_tot_rs_5b_rst)                AS app_tot_rs_5b_rst, " +
                        " SUM(app_tot_nc)                       AS app_tot_nc, " +
                        " SUM(app_tot_reject)                   AS app_tot_reject, " +
                        " SUM(app_tot_trades)                   AS app_tot_trades, " +
                        " " +
                        " SUM(app_rsc_os)                       AS app_rsc_os, " +
                        " SUM(app_rsc_es)                       AS app_rsc_es, " +
                        " SUM(app_rsc_rs_14)                    AS app_rsc_rs_14, " +
                        " SUM(app_rsc_rs_14_rst)                AS app_rsc_rs_14_rst, " +
                        " SUM(app_rsc_f1)                       AS app_rsc_f1, " +
                        " SUM(app_rsc_rs_5b)                    AS app_rsc_rs_5b, " +
                        " SUM(app_rsc_rs_5b_rst)                AS app_rsc_rs_5b_rst, " +
                        " SUM(app_rsc_nc)                       AS app_rsc_nc, " +
                        " SUM(app_rsc_reject)                   AS app_rsc_reject, " +
                        " SUM(app_rsc_trades)                   AS app_rsc_trades, " +
                        " " +
                        " SUM(reg_tot_sum_total)                AS reg_tot_sum_total, " +
                        " SUM(reg_tot_sum_seeds)                AS reg_tot_sum_seeds, " +
                        " SUM(reg_rsc_sum_total)                AS reg_rsc_sum_total, " +
                        " SUM(reg_rsc_sum_seeds)                AS reg_rsc_sum_seeds, " +
                        " " +
                        " SUM(reg_tot_os)                       AS reg_tot_os, " +
                        " SUM(reg_tot_es)                       AS reg_tot_es, " +
                        " SUM(reg_tot_rs_14)                    AS reg_tot_rs_14, " +
                        " SUM(reg_tot_rs_14_rst)                AS reg_tot_rs_14_rst, " +
                        " SUM(reg_tot_f1)                       AS reg_tot_f1, " +
                        " SUM(reg_tot_rs_5b)                    AS reg_tot_rs_5b, " +
                        " SUM(reg_tot_rs_5b_rst)                AS reg_tot_rs_5b_rst, " +
                        " SUM(reg_tot_nc)                       AS reg_tot_nc, " +
                        " SUM(reg_tot_reject)                   AS reg_tot_reject, " +
                        " SUM(reg_tot_trades)                   AS reg_tot_trades, " +
                        " " +
                        " SUM(reg_rsc_os)                       AS reg_rsc_os, " +
                        " SUM(reg_rsc_es)                       AS reg_rsc_es, " +
                        " SUM(reg_rsc_rs_14)                    AS reg_rsc_rs_14, " +
                        " SUM(reg_rsc_rs_14_rst)                AS reg_rsc_rs_14_rst, " +
                        " SUM(reg_rsc_f1)                       AS reg_rsc_f1, " +
                        " SUM(reg_rsc_rs_5b)                    AS reg_rsc_rs_5b, " +
                        " SUM(reg_rsc_rs_5b_rst)                AS reg_rsc_rs_5b_rst, " +
                        " SUM(reg_rsc_nc)                       AS reg_rsc_nc, " +
                        " SUM(reg_rsc_reject)                   AS reg_rsc_reject, " +
                        " SUM(reg_rsc_trades)                   AS reg_rsc_trades " +
                        " " +
                        " FROM ase.seed_production_cultures_groups_cultures AS sp_cultures " +
                        " LEFT JOIN common.cultures AS join_cultures ON join_cultures.id = sp_cultures.culture_id " +
                        " LEFT JOIN scrop_ret ON scrop_ret.culture_id = sp_cultures.culture_id " +
                        "    AND (CASE WHEN sp_cultures.flag_01 IS NOT NULL THEN scrop_ret.sort_sign_2 = sp_cultures.flag_01 ELSE true END) " +
//        "    AND (CASE WHEN sp_cultures.flag_02 IS NOT NULL THEN (scrop_ret.culture_union_kind = sp_cultures.flag_02 OR scrop_ret.culture_vegetable_kind = sp_cultures.flag_02) ELSE true END) " +
                        "    AND (CASE WHEN sp_cultures.crop_category IS NOT NULL THEN (scrop_ret.seedprod_crop_category = sp_cultures.crop_category) ELSE true END) " +
                        "    AND (CASE WHEN sp_cultures.crop_category_anti IS NOT NULL THEN (scrop_ret.seedprod_crop_category NOT IN (SELECT unnest(string_to_array(sp_cultures.crop_category_anti,',')))) ELSE true END) " +
                        "    AND (CASE WHEN sp_cultures.flag_01_anti IS NOT NULL THEN scrop_ret.sort_sign_2 NOT IN (SELECT unnest(string_to_array(sp_cultures.flag_01_anti,','))) ELSE true END) " +

                        "    AND (CASE WHEN sp_cultures.culture_season_id ISNULL THEN true ELSE sp_cultures.culture_season_id = (CASE WHEN scrop_ret.seedprod_culture_season_id ISNULL THEN join_cultures.default_season_id ELSE scrop_ret.seedprod_culture_season_id END) END) " +


                        " " +
                        " GROUP BY dep_ter_federal_district_id, dep_ter_region_id, sp_cultures.id, sp_cultures.turn, sp_cultures.culture_id, join_cultures.name, parent_group_id " +
                        " ORDER BY parent_id, turn " +
                        ")";

        log.info("seedCropsQueryBuilder " + base);

        /*language=SQL*/
        String for_cultures =
                " SELECT " +
                        " id, " +
                        " 'culture'                             AS type, " +
                        (seedProductionParameter.isAssaysInclude() ? detail_assays_agg : "null") + " AS assay_ids, " +
                        " turn_in, " +
                        " object_id, " +
                        " name, " +
                        " parent_id, " +
                        " 10                                    AS group_level, " +
                        " 0                                     AS turn_all, " +
                        " SUM(count_assays)                     AS count_assays, " +
                        " 0                                     AS culs_cnt, " +
                        " 0                                     AS groups_cnt, " +
                        " " +
                        " SUM(area_seeded_all)                  AS area_seeded_all, " +
                        " SUM(area_seeded_harvest)              AS area_seeded_harvest, " +
                        " SUM(area_harvest_all)                 AS area_harvest_all, " +
                        " SUM(area_harvest_app)                 AS area_harvest_app, " +
                        " SUM(area_harvest_reg)                 AS area_harvest_reg, " +
                        " SUM(area_forsale_all)                 AS area_forsale_all, " +
                        " SUM(area_forsale_app)                 AS area_forsale_app, " +
                        " SUM(area_forsale_reg)                 AS area_forsale_reg, " +
                        " SUM(check_ph_area_harvest)            AS check_ph_area_harvest, " +
                        " SUM(check_ph_mass_from_area_harvest)  AS check_ph_mass_from_area_harvest, " +
                        " " +
                        " SUM(app_tot_sum_total)                AS app_tot_sum_total, " +
                        " SUM(app_tot_sum_seeds)                AS app_tot_sum_seeds, " +
                        " SUM(app_rsc_sum_total)                AS app_rsc_sum_total, " +
                        " SUM(app_rsc_sum_seeds)                AS app_rsc_sum_seeds, " +
                        " " +
                        " SUM(app_tot_os)                       AS app_tot_os, " +
                        " SUM(app_tot_es)                       AS app_tot_es, " +
                        " SUM(app_tot_rs_14)                    AS app_tot_rs_14, " +
                        " SUM(app_tot_rs_14_rst)                AS app_tot_rs_14_rst, " +
                        " SUM(app_tot_f1)                       AS app_tot_f1, " +
                        " SUM(app_tot_rs_5b)                    AS app_tot_rs_5b, " +
                        " SUM(app_tot_rs_5b_rst)                AS app_tot_rs_5b_rst, " +
                        " SUM(app_tot_nc)                       AS app_tot_nc, " +
                        " SUM(app_tot_reject)                   AS app_tot_reject, " +
                        " SUM(app_tot_trades)                   AS app_tot_trades, " +
                        " " +
                        " SUM(app_rsc_os)                       AS app_rsc_os, " +
                        " SUM(app_rsc_es)                       AS app_rsc_es, " +
                        " SUM(app_rsc_rs_14)                    AS app_rsc_rs_14, " +
                        " SUM(app_rsc_rs_14_rst)                AS app_rsc_rs_14_rst, " +
                        " SUM(app_rsc_f1)                       AS app_rsc_f1, " +
                        " SUM(app_rsc_rs_5b)                    AS app_rsc_rs_5b, " +
                        " SUM(app_rsc_rs_5b_rst)                AS app_rsc_rs_5b_rst, " +
                        " SUM(app_rsc_nc)                       AS app_rsc_nc, " +
                        " SUM(app_rsc_reject)                   AS app_rsc_reject, " +
                        " SUM(app_rsc_trades)                   AS app_rsc_trades, " +
                        " " +
                        " SUM(reg_tot_sum_total)                AS reg_tot_sum_total, " +
                        " SUM(reg_tot_sum_seeds)                AS reg_tot_sum_seeds, " +
                        " SUM(reg_rsc_sum_total)                AS reg_rsc_sum_total, " +
                        " SUM(reg_rsc_sum_seeds)                AS reg_rsc_sum_seeds, " +
                        " " +
                        " SUM(reg_tot_os)                       AS reg_tot_os, " +
                        " SUM(reg_tot_es)                       AS reg_tot_es, " +
                        " SUM(reg_tot_rs_14)                    AS reg_tot_rs_14, " +
                        " SUM(reg_tot_rs_14_rst)                AS reg_tot_rs_14_rst, " +
                        " SUM(reg_tot_f1)                       AS reg_tot_f1, " +
                        " SUM(reg_tot_rs_5b)                    AS reg_tot_rs_5b, " +
                        " SUM(reg_tot_rs_5b_rst)                AS reg_tot_rs_5b_rst, " +
                        " SUM(reg_tot_nc)                       AS reg_tot_nc, " +
                        " SUM(reg_tot_reject)                   AS reg_tot_reject, " +
                        " SUM(reg_tot_trades)                   AS reg_tot_trades, " +
                        " " +
                        " SUM(reg_rsc_os)                       AS reg_rsc_os, " +
                        " SUM(reg_rsc_es)                       AS reg_rsc_es, " +
                        " SUM(reg_rsc_rs_14)                    AS reg_rsc_rs_14, " +
                        " SUM(reg_rsc_rs_14_rst)                AS reg_rsc_rs_14_rst, " +
                        " SUM(reg_rsc_f1)                       AS reg_rsc_f1, " +
                        " SUM(reg_rsc_rs_5b)                    AS reg_rsc_rs_5b, " +
                        " SUM(reg_rsc_rs_5b_rst)                AS reg_rsc_rs_5b_rst, " +
                        " SUM(reg_rsc_nc)                       AS reg_rsc_nc, " +
                        " SUM(reg_rsc_reject)                   AS reg_rsc_reject, " +
                        " SUM(reg_rsc_trades)                   AS reg_rsc_trades " +
                        " " +
                        " FROM sp_cultures " +
                        " GROUP BY id, turn_in, object_id, name, parent_id " +
                        " ORDER BY parent_id, turn_in";

//                "SELECT * FROM sp_cultures";

        /*language=SQL*/
        String for_regions = ", " +
                "ter_districts_and_regions AS ( " +
                "SELECT " +
                " (district_id * 1000) + COALESCE(region_id, 0) AS id, " +
                " type, " +
                " district_id, " +
                " region_id, " +
                " object_name, " +
                " turn_district, " +
                " turn_region " +
                "FROM ( " +
                " SELECT " +
                " 'country'                        AS type, " +
                " 0                                AS district_id, " +
                " 0                                AS region_id, " +
                " 'Всего'                          AS object_name, " +
                " 0                                AS turn_district, " +
                " 0                                AS turn_region " +
                " " +
                " UNION " +
                " SELECT " +
                " 'district'                       AS type, " +
                " ter_federal_districts.id         AS district_id, " +
                " null                             AS region_id, " +
                " ter_federal_districts.name_short AS object_name, " +
                " ter_federal_districts.turn       AS turn_district, " +
                " 0                                AS turn_region " +
                " FROM common.ter_federal_districts " +
                " UNION " +
                " SELECT " +
                " 'region'                         AS type, " +
                " ter_federal_districts.id         AS district_id, " +
                " ter_regions.id                   AS region_id, " +
                " ter_regions.name                 AS object_name, " +
                " ter_federal_districts.turn       AS turn_district, " +
                " ter_regions.turn                 AS turn_region " +
                " FROM common.ter_regions JOIN common.ter_federal_districts ON ter_federal_districts.id = ter_regions.federal_district_id " +
                " WHERE ter_regions.turn IS NOT NULL " +
                " ) AS union_districts_and_regions " +
                " ORDER BY turn_district, turn_region " +
                "), " +
                " " +
                "groups_01 AS ( " +
                " SELECT id, turn, name, parent_group_id " +
                " FROM ase.seed_production_cultures_groups " +
                " WHERE id = :culture_group), " +
                "groups_02 AS (SELECT id, turn, name, parent_group_id " +
                " FROM ase.seed_production_cultures_groups " +
                " WHERE parent_group_id IN (SELECT id FROM groups_01)), " +
                "groups_03 AS (SELECT id, turn, name, parent_group_id " +
                " FROM ase.seed_production_cultures_groups " +
                " WHERE parent_group_id IN (SELECT id FROM groups_02)), " +
                "groups_04 AS (SELECT id, turn, name, parent_group_id " +
                " FROM ase.seed_production_cultures_groups " +
                " WHERE parent_group_id IN (SELECT id FROM groups_03)), " +
                "groups_05 AS (SELECT id, turn, name, parent_group_id " +
                " FROM ase.seed_production_cultures_groups " +
                " WHERE parent_group_id IN (SELECT id FROM groups_04)), " +
                " " +
                "groups_all AS ( " +
                " SELECT * FROM groups_01 " +
                " UNION SELECT * FROM groups_02 " +
                " UNION SELECT * FROM groups_03 " +
                " UNION SELECT * FROM groups_04 " +
                " UNION SELECT * FROM groups_05 " +
                ") " +
                " " +
                "SELECT " +
                " ter_districts_and_regions.id          AS id, " +
                " ter_districts_and_regions.type        AS type, " +
                " (CASE WHEN ter_districts_and_regions.type = 'region'   THEN " + (seedProductionParameter.isAssaysInclude() ? detail_assays_agg : "null") + " ELSE null END) AS assay_ids, " +
                " turn_region                           AS turn_in, " +
                " (CASE WHEN ter_districts_and_regions.type = 'country'  THEN 0 " +
                "      WHEN ter_districts_and_regions.type = 'district' THEN ter_districts_and_regions.district_id " +
                "      WHEN ter_districts_and_regions.type = 'region'   THEN ter_districts_and_regions.region_id " +
                " ELSE 4 END)                           AS object_id, " +
                " ter_districts_and_regions.object_name AS name, " +
                " (CASE WHEN ter_districts_and_regions.type = 'country'  THEN null " +
                "      WHEN ter_districts_and_regions.type = 'district' THEN 0 " +
                "      WHEN ter_districts_and_regions.type = 'region'   THEN ter_districts_and_regions.district_id " +
                " ELSE 4 END)                           AS parent_id, " +
                " (CASE WHEN ter_districts_and_regions.type = 'country'  THEN 0 " +
                "      WHEN ter_districts_and_regions.type = 'district' THEN 1 " +
                "      WHEN ter_districts_and_regions.type = 'region'   THEN 2 " +
                " ELSE 4 END)                           AS group_level, " +
                " turn_region                           AS turn_all, " +
                " SUM(count_assays)                     AS count_assays, " +
                " 0                                     AS culs_cnt, " +
                " 0                                     AS groups_cnt, " +
                " " +
                " SUM(area_seeded_all)                  AS area_seeded_all, " +
                " SUM(area_seeded_harvest)              AS area_seeded_harvest, " +
                " SUM(area_harvest_all)                 AS area_harvest_all, " +
                " SUM(area_harvest_app)                 AS area_harvest_app, " +
                " SUM(area_harvest_reg)                 AS area_harvest_reg, " +
                " SUM(area_forsale_all)                 AS area_forsale_all, " +
                " SUM(area_forsale_app)                 AS area_forsale_app, " +
                " SUM(area_forsale_reg)                 AS area_forsale_reg, " +
                " SUM(check_ph_area_harvest)            AS check_ph_area_harvest, " +
                " SUM(check_ph_mass_from_area_harvest)  AS check_ph_mass_from_area_harvest, " +
                " " +
                " SUM(app_tot_sum_total)                AS app_tot_sum_total, " +
                " SUM(app_tot_sum_seeds)                AS app_tot_sum_seeds, " +
                " SUM(app_rsc_sum_total)                AS app_rsc_sum_total, " +
                " SUM(app_rsc_sum_seeds)                AS app_rsc_sum_seeds, " +
                " " +
                " SUM(app_tot_os)                       AS app_tot_os, " +
                " SUM(app_tot_es)                       AS app_tot_es, " +
                " SUM(app_tot_rs_14)                    AS app_tot_rs_14, " +
                " SUM(app_tot_rs_14_rst)                AS app_tot_rs_14_rst, " +
                " SUM(app_tot_f1)                       AS app_tot_f1, " +
                " SUM(app_tot_rs_5b)                    AS app_tot_rs_5b, " +
                " SUM(app_tot_rs_5b_rst)                AS app_tot_rs_5b_rst, " +
                " SUM(app_tot_nc)                       AS app_tot_nc, " +
                " SUM(app_tot_reject)                   AS app_tot_reject, " +
                " SUM(app_tot_trades)                   AS app_tot_trades, " +
                " " +
                " SUM(app_rsc_os)                       AS app_rsc_os, " +
                " SUM(app_rsc_es)                       AS app_rsc_es, " +
                " SUM(app_rsc_rs_14)                    AS app_rsc_rs_14, " +
                " SUM(app_rsc_rs_14_rst)                AS app_rsc_rs_14_rst, " +
                " SUM(app_rsc_f1)                       AS app_rsc_f1, " +
                " SUM(app_rsc_rs_5b)                    AS app_rsc_rs_5b, " +
                " SUM(app_rsc_rs_5b_rst)                AS app_rsc_rs_5b_rst, " +
                " SUM(app_rsc_nc)                       AS app_rsc_nc, " +
                " SUM(app_rsc_reject)                   AS app_rsc_reject, " +
                " SUM(app_rsc_trades)                   AS app_rsc_trades, " +
                " " +
                " SUM(reg_tot_sum_total)                AS reg_tot_sum_total, " +
                " SUM(reg_tot_sum_seeds)                AS reg_tot_sum_seeds, " +
                " SUM(reg_rsc_sum_total)                AS reg_rsc_sum_total, " +
                " SUM(reg_rsc_sum_seeds)                AS reg_rsc_sum_seeds, " +
                " " +
                " SUM(reg_tot_os)                       AS reg_tot_os, " +
                " SUM(reg_tot_es)                       AS reg_tot_es, " +
                " SUM(reg_tot_rs_14)                    AS reg_tot_rs_14, " +
                " SUM(reg_tot_rs_14_rst)                AS reg_tot_rs_14_rst, " +
                " SUM(reg_tot_f1)                       AS reg_tot_f1, " +
                " SUM(reg_tot_rs_5b)                    AS reg_tot_rs_5b, " +
                " SUM(reg_tot_rs_5b_rst)                AS reg_tot_rs_5b_rst, " +
                " SUM(reg_tot_nc)                       AS reg_tot_nc, " +
                " SUM(reg_tot_reject)                   AS reg_tot_reject, " +
                " SUM(reg_tot_trades)                   AS reg_tot_trades, " +
                " " +
                " SUM(reg_rsc_os)                       AS reg_rsc_os, " +
                " SUM(reg_rsc_es)                       AS reg_rsc_es, " +
                " SUM(reg_rsc_rs_14)                    AS reg_rsc_rs_14, " +
                " SUM(reg_rsc_rs_14_rst)                AS reg_rsc_rs_14_rst, " +
                " SUM(reg_rsc_f1)                       AS reg_rsc_f1, " +
                " SUM(reg_rsc_rs_5b)                    AS reg_rsc_rs_5b, " +
                " SUM(reg_rsc_rs_5b_rst)                AS reg_rsc_rs_5b_rst, " +
                " SUM(reg_rsc_nc)                       AS reg_rsc_nc, " +
                " SUM(reg_rsc_reject)                   AS reg_rsc_reject, " +
                " SUM(reg_rsc_trades)                   AS reg_rsc_trades " +
                " " +
                " FROM ter_districts_and_regions " +
                "LEFT JOIN sp_cultures ON " +
                "        (CASE WHEN ter_districts_and_regions.type = 'region' THEN ter_districts_and_regions.region_id = sp_cultures.dep_ter_region_id ELSE true END) " +
                "        AND (CASE WHEN ter_districts_and_regions.type = 'district' THEN ter_districts_and_regions.district_id = sp_cultures.dep_ter_federal_district_id ELSE true END) " +
                "        AND sp_cultures.parent_id IN (SELECT id FROM groups_all) " +
                "GROUP BY ter_districts_and_regions.id, ter_districts_and_regions.object_name, ter_districts_and_regions.type, region_id, district_id, turn_district, turn_region " +
                "ORDER BY ter_districts_and_regions.turn_district, ter_districts_and_regions.turn_region";

//        log.info("SQL construct " + seedProductionParameter.isSetDataPeriod() + " " + constructDepClause(seedProductionParameter));

        if (SeedProductionReportKindEnum.KIND_CULTURES_GROUPS.equals(seedProductionParameter.getSeedProdReportKind())) {
            return base + for_cultures;
        } else if (KIND_REGIONS.equals(seedProductionParameter.getSeedProdReportKind())) {
            return base + for_regions;
        }

        return null;
    }

}
