SELECT
    groups_end.id, 'group' AS type, null AS assay_ids, groups_end.turn_in, groups_end.id AS object_id, groups_end.name, groups_end.parent_id, groups_end.group_level, groups_end.turn_all,

    null AS count_assays,
    COUNT(culs.id) AS culs_cnt,
    COUNT(grups.id) AS groups_cnt,

    null AS area_seeded_all,
    null AS area_seeded_harvest,

    null AS area_harvest_all,

    null AS area_harvest_app,
    null AS area_harvest_reg,
    null AS area_forsale_all,
    null AS area_forsale_app,
    null AS area_forsale_reg,
    null AS check_ph_area_harvest,
    null AS check_ph_mass_from_area_harvest,

    null AS app_tot_sum_total,
    null AS app_tot_sum_seeds,
    null AS app_rsc_sum_total,
    null AS app_rsc_sum_seeds,

    null AS app_tot_os,
    null AS app_tot_es,
    null AS app_tot_rs_14,
    null AS app_tot_rs_14_rst,
    null AS app_tot_f1,
    null AS app_tot_rs_5b,
    null AS app_tot_rs_5b_rst,
    null AS app_tot_nc,
    null AS app_tot_reject,
    null AS app_tot_trades,

    null AS app_rsc_os,
    null AS app_rsc_es,
    null AS app_rsc_rs_14,
    null AS app_rsc_rs_14_rst,
    null AS app_rsc_f1,
    null AS app_rsc_rs_5b,
    null AS app_rsc_rs_5b_rst,
    null AS app_rsc_nc,
    null AS app_rsc_reject,
    null AS app_rsc_trades,

    null AS reg_tot_sum_total,
    null AS reg_tot_sum_seeds,
    null AS reg_rsc_sum_total,
    null AS reg_rsc_sum_seeds,

    null AS reg_tot_os,
    null AS reg_tot_es,
    null AS reg_tot_rs_14,
    null AS reg_tot_rs_14_rst,
    null AS reg_tot_f1,
    null AS reg_tot_rs_5b,
    null AS reg_tot_rs_5b_rst,
    null AS reg_tot_nc,
    null AS reg_tot_reject,
    null AS reg_tot_trades,

    null AS reg_rsc_os,
    null AS reg_rsc_es,
    null AS reg_rsc_rs_14,
    null AS reg_rsc_rs_14_rst,
    null AS reg_rsc_f1,
    null AS reg_rsc_rs_5b,
    null AS reg_rsc_rs_5b_rst,
    null AS reg_rsc_nc,
    null AS reg_rsc_reject,
    null AS reg_rsc_trades

FROM (SELECT id, turn AS turn_in, name, p_id AS parent_id, group_level,
             (COALESCE(t_0,0) + COALESCE(t_1,0) + COALESCE(t_2,0) + COALESCE(t_3,0) + COALESCE(t_4,0)) AS turn_all
      FROM
          (SELECT id, turn, name,
                  group_level,

                  (CASE WHEN group_level = 0 THEN null WHEN group_level = 1 THEN p_id WHEN group_level = 2 THEN p_p_id WHEN group_level = 3 THEN p_p_p_id WHEN group_level = 4 THEN p_p_p_p_id END) AS g_0,
                  (CASE WHEN group_level = 0 THEN null WHEN group_level = 1 THEN null WHEN group_level = 2 THEN p_id WHEN group_level = 3 THEN p_p_id WHEN group_level = 4 THEN p_p_p_id END) AS g_1,
                  (CASE WHEN group_level = 0 THEN null WHEN group_level = 1 THEN null WHEN group_level = 2 THEN null WHEN group_level = 3 THEN p_id WHEN group_level = 4 THEN p_p_id END) AS g_2,
                  (CASE WHEN group_level = 0 THEN null WHEN group_level = 1 THEN null WHEN group_level = 2 THEN null WHEN group_level = 3 THEN null WHEN group_level = 4 THEN p_id END) AS g_3,
                  (CASE WHEN group_level = 0 THEN turn WHEN group_level = 1 THEN p_turn WHEN group_level = 2 THEN p_p_turn WHEN group_level = 3 THEN p_p_p_turn WHEN group_level = 4 THEN p_p_p_p_turn END) * 1000000000000 AS t_0,
                  (CASE WHEN group_level = 0 THEN null WHEN group_level = 1 THEN turn WHEN group_level = 2 THEN p_turn WHEN group_level = 3 THEN p_p_turn WHEN group_level = 4 THEN p_p_p_turn END) * 10000000000 AS t_1,
                  (CASE WHEN group_level = 0 THEN null WHEN group_level = 1 THEN null WHEN group_level = 2 THEN turn WHEN group_level = 3 THEN p_turn WHEN group_level = 4 THEN p_p_turn END) * 1000000 AS t_2,
                  (CASE WHEN group_level = 0 THEN null WHEN group_level = 1 THEN null WHEN group_level = 2 THEN null WHEN group_level = 3 THEN turn WHEN group_level = 4 THEN p_turn END) * 1000 AS t_3,
                  (CASE WHEN group_level = 0 THEN null WHEN group_level = 1 THEN null WHEN group_level = 2 THEN null WHEN group_level = 3 THEN null WHEN group_level = 4 THEN turn END) * 1000 AS t_4,

                  p_id,
                  p_turn,
                  p_p_id,
                  p_p_turn,
                  p_p_p_id,
                  p_p_p_turn,
                  p_p_p_p_id,
                  p_p_p_p_turn
           FROM
               (SELECT gt.id, gt.turn, gt.name,
                       gt.parent_group_id AS p_id,

                       (CASE WHEN gt.parent_group_id IS NULL THEN 0 ELSE
                           (CASE WHEN p_gt.parent_group_id IS NULL THEN 1 ELSE
                               (CASE WHEN pp_gt.parent_group_id IS NULL THEN 2 ELSE
                                   (CASE WHEN ppp_gt.parent_group_id IS NULL THEN 3 ELSE 4 END)
                                   END)
                               END)
                           END) AS group_level,

                       p_gt.parent_group_id AS p_p_id,
                       p_gt.turn AS p_turn,
                       pp_gt.parent_group_id AS p_p_p_id,
                       pp_gt.turn AS p_p_turn,
                       ppp_gt.parent_group_id AS p_p_p_p_id,
                       ppp_gt.turn AS p_p_p_turn,
                       pppp_gt.parent_group_id AS p_p_p_p_p_id,
                       pppp_gt.turn AS p_p_p_p_turn
                FROM ase.seed_production_cultures_groups AS gt
                         LEFT JOIN ase.seed_production_cultures_groups AS p_gt ON p_gt.id = gt.parent_group_id
                         LEFT JOIN ase.seed_production_cultures_groups AS pp_gt ON pp_gt.id = p_gt.parent_group_id
                         LEFT JOIN ase.seed_production_cultures_groups AS ppp_gt ON ppp_gt.id = pp_gt.parent_group_id
                         LEFT JOIN ase.seed_production_cultures_groups AS pppp_gt ON pppp_gt.id = ppp_gt.parent_group_id
               ) AS groups
          ) AS groups_ret
     ) AS groups_end
         LEFT JOIN ase.seed_production_cultures_groups_cultures AS culs ON culs.parent_group_id = groups_end.id
         LEFT JOIN ase.seed_production_cultures_groups AS grups ON grups.parent_group_id = groups_end.id
GROUP BY groups_end.id, groups_end.turn_in, groups_end.name, groups_end.parent_id, groups_end.group_level, groups_end.turn_all

ORDER BY groups_end.turn_all
