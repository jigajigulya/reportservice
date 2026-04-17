package org.genom.reportservice.service;

import com.gnm.enums.ase.SeedProductionReportKindEnum;
import com.gnm.enums.ase.SeedProductionReportTypeEnum;
import com.gnm.model.ase.calc.SeedProductionMonStructureCommon;
import com.gnm.model.ase.mon.AssaySeedProdFullInfo;
import com.gnm.model.ase.mon.M10GDSct;
import com.gnm.model.ase.mon.Mon89Sct;
import com.gnm.model.ase.mon.MonCCSct;
import com.gnm.utils.Chronograph;
import com.gnm.utils.NumberUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.genom.reportservice.interfaces.SeedProductionParameterInt;
import org.genom.reportservice.repository.SeedProdRepository;
import org.genom.reportservice.utils.TreeNode;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.gnm.enums.ase.SeedProductionReportKindEnum.KIND_CULTURES_GROUPS;
import static com.gnm.enums.ase.SeedProductionReportKindEnum.KIND_REGIONS;
import static com.gnm.enums.ase.SeedProductionServiceTypeEnum.APPROBATION;
import static com.gnm.enums.ase.SeedProductionServiceTypeEnum.REGISTRATION;

@Service
@RequiredArgsConstructor
@Slf4j
public class SeedProductionMonitoringService {



    private final SeedProdRepository seedProdRepository;





    public TreeNode initSeedProductionMonitoringStructure(SeedProductionParameterInt seedProductionParameter) {


        log.info("seedProductionParameter type - " +
                seedProductionParameter.getSeedProdReportType() + " filterGroup " + seedProductionParameter.getFilterGroupName() +
                " ter_reg - " + seedProductionParameter.getRegionsJoinName() + " sep_reg - " + seedProductionParameter.getRegionDepartments() + " twns - " + seedProductionParameter.getTownships() + " deps - " + seedProductionParameter.getTownshipDepartments());


        if (SeedProductionReportTypeEnum.TYPE_M89.equals(seedProductionParameter.getSeedProdReportType())) {
            Chronograph.start(0);

            TreeNode root = new TreeNode(Mon89Sct.builder().rid(-1L).title("root").build(), null);
            List<Mon89Sct> stcs = seedProdRepository.getM89Structures(seedProductionParameter);

            for (Mon89Sct sc : stcs) {
                sc.clearZeros();
                TreeNode node_sc = new TreeNode(sc, root);
            }

            log.info("this.getM30BStructures() - " + stcs.size() + " time - " + Chronograph.getTime(0));

            seedProductionParameter.getGroupsAll().clear();
            seedProductionParameter.getGroupsAll().addAll(stcs);

            seedProductionParameter.setAssaysAll(new ArrayList<>());

            return root;
        } else if (SeedProductionReportTypeEnum.TYPE_M30B.equals(seedProductionParameter.getSeedProdReportType())
                || SeedProductionReportTypeEnum.TYPE_M32B.equals(seedProductionParameter.getSeedProdReportType())
                || SeedProductionReportTypeEnum.TYPE_M33B.equals(seedProductionParameter.getSeedProdReportType())) {
            Chronograph.start(0);

            Map<String, TreeNode> map_groups = new HashMap<>();

            TreeNode root = new TreeNode(MonCCSct.builder().id(-1L).type("root").name("root").build(), null);


            List<MonCCSct> stcs = new ArrayList<>();
            if (SeedProductionReportTypeEnum.TYPE_M30B.equals(seedProductionParameter.getSeedProdReportType())) {
                stcs = seedProdRepository.getM30BStructures(seedProductionParameter);
            } else if (SeedProductionReportTypeEnum.TYPE_M33B.equals(seedProductionParameter.getSeedProdReportType())) {
                stcs = seedProdRepository.getM33BStructures(seedProductionParameter);
            } else if (SeedProductionReportTypeEnum.TYPE_M32B.equals(seedProductionParameter.getSeedProdReportType())) {
                stcs = seedProdRepository.getM32BStructures(seedProductionParameter);
            }

            String district = "";
            String region = "";
            String parent_type = "";
            String parent_id = "";

            int log_rows = 100;
            int log_rows_cnt = 0;


            boolean node_expanded = false;
            List<TreeNode> nodes_expanded = new ArrayList<>();

            for (MonCCSct sc : stcs) {
                district = null;
                region = null;
                node_expanded = false;
                parent_type = "";
                sc.clearZeros();

                TreeNode node_parent = root;
                String type = sc.getType();

                parent_id = "" + sc.getTreeParentId();

                if (KIND_REGIONS.equals(seedProductionParameter.getSeedProdReportKind())) {
                    if ("sort".equals(type)) {
                        parent_type = "culture";

                        district = String.valueOf(sc.getTerFederalDistrictId());
                        region = String.valueOf(sc.getTerRegionId());
                    } else if ("culture".equals(type)) {
                        parent_type = "group";

                        district = String.valueOf(sc.getTerFederalDistrictId());
                        region = String.valueOf(sc.getTerRegionId());
                    } else if ("group".equals(type)) {
                        parent_type = "group";

                        if (seedProductionParameter.getFilterGroupId() != null) {
                            if (seedProductionParameter.getFilterGroupId().equals(sc.getId())) {
                                parent_type = "region";
                                parent_id = "" + sc.getTerRegionId();
                            }
                        } else {
                            if (sc.getTreeParentId() != null) {
                                parent_type = "group";
                                parent_id = "" + sc.getTreeParentId();
                            } else {
                                parent_type = "region";
                                parent_id = "" + sc.getTerRegionId();
                            }
                        }

                        district = String.valueOf(sc.getTerFederalDistrictId());
                        region = String.valueOf(sc.getTerRegionId());
                    } else if ("region".equals(type)) {
                        parent_type = "district";

                        district = String.valueOf(sc.getTerFederalDistrictId());
                    } else if ("district".equals(type)) {
                        parent_type = "country";
                        node_expanded = true;
                    } else if ("country".equals(type)) {
                        parent_type = "root";
                        node_expanded = true;
                    }

//                    node_parent = map_groups.get(parent_type + "." + parent_id);

                } else if (KIND_CULTURES_GROUPS.equals(seedProductionParameter.getSeedProdReportKind())) {

                    if ("sort".equals(type)) {
                        parent_type = "culture";
                    } else if ("culture".equals(type)) {
                        parent_type = "group";
                    } else if ("group".equals(type)) {
                        if (sc.getTreeParentId() != null) {
                            parent_type = "group";

                            if (sc.getGroupLevel() <= 1) {
                                node_expanded = true;
                            }

                        } else {
                            parent_type = "root";
                            node_expanded = true;
                        }
                    }

                    district = String.valueOf(sc.getTerFederalDistrictId());
                    region = String.valueOf(sc.getTerRegionId());
                }

                node_parent = map_groups.get(district + "." + region + "." + parent_type + "." + parent_id);

                TreeNode node_sc = new TreeNode(sc, parent_type.equals("root") ? root : node_parent);
                map_groups.put(sc.getTerFederalDistrictId() + "." + sc.getTerRegionId() + "." +  sc.getType() + "." + sc.getId(), node_sc);
                if (log_rows_cnt < log_rows) {
//                    log.info("put " + sc.getTerFederalDistrictId() + "." + sc.getTerRegionId() + "." + sc.getType() + "." + sc.getId() + " - " + sc.getTurnAll() + " || " + district + "." + region + "." + parent_type + "." + parent_id + " - " + sc.getNameView());
                    log_rows_cnt++;
                }

                if (node_expanded == true) {
                    nodes_expanded.add(node_sc);
                }
            }

            if (seedProductionParameter.getGroupsAll() == null) {
                seedProductionParameter.setGroupsAll(new ArrayList<>());
            }

            seedProductionParameter.getGroupsAll().clear();
            seedProductionParameter.getGroupsAll().addAll(stcs);

            seedProductionParameter.setAssaysAll(new ArrayList<>());

//            if (seedProductionParameter.isAssaysInclude()) {
//                if (culture.getAssaysList() != null) {
//                    if (!culture.getAssaysList().isEmpty()) {
//                        assaysSeedAll.addAll(culture.getAssaysList());
//                    }
//                }
//            }

            String filterGroup = "null";

            if (seedProductionParameter.getFilterGroupId() != null) {
                filterGroup = seedProductionParameter.getFilterGroupId() + " [" + seedProductionParameter.getFilterGroupNameView() + "]";
            }

            log.info("this.getM30BStructures() - filterGroup " + filterGroup + " size - "  + stcs.size() + " exp - " + nodes_expanded.size() + " time - " + Chronograph.getTime(0));

//            map_groups.get(0L).setExpanded(true);

            for (TreeNode node : nodes_expanded) {
                node.setExpanded(true);
            }

            return root;
        }

        if (SeedProductionReportTypeEnum.TYPE_M10G.equals(seedProductionParameter.getSeedProdReportType()) || SeedProductionReportTypeEnum.TYPE_M10D.equals(seedProductionParameter.getSeedProdReportType())) {
            Chronograph.start(0);

            Map<String, TreeNode> map_groups = new HashMap<>();

            TreeNode root = new TreeNode(M10GDSct.builder().id(-1L).type("root").name("root").build(), null);

            List<M10GDSct> stcs = seedProdRepository.getM10GDStructures(seedProductionParameter);

            String parent_type = "";
            String parent_id = "";

            boolean node_expanded = false;
            List<TreeNode> nodes_expanded = new ArrayList<>();

            for (M10GDSct sc : stcs) {
//                TreeNode node_parent = root;
//
//                sc.clearZeros();
//
//                if (sc.getTreeParentId() != null) {
//                    node_parent = map_groups.get(sc.getTreeParentId());
//                }
//
//
//                TreeNode node_now = new TreeNode(sc, node_parent);
//                map_groups.put(sc.getId(), node_now);

                node_expanded = false;
                parent_type = "";
                sc.clearZeros();

                TreeNode node_parent = root;
                String type = sc.getType();

                parent_id = "" + sc.getTreeParentId();

                if (KIND_REGIONS.equals(seedProductionParameter.getSeedProdReportKind())) {
                    if ("category".equals(type)) {
                        parent_type = "culture";
                    } else if ("culture".equals(type)) {
                        parent_type = "group";
                    } else if ("group".equals(type)) {
                        if (seedProductionParameter.getFilterGroupId() != null) {
                            if (seedProductionParameter.getFilterGroupId().equals(sc.getId())) {
                                parent_type = "region";
                                parent_id = "" + sc.getTerRegionId();

                            } else {
                                parent_type = "group";
                            }

                        } else {
                            parent_type = "group";
                        }

//                        if (sc.getTreeParentId() != null) {
//
//                        } else {
//                            parent_type = "region";
//                            parent_id = "" + sc.getTerRegionId();
//                        }

                    } else if ("region".equals(type)) {
                        parent_type = "district";
                    } else if ("district".equals(type)) {
                        parent_type = "country";
                        node_expanded = true;
                    } else if ("country".equals(type)) {
                        parent_type = "root";
                        node_expanded = true;
                    }

                    node_parent = map_groups.get(parent_type + "." + parent_id);

                    TreeNode node_sc = new TreeNode(sc.getType(), sc, parent_type.equals("root") ? root : node_parent);
                    map_groups.put(sc.getType() + "." + sc.getId(), node_sc);

                    if (node_expanded == true) {
                        nodes_expanded.add(node_sc);
                    }

                } else if (KIND_CULTURES_GROUPS.equals(seedProductionParameter.getSeedProdReportKind())) {
                    sc.clearZeros();

                    parent_type = "common";

                    if (sc.getTreeParentId() != null) {
                        node_parent = map_groups.get(parent_type + "." + parent_id);
                    }

                    TreeNode node_now = new TreeNode(sc.getType(), sc, node_parent);
                    map_groups.put("common" + "." + sc.getId(), node_now);

//                    log.info(sc.getType() + "." + sc.getId() + " - " + sc.getNameView());

                }

            }

            if (seedProductionParameter.getGroupsAll() == null) {
                seedProductionParameter.setGroupsAll(new ArrayList<>());
            }

            seedProductionParameter.getGroupsAll().clear();
            seedProductionParameter.getGroupsAll().addAll(stcs);

            seedProductionParameter.setAssaysAll(new ArrayList<>());

//            if (seedProductionParameter.isAssaysInclude()) {
//                if (culture.getAssaysList() != null) {
//                    if (!culture.getAssaysList().isEmpty()) {
//                        assaysSeedAll.addAll(culture.getAssaysList());
//                    }
//                }
//            }

            log.info("this.getM10GStructures() - " + stcs.size() + " time - " + Chronograph.getTime(0));

            for (TreeNode nc : root.getChildren()) {
                nc.setExpanded(true);

                if ("country".equals(nc.getType())) {
                    for (TreeNode cnc : nc.getChildren()) {
                        cnc.setExpanded(true);

//                        if ("district".equals(cnc.getType())) {
//                            for (TreeNode dnc : cnc.getChildren()) {
//                                dnc.setExpanded(true);
//                            }
//                        }


                    }
                }
            }

//            map_groups.get(Long.valueOf(0L)).setExpanded(true);

            return root;
        }


        if (SeedProductionReportKindEnum.KIND_CULTURES_GROUPS.equals(seedProductionParameter.getSeedProdReportKind())) {
            TreeNode root = new TreeNode(new SeedProductionMonStructureCommon(-1L, "root", 0L, 0L, null, null, "root"), null);

            List<SeedProductionMonStructureCommon> groups = seedProdRepository.getSeedProdMonStructureCommonGroups();
            List<SeedProductionMonStructureCommon> cultures = seedProdRepository.getSeedProdMonStructureCommon(seedProductionParameter);
            List<AssaySeedProdFullInfo> assaysSeedProd = new ArrayList<>();
            List<AssaySeedProdFullInfo> assaysSeedAll = new ArrayList<>();


            List<TreeNode> tn_groups = new ArrayList<>();

            for (SeedProductionMonStructureCommon group : groups) {
                if (group.getParentId() != null) {
                    TreeNode node_parent = tn_groups.stream().filter(treeNode -> group.getParentId().equals(((SeedProductionMonStructureCommon) treeNode.getData()).getId())).findFirst().get();
                    TreeNode node = new TreeNode(group.getType(), group, node_parent);

                    node.setExpanded(group.getCountChildGroups() > 0);

                    tn_groups.add(node);
                } else {
                    TreeNode node = new TreeNode(group.getType(), group, root);

                    node.setExpanded(true);

                    tn_groups.add(node);
                }
            }

            for (SeedProductionMonStructureCommon culture : cultures) {
                if (culture.getParentId() != null) {
                    TreeNode node_parent = tn_groups.stream().filter(treeNode -> culture.getParentId().equals(((SeedProductionMonStructureCommon) treeNode.getData()).getId())).findFirst().get();

                    SeedProductionMonStructureCommon group = (SeedProductionMonStructureCommon) node_parent.getData();

                    if (group.getCountChildCultures() > 1) {
                        TreeNode node = new TreeNode(culture.getType(), culture, node_parent);
                        tn_groups.add(node);

                        culture.setGroupLevel(group.getGroupLevel() + 1);
                    } else if (group.getCountChildCultures() == 1 && group.getCountChildGroups() == 0) {
                        group.setAssayIds(culture.getAssayIds());
                    }

                    group.sum(culture);

                    TreeNode node_p = node_parent;
                    for (int i = group.getGroupLevel(); i >= 0; i--) {
                        node_p = node_p.getParent();

                        SeedProductionMonStructureCommon group_p = (SeedProductionMonStructureCommon) node_p.getData();
                        group_p.sum(culture);
                    }
                } else {
                    TreeNode node = new TreeNode(culture.getType(), culture, root);

                    culture.setGroupLevel(1);

                    tn_groups.add(node);
                }


                if (culture.getAssaysList() != null) {
                    if (!culture.getAssaysList().isEmpty()) {
                        if (APPROBATION.equals(seedProductionParameter.getSeedProductionServiceType())) {
                            assaysSeedProd.addAll(culture.getAssaysList().stream().filter(as -> NumberUtils.moreThan(as.getApp_tot_sum_seeds(), 0.0)).collect(Collectors.toList()));
                        } else if (REGISTRATION.equals(seedProductionParameter.getSeedProductionServiceType())) {
                            assaysSeedProd.addAll(culture.getAssaysList().stream().filter(as -> NumberUtils.moreThan(as.getReg_tot_sum_seeds(), 0.0)).collect(Collectors.toList()));
                        }
                    }
                }

                if (seedProductionParameter.isAssaysInclude()) {
                    if (culture.getAssaysList() != null) {
                        if (!culture.getAssaysList().isEmpty()) {
                            assaysSeedAll.addAll(culture.getAssaysList());
                        }
                    }
                }
            }

            SeedProductionMonStructureCommon.clearZeros(groups);
            SeedProductionMonStructureCommon.clearZeros(cultures);

            seedProductionParameter.setCultures(cultures);
            seedProductionParameter.setGroups(groups);
            seedProductionParameter.setAssaysSeedProd(assaysSeedProd);
            seedProductionParameter.setAssaysAll(assaysSeedAll);

            return root;
        } else if (KIND_REGIONS.equals(seedProductionParameter.getSeedProdReportKind())) {
            TreeNode root = new TreeNode(new SeedProductionMonStructureCommon(-1L, "root", 0L, 0L, null, null, "root"), null);

            List<SeedProductionMonStructureCommon> regions_groups = seedProdRepository.getSeedProdMonStructureCommon(seedProductionParameter);
            List<TreeNode> tn_groups = new ArrayList<>();

            TreeNode node_country = null;

            for (SeedProductionMonStructureCommon group : regions_groups) {
                if (group.getType().equals("country")) {
                    node_country = new TreeNode(group.getType(), group, root);
                    node_country.setExpanded(true);
                    break;
                }
            }

            for (SeedProductionMonStructureCommon group : regions_groups) {
                if (group.getType().equals("district")) {
                    TreeNode node = new TreeNode(group.getType(), group, node_country);
                    node.setExpanded(true);
                    tn_groups.add(node);
                }
            }

            for (SeedProductionMonStructureCommon group : regions_groups) {
                if (group.getType().equals("region")) {
                    TreeNode node_parent = tn_groups.stream().filter(treeNode -> group.getParentId().equals(((SeedProductionMonStructureCommon) treeNode.getData()).getObjectId())).findFirst().get();
                    TreeNode node = new TreeNode(group.getType(), group, node_parent);
                    node.setExpanded(true);
                }
            }

            SeedProductionMonStructureCommon.clearZeros(regions_groups);
            return root;
        }

        return null;
    }
}
