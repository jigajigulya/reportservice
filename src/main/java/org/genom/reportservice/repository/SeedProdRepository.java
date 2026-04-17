package org.genom.reportservice.repository;

import com.gnm.criteria.SeedProductionParameter;
import com.gnm.enums.ase.SeedProductionReportTypeEnum;
import com.gnm.model.ase.calc.SeedProductionMonStructureCommon;
import com.gnm.model.ase.mon.M10GDSct;
import com.gnm.model.ase.mon.Mon89Sct;
import com.gnm.model.ase.mon.MonCCSct;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.genom.reportservice.interfaces.SeedProductionParameterInt;
import org.hibernate.Session;
import org.hibernate.query.NativeQuery;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static com.gnm.enums.ase.SeedProductionReportKindEnum.KIND_REGIONS;
import static org.genom.reportservice.utils.SeedProdSQLUtil.constructDepClause;

@Repository
@Slf4j
public class SeedProdRepository {


    @Autowired
    private ResourceLoader resourceLoader;
    @PersistenceContext
    private EntityManager em;


    @SneakyThrows
    @Transactional(readOnly = true)
    public List<Mon89Sct> getM89Structures(SeedProductionParameterInt seedProductionParameter) {


        Session session = em.unwrap(Session.class);
        Resource resource = resourceLoader.getResource("classpath:sql/oper89Stc.sql");
        String str_query = resource.getContentAsString(StandardCharsets.UTF_8);

        String str_where = constructDepClause(seedProductionParameter, "assraw", "true");

        str_query = str_query.replaceAll(":dep_ter_structure", str_where);

        log.info("getM89Structures str_where " + str_where);

        NativeQuery query = session.createNativeQuery(str_query, Mon89Sct.class);

        query.setParameter("harvest_year", seedProductionParameter.getSeedProdForHarvestYear());
        query.setParameter("coef", seedProductionParameter.getAreaCoef());
        query.setParameter("details", seedProductionParameter.isAssaysInclude());

       // if (seedProductionParameter.isSetDataPeriod()) {
//                query.setParameter("dateBegin", seedProductionParameter.getDataDateTimeFrom());
//                query.setParameter("dateEnd", seedProductionParameter.getDataDateTimeTo());
        //}

        return query.getResultList();
    }




    @SneakyThrows
    @Transactional(readOnly = true)
    public List<MonCCSct> getM30BStructures(SeedProductionParameterInt seedProductionParameter) {


        //String str_query = readFile("30b_struct.sql", StandardCharsets.UTF_8);
        Session session = em.unwrap(Session.class);
        String str_query = resourceLoader.getResource("classpath:sql/30b_struct.sql").getContentAsString(StandardCharsets.UTF_8);

        /*String str_where = constructDepClause(seedProductionParameter, "seeds_and_info_simple", "true");

        str_query = str_query.replaceAll(":dep_ter_structure", "str_where");*/

        NativeQuery query = session.createNativeQuery(str_query, MonCCSct.class);

        query.setParameter("harvest_year", seedProductionParameter.getSeedProdForHarvestYear());

        Long filter_group = null;
        if (KIND_REGIONS.equals(seedProductionParameter.getSeedProdReportKind()) && seedProductionParameter.getFilterGroupId() != null) {
            filter_group = seedProductionParameter.getFilterGroupId();
        }

        query.setParameter("need_id", filter_group);
        query.setParameter("sel_type", seedProductionParameter.getSeedProdReportKind().name());
        query.setParameter("data_layer", seedProductionParameter.getDataLayerSource().name());

        query.setParameter("details", seedProductionParameter.isAssaysInclude());

        //if (seedProductionParameter.isSetDataPeriod()) {
//                query.setParameter("dateBegin", seedProductionParameter.getDataDateTimeFrom());
//                query.setParameter("dateEnd", seedProductionParameter.getDataDateTimeTo());
        //}

        return query.getResultList();

    }


    @SneakyThrows
    @Transactional(readOnly = true)
    public List<MonCCSct> getM33BStructures(SeedProductionParameterInt seedProductionParameter) {
        Session session = em.unwrap(Session.class);
        String str_query = resourceLoader.getResource("classpath:sql/33b_struct.sql").getContentAsString(StandardCharsets.UTF_8);

        String str_where = constructDepClause(seedProductionParameter, "seeds_and_info_simple", "true");

        str_query = str_query.replaceAll(":dep_ter_structure", str_where);

        NativeQuery query = session.createNativeQuery(str_query, MonCCSct.class);

        query.setParameter("harvest_year", seedProductionParameter.getSeedProdForHarvestYear());

        Long filter_group = null;
        if (KIND_REGIONS.equals(seedProductionParameter.getSeedProdReportKind()) && seedProductionParameter.getFilterGroupId() != null) {
            filter_group = seedProductionParameter.getFilterGroupId();
        }

        query.setParameter("need_id", filter_group);
        query.setParameter("sel_type", seedProductionParameter.getSeedProdReportKind().name());
        query.setParameter("data_layer", seedProductionParameter.getDataLayerSource().name());

        query.setParameter("details", seedProductionParameter.isAssaysInclude());

        //if (seedProductionParameter.isSetDataPeriod()) {
//                query.setParameter("dateBegin", seedProductionParameter.getDataDateTimeFrom());
//                query.setParameter("dateEnd", seedProductionParameter.getDataDateTimeTo());
        //}

        return query.getResultList();
    }

    @SneakyThrows
    @Transactional(readOnly = true)
    public List<MonCCSct> getM32BStructures(SeedProductionParameterInt seedProductionParameter) {
        Session session = em.unwrap(Session.class);
        String str_query = resourceLoader.getResource("classpath:sql/32b_struct.sql").getContentAsString(StandardCharsets.UTF_8);

        String str_where = constructDepClause(seedProductionParameter, "seeds_and_info_simple", "true");

        str_query = str_query.replaceAll(":dep_ter_structure", str_where);

        NativeQuery query = session.createNativeQuery(str_query, MonCCSct.class);

        query.setParameter("harvest_year", seedProductionParameter.getSeedProdForHarvestYear());

        Long filter_group = null;
        if (KIND_REGIONS.equals(seedProductionParameter.getSeedProdReportKind()) && seedProductionParameter.getFilterGroupId() != null) {
            filter_group = seedProductionParameter.getFilterGroupId();
        }

        query.setParameter("need_id", filter_group);
        query.setParameter("sel_type", seedProductionParameter.getSeedProdReportKind().name());
//            query.setParameter("data_layer", seedProductionParameter.getDataLayerSource().name());

        query.setParameter("details", seedProductionParameter.isAssaysInclude());

        //if (seedProductionParameter.isSetDataPeriod()) {
//                query.setParameter("dateBegin", seedProductionParameter.getDataDateTimeFrom());
//                query.setParameter("dateEnd", seedProductionParameter.getDataDateTimeTo());
        //}

        return query.getResultList();

    }

    @SneakyThrows
    @Transactional(readOnly = true)
    public List<M10GDSct> getM10GDStructures(SeedProductionParameterInt seedProductionParameter) {
        String str_query = resourceLoader.getResource("classpath:sql/10GD.sql").getContentAsString(StandardCharsets.UTF_8);
        Session session = em.unwrap(Session.class);
        NativeQuery<M10GDSct> query = session.createNativeQuery(str_query, M10GDSct.class);
        query.setParameter("harvest_year", seedProductionParameter.getSeedProdForHarvestYear());
        query.setParameter("detail_assays", seedProductionParameter.isAssaysInclude());

        Long filter_group = null;
        if (KIND_REGIONS.equals(seedProductionParameter.getSeedProdReportKind()) && seedProductionParameter.getFilterGroupId() != null) {
            filter_group = seedProductionParameter.getFilterGroupId();

            log.info("    ... filter_group " + filter_group + " " + seedProductionParameter.getFilterGroupObjectId());
        }

        query.setParameter("need_id", filter_group);
        query.setParameter("sel_type", seedProductionParameter.getSeedProdReportKind().name());


        if (SeedProductionReportTypeEnum.TYPE_M10G.equals(seedProductionParameter.getSeedProdReportType())) {
            query.setParameter("rep_type", "10G");
        } else if (SeedProductionReportTypeEnum.TYPE_M10D.equals(seedProductionParameter.getSeedProdReportType())) {
            query.setParameter("rep_type", "10D");
        }

        if (seedProductionParameter.isSetDataPeriod()) {
            query.setParameter("dateBegin", seedProductionParameter.getDataDateTimeFrom());
            query.setParameter("dateEnd", seedProductionParameter.getDataDateTimeTo());
        }

        return query.getResultList();

    }

    @SneakyThrows
    @Transactional(readOnly = true)
    public List<SeedProductionMonStructureCommon> getSeedProdMonStructureCommonGroups() {
        log.info("getSeedProdMonStructureCommonGroups");
        Session session = em.unwrap(Session.class);
        NativeQuery query = session.createNativeQuery(resourceLoader.getResource("classpath:/sql/monStrctureCommonGroups.sql")
                        .getContentAsString(StandardCharsets.UTF_8)
                , SeedProductionMonStructureCommon.class);
        return query.getResultList();

    }


    @SneakyThrows
    @Transactional(readOnly = true)
    public List<SeedProductionMonStructureCommon> getSeedProdMonStructureCommon(SeedProductionParameterInt seedProductionParameter) {
        log.info("get getSedProdMonStructure {}", "Структура мониторинга покультурно " + " seedProductionParameter.isSetDataPeriod() - " + seedProductionParameter.isSetDataPeriod());
        Session session = em.unwrap(Session.class);

        NativeQuery query = session.createNativeQuery(SeedCropsQueryBuilder.build(seedProductionParameter), SeedProductionMonStructureCommon.class);

        if (seedProductionParameter.isSetDataPeriod()) {
            query.setParameter("dateBegin", seedProductionParameter.getDataDateTimeFrom());
            query.setParameter("dateEnd", seedProductionParameter.getDataDateTimeTo());
        }

        query.setParameter("coef", seedProductionParameter.getAreaCoef());
        query.setParameter("massCoef", seedProductionParameter.getMassCoef());
        query.setParameter("harvest_year", seedProductionParameter.getSeedProdForHarvestYear());

        if (KIND_REGIONS.equals(seedProductionParameter.getSeedProdReportKind())) {
            query.setParameter("culture_group", seedProductionParameter.getFilterGroupId());
        }

        return query.getResultList();
    }
}
