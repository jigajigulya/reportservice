package org.genom.reportservice.repository;



import com.gnm.enums.ReportKeysForFilterEnum;
import com.gnm.utils.HibernateUtil;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.genom.reportservice.LogExecTime;
import org.genom.reportservice.criteria.SeedsBackFillCriteria;
import org.genom.reportservice.model.SeedsBackFillView;
import org.hibernate.CacheMode;
import org.hibernate.HibernateException;
import org.hibernate.Session;
import org.hibernate.transform.Transformers;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@Repository
@Slf4j
public class BackFillRepository {
    @PersistenceContext
    private EntityManager entityManager;
    @Autowired
    private ResourceLoader resourceLoader;


    @SneakyThrows
    @Transactional(readOnly = true)
    @LogExecTime
    public List<SeedsBackFillView> findForView(SeedsBackFillCriteria seedsBackFillCriteria) {
//        LOGGER.info("findForView seedsBackFills for view");
        if (Objects.isNull(seedsBackFillCriteria)) return new ArrayList<>();

        Resource resource = resourceLoader.getResource("classpath:sql/backfillView.sql");
        String backfillsViewSql = "";
        try (InputStream str = resource.getInputStream()) {
            backfillsViewSql = new String(str.readAllBytes(), StandardCharsets.UTF_8);
        }
        new String(resource.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        Session unwrap = entityManager.unwrap(Session.class);
        return unwrap.createNativeQuery(String.format(backfillsViewSql, "    SELECT seeds_backfills.*\n" +
                        "    FROM ase.seeds_backfills\n" +
                        "             inner join common.department_structure on department_structure.id = seeds_backfills.department_id\n" +
                        "             inner join common.ter_townships on seeds_backfills.township_id = ter_townships.id\n" +
                        "             inner join common.ter_regions on ter_regions.id = ter_townships.region_id\n" +
                        "    where true \n" +
                        seedsBackFillCriteria.getTerRegionSQLClause() +
                        seedsBackFillCriteria.getDepartmentRegionSQLClause() +
                        seedsBackFillCriteria.getDepartmentsSQLClause() +
                        seedsBackFillCriteria.getDeletedSQLClause() +
                        seedsBackFillCriteria.getEndedSQLClause() +
                        seedsBackFillCriteria.getDateBeginBetweenSQLClause() +
                        seedsBackFillCriteria.getContractorSqlClause() +
                        seedsBackFillCriteria.fundTypePredicateSQL()
                ), SeedsBackFillView.class)
                .getResultList();


    }
}
