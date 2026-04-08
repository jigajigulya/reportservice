package org.genom.reportservice.repository;

import com.gnm.enums.ReportKeysForFilterEnum;
import com.gnm.model.ase.SeedsNeed;
import com.gnm.model.common.*;

import com.gnm.model.common.geo.TerTownship;
import com.gnm.utils.HibernateUtil;
import com.gnm.utils.ReportUtils;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import jakarta.persistence.criteria.*;
import lombok.extern.slf4j.Slf4j;
import org.genom.reportservice.model.CalculationNeed;
import org.genom.reportservice.model.TerTownshipLite;
import org.hibernate.HibernateException;
import org.hibernate.Session;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Repository
@Slf4j
public class SeedNeedsRepo {

    @PersistenceContext
    private EntityManager em;

    public List<CalculationNeed> getNeedsByFilteredMap(Map<ReportKeysForFilterEnum, Object> map, DepartmentStructure region) {
        log.info("get needs by map {}", map);
        Session session = em.unwrap(Session.class);
        CriteriaBuilder criteriaBuilder = session.getCriteriaBuilder();
        CriteriaQuery<CalculationNeed> criteriaQuery = criteriaBuilder.createQuery(CalculationNeed.class);
        Root<CalculationNeed> rootNeed = criteriaQuery.from(CalculationNeed.class);
        List<Predicate> predicates = new ArrayList<>();
        predicates.add(criteriaBuilder.equal(rootNeed.get("region"), region.getId()));
        predicates.add(criteriaBuilder.isNull(rootNeed.get("deleted")));
        predicates.add(criteriaBuilder.greaterThanOrEqualTo(rootNeed.get("dateEnd"), LocalDateTime.now()));
        /*predicates.add(new DepartmentStructureCriteria(criteriaBuilder, rootNeed.get(SeedsNeed_.DEPARTMENT), null).availableClause());*/
        map.forEach((key, val) -> {
            switch (key) {
                case TOWNSHIP:
                    if (val != null) {
                        predicates.add(criteriaBuilder.equal(rootNeed.get("township").get("id"), ((TerTownship) val).getId()));
                    }
                    break;
                case CONTRACTOR:
                    if (val != null)
                        predicates.add(criteriaBuilder.equal(rootNeed.get("contractor").get("id"), ((Contractor) val).getId()));
                    break;
                case ORG_TYPE:
                    if (val != null) {

                        predicates.add(criteriaBuilder.equal(rootNeed.get("organizationalFormCon"), ((OrganizationalForm) val).getId()));
                    }
                    break;
                case SH_FORM:
                    if (val != null) {
                        predicates.add(criteriaBuilder.equal(rootNeed.get("groupContractorInvestor"), ((GroupContractorInvestor) val).getId()));
                    }
                    break;
                case SEASON:
                    if (val != null) {
                        predicates.add(criteriaBuilder.equal(rootNeed.get("cultureSeason"), ((CultureSeason) val).getId()));
                    }
                    break;
                case TYPE_CULTURE:
                    if (val != null) {
                        predicates.add(criteriaBuilder.equal(rootNeed.get("cultureGroup"), ((CultureGroup) val).getId()));
                    }
                    break;
                case CULTURE:
                    if (val != null)
                        predicates.add(rootNeed.get("culture").get("id").in(((Culture) val).getId()));
                    break;
                case NEED:
                    if (val != null && !ReportUtils.onlyGroupingType(map) && !ReportUtils.onlyGroupingType(map)) {
                        Double[] arr = ((List<Double>) val).toArray(Double[]::new);
                        if (arr[0] != null && arr[1] != null) {
                            if (arr[0] == 0.0) {
                                predicates.add(criteriaBuilder.or(criteriaBuilder.between(rootNeed.get("needCount"), arr[0], arr[1]), criteriaBuilder.isNull(rootNeed.get("needCount"))));
                            } else
                                predicates.add(criteriaBuilder.between(rootNeed.get("needCount"), arr[0], arr[1]));

                        }
                    }
                    break;
                case FILLED_UP:
                    if (val != null && !ReportUtils.onlyGroupingType(map)) {
                        Double[] arr = ((List<Double>) val).toArray(Double[]::new);
                        if (arr[0] != null && arr[1] != null) {
                            if (arr[0] == 0.0) {
                                predicates.add(criteriaBuilder.or(criteriaBuilder.between(rootNeed.get("fillSum"), arr[0], arr[1]), criteriaBuilder.isNull(rootNeed.get("fillSum"))));
                            } else
                                predicates.add(criteriaBuilder.between(rootNeed.get("fillSum"), arr[0], arr[1]));
                        }
                    }
                    break;
                case CURRENT_FILL:
                    if (val != null && !ReportUtils.onlyGroupingType(map)) {
                        Double[] arr = ((List<Double>) val).toArray(Double[]::new);
                        if (arr[0] != null && arr[1] != null) {
                            if (arr[0] == 0.0) {
                                predicates.add(criteriaBuilder.or(criteriaBuilder.between(rootNeed.get("currentFillSum"), arr[0], arr[1]), criteriaBuilder.isNull(rootNeed.get("currentFillSum"))));
                            } else
                                predicates.add(criteriaBuilder.between(rootNeed.get("currentFillSum"), arr[0], arr[1]));
                        }
                    }
                    break;
                case PROC_NEED:
                    if (val != null && !ReportUtils.onlyGroupingType(map)) {
                        Double[] arr = ((List<Double>) val).toArray(Double[]::new);
                        if (arr[0] != null && arr[1] != null) {
                            Expression<Double> exp = criteriaBuilder.prod(criteriaBuilder.quot(rootNeed.get("fillSum"), rootNeed.get("needCount")), 100).as(Double.class);
                            predicates.add(criteriaBuilder.and(criteriaBuilder.isNotNull(rootNeed.get("needCount")), criteriaBuilder.notEqual(rootNeed.get("needCount"), 0.0), criteriaBuilder.between(exp, arr[0], arr[1])));
                        }
                    }
                    break;

            }
        });

        return session.createQuery(criteriaQuery.
                        where(criteriaBuilder.and(predicates.toArray(new Predicate[predicates.size()])))).
                getResultList();

    }
}
