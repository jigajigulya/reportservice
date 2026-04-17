package org.genom.reportservice.repository;

import com.gnm.enums.DepartmentStructureTypeEnum;
import org.genom.reportservice.model.DepartmentLite;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface DepRepository extends JpaRepository<DepartmentLite, Long> {

    Integer countByTypeAndParentId(DepartmentStructureTypeEnum type, Long parentId);
}
