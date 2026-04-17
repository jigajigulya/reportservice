package org.genom.reportservice.service;

import com.gnm.enums.DepartmentStructureTypeEnum;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.genom.reportservice.repository.DepRepository;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
public class DepService {

    private final DepRepository depRepository;

    public Integer getCountDeps(DepartmentStructureTypeEnum departmentStructureTypeEnum, Long departmentRegionId) {
        log.info("get count deps info");
        return depRepository.countByTypeAndParentId(departmentStructureTypeEnum, departmentRegionId);
    }
}
