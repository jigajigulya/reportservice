package org.genom.reportservice.repository;


import com.gnm.model.common.DepartmentStructure;
import com.gnm.model.common.geo.TerRegion;
import org.genom.reportservice.criteria.SeedsBackFillCriteria;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest;
import org.springframework.boot.jdbc.test.autoconfigure.AutoConfigureTestDatabase;
import org.springframework.context.annotation.Import;

import java.time.LocalDateTime;

@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Import(BackFillRepository.class)
public class BackFillRepoTest {

    @Autowired
    private BackFillRepository backFillRepository;


    @Test
    public void testQuery() {
        LocalDateTime dateBegin = LocalDateTime.now().minusYears(1);
        LocalDateTime dateFinish = LocalDateTime.now();

        SeedsBackFillCriteria criteria = SeedsBackFillCriteria.builder()
                .terRegion(TerRegion.builder().id(53L).build())
                .departmentRegion(DepartmentStructure.builder().id(250L).build())
                .dateBegin(dateBegin)
                .dateFinish(dateFinish)
                .build();
        Assertions.assertFalse(backFillRepository.findForView(criteria).isEmpty());

    }
}
