package org.genom.reportservice.repository;

import com.gnm.model.common.CropKindCulture;
import com.gnm.model.common.DepartmentStructure;
import com.gnm.model.common.geo.TerTownship;
import org.genom.reportservice.criteria.PhytoExpertizeCriteria;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest;
import org.springframework.boot.jdbc.test.autoconfigure.AutoConfigureTestDatabase;
import org.springframework.context.annotation.Import;

import java.time.LocalDateTime;
import java.util.List;

@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Import(PhytoExpertizeRepository.class)
public class PhytoExpertizeRepositoryTest {
    @Autowired
    private PhytoExpertizeRepository repository;

    @Test
    public void findForQualReport() {
        List<DepartmentStructure> deps = List.of(DepartmentStructure.builder().id(2614L).build());
        List<TerTownship> towns = List.of(TerTownship.builder().id(85232L).build());
        LocalDateTime dateBegin = LocalDateTime.now().minusYears(1);
        LocalDateTime dateFinish = LocalDateTime.now();
        CropKindCulture cropKindCulture = new CropKindCulture();
        cropKindCulture.setId(118L);
        List<CropKindCulture> kinds = List.of(cropKindCulture);
        Assertions.assertFalse(repository.findForQualReport(PhytoExpertizeCriteria.builder()
                .departments(deps)
                .towns(towns)
                .dateBegin(dateBegin)
                .dateFinish(dateFinish)
                .selectedCropKinds(kinds)
                .build()).isEmpty()
        );


    }

}
