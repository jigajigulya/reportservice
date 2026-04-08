package org.genom.reportservice.reporter;

import com.gnm.model.common.CropKindCulture;
import com.gnm.model.common.DepartmentStructure;
import com.gnm.model.common.geo.TerTownship;
import org.genom.reportservice.criteria.PhytoExpertizeCriteria;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.time.LocalDateTime;
import java.util.List;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class PhytoExpReporterTest {
    @Autowired
    private PhytoExpQualReporter reporter;

    @Test
    public void testReport() {
        List<DepartmentStructure> deps = List.of(DepartmentStructure.builder().id(2614L).build());
        List<TerTownship> towns = List.of(TerTownship.builder().id(85232L).build());
        LocalDateTime dateBegin = LocalDateTime.now().minusYears(1);
        LocalDateTime dateFinish = LocalDateTime.now();
        CropKindCulture cropKindCulture = new CropKindCulture();
        cropKindCulture.setId(118L);
        List<CropKindCulture> kinds = List.of(cropKindCulture);
        PhytoExpertizeCriteria criteria = PhytoExpertizeCriteria.builder()
                .departments(deps)
                .towns(towns)
                .dateBegin(dateBegin)
                .dateFinish(dateFinish)
                .selectedCropKinds(kinds)
                .build();
        byte[] download = reporter.download(criteria);
        Assertions.assertNotNull(download);


    }
}
