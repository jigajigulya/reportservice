package org.genom.reportservice.service;

import com.gnm.dao.common.TerrainDAO;
import com.gnm.model.common.geo.TerRegion;
import com.gnm.model.common.geo.TerTownship;
import com.gnm.model.pmon.CommonAssayReport;
import com.gnm.model.pmon.calc.AssayCommonReport;
import lombok.RequiredArgsConstructor;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

@SpringBootTest

public class ReporterServiceTest {

    @Autowired
    private ReporterService reporterService;

    @Test
    public void findAssaysCommons() {

        /*List<TerTownship> tatar = List.of(TerTownship.builder().id(92201L).build());
        List<AssayCommonReport> assays = reporterService.findAssays(
                CommonAssayReport.builder().dateBegin(LocalDateTime.now().minusYears(1))
                        .dateEnd(LocalDateTime.now())
                        .townships(tatar)
                        .selectedTownships(tatar)
                        .build(),
                new ArrayList<>(),
                new ArrayList<>(),
                new ArrayList<>(),
                new ArrayList<>()
        );
        Assertions.assertTrue(!assays.isEmpty());*/
    }

}
