package org.genom.reportservice.reporter;

import com.gnm.enums.ReportKeysForFilterEnum;
import com.gnm.enums.ase.SeedsBackFillPurposeEnum;
import com.gnm.model.common.CultureSeason;
import com.gnm.model.common.DepartmentStructure;
import com.gnm.model.common.geo.TerRegion;
import com.gnm.model.common.geo.TerTownship;
import com.gnm.utils.GsonUtil;
import com.google.gson.Gson;
import org.genom.reportservice.conroller.ReportController;
import org.genom.reportservice.criteria.SeedsBackFillCriteria;
import org.genom.reportservice.model.BackFillApiParam;
import org.genom.reportservice.repository.BackFillRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.reactive.server.WebTestClient;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;

@SpringBootTest
public class BackFillReporterTest {


    @Autowired
    private BackFillReporter backFillReporter;

    @Test
    public void testReporter() {
        List<DepartmentStructure> deps = List.of(DepartmentStructure.builder().id(2614L).build());
        List<TerTownship> towns = List.of(TerTownship.builder().id(85232L).build());
        TerRegion region = TerRegion.builder().id(85L).build();
        LocalDateTime dateBegin = LocalDateTime.now().minusYears(1);
        LocalDateTime dateFinish = LocalDateTime.now();
        BackFillApiParam backFillApiParam = new BackFillApiParam();
        backFillApiParam.setCriteria(SeedsBackFillCriteria.builder()
                .dateBegin(dateBegin)
                .dateFinish(dateFinish)
                .departments(deps)
                .terRegion(region)
                .build());
        HashMap<ReportKeysForFilterEnum, Object> map = new HashMap<>();
        map.put(ReportKeysForFilterEnum.SEASON, CultureSeason.builder().id(1L).build());
        map.put(ReportKeysForFilterEnum.PURPOSE, SeedsBackFillPurposeEnum.OWN);
        backFillApiParam.setMap(map);
        Gson gsonForReport = GsonUtil.getGsonForReport();
        /*WebTestClient testClient = WebTestClient.bindToController(new ReportController(
                null,
                null,
                backFillReporter
                )).build();
        testClient = testClient.mutate()
                        .responseTimeout(Duration.ofSeconds(50000))
                                .build();
        testClient.post()
                .uri("/reports/backfillreport")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(gsonForReport.toJson(backFillApiParam))
                .exchange()
                .expectStatus().isOk();*/
    }
}
