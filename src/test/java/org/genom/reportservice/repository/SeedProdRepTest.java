package org.genom.reportservice.repository;

import com.gnm.enums.ase.DataLayerSourceEnum;
import com.gnm.enums.ase.SeedProductionReportKindEnum;
import com.gnm.model.ase.mon.MonCCSct;
import org.checkerframework.checker.units.qual.A;
import org.genom.reportservice.model.SeedProdParam;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest;
import org.springframework.boot.jdbc.test.autoconfigure.AutoConfigureTestDatabase;
import org.springframework.context.annotation.Import;

import java.util.List;

@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Import(SeedProdRepository.class)
public class SeedProdRepTest {

    @Autowired
    private SeedProdRepository seedProdRepository;


    @Test
    public void testSql() {
        SeedProdParam seedProdParam = SeedProdParam.builder().build();
        seedProdParam.setSeedProdForHarvestYear(2025);
        seedProdParam.setSeedProdReportKind(SeedProductionReportKindEnum.KIND_CULTURES_GROUPS);
        seedProdParam.setDataLayerSource(DataLayerSourceEnum.FULL);
        seedProdParam.setAssaysInclude(true);

        List<MonCCSct> m30BStructures = seedProdRepository.getM30BStructures(seedProdParam);
        Assertions.assertFalse(m30BStructures.isEmpty());

    }
}
