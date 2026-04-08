package org.genom.reportservice.repository;

import com.gnm.enums.ContractorOrganizationalFormEnum;
import com.gnm.enums.ReportKeysForFilterEnum;
import com.gnm.model.common.*;
import com.gnm.model.common.geo.TerTownship;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest;
import org.springframework.boot.jdbc.test.autoconfigure.AutoConfigureTestDatabase;
import org.springframework.context.annotation.Import;

import java.util.HashMap;
import java.util.Map;

@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Import(SeedNeedsRepo.class)
public class SeedNeedsRepoTest {

    @Autowired
    private SeedNeedsRepo seedNeedsRepo;

    @Test
    public void getNeedsByFilteredMap() {
        Map<ReportKeysForFilterEnum, Object> obj = new HashMap<>();
        //obj.put(ReportKeysForFilterEnum.CULTURE, Culture.builder().id(344L).build());
        //obj.put(ReportKeysForFilterEnum.CONTRACTOR, Contractor.builder().id(104155L).build());
        //obj.put(ReportKeysForFilterEnum.ORG_TYPE, OrganizationalForm.builder().id(2L).build());
        //obj.put(ReportKeysForFilterEnum.SH_FORM, GroupContractorInvestor.builder().id(2L).build());
        /*obj.put(ReportKeysForFilterEnum.SEASON, CultureSeason.builder().id(2L).build());
        obj.put(ReportKeysForFilterEnum.TYPE_CULTURE, CultureGroup.builder().id(8L).build());*/
        obj.put(ReportKeysForFilterEnum.TOWNSHIP, TerTownship.builder().id(92252L).build());

        Assertions.assertFalse(seedNeedsRepo.getNeedsByFilteredMap(
                obj,
                DepartmentStructure.builder().id(2L).build()
        ).isEmpty());
    }
}
