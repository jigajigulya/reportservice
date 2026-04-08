package org.genom.reportservice.repository;

import lombok.extern.slf4j.Slf4j;
import org.genom.reportservice.model.ContractorLite;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.Optional;

@SpringBootTest
@Slf4j
public class ContractorRepoTest {

    @Autowired
    private ContractorRepo contractorRepo;

    @Test
    public void test() {
        Optional<ContractorLite> byId = contractorRepo.findById(92389L);
        Assertions.assertTrue(byId.isPresent());
        log.info("cons {}", byId.get().getNameShort());
    }
}
