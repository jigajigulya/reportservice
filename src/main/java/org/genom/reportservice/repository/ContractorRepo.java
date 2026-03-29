package org.genom.reportservice.repository;

import org.genom.reportservice.model.ContractorLite;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface ContractorRepo extends JpaRepository<ContractorLite, Long> {
}
