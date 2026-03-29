package org.genom.reportservice.model;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.ToString;

import java.io.Serializable;

@Entity
@Table(name = "contractors", schema = "common")
public class ContractorLite implements Serializable {
    @Id
    private Long id;

    @Column(name = "name_short")
    @Getter
    private String nameShort;
}
