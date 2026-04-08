package org.genom.reportservice.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Entity
@Table(name = "ter_townships")
@AllArgsConstructor
@NoArgsConstructor
@Getter
public class TerTownshipLite implements Serializable {
    @Id
    private Long id;

    @Column(name = "name")
    private String name;
}
