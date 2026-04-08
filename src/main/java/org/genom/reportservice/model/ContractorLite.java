package org.genom.reportservice.model;

import com.gnm.interfaces.ConInfoInt;
import com.gnm.interfaces.ContractorInt;
import com.gnm.model.common.geo.TerTownship;
import com.google.gson.annotations.Expose;
import jakarta.persistence.*;
import lombok.Getter;
import lombok.ToString;

import java.io.Serializable;

@Entity
@Table(name = "contractors", schema = "common")
public class ContractorLite implements Serializable, ConInfoInt {
    @Id
    private Long id;

    @Column(name = "name_short")
    @Getter
    private String nameShort;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "ter_township_id")
    @Getter
    private TerTownshipLite terrainTownship;

    @Override
    public String getTerrainTownshipName() {
        return terrainTownship == null ? null : terrainTownship.getName();
    }
}
