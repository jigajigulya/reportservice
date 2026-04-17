package org.genom.reportservice.model;

import com.gnm.enums.DepartmentStructureTypeEnum;
import jakarta.persistence.*;
import lombok.Data;

import java.io.Serializable;

@Entity
@Table(name = "department_structure", schema = "common")
@Data
public class DepartmentLite implements Serializable {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column
    private String name;

    @Column
    @Enumerated(EnumType.STRING)
    private DepartmentStructureTypeEnum type;

    @Column(name = "parent_id")
    private Long parentId;

}
