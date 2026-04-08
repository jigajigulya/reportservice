package org.genom.reportservice.model;

import com.gnm.interfaces.CalcNeedInt;
import com.gnm.model.common.Contractor;
import com.gnm.model.common.Culture;
import com.gnm.model.common.DepartmentStructure;
import com.gnm.model.common.OrganizationalForm;
import com.gnm.model.common.geo.TerTownship;
import jakarta.persistence.*;
import lombok.*;
import org.hibernate.annotations.Immutable;
import org.hibernate.annotations.Subselect;


import java.time.LocalDateTime;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@Entity
@Immutable
@Builder
@Subselect("WITH con_backfills AS (\n" +
        "    SELECT ase.seeds_backfills.id,\n" +
        "           contractor_id,\n" +
        "           culture_sort_code,\n" +
        "           common.culture_sorts.culture_id AS culture_id,\n" +
        "           fill_all,\n" +
        "           current_fill\n" +
        "    FROM ase.seeds_backfills\n" +
        "             JOIN common.culture_sorts ON ase.seeds_backfills.culture_sort_code = common.culture_sorts.code where date_finished isnull)\n" +
        "\n" +
        "\n" +
        "SELECT ase.seeds_needs.id,\n" +
        "       ase.seeds_needs.ter_township_id                               as township,\n" +
        "       ase.seeds_needs.contractor_id                                 as contractor,\n" +
        "       ter_townships.name as township_name, " +
        "       common.department_structure.parent_id                         as region,\n" +
        "       ase.seeds_needs.culture_id                                    as culture,\n" +
        "       contractors.organizational_form                                 as organizational_form_con,\n" +
        "       contractors.group_investor_id                                 as group_contractor_investor,\n" +
        "       culture_group_id                                 as culture_group,\n" +
        "       culture_season_id                                 as culture_season,\n" +
        "       cultures.name                                 as culture_name,\n" +
        "       SUM(ase.seeds_needs.need_count)                                  as need_count,\n" +
        "       ase.seeds_needs.deleted,\n" +
        "       ase.seeds_needs.date_begin as date_begin,\n" +
        "       ase.seeds_needs.date_end as date_end,\n" +
        "       (SELECT SUM(con_backfills.fill_all)\n" +
        "        FROM con_backfills\n" +
        "        WHERE ase.seeds_needs.contractor_id = con_backfills.contractor_id\n" +
        "          AND ase.seeds_needs.culture_id = con_backfills.culture_id) AS fill_sum,\n" +
        "       (SELECT SUM(con_backfills.current_fill)\n" +
        "        FROM con_backfills\n" +
        "        WHERE ase.seeds_needs.contractor_id = con_backfills.contractor_id\n" +
        "          AND ase.seeds_needs.culture_id = con_backfills.culture_id) AS current_fill_sum\n" +
        "\n" +
        "FROM ase.seeds_needs\n" +
        "         JOIN common.cultures\n" +
        "              ON ase.seeds_needs.culture_id = common.cultures.id\n" +
        "         JOIN common.department_structure ON ase.seeds_needs.department_id = common.department_structure.id\n" +
        "         JOIN common.contractors ON ase.seeds_needs.contractor_id = common.contractors.id\n" +
        "         JOIN common.ter_townships ON ase.seeds_needs.ter_township_id = common.ter_townships.id\n" +
        "\n" +
        "\n" +
        "GROUP BY ase.seeds_needs.id, ter_townships.name, seeds_needs.ter_township_id, common.contractors.name_short,\n" +
        "         ase.seeds_needs.contractor_id,contractors.organizational_form,contractors.group_investor_id,culture_group_id,culture_season_id, ase.seeds_needs.culture_id, common.cultures.name,\n" +
        "         common.department_structure.parent_id\n" +
        "\n" +
        "ORDER BY common.ter_townships.name, common.contractors.name_short, common.cultures.name"
)
public class CalculationNeed implements CalcNeedInt {
    @Id
    private Long id;
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "township")
    private TerTownshipLite township;
    /*@Column
    private String townshipName;*/
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "contractor")
    private ContractorLite contractor;
    /*@Column
    private Long organizationalFormCon;*/

    @Column(name = "region")
    private Long region;
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "culture")
    private CultureLite culture;
    private Long organizationalFormCon;
    private Long cultureGroup;
    private Long cultureSeason;
    private String cultureName;
    private Long groupContractorInvestor;
    private Double needCount;
    private Double fillSum;
    private Double currentFillSum;
    private LocalDateTime deleted;
    private LocalDateTime dateBegin;
    private LocalDateTime dateEnd;


    @Override
    public boolean conIsNull() {
        return contractor == null;
    }

    @Override
    public String getConNameShort() {
        return contractor.getNameShort();
    }

    @Override
    public boolean cultureIsNull() {
        return culture == null;
    }

    @Override
    public boolean townshipIsNull() {
        return township == null;
    }

    @Override
    public String getTownshipName() {
        return township.getName();
    }
}
