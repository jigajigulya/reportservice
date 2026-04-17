package org.genom.reportservice.model;

import com.gnm.model.ase.mon.MonCCCommon;
import com.gnm.model.ase.wrapper.SeedsInfoWrapper;
import com.gnm.utils.NumberUtils;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.genom.reportservice.interfaces.MonCCsctProjection;
import org.hibernate.annotations.Type;

import org.hibernate.proxy.HibernateProxy;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@Entity
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@Slf4j
public class MonCCSct implements Serializable, MonCCCommon, MonCCsctProjection {

    @Column
    @Id
    private Long rid;

    @Column
    private Long id;

    @Column
    private String type;

    @Column(name = "tree_parent_id")
    private Long treeParentId;

    @Column(name = "stc_object_id")
    private Long structureObjectId;

    @Column(name = "stc_parent_id")
    private Long structureParentId;

    @Column(name = "group_level")
    private Long groupLevel;

    @Column(name = "turn_in")
    private Long turnIn;
    @Column(name = "turn_all")
    private Long turnAll;

    @Column(name = "ter_federal_district_turn")
    private Long terFederalDistrictTurn;
    @Column(name = "ter_region_turn")
    private Long terRegionTurn;

    @Column(name = "view_name")
    private String nameView;
    @Column
    private String name;

    @Column(name = "cnt_groups")
    private Long countChildGroups;

    @Column(name = "cnt_cultures")
    private Long countChildCultures;

    @Column(name = "culture_name")
    private String cultureName;

    @Column(name = "culture_id")
    private Long cultureId;

    @Column(name = "sort_type")
    private String sortType;

    @Column(name = "sort_code")
    private String sortCode;

    @Column(name = "sort_name")
    private String sortName;

    @Column(name = "sort_region")
    private String sortRegion;

    @Column(name = "sort_year")
    private Integer sortYear;

    @Column(name = "sort_originator_main")
    private Long sortOriginatorMain;

    @Column(name = "sort_originator_main_name")
    private String sortOriginatorMainName;

    @Column(name = "sort_originator_main_country_code_iso")
    private String sortOriginatorMainCountryCodeIso;

    @Column(name = "sort_originator_main_country_name")
    private String sortOriginatorMainCountryName;

    @Column(name = "sort_country_code_iso")
    private String sortCountryCodeIso;

    @Column(name = "sort_country_name")
    private String sortCountryName;


    @Column(name = "seedprod_seeds_kind")
    private String seedprodSeedsKind;


    @Column(name = "qual_checked")
    private Double qualChecked;
    @Column(name = "qual_conditioned")
    private Double qualConditioned;
    @Column(name = "qual_ncon_all")
    private Double qualNConAll;
    @Column(name = "qual_ncon_pests")
    private Double qualNConPests;
    @Column(name = "qual_ncon_debris")
    private Double qualNConDebris;
    @Column(name = "qual_ncon_hum")
    private Double qualNConHum;
    @Column(name = "qual_ncon_germ")
    private Double qualNConGerm;
    @Column(name = "qual_ncon_germ_lt_10")
    private Double qualNConGermLT10;
    @Column(name = "qual_ncon_germ_10_20")
    private Double qualNConGerm1020;


    @Column(name = "count_backfills")
    private Long countBackfills;


    @Getter(AccessLevel.NONE)
    @Column(name = "ids_backfills",columnDefinition = "bigint[]")
    private Object idsBackFills;

    @Getter(AccessLevel.NONE)
    @Column(name = "backfills_data", columnDefinition = "jsonb")
    private String backfillsData;

    @Column(name = "count_infos")
    private Long countInfos;

    @Getter(AccessLevel.NONE)
    @Column(name = "ids_infos",columnDefinition = "bigint[]")
    private Object idsInfos;

    @Getter(AccessLevel.NONE)
    @Column(name = "infos_data", columnDefinition = "jsonb")
    private String infosData;

    @Column(name = "ter_region_id")
    private Long terRegionId;

    @Column(name = "ter_region_name")
    private String terRegionName;

    @Column(name = "ter_federal_district_id")
    private Long terFederalDistrictId;

    @Column(name = "ter_federal_district_name")
    private String terFederalDistrictName;


    @Column(name = "fill_all")
    private Double fillAll;

    @Column(name = "fill_seed_fund")
    private Double fillSeedFund;

    @Column(name = "fill_seed_for_sale")
    private Double fillSeedForSale;

    @Column(name = "fill_removed_seed_fund")
    private Double fillRemovedSeedFund;

    @Column(name = "fill_AVALIABLE")
    private Double fill_AVALIABLE;

    @Column(name = "fill_SOWN")
    private Double fill_SOWN;

    @Column(name = "fill_DELETED")
    private Double fill_DELETED;

    @Column(name = "fill_SOLD")
    private Double fill_SOLD;

    @Column(name = "fill_DEFECTED")
    private Double fill_DEFECTED;

    @Column(name = "fill_FED")
    private Double fill_FED;

    @Column(name = "fill_REFORMED")
    private Double fill_REFORMED;


    @Column(name = "ru_fill_all")
    private Double ru_fillAll;

    @Column(name = "ru_fill_localized")
    private Double ru_fillLocalized;


    @Column(name = "ru_fill_purchase_en")
    private Double ru_fillPurchaseEn;

    @Column(name = "en_fill_all")
    private Double en_fillAll;

    @Column(name = "en_fill_localized")
    private Double en_fillLocalized;

    @Column(name = "en_fill_purchase_en")
    private Double en_fillPurchaseEn;

    @Column(name = "en_fill_purchase_en_processed")
    private Double en_fillPurchaseEnProcessed;

    @Column(name = "nr_fill_all")
    private Double nr_fillAll;

    @Column(name = "nr_fill_purchase_en")
    private Double nr_fillPurchaseEn;

    @Column(name = "rp_nc")
    private Double rp_nc;

    @Column(name = "rp_os")
    private Double rp_os;

    @Column(name = "rp_es")
    private Double rp_es;

    @Column(name = "rp_rep_pp1")
    private Double rp_rep_pp1;

    @Column(name = "rp_rep_sse")
    private Double rp_rep_sse;

    @Column(name = "rp_rep_se")
    private Double rp_rep_se;

    @Column(name = "rp_rs_1")
    private Double rp_rs_1;

    @Column(name = "rp_rs_1_rst")
    private Double rp_rs_1_rst;

    @Column(name = "rp_rs_2")
    private Double rp_rs_2;

    @Column(name = "rp_rs_2_rst")
    private Double rp_rs_2_rst;

    @Column(name = "rp_rs_3")
    private Double rp_rs_3;

    @Column(name = "rp_rs_3_rst")
    private Double rp_rs_3_rst;

    @Column(name = "rp_rs_4")
    private Double rp_rs_4;

    @Column(name = "rp_rs_4_rst")
    private Double rp_rs_4_rst;

    @Column(name = "rp_f1")
    private Double rp_f1;

    @Column(name = "rp_rs_5b")
    private Double rp_rs_5b;

    @Column(name = "rp_rs_5b_rst")
    private Double rp_rs_5b_rst;

    @Column(name = "rp_not")
    private Double rp_not;


    @Column(name = "sa_all")
    private Double sa_all;

    @Column(name = "sa_nc")
    private Double sa_nc;
    @Column(name = "sa_os")
    private Double sa_os;
    @Column(name = "sa_es")
    private Double sa_es;
    @Column(name = "sa_rep_pp1")
    private Double sa_rep_pp1;
    @Column(name = "sa_rep_sse")
    private Double sa_rep_sse;
    @Column(name = "sa_rep_se")
    private Double sa_rep_se;
    @Column(name = "sa_rs_1")
    private Double sa_rs_1;
    @Column(name = "sa_rs_1_rst")
    private Double sa_rs_1_rst;
    @Column(name = "sa_rs_2")
    private Double sa_rs_2;
    @Column(name = "sa_rs_2_rst")
    private Double sa_rs_2_rst;
    @Column(name = "sa_rs_3")
    private Double sa_rs_3;
    @Column(name = "sa_rs_3_rst")
    private Double sa_rs_3_rst;
    @Column(name = "sa_rs_4")
    private Double sa_rs_4;
    @Column(name = "sa_rs_4_rst")
    private Double sa_rs_4_rst;
    @Column(name = "sa_f1")
    private Double sa_f1;
    @Column(name = "sa_rs_5b")
    private Double sa_rs_5b;
    @Column(name = "sa_rs_5b_rst")
    private Double sa_rs_5b_rst;
    @Column(name = "sa_not")
    private Double sa_not;

    @Column(name = "fill_purchase_en")
    private Double fillPurchaseEn;

    @Column(name = "fill_purchase_en_processed")
    private Double fillPurchaseEnProcessed;


    @Override
    public List<SeedsInfoWrapper> getBackfillsData() {
        return null;
    }

    @Override
    public List<SeedsInfoWrapper> getInfosData() {
        return null;
    }

    public Double getFillPurchaseEnPercAll(){
        return NumberUtils.calcPercent(fillSeedFund, fillPurchaseEn);
    }

    public Double getNrFillPurchaseEnPercAll(){
        return NumberUtils.calcPercent(fillSeedFund, nr_fillAll);
    }

    public Double getRuFillPurchaseEnPercAll(){
        return NumberUtils.calcPercent(fillSeedFund, ru_fillAll);
    }

    public Double getEnFillPurchaseEnPercAll(){
        return NumberUtils.calcPercent(fillSeedFund, en_fillAll);
    }

    public void clearZeros() {

        this.qualChecked = NumberUtils.clearZero(this.qualChecked);
        this.qualConditioned = NumberUtils.clearZero(this.qualConditioned);
        this.qualNConAll = NumberUtils.clearZero(this.qualNConAll);
        this.qualNConPests = NumberUtils.clearZero(this.qualNConPests);
        this.qualNConDebris = NumberUtils.clearZero(this.qualNConDebris);
        this.qualNConHum = NumberUtils.clearZero(this.qualNConHum);
        this.qualNConGerm = NumberUtils.clearZero(this.qualNConGerm);
        this.qualNConGermLT10 = NumberUtils.clearZero(this.qualNConGermLT10);
        this.qualNConGerm1020 = NumberUtils.clearZero(this.qualNConGerm1020);

        this.countBackfills = NumberUtils.clearZero(this.countBackfills);
        this.countInfos = NumberUtils.clearZero(this.countInfos);
        this.fillAll = NumberUtils.clearZero(this.fillAll);

        this.fillSeedFund = NumberUtils.clearZero(this.fillSeedFund);
        this.fillSeedForSale = NumberUtils.clearZero(this.fillSeedForSale);
        this.fillRemovedSeedFund = NumberUtils.clearZero(this.fillRemovedSeedFund);

        this.fill_AVALIABLE = NumberUtils.clearZero(this.fill_AVALIABLE);
        this.fill_SOWN = NumberUtils.clearZero(this.fill_SOWN);
        this.fill_DELETED = NumberUtils.clearZero(this.fill_DELETED);
        this.fill_SOLD = NumberUtils.clearZero(this.fill_SOLD);
        this.fill_DEFECTED = NumberUtils.clearZero(this.fill_DEFECTED);
        this.fill_FED = NumberUtils.clearZero(this.fill_FED);
        this.fill_REFORMED = NumberUtils.clearZero(this.fill_REFORMED);

        this.ru_fillAll = NumberUtils.clearZero(this.ru_fillAll);
        this.ru_fillLocalized = NumberUtils.clearZero(this.ru_fillLocalized);
        this.ru_fillPurchaseEn = NumberUtils.clearZero(this.ru_fillPurchaseEn);
        this.en_fillAll = NumberUtils.clearZero(this.en_fillAll);
        this.en_fillLocalized = NumberUtils.clearZero(this.en_fillLocalized);
        this.en_fillPurchaseEn = NumberUtils.clearZero(this.en_fillPurchaseEn);
        this.en_fillPurchaseEnProcessed = NumberUtils.clearZero(this.en_fillPurchaseEnProcessed);
        this.nr_fillAll = NumberUtils.clearZero(this.nr_fillAll);
        this.nr_fillPurchaseEn = NumberUtils.clearZero(this.nr_fillPurchaseEn);
        this.rp_nc = NumberUtils.clearZero(this.rp_nc);
        this.rp_os = NumberUtils.clearZero(this.rp_os);
        this.rp_es = NumberUtils.clearZero(this.rp_es);

        this.rp_rep_pp1 = NumberUtils.clearZero(this.rp_rep_pp1);
        this.rp_rep_sse = NumberUtils.clearZero(this.rp_rep_sse);
        this.rp_rep_se = NumberUtils.clearZero(this.rp_rep_se);

        this.rp_rs_1 = NumberUtils.clearZero(this.rp_rs_1);
        this.rp_rs_1_rst = NumberUtils.clearZero(this.rp_rs_1_rst);
        this.rp_rs_2 = NumberUtils.clearZero(this.rp_rs_2);
        this.rp_rs_2_rst = NumberUtils.clearZero(this.rp_rs_2_rst);
        this.rp_rs_3 = NumberUtils.clearZero(this.rp_rs_3);
        this.rp_rs_3_rst = NumberUtils.clearZero(this.rp_rs_3_rst);
        this.rp_rs_4 = NumberUtils.clearZero(this.rp_rs_4);
        this.rp_rs_4_rst = NumberUtils.clearZero(this.rp_rs_4_rst);
        this.rp_f1 = NumberUtils.clearZero(this.rp_f1);
        this.rp_rs_5b = NumberUtils.clearZero(this.rp_rs_5b);
        this.rp_rs_5b_rst = NumberUtils.clearZero(this.rp_rs_5b_rst);
        this.rp_not = NumberUtils.clearZero(this.rp_not);

        this.sa_all = NumberUtils.clearZero(this.sa_all);
        this.sa_nc = NumberUtils.clearZero(this.sa_nc);
        this.sa_os = NumberUtils.clearZero(this.sa_os);
        this.sa_es = NumberUtils.clearZero(this.sa_es);
        this.sa_rep_pp1 = NumberUtils.clearZero(this.sa_rep_pp1);
        this.sa_rep_sse = NumberUtils.clearZero(this.sa_rep_sse);
        this.sa_rep_se = NumberUtils.clearZero(this.sa_rep_se);
        this.sa_rs_1 = NumberUtils.clearZero(this.sa_rs_1);
        this.sa_rs_1_rst = NumberUtils.clearZero(this.sa_rs_1_rst);
        this.sa_rs_2 = NumberUtils.clearZero(this.sa_rs_2);
        this.sa_rs_2_rst = NumberUtils.clearZero(this.sa_rs_2_rst);
        this.sa_rs_3 = NumberUtils.clearZero(this.sa_rs_3);
        this.sa_rs_3_rst = NumberUtils.clearZero(this.sa_rs_3_rst);
        this.sa_rs_4 = NumberUtils.clearZero(this.sa_rs_4);
        this.sa_rs_4_rst = NumberUtils.clearZero(this.sa_rs_4_rst);
        this.sa_f1 = NumberUtils.clearZero(this.sa_f1);
        this.sa_rs_5b = NumberUtils.clearZero(this.sa_rs_5b);
        this.sa_rs_5b_rst = NumberUtils.clearZero(this.sa_rs_5b_rst);
        this.sa_not = NumberUtils.clearZero(this.sa_not);

        this.fillPurchaseEn = NumberUtils.clearZero(this.fillPurchaseEn);
        this.fillPurchaseEnProcessed = NumberUtils.clearZero(this.fillPurchaseEnProcessed);
    }


    @Override
    public void sum(MonCCCommon argument) {

        this.setQualChecked(NumberUtils.sumDoublesNative(this.getQualChecked(), argument.getQualChecked()));
        this.setQualConditioned(NumberUtils.sumDoublesNative(this.getQualConditioned(), argument.getQualConditioned()));
        this.setQualNConAll(NumberUtils.sumDoublesNative(this.getQualNConAll(), argument.getQualNConAll()));
        this.setQualNConPests(NumberUtils.sumDoublesNative(this.getQualNConPests(), argument.getQualNConPests()));
        this.setQualNConDebris(NumberUtils.sumDoublesNative(this.getQualNConDebris(), argument.getQualNConDebris()));
        this.setQualNConHum(NumberUtils.sumDoublesNative(this.getQualNConHum(), argument.getQualNConHum()));
        this.setQualNConGerm(NumberUtils.sumDoublesNative(this.getQualNConGerm(), argument.getQualNConGerm()));
        this.setQualNConGermLT10(NumberUtils.sumDoublesNative(this.getQualNConGermLT10(), argument.getQualNConGermLT10()));
        this.setQualNConGerm1020(NumberUtils.sumDoublesNative(this.getQualNConGerm1020(), argument.getQualNConGerm1020()));

        this.setCountBackfills(NumberUtils.sumLongsNative(this.getCountBackfills(), argument.getCountBackfills()));
        this.setCountInfos(NumberUtils.sumLongsNative(this.getCountInfos(), argument.getCountInfos()));

        this.setFillAll(NumberUtils.sumDoublesNative(this.getFillAll(), argument.getFillAll()));

        this.setFillSeedFund(NumberUtils.sumDoublesNative(this.getFillSeedFund(), argument.getFillSeedFund()));
        this.setFillSeedForSale(NumberUtils.sumDoublesNative(this.getFillSeedForSale(), argument.getFillSeedForSale()));
        this.setFillRemovedSeedFund(NumberUtils.sumDoublesNative(this.getFillRemovedSeedFund(), argument.getFillRemovedSeedFund()));


        this.setFill_AVALIABLE(NumberUtils.sumDoublesNative(this.getFill_AVALIABLE(), argument.getFill_AVALIABLE()));
        this.setFill_SOWN(NumberUtils.sumDoublesNative(this.getFill_SOWN(), argument.getFill_SOWN()));
        this.setFill_DELETED(NumberUtils.sumDoublesNative(this.getFill_DELETED(), argument.getFill_DELETED()));
        this.setFill_SOLD(NumberUtils.sumDoublesNative(this.getFill_SOLD(), argument.getFill_SOLD()));
        this.setFill_DEFECTED(NumberUtils.sumDoublesNative(this.getFill_DEFECTED(), argument.getFill_DEFECTED()));
        this.setFill_FED(NumberUtils.sumDoublesNative(this.getFill_FED(), argument.getFill_FED()));
        this.setFill_REFORMED(NumberUtils.sumDoublesNative(this.getFill_REFORMED(), argument.getFill_REFORMED()));

        this.setRu_fillAll(NumberUtils.sumDoublesNative(this.getRu_fillAll(), argument.getRu_fillAll()));
        this.setRu_fillLocalized(NumberUtils.sumDoublesNative(this.getRu_fillLocalized(), argument.getRu_fillLocalized()));
        this.setRu_fillPurchaseEn(NumberUtils.sumDoublesNative(this.getRu_fillPurchaseEn(), argument.getRu_fillPurchaseEn()));

        this.setEn_fillAll(NumberUtils.sumDoublesNative(this.getEn_fillAll(), argument.getEn_fillAll()));
        this.setEn_fillLocalized(NumberUtils.sumDoublesNative(this.getEn_fillLocalized(), argument.getEn_fillLocalized()));
        this.setEn_fillPurchaseEn(NumberUtils.sumDoublesNative(this.getEn_fillPurchaseEn(), argument.getEn_fillPurchaseEn()));
        this.setEn_fillPurchaseEnProcessed(NumberUtils.sumDoublesNative(this.getEn_fillPurchaseEnProcessed(), argument.getEn_fillPurchaseEnProcessed()));

        this.setNr_fillAll(NumberUtils.sumDoublesNative(this.getNr_fillAll(), argument.getNr_fillAll()));
        this.setNr_fillPurchaseEn(NumberUtils.sumDoublesNative(this.getNr_fillPurchaseEn(), argument.getNr_fillPurchaseEn()));

        this.setRp_nc(NumberUtils.sumDoublesNative(this.getRp_nc(), argument.getRp_nc()));
        this.setRp_os(NumberUtils.sumDoublesNative(this.getRp_os(), argument.getRp_os()));
        this.setRp_es(NumberUtils.sumDoublesNative(this.getRp_es(), argument.getRp_es()));

        this.setRp_rep_pp1(NumberUtils.sumDoublesNative(this.getRp_rep_pp1(), argument.getRp_rep_pp1()));
        this.setRp_rep_sse(NumberUtils.sumDoublesNative(this.getRp_rep_sse(), argument.getRp_rep_sse()));
        this.setRp_rep_se(NumberUtils.sumDoublesNative(this.getRp_rep_se(), argument.getRp_rep_se()));

        this.setRp_rs_1(NumberUtils.sumDoublesNative(this.getRp_rs_1(), argument.getRp_rs_1()));
        this.setRp_rs_1_rst(NumberUtils.sumDoublesNative(this.getRp_rs_1_rst(), argument.getRp_rs_1_rst()));
        this.setRp_rs_2(NumberUtils.sumDoublesNative(this.getRp_rs_2(), argument.getRp_rs_2()));
        this.setRp_rs_2_rst(NumberUtils.sumDoublesNative(this.getRp_rs_2_rst(), argument.getRp_rs_2_rst()));
        this.setRp_rs_3(NumberUtils.sumDoublesNative(this.getRp_rs_3(), argument.getRp_rs_3()));
        this.setRp_rs_3_rst(NumberUtils.sumDoublesNative(this.getRp_rs_3_rst(), argument.getRp_rs_3_rst()));
        this.setRp_rs_4(NumberUtils.sumDoublesNative(this.getRp_rs_4(), argument.getRp_rs_4()));
        this.setRp_rs_4_rst(NumberUtils.sumDoublesNative(this.getRp_rs_4_rst(), argument.getRp_rs_4_rst()));
        this.setRp_f1(NumberUtils.sumDoublesNative(this.getRp_f1(), argument.getRp_f1()));
        this.setRp_rs_5b(NumberUtils.sumDoublesNative(this.getRp_rs_5b(), argument.getRp_rs_5b()));
        this.setRp_rs_5b_rst(NumberUtils.sumDoublesNative(this.getRp_rs_5b_rst(), argument.getRp_rs_5b_rst()));
        this.setRp_not(NumberUtils.sumDoublesNative(this.getRp_not(), argument.getRp_not()));

        this.setSa_all(NumberUtils.sumDoublesNative(this.getSa_nc(), argument.getSa_all()));
        this.setSa_nc(NumberUtils.sumDoublesNative(this.getSa_nc(), argument.getSa_nc()));
        this.setSa_os(NumberUtils.sumDoublesNative(this.getSa_os(), argument.getSa_os()));
        this.setSa_es(NumberUtils.sumDoublesNative(this.getSa_es(), argument.getSa_es()));
        this.setSa_rep_pp1(NumberUtils.sumDoublesNative(this.getSa_rep_pp1(), argument.getSa_rep_pp1()));
        this.setSa_rep_sse(NumberUtils.sumDoublesNative(this.getSa_rep_sse(), argument.getSa_rep_sse()));
        this.setSa_rep_se(NumberUtils.sumDoublesNative(this.getSa_rep_se(), argument.getSa_rep_se()));
        this.setSa_rs_1(NumberUtils.sumDoublesNative(this.getSa_rs_1(), argument.getSa_rs_1()));
        this.setSa_rs_1_rst(NumberUtils.sumDoublesNative(this.getSa_rs_1_rst(), argument.getSa_rs_1_rst()));
        this.setSa_rs_2(NumberUtils.sumDoublesNative(this.getSa_rs_2(), argument.getSa_rs_2()));
        this.setSa_rs_2_rst(NumberUtils.sumDoublesNative(this.getSa_rs_2_rst(), argument.getSa_rs_2_rst()));
        this.setSa_rs_3(NumberUtils.sumDoublesNative(this.getSa_rs_3(), argument.getSa_rs_3()));
        this.setSa_rs_3_rst(NumberUtils.sumDoublesNative(this.getSa_rs_3_rst(), argument.getSa_rs_3_rst()));
        this.setSa_rs_4(NumberUtils.sumDoublesNative(this.getSa_rs_4(), argument.getSa_rs_4()));
        this.setSa_rs_4_rst(NumberUtils.sumDoublesNative(this.getSa_rs_4_rst(), argument.getSa_rs_4_rst()));
        this.setSa_f1(NumberUtils.sumDoublesNative(this.getSa_f1(), argument.getSa_f1()));
        this.setSa_rs_5b(NumberUtils.sumDoublesNative(this.getSa_rs_5b(), argument.getSa_rs_5b()));
        this.setSa_rs_5b_rst(NumberUtils.sumDoublesNative(this.getSa_rs_5b_rst(), argument.getSa_rs_5b_rst()));
        this.setSa_not(NumberUtils.sumDoublesNative(this.getSa_not(), argument.getSa_not()));

        this.setFillPurchaseEn(NumberUtils.sumDoublesNative(this.getFillPurchaseEn(), argument.getFillPurchaseEn()));
        this.setFillPurchaseEnProcessed(NumberUtils.sumDoublesNative(this.getFillPurchaseEnProcessed(), argument.getFillPurchaseEnProcessed()));
    }

    @Override
    public final boolean equals(Object o) {
        if (this == o) return true;
        if (o == null) return false;
        Class<?> oEffectiveClass = o instanceof HibernateProxy ? ((HibernateProxy) o).getHibernateLazyInitializer().getPersistentClass() : o.getClass();
        Class<?> thisEffectiveClass = this instanceof HibernateProxy ? ((HibernateProxy) this).getHibernateLazyInitializer().getPersistentClass() : this.getClass();
        if (thisEffectiveClass != oEffectiveClass) return false;
        MonCCSct monCCSct = (MonCCSct) o;
        return getRid() != null && Objects.equals(getRid(), monCCSct.getRid());
    }

    @Override
    public final int hashCode() {
        return this instanceof HibernateProxy ? ((HibernateProxy) this).getHibernateLazyInitializer().getPersistentClass().hashCode() : getClass().hashCode();
    }

    @Override
    public Object backFillsIds() {
        return idsBackFills;
    }

    @Override
    public String backfillsData() {
        return backfillsData;
    }

    @Override
    public Object idsInfo() {
        return idsInfos;
    }

    @Override
    public String infosData() {
        return infosData;
    }
}
