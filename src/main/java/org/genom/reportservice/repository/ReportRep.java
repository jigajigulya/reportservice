package org.genom.reportservice.repository;

import com.gnm.enums.CropTypeEnum;
import com.gnm.enums.SourceCustomerEnum;
import com.gnm.model.common.MeasureUnit;
import com.gnm.model.common.geo.ClimaticZone;
import com.gnm.model.common.geo.TerTownship;
import com.gnm.model.pmon.AddDataField;
import com.gnm.model.pmon.CommonAssayReport;
import com.gnm.model.pmon.calc.AssayCommonReport;
import com.gnm.model.pmon.calc.CropTypeAndKindCulture;
import com.gnm.model.pmon.calc.PhytoSubjectState;
import com.gnm.model.pmon.calc.SubjectCommonReport;
import com.gnm.model.pmon.wrapper.PurposeWrapper;
import com.gnm.utils.Chronograph;
import com.gnm.utils.adapters.LocalDateAdapter;
import com.gnm.utils.adapters.LocalDateTimeAdapter;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import com.sun.jdi.LongType;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import jakarta.persistence.Query;
import org.genom.reportservice.comparator.commonReport.AssayReportWOClmComparator;
import org.genom.reportservice.comparator.commonReport.AssayReportWithClmComparator;
import org.hibernate.Session;
import org.hibernate.query.NativeQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.ObjectMapper;

import java.lang.reflect.Type;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.gnm.utils.CollectionUtils.isNotNullOrNotEmpty;
import static java.util.Optional.ofNullable;

@Repository
public class ReportRep {

    public static final Logger log = LoggerFactory.getLogger(ReportRep.class);

    @PersistenceContext
    private EntityManager em;
    private Gson gson;
    private Type subjectToken = new TypeToken<List<SubjectCommonReport>>() {
    }.getType();
    private Type fieldsToken = new TypeToken<List<AddDataField>>() {
    }.getType();

    @Transactional
    public List<AssayCommonReport> findAssaysStatesCommonReport(CommonAssayReport cmPar, List<PhytoSubjectState> weeds, List<PhytoSubjectState> diseases, List<PhytoSubjectState> pests, List<CropTypeAndKindCulture> typeAndKindCultures) {
        gson = new GsonBuilder()
                .serializeNulls()
                .registerTypeAdapter(LocalDate.class, new LocalDateAdapter())
                .registerTypeAdapter(LocalDateTime.class, new LocalDateTimeAdapter()).create();

        LocalDateTime dateBegin = cmPar.getDateBegin();
        LocalDateTime dateEnd = cmPar.getDateEnd();
        List<PurposeWrapper> purposes = cmPar.getSelectedPurposes();
        List<TerTownship> townships = cmPar.getSelectedTownships();
        List<ClimaticZone> zones = cmPar.getSelectedZones();
        List<MeasureUnit> units = cmPar.getUnits();
        if (dateBegin != null && dateEnd != null && (townships != null || zones != null) && weeds != null && diseases != null && pests != null) {
            boolean byZones = townships == null && zones != null;

            List<Long> unitsIds = new ArrayList<>();
            if (isNotNullOrNotEmpty(units)) {
                unitsIds = units.stream().map(MeasureUnit::getId).collect(Collectors.toList());
            }

            List<Long> twns_ids;
            List<Long> zons_ids;
            List<Long> subjects_ids = new ArrayList<>();
            ArrayList<String> names_purposes = new ArrayList<>();

            if (isNotNullOrNotEmpty(townships)) {
                twns_ids = townships.stream().map(TerTownship::getId).collect(Collectors.toList());
            } else {
                twns_ids = new ArrayList<>();
                twns_ids.add(0L);
            }

            if (isNotNullOrNotEmpty(zones)) {
                zons_ids = zones.stream().map(ClimaticZone::getId).collect(Collectors.toList());
            } else {
                zons_ids = new ArrayList<>();
                zons_ids.add(0L);
            }

            boolean purpose_null = false;

            if (isNotNullOrNotEmpty(purposes)) {
                for (PurposeWrapper purpose : purposes) {
                    if (purpose.getSourceCustomer() != null) {
                        names_purposes.add("'" + purpose.getSourceCustomer().toString() + "'");
                    } else {
                        names_purposes.add(null);
                        purpose_null = true;
                    }
                }
            } else {
                names_purposes = new ArrayList<>();
                names_purposes.add("''");
            }

            String s_names_purposes = String.join(",", names_purposes);


            if (isNotNullOrNotEmpty(weeds)) {
                subjects_ids.addAll(weeds.stream().map(PhytoSubjectState::getId).collect(Collectors.toList()));
            }

            if (isNotNullOrNotEmpty(diseases)) {
                subjects_ids.addAll(diseases.stream().map(PhytoSubjectState::getId).collect(Collectors.toList()));
            }

            if (isNotNullOrNotEmpty(pests)) {
                subjects_ids.addAll(pests.stream().map(PhytoSubjectState::getId).collect(Collectors.toList()));
            }
            if (!isNotNullOrNotEmpty(subjects_ids)) {
                subjects_ids.add(0L);
            }

            if (isNotNullOrNotEmpty(zones)) {
                zons_ids = zones.stream().map(ClimaticZone::getId).collect(Collectors.toList());
            } else {
                zons_ids = new ArrayList<>();
                zons_ids.add(0L);
            }


            StringBuilder ctakc_where = new StringBuilder("");

            if (isNotNullOrNotEmpty(typeAndKindCultures)) {
                ctakc_where.append(" AND " + (typeAndKindCultures.size() > 1 ? "(" : ""));
                boolean first = true;

                for (CropTypeAndKindCulture ctakc : typeAndKindCultures.stream().filter(cpKind -> cpKind.getCropType() != null || cpKind.getCropKindCulture() != null).collect(Collectors.toList())) {
                    ctakc_where.append((first) ? "" : " OR ");

                    if (CropTypeEnum.SOWING.equals(ctakc.getCropType()) && ctakc.getCropKindCulture() != null) {
                        ctakc_where.append("(assays.crop_current_croptype = '" + ctakc.getCropType().name() + "' AND cultures.crop_kind_culture_id = " + ctakc.getCropKindCulture().getId() + ")");
                    } else if (CropTypeEnum.SOWING.equals(ctakc.getCropType()) && ctakc.getCropKindCulture() == null) {
                        ctakc_where.append("(assays.crop_current_croptype = '" + ctakc.getCropType().name() + "' AND cultures.crop_kind_culture_id IS NULL)");
                    } else {
                        ctakc_where.append("assays.crop_current_croptype = '" + ctakc.getCropType().name() + "'");
                    }

                    first = false;
                }

                ctakc_where.append((typeAndKindCultures.size() > 1 ? ")" : ""));
            }
            String seedProdClause = "";
            if (cmPar.getSelectedCommonReportSeedProdFlagEnum() != null) {
                seedProdClause = switch (cmPar.getSelectedCommonReportSeedProdFlagEnum()) {
                    case ONLY_SEED_PROD -> " AND assays.is_seedprod ";
                    case WO_SEED_PROD -> " AND not assays.is_seedprod ";
                    default -> "";
                };
            }

            String att1OnlyClause = "";
            if (cmPar.isAtt1Include()) {
                att1OnlyClause = " AND (assay_subjects_states.att1_include = true or assay_subjects_states.is_winter_reserve)";
            }



                boolean statusesNonNulls = isNotNullOrNotEmpty(cmPar.getStatusList());

            Session unwrap = em.unwrap(Session.class);
            NativeQuery query = unwrap.createNativeQuery("with assays_criteria as (SELECT DISTINCT public.assays.id FROM public.assays\n" +
                    "LEFT JOIN " +
                    "(SELECT  CAST(CONCAT(COALESCE(subjects.phytosubject_code,100), COALESCE(subjects.measureunit_id,0), COALESCE(subjects.phase_evolution_id,0)) AS bigint) AS id, \n" +
                    "                subjects.assay_id AS assay_id, \n" +
                    "                subjects.phytosubject_code AS phytosubject_code, \n" +
                    "                subjects.phytosubject_name AS phytosubject_name, \n" +
                    "                subjects.phase_evolution_id AS subjectphaseevolution_id, \n" +
                    "                subjects.measureunit_id AS measureunit_id," +
                    "                subjects.att1_include," +
                    "               subjects.is_winter_reserve \n" +
                    "\n" +
                    "FROM (SELECT pmon.assay_subjects.assay_id, pmon.assay_subjects.phytosubject_code,pmon.assay_subjects.phyto_row_id notnull as att1_include,pmon.assay_subjects.is_winter_reserve,\n" +
                    "   (SELECT common.phyto_subjects.name FROM common.phyto_subjects WHERE common.phyto_subjects.code = pmon.assay_subjects.phytosubject_code LIMIT 1) AS phytosubject_name,\n" +
                    "   (CASE WHEN pmon.assay_subjects.phytotype_id = 3 THEN COALESCE(pmon.assay_subjects.measureunit_id, 68) WHEN pmon.assay_subjects.phytotype_id = 1 THEN COALESCE(pmon.assay_subjects.measureunit_id, 67) ELSE null END) AS measureunit_id, \n" +
                    "   (CASE WHEN pmon.assay_subjects.phytotype_id = 3 THEN COALESCE(pmon.assay_subjects.phase_evolution_id, 6) ELSE null END) AS phase_evolution_id\n" +
                    " FROM pmon.assay_subjects ) AS subjects) " +
                    " AS assay_subjects_states " + " ON assay_subjects_states.assay_id = public.assays.id\n" +
//                        "    JOIN pmon.assay_subjects ON pmon.assay_subjects.assay_id = public.assays.id\n" +
                    "    JOIN common.users ON common.users.id = assays.user_id\n" +
                    "    LEFT JOIN common.cultures ON common.cultures.id = public.assays.crop_current_culture_id\n" +
                    "    JOIN common.ter_townships ON assays.township_id = common.ter_townships.id\n" +
                    "     LEFT JOIN public.climatic_zones ON public.climatic_zones.gid = common.ter_townships.zone_id\n" +
                    "    WHERE date_assay BETWEEN :dateBegin AND :dateEnd\n" +
                    (statusesNonNulls ? " AND assays.status in (:statuses) " : "\n") +
                    (purpose_null ? " AND (source_customer IN (" + s_names_purposes + ") OR source_customer ISNULL)" : " AND source_customer IN (" + s_names_purposes + ")") +
                    (!unitsIds.isEmpty() ? " AND unit_id in (:unitsIds) " : "") +
                    (byZones ? " AND common.ter_townships.zone_id IN (:zones) " : " AND common.ter_townships.id IN (:townships) ") +
                    seedProdClause +
                    att1OnlyClause +
                    "        AND (assay_subjects_states.id IN (:subjects) OR assay_subjects_states.id IS NULL ) \n" +
                    ctakc_where.toString() + ") ,  subjects as (select subj.id                                          as id,\n" +
                    "                         subj.assay_id,\n" +
                    "                         subj.phytotype_id                                as phyto_type,\n" +
                    "                         phsubh.code,\n" +
                    "                         phsubh.name                                      as subject_name,\n" +
                    "                         phsubh.name_lat,\n" +
                    "                         phase.id                                         as phase_id,\n" +
                    "                         phase.name                                       as phase_name,\n" +
                    "                         mes.msu_id as unit_id,\n" +
                    "                         mes.msu_name_short                               as unit_name,\n" +
                    "                         subj.sub_protection_type                         as protection_type,\n" +
                    "                         subj.method,\n" +
                    "                         subj.turn,\n" +
                    "                         subj.sub_count                                   as count,\n" +
                    "                         coalesce(subj.not_found, false)                  as not_found,\n" +
                    "                         subj.sub_disease_r                               as disease_r,\n" +
                    "                         subj.sub_disease_p                               as disease_p,\n" +
                    "                         subj.stage_evolution                               as stage_evolution_id,\n" +
                    "                         phsubh.group_id                               as group_id,\n" +
                    "                         subj.sub_damage                               as damage,\n" +
                    "                         subj.damage_area                               as damage_area,\n" +
                    "                         subj.damage_sum                               as damage_sum,\n" +
                    "                         subj.financial_receipts                               as financial_receipts,\n" +
                    "                         subj.technologies                               as technologies,\n" +
                    "                         subj.percentage_of_viable                               as percentage_of_viable,\n" +
                    "                         phytoRow.report_row                               as phyto_report_row,\n" +
                    "                         subj.treatment_type                               as treatment_type,\n" +
                    "                         subj.treatment_type_detail                               as treatment_type_detail,\n" +
                    "                         subj.is_winter_reserve,\n" +
                    "                         subj.eth_from                               as eth_from,\n" +
                    "       jsonb_agg(cast(\n" +
                    "               ROW (subject_of_pest.id, subject_of_pest.assay_id, subject_of_pest.phytotype_id, subject_of_pest.phytosubject_code,null, null,subject_of_pest.phase_evolution_id,null,subject_of_pest.measureunit_id,null,null," +
                    "subject_of_pest.method,subject_of_pest.turn,subject_of_pest.sub_count,subject_of_pest.not_found,subject_of_pest.sub_disease_r,subject_of_pest.sub_disease_p,null,subject_of_pest.treatment_percentage," +
                    "subject_of_pest.treatment_pow,subject_of_pest.stage_evolution,phsub_of_pest.group_id,subject_of_pest.sub_damage,subject_of_pest.damage_sum,subject_of_pest.damage_area," +
                    "subject_of_pest.technologies,subject_of_pest.financial_receipts,subject_of_pest.percentage_of_viable, phytoRow_of_pest.report_row, phsub_of_pest_group.group_id,subject_of_pest.treatment_type,subject_of_pest.treatment_type_detail,subject_of_pest.eth_from,subject_of_pest.is_winter_reserve,null,null)" +
                    " as subject_wrapper_type)) as protection_processed_types,\n" +
                    "                         subj.treatment_percentage as treatment_percentage," +
                    "                         subj.treatment_pow as treatment_pow,\n" +
                    "                         group_subject.group_id              as parent_group_id\n" +
                    "                  from pmon.assay_subjects subj\n" +
                    "JOIN assays_criteria criteria ON criteria.id = subj.assay_id" +
                    "                           left join common.phyto_subjects phsubh on subj.phytosubject_code = phsubh.code\n" +
                    "                           left join common.phyto_subjects_groups group_subject on group_subject.id = phsubh.group_id\n" +
                    "                           left join pmon.phyto_rows phytoRow on subj.phyto_row_id = phytoRow.id\n" +
                    "                           left join common.subject_phase_evolution phase on phase.id = subj.phase_evolution_id\n" +
                    "                           left join common.meashure_units mes on mes.msu_id = subj.measureunit_id\n" +
                    "                           left join pmon.subjects_pesticides subject_pest on subject_pest.pesticide_id = subj.id\n" +
                    "                           left join pmon.assay_subjects subject_of_pest on subject_of_pest.id = subject_pest.subject_id" +
                    "                           left join common.phyto_subjects phsub_of_pest on phsub_of_pest.code = subject_of_pest.phytosubject_code" +
                    "                           left join common.phyto_subjects_groups phsub_of_pest_group on phsub_of_pest_group.id = phsub_of_pest.group_id" +
                    "                           left join pmon.phyto_rows phytoRow_of_pest on phytoRow_of_pest.id = subject_of_pest.phyto_row_id\n" +
                    "                  group by subj.id, phsubh.code, phsubh.name, phsubh.name_lat,group_subject.group_id, phase.id, phase.name,\n" +
                    "                           mes.msu_id, mes.msu_name_short,phsubh.group_id,phytoRow.report_row)\n" +
                    "\n" +
                    "\n" +
                    "select cast(assay.id as bigint),\n" +
                    "       assay.source_customer,\n" +
                    "       assay.date_assay            as date,\n" +
                    "       cast(twn.id as bigint)                      as twn_id,\n" +
                    "       twn.name                    as twn_name,\n" +
                    "       twn.region_id                    as region_id,\n" +
                    "       cast(cons.id as bigint)                     as contractor_id,\n" +
                    "       cons.name_short             as contractor_name,\n" +
                    "       cast(climatic.gid as bigint) as climatic_zone_id,\n" +
                    "       climatic.name as climatic_zone_name,\n" +
                    "       cast(climatic.turn as bigint) as climatic_zone_turn," +
                    "       assay.cropfield_area,\n" +
                    "       assay.cropfield_name,\n" +
                    "       assay.cropfield_number,\n" +
                    "       assay.polygon_area,\n" +
                    "       assay.crop_current_croptype as culture_crop_type,\n" +
                    "       kind_culture.name           as culture_crop_kind,\n" +
                    "       culture.name                as culture,\n" +
                    "       cast(culture.id as bigint)                as culture_id,\n" +
                    "       culture.culture_season_id                as culture_season,\n" +
                    "       assay.plants_count,\n" +
                    "       phase.name                  as culture_phase,\n" +
                    "       sort.name                   as culture_sort,\n" +
                    "       assay.is_protection                   as is_protection,\n" +
                    "       assay.adddata_fields,\n" +
                    "       cast(assay.unit_id as bigint),\n" +
                    "       jsonb_agg(cast(\n" +
                    "               ROW (subject.id, subject.assay_id, subject.phyto_type, subject.code,subject.subject_name," +
                    " subject.name_lat,subject.phase_id,subject.phase_name,subject.unit_id," +
                    "subject.unit_name,subject.protection_type,subject.method,subject.turn,subject.count,subject.not_found,subject.disease_r,subject.disease_p," +
                    "subject.protection_processed_types,subject.treatment_percentage,subject.treatment_pow,subject.stage_evolution_id,subject.group_id,subject.damage," +
                    "subject.damage_sum,subject.damage_area,subject.technologies,subject.financial_receipts,subject.percentage_of_viable," +
                    " subject.phyto_report_row,subject.parent_group_id,subject.treatment_type,subject.treatment_type_detail,subject.eth_from,subject.is_winter_reserve,null,null) as subject_wrapper_type)) as subjects\n" +
                    "from assays assay\n" +
                    "JOIN assays_criteria criteria ON criteria.id = assay.id" +
                    "         LEFT JOIN common.ter_townships twn on twn.id = assay.township_id\n" +

                    "         LEFT JOIN public.climatic_zones climatic on climatic.gid = twn.zone_id" +
                    "         LEFT JOIN common.contractors cons on cons.id = assay.contractor_id\n" +
                    "         LEFT JOIN common.cultures culture on culture.id = assay.crop_current_culture_id\n" +
                    "         LEFT JOIN common.crop_kinds_cultures kind_culture on culture.crop_kind_culture_id = kind_culture.id\n" +
                    "         LEFT JOIN common.culture_sorts sort on sort.code = assay.crop_current_culturesort_code\n" +
                    "         LEFT JOIN common.phases phase on phase.id = assay.culture_phase_id\n" +
                    "         LEFT JOIN subjects subject on subject.assay_id = assay.id "
                    + (cmPar.isExcludeMouseCurrentYear() ? " and case when (subject.group_id = 63 or subject.parent_group_id = 63) then extract(year from sowing_date) is distinct from extract(year from cast (:dateEnd as date)) else true end " : "") +
                    "         group by assay.id, twn.id, twn.name,twn.region_id, cons.id, cons.name_short,\n" +
                    "         climatic.gid,climatic.name,climatic.turn,kind_culture.name, culture.name, phase.name, sort.name,culture.id, culture.culture_season_id " + cmPar.readOrderBy());
            query.setParameter("dateBegin", dateBegin);
                query.setParameter("dateEnd", dateEnd);
                query.setParameter((byZones ? "zones" : "townships"), (byZones ? zons_ids : twns_ids));
                query.setParameter("subjects", subjects_ids);
                if (!unitsIds.isEmpty()) {
                    query.setParameter("unitsIds", unitsIds);
                }
                if (statusesNonNulls) {
                    query.setParameter("statuses", cmPar.getStatusList().stream().map(Enum::name).collect(Collectors.toList()));
                }


                Chronograph.start(0);

                log.info("CROP TYPE {}", ctakc_where);
            List resultList = query.getResultList();
            List<AssayCommonReport> assays = (List<AssayCommonReport>) resultList.stream().map(item -> convert((Object[]) item))
                        .collect(Collectors.toList());
                log.info("TRACE findAssaysStatesCommonReport {}", Chronograph.stop(0));
                if (cmPar.getOrderBy() == null)
                    assays.sort(byZones ? new AssayReportWithClmComparator() : new AssayReportWOClmComparator());
                return assays;

        }

        return new ArrayList<>();
    }

    private AssayCommonReport convert(Object[] objects) {

        return AssayCommonReport
                .builder()
                .id((Long)objects[0])
                .sourceCustomerEnum(ofNullable(objects[1]).map(item -> SourceCustomerEnum.valueOf((String) item)).orElse(null))
                .date((LocalDateTime) objects[2])
                .twnId(((Long) objects[3]))
                .twnName((String) objects[4])
                .regionId((Long) objects[5])
                .contractorId((Long) objects[6])
                .contractorName((String) objects[7])
                .climaticZoneId((Long) objects[8])
                .climaticZoneName((String) objects[9])
                .climaticZoneTurn((Long) objects[10])
                .cropFieldArea((Double) objects[11])
                .cropFieldName((String) objects[12])
                .cropFieldNumber((String) objects[13])
                .polygonArea((Double) objects[14])
                .cultureCropType(ofNullable(objects[15]).map(item -> CropTypeEnum.valueOf((String) item)).orElse(null))
                .cultureCropKind((String) objects[16])
                .culture((String) objects[17])
                .cultureId((Long) objects[18])
                .cultureSeason((Long) objects[19])
                .plantsCount((Double) objects[20])
                .culturePhase((String) objects[21])
                .cultureSort((String) objects[22])
                .isProtection((Boolean) objects[23])
                .addDataFields(gson.fromJson((String) objects[24],fieldsToken))
                .unitId((Long) objects[25])
                .subjects(gson.fromJson((String) objects[objects.length - 1],subjectToken))
                .build();
    }






}
