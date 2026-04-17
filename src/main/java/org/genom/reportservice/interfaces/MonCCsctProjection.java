package org.genom.reportservice.interfaces;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gnm.model.ase.wrapper.SeedsInfoWrapper;
import org.springframework.beans.factory.annotation.Value;

import javax.sql.rowset.serial.SerialArray;
import java.sql.Array;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public interface MonCCsctProjection {

    @Value("#{target.ids_backfills}")
    Object backFillsIds();

    @Value("#{target.backfills_data}")
    String backfillsData();

    @Value("#{target.idsInfo}")
    Object idsInfo();

    @Value("#{target.infos_data}")
    String infosData();



    default List<Long> getIdsBackFills() {
        Object arr = backFillsIds();
        if (arr == null) {
            return new ArrayList<>();
        }
        if (arr instanceof java.sql.Array) {
            Long[] array = new Long[0];
            try {
                array = (Long[]) ((Array) arr).getArray();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            return Arrays.asList(array);

        }
        return new ArrayList<>();
    }

    default List<Long> getIdsInfos() {
        Object arr = idsInfo();
        if (arr == null) {
            return new ArrayList<>();
        }
        if (arr instanceof java.sql.Array) {
            Long[] array = new Long[0];
            try {
                array = (Long[]) ((Array) arr).getArray();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            return Arrays.asList(array);

        }
        return new ArrayList<>();
    }


    default List<SeedsInfoWrapper> getBackFillsData() {
        String json = backfillsData();
        if (json == null) return new ArrayList<>();
        try {
            return new ObjectMapper().readValue(json, new TypeReference<List<SeedsInfoWrapper>>() {});
        } catch (Exception e) {
            return new ArrayList<>();
        }
    }


    default List<SeedsInfoWrapper> getInfosData() {
        String json = infosData();
        if (json == null) return new ArrayList<>();
        try {
            return new ObjectMapper().readValue(json, new TypeReference<List<SeedsInfoWrapper>>() {});
        } catch (Exception e) {
            return new ArrayList<>();
        }
    }
}
