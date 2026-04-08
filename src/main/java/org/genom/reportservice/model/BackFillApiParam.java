package org.genom.reportservice.model;

import com.gnm.enums.ReportKeysForFilterEnum;

import com.google.gson.annotations.Expose;
import lombok.*;
import org.genom.reportservice.criteria.SeedsBackFillCriteria;

import java.util.Map;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@EqualsAndHashCode
public class BackFillApiParam {
    @Expose
    private SeedsBackFillCriteria criteria;
    @Expose
    private Map<ReportKeysForFilterEnum, Object> map;
}
