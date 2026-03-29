package org.genom.reportservice.comparator.commonReport;

import com.gnm.model.pmon.calc.AssayCommonReport;

import java.util.Comparator;

public class AssayReportWithClmComparator implements Comparator<AssayCommonReport>, AssayReportComparator {
    @Override
    public int compare(AssayCommonReport o1, AssayCommonReport o2) {
        if (o1 == null || o2 == null) {
            if (o1 == null && o2 == null) {
                return 0;
            } else {
                if (o1 == null) {
                    return 1;
                } else {
                    return -1;
                }
            }
        } else {
            if (o1.getTwnId() == null && o2.getTwnId() == null) {
                return chainCons(o1, o2);
            } else {
                if (o1.getTwnId() == null) {
                    return 1;
                } else if (o2.getTwnId() == null) {
                    return -1;
                } else {
                    if (o1.getClimaticZoneId() == null && o2.getClimaticZoneId() == null) {
                        return chainTowns(o1, o2);
                    } else {
                        return chainClimatic(o1, o2);
                    }
                }
            }
        }
    }
}
