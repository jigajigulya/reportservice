package org.genom.reportservice.comparator.commonReport;

import com.gnm.model.pmon.calc.AssayCommonReport;

public interface AssayReportComparator {

    default int chainClimatic(AssayCommonReport o1, AssayCommonReport o2) {
        if (o1.getClimaticZoneId() == null) {
            return 1;
        } else if (o2.getClimaticZoneId() == null) {
            return -1;
        } else {
            int clmz = o1.getClimaticZoneTurn().compareTo(o2.getClimaticZoneTurn());
            if (clmz == 0) {
                return chainTowns(o1, o2);
            } else {
                return clmz;
            }
        }
    }

    default int chainCons(AssayCommonReport o1, AssayCommonReport o2) {
        if (o1.getContractorId() == null && o2.getContractorId() == null) {
            return chainCropType(o1, o2);
        } else {
            if (o1.getContractorId() == null)
                return 1;
            else if (o2.getContractorId() == null) {
                return -1;
            } else {
                int cons = o1.getContractorId().compareTo(o2.getContractorId());
                if (cons == 0) {
                    return chainCropType(o1, o2);
                } else {
                    return cons;
                }
            }
        }
    }

    default int chainTowns(AssayCommonReport o1, AssayCommonReport o2) {
        int ters = o1.getTwnName().compareTo(o2.getTwnName());
        if (ters == 0) {
            return chainCons(o1, o2);
        } else {
            return ters;
        }
    }

    default int chainCropType(AssayCommonReport o1, AssayCommonReport o2) {
        if (o1.getCultureCropType() == null && o2.getCultureCropType() == null) {
            return chainCulture(o1, o2);
        } else {
            if (o1.getCultureCropType() == null) {
                return 1;
            } else if (o2.getCultureCropType() == null) {
                return -1;
            } else {
                int cropType = o1.getCultureCropType().getName().compareTo(o2.getCultureCropType().getName());
                if (cropType == 0) {
                    return chainCulture(o1, o2);
                } else {
                    return cropType;
                }
            }
        }
    }

    default int chainCulture(AssayCommonReport o1, AssayCommonReport o2) {
        if (o1.getCulture() == null && o2.getCulture() == null) {
            return chainDate(o1, o2);
        } else {
            if (o1.getCulture() == null) {
                return 1;
            } else if (o2.getCulture() == null) {
                return -1;
            } else {
                int date = o1.getCulture().compareTo(o2.getCulture());
                if (date == 0) {
                    return chainDate(o1, o2);
                } else {
                    return date;
                }
            }
        }
    }

    default int chainDate(AssayCommonReport o1, AssayCommonReport o2) {
        if (o1.getDate() == null && o2.getDate() == null) {
            return 0;
        } else {
            if (o1.getDate() == null)
                return 1;
            else if (o2.getDate() == null)
                return -1;
            else {
                return o1.getDate().compareTo(o2.getDate());
            }
        }
    }
}
