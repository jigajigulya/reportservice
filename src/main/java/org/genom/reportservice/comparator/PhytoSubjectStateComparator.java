package org.genom.reportservice.comparator;

import com.gnm.enums.PhytoTypeEnum;
import com.gnm.model.pmon.calc.PhytoSubjectState;

import java.util.Comparator;

public class PhytoSubjectStateComparator implements Comparator<PhytoSubjectState>{
    @Override
    public int compare(PhytoSubjectState o1, PhytoSubjectState o2) {
//        o1 < o2
//        o1 = o2
//        o1 > o2

        if (o1 != null && o2 != null) {

            if (o1.getPhytosubjectCode() != null && o2.getPhytosubjectCode() != null) {
                int phytoType_comp = comparePhytoType(o1.getPhytoType(), o2.getPhytoType());

                if (phytoType_comp == 0) {
                    int name_comp = compareName(o1.getPhytoSubjectName(), o1.getPhytoSubjectName());
                    if (name_comp == 0){
                        int name_lat_comp = compareName(o1.getPhytosubjectNameLatin(), o1.getPhytosubjectNameLatin());

                        if (name_lat_comp == 0){
                            int code_comp = compareSubjectCode(o1.getPhytosubjectCode(), o2.getPhytosubjectCode());

                            if (code_comp == 0) {
                                int ev_comp = compareSubjectPhaseEvolution(o1.getSubjectPhaseevolutionId(), o2.getSubjectPhaseevolutionId());
                                if (ev_comp == 0) {
                                    return compareMeasureUnit(o1.getMeasureunitId(), o2.getMeasureunitId());
                                }
                                return ev_comp;
                            }
                            return code_comp;
                        }
                        return name_lat_comp;
                    }

                    return name_comp;
                }

                return phytoType_comp;
            } else if (o1.getPhytosubjectCode() != null && o2.getPhytosubjectCode() == null) {
                return 1;
            } else if (o1.getPhytosubjectCode() == null && o2.getPhytosubjectCode() != null) {
                return -1;
            } else {
                int ev_comp = compareSubjectPhaseEvolution(o1.getSubjectPhaseevolutionId(), o2.getSubjectPhaseevolutionId());

                if (ev_comp == 0) {
                    return compareMeasureUnit(o1.getMeasureunitId(), o2.getMeasureunitId());
                }

                return ev_comp;
            }
        } else if (o1 != null && o2 == null) {
            return 1;
        } else if (o1 == null && o2 != null) {
            return -1;
        }

        return 0;
    }

    private int compareName(String name1, String name2) {
        if (name1 != null && name2 != null) {
            return name1.compareTo(name2);
        } else if (name1 != null && name2 == null) {
            return 1;
        } else if (name1 == null && name2 != null) {
            return -1;
        } else {
            return 0;
        }
    }

    private int comparePhytoType(PhytoTypeEnum pt1, PhytoTypeEnum pt2) {
        if (pt1 != null && pt2 != null) {
            return compareName(pt1.getName(), pt2.getName());
        } else if (pt1 != null && pt2 == null) {
            return 1;
        } else if (pt1 == null && pt2 != null) {
            return -1;
        } else {
            return 0;
        }
    }

    private int compareSubjectCode(Long code1, Long code2) {
        if (code1 != null && code2 != null) {
            return code1.compareTo(code2);
        } else if (code1 != null && code2 == null) {
            return 1;
        } else if (code1 == null && code2 != null) {
            return -1;
        } else {
            return 0;
        }
    }

    private int compareSubjectPhaseEvolution(Long spe1, Long spe2) {
        if (spe1 != null && spe2 != null) {
            return spe1.compareTo(spe2);
        } else if (spe1 != null && spe2 == null) {
            return 1;
        } else if (spe1 == null && spe2 != null) {
            return -1;
        } else {
            return 0;
        }
    }

    private int compareMeasureUnit(Long m1, Long m2) {
        if (m1 != null && m2 != null) {
            return m1.compareTo(m2);
        } else if (m1 != null && m2 == null) {
            return 1;
        } else if (m1 == null && m2 != null) {
            return -1;
        } else {
            return 0;
        }
    }
}
