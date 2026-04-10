package org.genom.reportservice.service;

import com.gnm.model.pmon.CommonAssayReport;
import com.gnm.model.pmon.calc.AssayCommonReport;
import com.gnm.model.pmon.calc.CropTypeAndKindCulture;
import com.gnm.model.pmon.calc.PhytoSubjectState;
import org.genom.reportservice.LogExecTime;
import org.genom.reportservice.repository.ReportRep;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;


@Service
public class ReporterService {

    private final ReportRep reporter;

    public ReporterService(ReportRep reporter) {
        this.reporter = reporter;
    }

    public List<AssayCommonReport> findAssaysForWeedsCommonReport(CommonAssayReport commonAssayReport,
                                                                  List<CropTypeAndKindCulture> typeAndKindCultures) {
        return findAssays(
                commonAssayReport,
                commonAssayReport.getSelectedWeedsStates(),
                new ArrayList<>(),
                new ArrayList<>(),
                typeAndKindCultures
        );
    }


    public List<AssayCommonReport> findAssaysForDiseaseCommonReport(CommonAssayReport commonAssayReport,
                                                                    List<CropTypeAndKindCulture> typeAndKindCultures) {
        return findAssays(
                commonAssayReport,
                new ArrayList<>(),
                commonAssayReport.getSelectedDiseasesStates(),
                new ArrayList<>(),
                typeAndKindCultures
        );
    }


    public List<AssayCommonReport> findAssaysForPestCommonReport(CommonAssayReport commonAssayReport,
                                                                 List<CropTypeAndKindCulture> typeAndKindCultures) {
        return findAssays(
                commonAssayReport,
                new ArrayList<>(),
                new ArrayList<>(),
                commonAssayReport.getSelectedPestsStates(),
                typeAndKindCultures
        );
    }


    public List<AssayCommonReport> findOnlyAssaysStatesCommonReport(CommonAssayReport report, List<CropTypeAndKindCulture> ctakc_list) {
        return findAssays(
                report,
                report.getSelectedWeedsStates(),
                report.getSelectedDiseasesStates(),
                report.getSelectedPestsStates(),
                ctakc_list
        );
    }

    @LogExecTime
    public List<AssayCommonReport> findAssays(CommonAssayReport commonAssayReport,
                                               List<PhytoSubjectState> weeds,
                                               List<PhytoSubjectState> diseases,
                                               List<PhytoSubjectState> pests,
                                               List<CropTypeAndKindCulture> typeAndKindCultures) {
        return reporter.findAssaysStatesCommonReport(commonAssayReport, weeds, diseases, pests, typeAndKindCultures);

    }
}
