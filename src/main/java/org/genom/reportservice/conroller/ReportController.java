package org.genom.reportservice.conroller;

import com.gnm.criteria.SeedProductionParameter;
import com.gnm.model.common.DepartmentStructure;
import com.gnm.model.common.geo.ClimaticZone;
import com.gnm.model.common.geo.TerTownship;
import com.gnm.model.pmon.CommonAssayReport;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbookFactory;
import org.genom.reportservice.criteria.PhytoExpertizeCriteria;
import org.genom.reportservice.model.BackFillApiParam;
import org.genom.reportservice.model.SeedProdParam;
import org.genom.reportservice.reporter.AssayCommonReporter01;
import org.genom.reportservice.reporter.BackFillReporter;
import org.genom.reportservice.reporter.M30BReporter;
import org.genom.reportservice.reporter.PhytoExpQualReporter;
import org.genom.reportservice.repository.BackFillRepository;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.context.annotation.RequestScope;

import java.io.*;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@RequestScope
@RestController
@RequestMapping(path = "reports", produces = "application/json")
@CrossOrigin(origins = {"http://localhost:8080/pmon", "https://rscagex.ru/pmon/"})
@Slf4j
@RequiredArgsConstructor
public class ReportController {

    private final AssayCommonReporter01 reporter01;
    private final PhytoExpQualReporter phytoExpReporter;
    private final BackFillReporter backFillReporter;
    private final M30BReporter m30BReporter;
    /*.queryParam("dateBegin", dateBegin)
                .queryParam("dateEnd", dateEnd)
                .queryParam("towns",Stream.ofNullable(selectedTownships).flatMap(Collection::stream).map(TerTownship::getId).collect(Collectors.toList()))
            .queryParam("zones", Stream.ofNullable(selectedZones).flatMap(Collection::stream).map(ClimaticZone::getId).collect(Collectors.toList()))
            .queryParam("dep_reg_id",Optional.ofNullable(departmentRegion).map(DepartmentStructure::getId).orElse(null))
            .queryParam("commreppar", report)*/

    @PostMapping(value = "/commonreportassay", consumes = "application/json")
    public ResponseEntity<byte[]> getReport(@RequestBody CommonAssayReport commonAssayReport) {
        byte[] bytes = reporter01.makeReportCommonAssay01(
                commonAssayReport
        );
        return ResponseEntity.ok()
                .contentType(MediaType.parseMediaType("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"))
                .body(bytes);

    }

    @PostMapping(value = "/phytoexpreport", consumes = "application/json")
    public ResponseEntity<byte[]> getReport(@RequestBody PhytoExpertizeCriteria criteria) {
        byte[] bytes = phytoExpReporter.download(
                criteria
        );
        return ResponseEntity.ok()
                .contentType(MediaType.parseMediaType("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"))
                .body(bytes);

    }

    @PostMapping(value = "/backfillreport", consumes = "application/json")
    public ResponseEntity<byte[]> getReport(@RequestBody BackFillApiParam criteria) {
        byte[] bytes = backFillReporter.constructExcel(
                criteria
        );
        return ResponseEntity.ok()
                .contentType(MediaType.parseMediaType("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"))
                .body(bytes);

    }

    @PostMapping(value = "/30b", consumes = "application/json")
    public ResponseEntity<byte[]> getReport(@RequestBody SeedProdParam seedProductionParameter) {
        byte[] bytes = m30BReporter.constructExcel(
                seedProductionParameter
        );
        return ResponseEntity.ok()
                .contentType(MediaType.parseMediaType("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"))
                .body(bytes);

    }
}
