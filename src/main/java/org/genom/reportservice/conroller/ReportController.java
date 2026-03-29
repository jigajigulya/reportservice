package org.genom.reportservice.conroller;

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
import org.genom.reportservice.reporter.AssayCommonReporter01;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.*;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@RestController
@RequestMapping(path = "reports", produces = "application/json")
@CrossOrigin(origins = {"http://localhost:8080/pmon", "https://rscagex.ru/pmon/"})
@Slf4j
@RequiredArgsConstructor
public class ReportController {

    private final AssayCommonReporter01 reporter01;
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
}
