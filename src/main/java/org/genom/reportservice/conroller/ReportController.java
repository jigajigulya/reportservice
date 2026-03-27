package org.genom.reportservice.conroller;

import lombok.extern.slf4j.Slf4j;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbookFactory;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.*;

@RestController
@RequestMapping(path = "reports", produces = "application/json")
@CrossOrigin(origins = {"http://localhost:8080/pmon", "https://rscagex.ru/pmon/"})
@Slf4j
public class ReportController {


    @GetMapping("/commonreportassay")
    public byte[] getReport() {

        try (InputStream fileInputStream = getClass().getClassLoader().getResourceAsStream("excel/ATT_4A.xlsx"); Workbook workbook = XSSFWorkbookFactory.createWorkbook(OPCPackage.open(fileInputStream))) {
            ByteArrayOutputStream bo = new ByteArrayOutputStream();
            workbook.write(bo);
            return bo.toByteArray();
        } catch (IOException | InvalidFormatException e) {
            return new byte[0];


        }
    }
}
