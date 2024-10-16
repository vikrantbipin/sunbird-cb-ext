package org.sunbird.bpreports.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.sunbird.bpreports.service.BPReportsService;
import org.sunbird.common.model.SBApiResponse;
import org.sunbird.common.util.Constants;

import java.util.Map;


@RestController
@RequestMapping("/bp")
public class BPReportsController {

    @Autowired
    private BPReportsService bpReportsService;

    @PostMapping("/generate/report")
    public ResponseEntity<SBApiResponse> generateBPReport(@RequestHeader(Constants.X_AUTH_TOKEN) String authToken, @RequestBody Map<String, Object> requestBody) {
        SBApiResponse response = bpReportsService.generateBPReport(requestBody, authToken);
        return new ResponseEntity<>(response, response.getResponseCode());

    }

    @PostMapping("/download/report")
    public ResponseEntity<SBApiResponse> downloadBPReport(@RequestHeader(Constants.X_AUTH_TOKEN) String authToken, @RequestBody Map<String, Object> requestBody) {
        SBApiResponse response = bpReportsService.downloadBPReport(requestBody, authToken);
        return new ResponseEntity<>(response, response.getResponseCode());

    }
}
