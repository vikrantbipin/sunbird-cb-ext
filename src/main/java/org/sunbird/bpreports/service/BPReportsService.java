package org.sunbird.bpreports.service;

import org.sunbird.common.model.SBApiResponse;

import java.util.Map;

public interface BPReportsService {

    public SBApiResponse generateBPReport(Map<String, Object> requestBody, String authToken);

    public SBApiResponse downloadBPReport(Map<String, Object> requestBody, String authToken);
}
