package org.sunbird.bpreports.service;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;
import org.sunbird.cassandra.utils.CassandraOperationImpl;
import org.sunbird.common.model.SBApiResponse;
import org.sunbird.common.util.AccessTokenValidator;
import org.sunbird.common.util.CbExtServerProperties;
import org.sunbird.common.util.Constants;
import org.sunbird.common.util.ProjectUtil;
import org.sunbird.core.producer.Producer;
import org.sunbird.user.service.UserUtilityService;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class BPReportsServiceImpl implements BPReportsService {

    private static final Logger logger = LoggerFactory.getLogger(BPReportsServiceImpl.class);

    @Autowired
    AccessTokenValidator accessTokenValidator;

    @Autowired
    private UserUtilityService userUtilityService;

    @Autowired
    private CassandraOperationImpl cassandraOperation;

    @Autowired
    Producer kafkaProducer;

    @Autowired
    CbExtServerProperties serverProperties;

    @Override
    public SBApiResponse generateBPReport(Map<String, Object> requestBody, String authToken) {
        SBApiResponse response = ProjectUtil.createDefaultResponse(Constants.BP_REPORT_GENERATE_API);
        try {
//            String userId = accessTokenValidator.fetchUserIdFromAccessToken(authToken);
            String userId = "95a357c6-99d9-43d2-8d1b-42bea6f8c132";
            if (StringUtils.isBlank(userId)) {
                updateErrorDetails(response, "Failed to read user details from access token.", HttpStatus.BAD_REQUEST);
                return response;
            }

            String courseId = (String) requestBody.get(Constants.COURSE_ID);
            String batchId = (String) requestBody.get(Constants.BATCH_ID);
            String orgId = (String) requestBody.get(Constants.ORG_ID);
            String profileSurveyId = (String) requestBody.get(Constants.PROFILE_SURVEY_ID);
            SBApiResponse errResponse = validateGenerateReportRequestBody(requestBody);
            if (!ObjectUtils.isEmpty(errResponse)) {
                return response;
            }

            Map<String, Map<String, String>> userInfoMap = new HashMap<>();
            userUtilityService.getUserDetailsFromDB(
                    Arrays.asList(userId),
                    Arrays.asList(Constants.ROOT_ORG_ID, Constants.USER_ID), userInfoMap);
            String userOrgId = userInfoMap.get(userId).get(Constants.ROOT_ORG_ID);
            if (StringUtils.isBlank(userOrgId) || !userOrgId.equals(orgId)) {
                updateErrorDetails(response, "Requested User is not belongs to same organisation.", HttpStatus.BAD_REQUEST);
                return response;
            }

            Map<String, Object> keyMap = new HashMap<>();
            keyMap.put(Constants.ORG_ID, orgId);
            keyMap.put(Constants.COURSE_ID, courseId);
            keyMap.put(Constants.BATCH_ID, batchId);

            List<Map<String, Object>> existingReportDetails = cassandraOperation.getRecordsByProperties(Constants.KEYSPACE_SUNBIRD,
                    Constants.BP_ENROLMENT_REPORT_TABLE, keyMap, null);

            if (!CollectionUtils.isEmpty(existingReportDetails)) {
                String status = (String) existingReportDetails.get(0).get(Constants.STATUS);
                if (Constants.STATUS_IN_PROGRESS_UPPERCASE.equalsIgnoreCase(status)) {
                    response.getParams().setStatus(Constants.SUCCESS);
                    response.getParams().setErrmsg("Report generation is in-progress.");
                    response.setResponseCode(HttpStatus.OK);
                    return response;
                } else {
                    logger.info("Update BP report details::started");
                    return updateReportDetailsInDBAndTriggerKafkaEvent(userId, requestBody);

                }

            } else {
                logger.info("Insert BP report details into DB::started");
                return insertReportDetailsInDBAndTriggerKafkaEvent(userId, requestBody);
            }


        } catch (Exception e) {
            logger.error("Error while processing the request", e);
            updateErrorDetails(response, "Error while processing the request", HttpStatus.INTERNAL_SERVER_ERROR);
            return response;
        }
    }

    private SBApiResponse validateGenerateReportRequestBody(Map<String, Object> requestBody) {
        SBApiResponse response = ProjectUtil.createDefaultResponse(Constants.BP_REPORT_GENERATE_API);
        if (CollectionUtils.isEmpty(requestBody)) {
            updateErrorDetails(response, Constants.INVALID_REQUEST, HttpStatus.BAD_REQUEST);
            return response;
        }
        if (StringUtils.isEmpty((String) requestBody.get(Constants.COURSE_ID))) {
            updateErrorDetails(response, Constants.COURSE_ID_MISSING, HttpStatus.BAD_REQUEST);
            return response;
        }
        if (StringUtils.isEmpty((String) requestBody.get(Constants.BATCH_ID))) {
            updateErrorDetails(response, Constants.BATCH_ID_MISSING, HttpStatus.BAD_REQUEST);
            return response;
        }
        if (StringUtils.isEmpty((String) requestBody.get(Constants.ORG_ID))) {
            updateErrorDetails(response, Constants.ORG_ID_KEY_MISSING, HttpStatus.BAD_REQUEST);
            return response;
        }
        if (StringUtils.isEmpty((String) requestBody.get(Constants.PROFILE_SURVEY_ID))) {
            updateErrorDetails(response, Constants.PROFILE_SURVEY_ID_MISSING, HttpStatus.BAD_REQUEST);
            return response;
        }
        return null;
    }

    private void updateErrorDetails(SBApiResponse response, String errMsg, HttpStatus responseCode) {
        response.getParams().setStatus(Constants.FAILED);
        response.getParams().setErrmsg(errMsg);
        response.setResponseCode(responseCode);
    }

    private SBApiResponse insertReportDetailsInDBAndTriggerKafkaEvent(String userId, Map<String, Object> requestBody) {
        SBApiResponse response = ProjectUtil.createDefaultResponse(Constants.BP_REPORT_GENERATE_API);

        try {
            Map<String, Object> dbRequest = new HashMap<>();
            dbRequest.put(Constants.ORG_ID, requestBody.get(Constants.ORG_ID));
            dbRequest.put(Constants.COURSE_ID, requestBody.get(Constants.COURSE_ID));
            dbRequest.put(Constants.BATCH_ID, requestBody.get(Constants.BATCH_ID));
            dbRequest.put(Constants.PROFILE_SURVEY_ID, requestBody.get(Constants.PROFILE_SURVEY_ID));
            dbRequest.put(Constants.STATUS, Constants.STATUS_IN_PROGRESS_UPPERCASE);
            dbRequest.put(Constants.CREATED_BY, userId);
            SBApiResponse dbResponse = cassandraOperation.insertRecord(Constants.SUNBIRD_KEY_SPACE_NAME, Constants.BP_ENROLMENT_REPORT_TABLE, dbRequest);

            if (dbResponse.get(Constants.RESPONSE).equals(Constants.SUCCESS)) {
                Map<String, Object> kafkaRequest = new HashMap<>();
                kafkaRequest.put(Constants.ORG_ID, requestBody.get(Constants.ORG_ID));
                kafkaRequest.put(Constants.COURSE_ID, requestBody.get(Constants.COURSE_ID));
                kafkaRequest.put(Constants.BATCH_ID, requestBody.get(Constants.BATCH_ID));
                kafkaRequest.put(Constants.PROFILE_SURVEY_ID, requestBody.get(Constants.PROFILE_SURVEY_ID));
                kafkaRequest.put(Constants.STATUS, Constants.STATUS_IN_PROGRESS_UPPERCASE);
                kafkaRequest.put(Constants.CREATED_BY, userId);
                kafkaProducer.push(serverProperties.getKafkaTopicBPReport(), kafkaRequest);

                response.getResult().put(Constants.STATUS, Constants.SUCCESS);
                response.getParams().setStatus(Constants.SUCCESS);
                response.setResponseCode(HttpStatus.OK);
            } else {
                logger.error("Error while inserting record in the DB");
                updateErrorDetails(response, "Error while processing the request", HttpStatus.INTERNAL_SERVER_ERROR);
                return response;
            }

        } catch (Exception e) {
            logger.error("Error while inserting record in the DB", e);
            updateErrorDetails(response, "Error while processing the request", HttpStatus.INTERNAL_SERVER_ERROR);
            return response;

        }
        logger.info("Insert BP report details into DB::started");
        return response;

    }

    private SBApiResponse updateReportDetailsInDBAndTriggerKafkaEvent(String userId, Map<String, Object> requestBody) {
        SBApiResponse response = ProjectUtil.createDefaultResponse(Constants.BP_REPORT_GENERATE_API);
        try {
            Map<String, Object> compositeKey = new HashMap<>();
            Map<String, Object> updateAttributes = new HashMap<>();
            compositeKey.put(Constants.ORG_ID, requestBody.get(Constants.ORG_ID));
            compositeKey.put(Constants.COURSE_ID, requestBody.get(Constants.COURSE_ID));
            compositeKey.put(Constants.BATCH_ID, requestBody.get(Constants.BATCH_ID));

            updateAttributes.put(Constants.STATUS, Constants.STATUS_IN_PROGRESS_UPPERCASE);
            updateAttributes.put(Constants.CREATED_BY, userId);
            Map<String, Object> updateResponse = cassandraOperation.updateRecord(Constants.SUNBIRD_KEY_SPACE_NAME, Constants.BP_ENROLMENT_REPORT_TABLE, updateAttributes, compositeKey);

            if (updateResponse.get(Constants.RESPONSE).equals(Constants.SUCCESS)) {
                Map<String, Object> kafkaRequest = new HashMap<>();
                kafkaRequest.put(Constants.ORG_ID, requestBody.get(Constants.ORG_ID));
                kafkaRequest.put(Constants.COURSE_ID, requestBody.get(Constants.COURSE_ID));
                kafkaRequest.put(Constants.BATCH_ID, requestBody.get(Constants.BATCH_ID));
                kafkaRequest.put(Constants.PROFILE_SURVEY_ID, requestBody.get(Constants.PROFILE_SURVEY_ID));
                kafkaRequest.put(Constants.STATUS, Constants.STATUS_IN_PROGRESS_UPPERCASE);
                kafkaRequest.put(Constants.CREATED_BY, userId);
                kafkaProducer.push(serverProperties.getKafkaTopicBPReport(), kafkaRequest);

                response.getResult().put(Constants.STATUS, Constants.SUCCESS);
                response.getParams().setStatus(Constants.SUCCESS);
                response.setResponseCode(HttpStatus.OK);
            } else {
                logger.error("Error while inserting record in the DB");
                updateErrorDetails(response, "Error while processing the request", HttpStatus.INTERNAL_SERVER_ERROR);
                return response;
            }

        } catch (Exception e) {
            logger.error("Error while inserting record in the DB", e);
            updateErrorDetails(response, "Error while processing the request", HttpStatus.INTERNAL_SERVER_ERROR);
            return response;

        }
        logger.info("Updating BP report details::end");
        return response;
    }

    public SBApiResponse downloadBPReport(Map<String, Object> requestBody, String authToken) {
        SBApiResponse response = ProjectUtil.createDefaultResponse(Constants.BP_REPORT_DOWNLOAD_API);
        try {
            // String userId = accessTokenValidator.fetchUserIdFromAccessToken(authToken);
            String userId = "95a357c6-99d9-43d2-8d1b-42bea6f8c132";
            if (StringUtils.isBlank(userId)) {
                updateErrorDetails(response, "Failed to read user details from access token.", HttpStatus.BAD_REQUEST);
                return response;
            }

            String courseId = (String) requestBody.get(Constants.COURSE_ID);
            String batchId = (String) requestBody.get(Constants.BATCH_ID);
            String orgId = (String) requestBody.get(Constants.ORG_ID);
            SBApiResponse errResponse = validateDownloadReportRequestBody(requestBody);
            if (!ObjectUtils.isEmpty(errResponse)) {
                return errResponse;
            }

            Map<String, Map<String, String>> userInfoMap = new HashMap<>();
            userUtilityService.getUserDetailsFromDB(
                    Arrays.asList(userId),
                    Arrays.asList(Constants.ROOT_ORG_ID, Constants.USER_ID), userInfoMap);
            String userOrgId = userInfoMap.get(userId).get(Constants.ROOT_ORG_ID);
            if (StringUtils.isBlank(userOrgId) || !userOrgId.equals(orgId)) {
                updateErrorDetails(response, "Requested User is not belongs to same organisation.", HttpStatus.BAD_REQUEST);
                return response;
            }

            Map<String, Object> keyMap = new HashMap<>();
            keyMap.put(Constants.ORG_ID, orgId);
            keyMap.put(Constants.COURSE_ID, courseId);
            keyMap.put(Constants.BATCH_ID, batchId);
            List<Map<String, Object>> existingReportDetails = cassandraOperation.getRecordsByProperties(Constants.KEYSPACE_SUNBIRD,
                    Constants.BP_ENROLMENT_REPORT_TABLE, keyMap, null);

            if (!CollectionUtils.isEmpty(existingReportDetails)) {
                String status = (String) existingReportDetails.get(0).get(Constants.STATUS);
                String downloadLink = (String) existingReportDetails.get(0).get(Constants.DOWNLOAD_LINK);
                if (StringUtils.isEmpty(status)) {
                    updateErrorDetails(response, "Report is not available. Please generate the report", HttpStatus.OK);
                    return response;
                }
                if (Constants.STATUS_IN_PROGRESS_UPPERCASE.equalsIgnoreCase(status)) {
                    updateErrorDetails(response, "Report generation is in-progress. Please wait for a while", HttpStatus.OK);
                    return response;
                } else {
                    if (Constants.COMPLETED_UPPER_CASE.equalsIgnoreCase(status) && StringUtils.isEmpty(downloadLink)) {
                        updateErrorDetails(response, "Report is not available. Please generate the report", HttpStatus.OK);
                        return response;
                    } else {
                        response.getParams().setStatus(Constants.SUCCESS);
                        response.setResponseCode(HttpStatus.OK);
                        response.getResult().put(Constants.DOWNLOAD_LINK, downloadLink);
                        return response;
                    }
                }
            } else {
                updateErrorDetails(response, "Report is not available. Please generate the report", HttpStatus.OK);
                return response;
            }
        } catch (Exception e) {
            logger.error("Error while processing the download request", e);
            updateErrorDetails(response, "Error while processing the download request", HttpStatus.INTERNAL_SERVER_ERROR);
            return response;
        }
    }

    private SBApiResponse validateDownloadReportRequestBody(Map<String, Object> requestBody) {
        SBApiResponse response = ProjectUtil.createDefaultResponse(Constants.BP_REPORT_GENERATE_API);
        if (CollectionUtils.isEmpty(requestBody)) {
            updateErrorDetails(response, Constants.INVALID_REQUEST, HttpStatus.BAD_REQUEST);
            return response;
        }
        if (StringUtils.isEmpty((String) requestBody.get(Constants.COURSE_ID))) {
            updateErrorDetails(response, Constants.COURSE_ID_MISSING, HttpStatus.BAD_REQUEST);
            return response;
        }
        if (StringUtils.isEmpty((String) requestBody.get(Constants.BATCH_ID))) {
            updateErrorDetails(response, Constants.BATCH_ID_MISSING, HttpStatus.BAD_REQUEST);
            return response;
        }
        if (StringUtils.isEmpty((String) requestBody.get(Constants.ORG_ID))) {
            updateErrorDetails(response, Constants.ORG_ID_KEY_MISSING, HttpStatus.BAD_REQUEST);
            return response;
        }
        return null;
    }

}
