package org.sunbird.cqfassessment.service;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.util.ObjectUtils;
import org.sunbird.cassandra.utils.CassandraOperation;
import org.sunbird.common.model.SBApiResponse;
import org.sunbird.common.util.AccessTokenValidator;
import org.sunbird.common.util.Constants;
import org.sunbird.common.util.ProjectUtil;

import java.util.*;

/**
 * Service implementation for managing CQF Assessments.
 */

@Service
public class CQFAssessmentServiceImpl implements CQFAssessmentService {
    private final Logger logger = LoggerFactory.getLogger(CQFAssessmentServiceImpl.class);

    @Autowired
    AccessTokenValidator accessTokenValidator;

    @Autowired
    CassandraOperation cassandraOperation;


    /**
     * Creates a entry for new CQF Assessment.
     *
     * @param authToken   the authentication token for the request
     * @param requestBody the request body containing the assessmentId and the status
     * @return the API response containing the created assessment details
     */
    @Override
    public SBApiResponse createCQFAssessment(String authToken, Map<String, Object> requestBody) {
        logger.info("CQFAssessmentServiceImpl::createCQFAssessment.. started");
        SBApiResponse outgoingResponse = ProjectUtil.createDefaultResponse(Constants.CQF_API_CREATE_ASSESSMENT);
        String userId = accessTokenValidator.fetchUserIdFromAccessToken(authToken);
        if (ObjectUtils.isEmpty(userId)) {
            updateErrorDetails(outgoingResponse, HttpStatus.BAD_REQUEST);
            return outgoingResponse;
        }
        String errMsg = validateRequest(requestBody, outgoingResponse);
        if (StringUtils.isNotBlank(errMsg)) {
            return outgoingResponse;
        }
        checkActiveCqfAssessments(requestBody);
        Map<String, Object> request = new HashMap<>();
        request.put(Constants.ASSESSMENT_ID_KEY, requestBody.get(Constants.ASSESSMENT_ID_KEY));
        request.put(Constants.ACTIVE_STATUS, requestBody.get(Constants.ACTIVE_STATUS));
        return cassandraOperation.insertRecord(Constants.KEYSPACE_SUNBIRD, Constants.CQF_ASSESSMENT_TRACKING, request);
    }

    /**
     * Updates an existing CQF Assessment.
     *
     * @param requestBody             the request body containing the updated assessment status
     * @param authToken               the authentication token for the request
     * @param cqfAssessmentIdentifier the identifier of the assessment to update
     * @return the API response containing the updated assessment details
     */
    @Override
    public SBApiResponse updateCQFAssessment(Map<String, Object> requestBody, String authToken, String cqfAssessmentIdentifier) {
        logger.info("CQFAssessmentServiceImpl::updateCQFAssessment.. started");
        SBApiResponse response = ProjectUtil.createDefaultResponse(Constants.CQF_API_UPDATE_ASSESSMENT);
        String userId = accessTokenValidator.fetchUserIdFromAccessToken(authToken);
        if (ObjectUtils.isEmpty(userId)) {
            updateErrorDetails(response, HttpStatus.BAD_REQUEST);
            return response;
        }
        String errMsg = validateRequest(requestBody, response);
        if (StringUtils.isNotBlank(errMsg)) {
            return response;
        }
        checkActiveCqfAssessments(requestBody);
        Map<String, Object> request = new HashMap<>();
        request.put(Constants.ACTIVE_STATUS, requestBody.get(Constants.ACTIVE_STATUS));
        Map<String, Object> compositeKeyMap = new HashMap<>();
        compositeKeyMap.put(Constants.ASSESSMENT_ID_KEY, cqfAssessmentIdentifier);
        Map<String, Object> resp = cassandraOperation.updateRecord(Constants.KEYSPACE_SUNBIRD, Constants.CQF_ASSESSMENT_TRACKING, request, compositeKeyMap);
        response.getResult().put(Constants.CQF_ASSESSMENT_DATA, resp);
        return response;
    }

    /**
     * Retrieves a CQF Assessment by its identifier.
     *
     * @param authToken               the authentication token for the request
     * @param cqfAssessmentIdentifier the identifier of the assessment to retrieve
     * @return the API response containing the assessment details
     */
    @Override
    public SBApiResponse getCQFAssessment(String authToken, String cqfAssessmentIdentifier) {
        logger.info("CQFAssessmentServiceImpl::getCQFAssessment... Started");
        SBApiResponse response = ProjectUtil.createDefaultResponse(Constants.CQF_API_READ_ASSESSMENT);
        String userId = accessTokenValidator.fetchUserIdFromAccessToken(authToken);
        if (StringUtils.isBlank(userId)) {
            updateErrorDetails(response, HttpStatus.INTERNAL_SERVER_ERROR);
            return response;
        }
        Map<String, Object> propertyMap = new HashMap<>();
        propertyMap.put(Constants.ASSESSMENT_ID_KEY, cqfAssessmentIdentifier);
        Map<String, Object> cqfAssessmentDataList = cassandraOperation.getRecordsByProperties(Constants.KEYSPACE_SUNBIRD, Constants.CQF_ASSESSMENT_TRACKING, propertyMap, Arrays.asList(Constants.ASSESSMENT_ID_KEY, Constants.ACTIVE_STATUS), Constants.ASSESSMENT_ID_KEY);
        response.getResult().put(Constants.CQF_ASSESSMENT_DATA, cqfAssessmentDataList);
        return response;
    }


    /**
     * Lists all CQF Assessments.
     *
     * @param authToken the authentication token for the request
     * @return the API response containing the list of assessments
     */
    @Override
    public SBApiResponse listCQFAssessments(String authToken) {
        logger.info("CQFAssessmentServiceImpl::listCQFAssessments... Started");
        SBApiResponse response = ProjectUtil.createDefaultResponse(Constants.CQF_API_LIST_ASSESSMENT);
        String userId = accessTokenValidator.fetchUserIdFromAccessToken(authToken);
        if (StringUtils.isBlank(userId)) {
            updateErrorDetails(response, HttpStatus.INTERNAL_SERVER_ERROR);
            return response;
        }
        Map<String, Object> propertyMap = new HashMap<>();
        List<Map<String, Object>> cqfAssessmentDataList = cassandraOperation.getRecordsByProperties(Constants.KEYSPACE_SUNBIRD, Constants.CQF_ASSESSMENT_TRACKING, propertyMap, new ArrayList<>());
        response.getResult().put(Constants.CQF_ASSESSMENT_DATA, cqfAssessmentDataList);
        return response;
    }

    /**
     * Updates the error details in the API response.
     *
     * @param response The API response object.
     * @param responseCode The HTTP status code.
     */
    private void updateErrorDetails(SBApiResponse response, HttpStatus responseCode) {
        response.getParams().setStatus(Constants.FAILED);
        response.getParams().setErrmsg(Constants.USER_ID_DOESNT_EXIST);
        response.setResponseCode(responseCode);
    }



    /**
     * Validates the request and updates the API response accordingly.
     *
     * @param request The request object.
     * @param response The API response object.
     * @return An error message if the request is invalid, otherwise an empty string.
     */
    private String validateRequest(Map<String, Object> request, SBApiResponse response) {
        if (MapUtils.isEmpty(request)) {
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErrmsg("RequestBody is missing");
            response.setResponseCode(HttpStatus.BAD_REQUEST);
            return "Request Body is missing";
        }
        else if (StringUtils.isBlank((String) request.get(Constants.ASSESSMENT_ID_KEY))) {
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErrmsg("Assessment Id is missing");
            response.setResponseCode(HttpStatus.BAD_REQUEST);
            return "Assessment Id is missing";
        } else if (StringUtils.isBlank((String) request.get(Constants.ACTIVE_STATUS))) {
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErrmsg("Active status is missing");
            response.setResponseCode(HttpStatus.BAD_REQUEST);
            return "Active status is missing";
        }
        return "";
    }

    /**
     * Checks for active CQF assessments and updates their status to inactive if necessary.
     *
     * @param requestBody The request body containing the active status.
     */
    public void checkActiveCqfAssessments(Map<String, Object> requestBody) {
        String activeStatus = (String) requestBody.get(Constants.ACTIVE_STATUS);
        if ("active".equals(activeStatus)) {
            Map<String, Object> propertyMap = new HashMap<>();
            List<Map<String, Object>> recordsToUpdate = cassandraOperation.getRecordsByProperties(Constants.KEYSPACE_SUNBIRD, Constants.CQF_ASSESSMENT_TRACKING, propertyMap, new ArrayList<>());
            if (recordsToUpdate.stream()
                    .anyMatch(assessmentRecord -> assessmentRecord.get(Constants.ACTIVE_STATUS).equals("active"))) {
                recordsToUpdate.forEach(assessmentRecord -> {
                    Map<String, Object> request = new HashMap<>();
                    request.put(Constants.ACTIVE_STATUS, "inactive");
                    Map<String, Object> compositeKeyMap = new HashMap<>();
                    compositeKeyMap.put(Constants.ASSESSMENT_ID_KEY, assessmentRecord.get(Constants.ASSESSMENT_ID_KEY));
                    cassandraOperation.updateRecord(Constants.KEYSPACE_SUNBIRD, Constants.CQF_ASSESSMENT_TRACKING, request, compositeKeyMap);
                });
            }
        }
    }
}