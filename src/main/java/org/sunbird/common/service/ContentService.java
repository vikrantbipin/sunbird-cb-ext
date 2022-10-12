

package org.sunbird.common.service;

import java.util.List;
import java.util.Map;

import org.sunbird.common.model.SunbirdApiResp;
import org.sunbird.common.model.SunbirdApiUserCourseListResp;

public interface ContentService {

	public SunbirdApiResp getHeirarchyResponse(String contentId);
	
	public SunbirdApiResp getAssessmentHierachyResponse(String assessmentId);

	public List<String> getParticipantsList(String xAuthUser, List<String> batchIdList);

	public List<String> getParticipantsForBatch(String xAuthUser, String batchId);

	public SunbirdApiUserCourseListResp getUserCourseListResponse(String authToken, String userId);
	
	public SunbirdApiResp getQuestionListDetails(List<String> questionIdList);
	
	public Map<String, Object> searchLiveContent(String contentId);
}

