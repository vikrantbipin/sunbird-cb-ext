package org.sunbird.consumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.batik.transcoder.TranscoderInput;
import org.apache.batik.transcoder.TranscoderOutput;
import org.apache.batik.transcoder.image.PNGTranscoder;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.jcodings.exception.TranscoderException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.*;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import org.springframework.web.client.RestTemplate;
import org.sunbird.cassandra.utils.CassandraOperation;
import org.sunbird.common.model.SBApiResponse;
import org.sunbird.common.util.CbExtServerProperties;
import org.sunbird.common.util.Constants;
import org.sunbird.consumer.security.EncryptionService;
import org.sunbird.storage.service.StorageService;

import java.io.*;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
public class KafkaConsumer {
    private Logger logger = LoggerFactory.getLogger(getClass().getName());

    @Autowired
    private CassandraOperation cassandraOperation;

    @Autowired
    private ObjectMapper mapper;

    @Autowired
    private RestTemplate restTemplate;

    @Autowired
    private StorageService storageService;

    @Autowired
    CbExtServerProperties serverProperties;

    @Autowired
    EncryptionService encryptionService;

    @KafkaListener(topics = "${spring.kafka.public.assessment.topic.name}", groupId = "${spring.kafka.public.assessment.consumer.group.id}")
    public void publicAssessmentCertificateEmailNotification(ConsumerRecord<String, String> data) throws IOException {
        logger.info("KafkaConsumer::publicAssessmentCertificateEmailNotification:topic name: {} and recievedData: {}", data.topic(), data.value());
        Map<String, Object> userCourseEnrollMap = mapper.readValue(data.value(), HashMap.class);
        String email=userCourseEnrollMap.get(Constants.PUBLIC_USER_ID).toString();
        String encryptedEmail=encryptionService.encryptData(email);
        Map<String, Object> propertyMap = new HashMap<>();
        propertyMap.put(Constants.PUBLIC_USER_ID, encryptedEmail);
        propertyMap.put(Constants.PUBLIC_CONTEXT_ID, userCourseEnrollMap.get(Constants.PUBLIC_CONTEXT_ID));
        propertyMap.put(Constants.PUBLIC_ASSESSMENT_ID, userCourseEnrollMap.get(Constants.PUBLIC_ASSESSMENT_ID));
        List<Map<String, Object>> listOfMasterData = cassandraOperation.getRecordsByPropertiesWithoutFiltering(Constants.KEYSPACE_SUNBIRD, Constants.TABLE_PUBLIC_USER_ASSESSMENT_DATA, propertyMap, null, 1);
        if (!CollectionUtils.isEmpty(listOfMasterData)) {
            Map<String, Object> dbData = listOfMasterData.get(0);
            JsonNode jsonNode = mapper.convertValue(dbData, JsonNode.class);
            String certificateId=jsonNode.get("issued_certificates").get("certificateid").asText();
            logger.info("certificate id of the user {}",certificateId);
            propertyMap.put(Constants.START_TIME,dbData.get(Constants.START_TIME));
            String certlink = publicUserCertificateDownload(certificateId);
            Map<String, Object> updatedMap = new HashMap<>();
            updatedMap.put(Constants.CERT_PUBLICURL, certlink);
            cassandraOperation.updateRecord(Constants.KEYSPACE_SUNBIRD, Constants.TABLE_PUBLIC_USER_ASSESSMENT_DATA, updatedMap, propertyMap);
            //callEmailNotifyApi(propertyMap);
        }
    }

    private void callEmailNotifyApi(Map<String, Object> propertyMap) {
        logger.info("StorageServiceImpl :: callCertRegistryApi");
        try {
            String url = serverProperties.getPublicAssessmentServiceHost() + serverProperties.getPublicAssessmentEmailNotifyUrl();
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            String requestBody = mapper.writeValueAsString(propertyMap);  // Convert the propertyMap to JSON
            HttpEntity<String> entity = new HttpEntity<>(requestBody, headers);
            ResponseEntity<JsonNode> response = restTemplate.exchange(
                    url,
                    HttpMethod.POST,
                    entity,
                    JsonNode.class
            );
            if (response.getStatusCode().is2xxSuccessful()) {
                logger.info("Email notification sent successfully");
            } else {
                logger.error("error while sending mail");
                throw new RuntimeException("Failed to retrieve externalId. Status code: " + response.getStatusCodeValue());
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private String publicUserCertificateDownload(String certificateid) {
        logger.info("KafkaConsumer :: publicUserCertificateDownload");
        try {
            String data = callCertRegistryApi(certificateid);
            String svgInput=URLDecoder.decode(data);
            String outputPath ="/tmp/"+certificateid+"_certificate.png";
            convertSvgToPng(svgInput,outputPath);
            File mFile=new File(outputPath);
            if (mFile != null && mFile.exists()) {
                logger.info("File name uploading into bucket {}",mFile.getAbsolutePath());
                SBApiResponse response = storageService.uploadFile(
                        mFile,
                        serverProperties.getPublicAssessmentCloudCertificateFolderName(),
                        serverProperties.getCloudProfileImageContainerName()
                );
                return response.getResult().get(Constants.URL).toString();
            }else{
                logger.error("File Not found");
                throw new RuntimeException("File Not found");
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void convertSvgToPng(String svgString, String outputPath) throws IOException, TranscoderException, org.apache.batik.transcoder.TranscoderException {
        svgString = cleanInvalidStyles(svgString);
        svgString = removeImageTags(svgString);
        ByteArrayInputStream inputStream = new ByteArrayInputStream(svgString.getBytes(StandardCharsets.UTF_8));
        TranscoderInput input = new TranscoderInput(inputStream);
        OutputStream outputStream = new FileOutputStream(outputPath);
        PNGTranscoder transcoder = new PNGTranscoder();
        TranscoderOutput output = new TranscoderOutput(outputStream);
        transcoder.transcode(input, output);
        outputStream.flush();
        outputStream.close();
        inputStream.close();
    }

    public static String cleanInvalidStyles(String svgContent) {
        // Replace 'display: inline-block;' with 'display: inline;' or 'display: block;'
        return svgContent.replaceAll("display:\\s*inline-block;", "display: inline;"); // or "display: block;"
    }

    public static String removeImageTags(String svgContent) {
        // Regular expression to match <image> elements in the SVG
        return svgContent.replaceAll("<image [^>]*>", "");
    }


//    private File convertDataToPdf(String data) {
//        try {
//            Path tempFilePath = Files.createTempFile("/tmp/certificate", ".svg");
//            File outputFile = tempFilePath.toFile();
//            if (data.startsWith("data:image/svg+xml,")) {
//                data = data.replaceFirst("data:image/svg\\+xml,", "");
//            }
//            String svgContent = URLDecoder.decode(data, StandardCharsets.UTF_8.name());
//            try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile))) {
//                logger.info("writing converted file to ");
//                writer.write(svgContent);
//            }
//            return outputFile;
//        } catch (IOException e) {
//            throw new RuntimeException("Failed to create SVG file", e);
//        }
//    }

    private String callCertRegistryApi(String certificateid) {
        logger.info("StorageServiceImpl :: callCertRegistryApi");
        try {
            String url = serverProperties.getCertRegistryServiceBaseUrl() + serverProperties.getCertRegistryCertificateDownloadUrl() + certificateid;
            HttpHeaders headers = new HttpHeaders();
            HttpEntity<String> entity = new HttpEntity<>(headers);
            ResponseEntity<JsonNode> response = restTemplate.exchange(
                    url,
                    HttpMethod.GET,
                    entity,
                    JsonNode.class
            );
            if (response.getStatusCode().is2xxSuccessful()) {
                String printUri = response.getBody().path("result").get("printUri").asText().replace("data:image/svg+xml,","");
                return printUri;
            } else {
                throw new RuntimeException("Failed to retrieve externalId. Status code: " + response.getStatusCodeValue());
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
