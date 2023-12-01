package northeastern.xiaosongzhai;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SNSEvent;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.*;
import com.google.common.collect.Lists;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.commons.io.FileUtils;

import java.io.*;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

/**
 * @Author: Xiaosong Zhai
 * @date: 2023/11/28 00:22
 * @Description: sns event handler
 */
public class SnsEventHandler implements RequestHandler<SNSEvent,String> {

    private static final String DYNAMODB_TABLE_NAME = "emailTrackingTable";

    @Override
    public String handleRequest(SNSEvent snsEvent, Context context) {
        String filePath = "/tmp";
        String projectId = "csye6225-demo-406000";

        try {
            context.getLogger().log("Received SNS event: " + snsEvent);
            // get the message
            SNSEvent.SNSRecord record = snsEvent.getRecords().get(0);
            SNSEvent.SNS snsMessage = record.getSNS();
            String message = snsMessage.getMessage();

            // paras the message
            JsonObject messageJson = JsonParser.parseString(message).getAsJsonObject();
            String submissionUrl = messageJson.get("submissionUrl").getAsString();
            String userEmail = messageJson.get("userEmail").getAsString();

            context.getLogger().log("submissionUrl: " + submissionUrl);
            context.getLogger().log("userEmail: " + userEmail);

            // get the file name from the url
            String fileName = extractFileNameFromURL(submissionUrl);
            context.getLogger().log("fileName: " + fileName);


            // download the file to resources and upload to google cloud storage
            try{
                // download the file from the url
                downloadHttpUrl(submissionUrl, filePath, fileName);
                context.getLogger().log("downloaded file: " + filePath + "/" + fileName);

                // get google cloud storage credentials from lambda environment variables
                String gcsCredentialsString = System.getenv("gcpCredentialsSecret");

                // base64 decode the credentials to json string
                byte[] decode = Base64.getDecoder().decode(gcsCredentialsString);
                gcsCredentialsString = new String(decode, StandardCharsets.UTF_8);

                // to json object
                JsonObject gcsCredentialsJson = JsonParser.parseString(gcsCredentialsString).getAsJsonObject();

                // convert gcsCredentialsJson to json file
                File gcsCredentialsFile = new File(filePath + "/gcsCredentials.json");
                FileUtils.writeStringToFile(gcsCredentialsFile, gcsCredentialsJson.toString(), StandardCharsets.UTF_8);
                context.getLogger().log("created gcsCredentialsFile");

                GoogleCredentials credentials = GoogleCredentials
                        .fromStream(new FileInputStream(gcsCredentialsFile))
                        .createScoped(Lists.newArrayList("https://www.googleapis.com/auth/cloud-platform"));
                context.getLogger().log("created credentials");

                // get the bucket name
                String bucketName = getBucketName(credentials, projectId);
                context.getLogger().log("bucketName: " + bucketName);
                // upload the file to GCS
                uploadObject(projectId, bucketName, fileName, filePath, credentials);
                context.getLogger().log("uploaded file: " + filePath + "/" + fileName);

                // get the object details
                Map<String, String> objectDetails = getObjectDetails(credentials, projectId, bucketName, fileName);
                String objectName = objectDetails.get("name");
                String contentType = objectDetails.get("contentType");
                String size = objectDetails.get("size");
                context.getLogger().log("objectName: " + objectName);
                context.getLogger().log("contentType: " + contentType);
                context.getLogger().log("size: " + size);

                // send mail to user
                String apiKay = System.getenv("apiKay");
                MailSender.sendMail(apiKay, userEmail, "Your submission has been downloaded" + "\n" +
                        "objectName: " + objectName + "\n" +
                        "contentType: " + contentType + "\n" +
                        "size: " + size);
                String status = "success";
                context.getLogger().log("sent mail to user: " + userEmail);

                // update dynamodb
                trackEmailStatus(userEmail,status);

            }catch (Exception e){
                context.getLogger().log("Error downloading file: " + e);
                String status = "failed";
                String apiKey = System.getenv("apiKey");
                trackEmailStatus(userEmail,status);
                MailSender.sendMail(apiKey, userEmail, "Your submission download failed. The reason is: " + e);
                e.printStackTrace();
            }
            return "SNS event handled";
        } catch (Exception e) {
            context.getLogger().log("Error handling SNS event: " + e);
            throw e;
        }
    }

    public void trackEmailStatus(String userEmail, String status) {
        AmazonDynamoDB ddb = AmazonDynamoDBClientBuilder.defaultClient();
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("UserEmail", new AttributeValue(userEmail));
        item.put("Status", new AttributeValue(status));
        item.put("Timestamp", new AttributeValue().withN(Long.toString(System.currentTimeMillis())));
        PutItemRequest putItemRequest = new PutItemRequest()
                .withTableName(DYNAMODB_TABLE_NAME)
                .withItem(item);
        ddb.putItem(putItemRequest);
    }

    public static String getBucketName(GoogleCredentials credentials, String projectId) {
        Storage storage = StorageOptions.newBuilder()
                .setCredentials(credentials)
                .setProjectId(projectId)
                .build()
                .getService();
        System.out.println("storage: " + storage);

        String bucketName = "";
        // list buckets in the project
        System.out.println("My buckets:");
        for (Bucket bucket : storage.list().iterateAll()) {
            System.out.println(bucket.toString());
            if (bucket.getName().startsWith("csye6225-")) {
                bucketName = bucket.getName();
            }
        }
        return bucketName;
    }

    public static Map<String,String> getObjectDetails(GoogleCredentials credentials,String projectId, String bucketName, String objectName) {
        Storage storage = StorageOptions.newBuilder()
                .setCredentials(credentials)
                .setProjectId(projectId)
                .build()
                .getService();

        Map<String,String> map = new HashMap<>();
        BlobId blobId = BlobId.of(bucketName, objectName);
        Blob blob = storage.get(blobId);
        if (blob != null) {
            String name = blob.getName();
            String contentType = blob.getContentType();
            long size = blob.getSize();
            map.put("name", name);
            map.put("contentType", contentType);
            map.put("size", String.valueOf(size));
        } else {
            System.out.println("No such object");
        }
        return map;
    }

    private static String extractFileNameFromURL(String fileUrl) {
        // Check if URL is null or empty
        if (fileUrl == null || fileUrl.isEmpty()) {
            return "";
        }

        // Use the last part of the URL as the file name
        String[] parts = fileUrl.split("/");
        return parts[parts.length - 1];
    }

    private void downloadHttpUrl(String submissionUrl, String filePath, String fileName) {
        try {
            URL httpurl = new URL(submissionUrl);
            File dirfile = new File(filePath);
            if (!dirfile.exists()) {
                dirfile.mkdirs();
            }
            FileUtils.copyURLToFile(httpurl, new File(filePath + "/" + fileName));

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void uploadObject(String projectId,
                                    String bucketName,
                                    String objectName,
                                    String filePath,
                                    GoogleCredentials credentials) throws IOException {
        // The ID of your GCP project
        // String projectId = "your-project-id";

        // The ID of your GCS bucket
        // String bucketName = "your-unique-bucket-name";

        // The ID of your GCS object
        // String objectName = "your-object-name";

        // The path to your file to upload
        // String filePath = "path/to/your/file"

        // Instantiate a Google Cloud Storage client
        Storage storage = StorageOptions.newBuilder()
                .setCredentials(credentials)
                .setProjectId(projectId)
                .build()
                .getService();

        BlobId blobId = BlobId.of(bucketName, objectName);
        BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();

        // Optional: set a generation-match precondition to avoid potential race
        // conditions and data corruptions. The request returns a 412 error if the
        // preconditions are not met.
        Storage.BlobWriteOption precondition;
        if (storage.get(bucketName, objectName) == null) {
            // For a target object that does not yet exist, set the DoesNotExist precondition.
            // This will cause the request to fail if the object is created before the request runs.
            precondition = Storage.BlobWriteOption.doesNotExist();
            System.out.println("File " + filePath + " uploaded to bucket " + bucketName + " as " + objectName);
        } else {
            // If the destination already exists in your bucket, instead set a generation-match
            // precondition. This will cause the request to fail if the existing object's generation
            // changes before the request runs.
            precondition =
                    Storage.BlobWriteOption.generationMatch(
                            storage.get(bucketName, objectName).getGeneration());
            System.out.println("File " + filePath + " uploaded to bucket " + bucketName + " as " + objectName + " with precondition");
        }
        storage.createFrom(blobInfo, Paths.get(filePath + "/" + objectName), precondition);

        System.out.println(
                "File " + filePath + " uploaded to bucket " + bucketName + " as " + objectName);
    }
}
