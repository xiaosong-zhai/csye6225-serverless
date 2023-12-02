package northeastern.xiaosongzhai;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
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

    private static final String FILE_PATH = "/tmp";
    private static final String PROJECT_ID = "csye6225-demo-406000";
    private static final String DYNAMODB_TABLE_NAME = "emailTrackingTable";

    @Override
    public String handleRequest(SNSEvent snsEvent, Context context) {
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

            // process the submission
            processSubmission(submissionUrl, userEmail, fileName, context);
            return "SNS event handled";
        } catch (Exception e) {
            context.getLogger().log("Error handling SNS event: " + e);
            return "Error";
        }
    }

    private void processSubmission(String submissionUrl, String userEmail, String fileName, Context context) {
        try {
            boolean downloadSuccessful = downloadFile(submissionUrl, fileName, context);
            if (downloadSuccessful) {
                GoogleCredentials credentials = getGoogleCredentials(context);
                String bucketName = getBucketName(credentials, context);
                uploadObject(bucketName, fileName, credentials);

                Map<String, String> objectDetails = getObjectDetails(credentials, bucketName, fileName);
                boolean emailSent = sendSuccessEmail(userEmail, objectDetails, context);
                trackEmailStatus(userEmail, emailSent ? "success" : "failed", context);
            } else {
                sendFailureEmail(userEmail, context);
                trackEmailStatus(userEmail, "failed", context);
            }
        } catch (Exception e) {
            context.getLogger().log("Error processing submission: " + e.getMessage());
            sendFailureEmail(userEmail, context);
            trackEmailStatus(userEmail, "failed", context);
        }
    }

    private GoogleCredentials getGoogleCredentials(Context context) {
        try {
            // get google cloud storage credentials from lambda environment variables
            String gcsCredentialsString = System.getenv("gcpCredentialsSecret");

            // base64 decode the credentials to json string
            byte[] decode = Base64.getDecoder().decode(gcsCredentialsString);
            gcsCredentialsString = new String(decode, StandardCharsets.UTF_8);

            // to json object
            JsonObject gcsCredentialsJson = JsonParser.parseString(gcsCredentialsString).getAsJsonObject();

            // convert gcsCredentialsJson to json file
            File gcsCredentialsFile = new File(FILE_PATH + "/gcsCredentials.json");
            FileUtils.writeStringToFile(gcsCredentialsFile, gcsCredentialsJson.toString(), StandardCharsets.UTF_8);
            context.getLogger().log("created gcsCredentialsFile");

            GoogleCredentials credentials = GoogleCredentials
                    .fromStream(new FileInputStream(gcsCredentialsFile))
                    .createScoped(Lists.newArrayList("https://www.googleapis.com/auth/cloud-platform"));
            context.getLogger().log("created credentials");
            return credentials;
        } catch (Exception e) {
            context.getLogger().log("Error getting google credentials: " + e.getMessage());
            e.printStackTrace();
            return null;
        }
    }

    private void trackEmailStatus(String userEmail, String status, Context context) {
        AmazonDynamoDB ddb = AmazonDynamoDBClientBuilder.defaultClient();
        HashMap<String, AttributeValue> item = new HashMap<>();
        item.put("email", new AttributeValue(userEmail));
        item.put("timestamp", new AttributeValue().withN(Long.toString(System.currentTimeMillis())));
        item.put("Status", new AttributeValue(status));
        PutItemRequest putItemRequest = new PutItemRequest()
                .withTableName(DYNAMODB_TABLE_NAME)
                .withItem(item);
        try {
            ddb.putItem(putItemRequest);
            System.out.println( DYNAMODB_TABLE_NAME +" was successfully updated");
            context.getLogger().log("DynamoDB table updated");

        } catch (ResourceNotFoundException e) {
            System.err.format("Error: The Amazon DynamoDB table \"%s\" can't be found.\n", DYNAMODB_TABLE_NAME);
            System.err.println("Be sure that it exists and that you've typed its name correctly!");
            context.getLogger().log("Error: The Amazon DynamoDB table \"%s\" can't be found.\n" + DYNAMODB_TABLE_NAME);
            System.exit(1);
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            context.getLogger().log("Error: " + e.getMessage());
            System.exit(1);
        }
    }

    private static String getBucketName(GoogleCredentials credentials, Context context) {
        Storage storage = StorageOptions.newBuilder()
                .setCredentials(credentials)
                .setProjectId(SnsEventHandler.PROJECT_ID)
                .build()
                .getService();
        System.out.println("storage: " + storage);

        String bucketName = "";
        // list buckets in the project
        context.getLogger().log("My buckets:");
        for (Bucket bucket : storage.list().iterateAll()) {
            System.out.println(bucket.toString());
            if (bucket.getName().startsWith("csye6225-")) {
                bucketName = bucket.getName();
            }
        }
        return bucketName;
    }

    private static Map<String,String> getObjectDetails(GoogleCredentials credentials, String bucketName, String objectName) {
        Storage storage = StorageOptions.newBuilder()
                .setCredentials(credentials)
                .setProjectId(SnsEventHandler.PROJECT_ID)
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
        // Check if the URL is null or empty
        if (fileUrl == null || fileUrl.isEmpty()) {
            return "";
        }

        // Use the last part of the URL as the file name
        String[] parts = fileUrl.split("/");
        return parts[parts.length - 1];
    }

    private boolean downloadFile(String submissionUrl, String fileName, Context context) {
        try {
            URL httpurl = new URL(submissionUrl);
            File dirfile = new File(FILE_PATH);
            if (!dirfile.exists()) {
                dirfile.mkdirs();
            }
            FileUtils.copyURLToFile(httpurl, new File(FILE_PATH + "/" + fileName));
            context.getLogger().log("Downloaded file: " + FILE_PATH + "/" + fileName);
            return true;
        } catch (Exception e) {
            context.getLogger().log("Error downloading file: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }

    private boolean sendSuccessEmail(String userEmail, Map<String, String> objectDetails, Context context) {
        try {
            String apiKay = System.getenv("apiKay");
            MailSender.sendMail(apiKay, userEmail, "Your submission has been downloaded" + "\n" +
                    "objectName: " + objectDetails.get("name") + "\n" +
                    "contentType: " + objectDetails.get("contentType") + "\n" +
                    "size: " + objectDetails.get("size"));
            context.getLogger().log("Sent mail to user: " + userEmail);
            return true;
        } catch (Exception e) {
            context.getLogger().log("Error sending email: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }

    private void sendFailureEmail(String userEmail, Context context) {
        try {
            String apiKey = System.getenv("apiKey");
            MailSender.sendMail(apiKey, userEmail, "Your submission download failed.");
            context.getLogger().log("Sent failure mail to user: " + userEmail);
        } catch (Exception e) {
            context.getLogger().log("Error sending failure email: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void uploadObject(String bucketName,
                                     String objectName,
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
                .setProjectId(SnsEventHandler.PROJECT_ID)
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
            System.out.println("File " + SnsEventHandler.FILE_PATH + " uploaded to bucket " + bucketName + " as " + objectName);
        } else {
            // If the destination already exists in your bucket, instead set a generation-match
            // precondition. This will cause the request to fail if the existing object's generation
            // changes before the request runs.
            precondition =
                    Storage.BlobWriteOption.generationMatch(
                            storage.get(bucketName, objectName).getGeneration());
            System.out.println("File " + SnsEventHandler.FILE_PATH + " uploaded to bucket " + bucketName + " as " + objectName + " with precondition");
        }
        storage.createFrom(blobInfo, Paths.get(SnsEventHandler.FILE_PATH + "/" + objectName), precondition);

        System.out.println(
                "File " + SnsEventHandler.FILE_PATH + " uploaded to bucket " + bucketName + " as " + objectName);
    }
}
