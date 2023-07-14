package ru.yandex.cloud.custom.ddb;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.stream.Collectors;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 * This class reads the AWS key id and secret from Yandex Cloud Lockbox entry.
 * @author zinal
 */
public class YcLockboxAwsCredentials implements AWSCredentialsProvider {

    public static final String ENTRY_ID = "key-id";
    public static final String ENTRY_SECRET = "key-secret";

    private final AWSCredentials credentials;

    public YcLockboxAwsCredentials(String lockboxEntry) {
        String keyId = null;
        String keySecret = null;
        String token = null;
        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            // 1. Grab the service account token
            HttpGet httpGet = new HttpGet("http://169.254.169.254/computeMetadata"
                    + "/v1/instance/service-accounts/default/token");
            httpGet.setHeader("Metadata-Flavor", "Google");
            try (CloseableHttpResponse r = httpClient.execute(httpGet)) {
                HttpEntity he = r.getEntity();
                JSONObject root = parse(he.getContent());
                EntityUtils.consume(he);
                token = root.getString("access_token");
            }
            // 2. Grab the lockbox entry
            httpGet = new HttpGet("https://payload.lockbox.api.cloud.yandex.net/"
                    + "lockbox/v1/secrets/" + lockboxEntry + "/payload");
            httpGet.setHeader("Authorization", "Bearer " + token);
            try (CloseableHttpResponse r = httpClient.execute(httpGet)) {
                int statusCode = r.getStatusLine().getStatusCode();
                if (statusCode != 200) {
                    throw new RuntimeException("Failed to retrieve lockbox entry "
                            + lockboxEntry + ", status " + String.valueOf(statusCode)
                            + ": " + r.getStatusLine().getReasonPhrase());
                }
                HttpEntity he = r.getEntity();
                JSONObject root = parse(he.getContent());
                EntityUtils.consume(he);
                JSONArray entries = root.getJSONArray("entries");
                for (int i=0; i<entries.length(); ++i) {
                    JSONObject entry = entries.getJSONObject(i);
                    String entryKey = entry.getString("key");
                    if ( ENTRY_ID.equalsIgnoreCase(entryKey) ) {
                        keyId = entry.getString("textValue");
                    } else if ( ENTRY_SECRET.equalsIgnoreCase(entryKey) ) {
                        keySecret = entry.getString("textValue");
                    }
                }
            }
        } catch(IOException ix) {
            throw new RuntimeException("Failed to retrieve lockbox entry " + lockboxEntry, ix);
        }

        if (keyId==null || keySecret==null) {
            throw new RuntimeException("Lockbox entry " + lockboxEntry + " does not contain "
                    + "the required entries (" + ENTRY_ID + "," + ENTRY_SECRET + ")");
        }
        this.credentials = new BasicAWSCredentials(keyId, keySecret);
    }

    @Override
    public AWSCredentials getCredentials() {
        return credentials;
    }

    @Override
    public void refresh() {
        /* noop */
    }

    private JSONObject parse(InputStream is) throws IOException {
        final String text = new BufferedReader(
                new InputStreamReader(is, StandardCharsets.UTF_8))
                .lines().collect(Collectors.joining("\n"));
        return new JSONObject(text);
    }

}
