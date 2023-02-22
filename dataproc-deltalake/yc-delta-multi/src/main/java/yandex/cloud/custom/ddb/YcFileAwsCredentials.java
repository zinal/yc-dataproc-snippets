package yandex.cloud.custom.ddb;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * This class reads the AWS key id and secret from the specified properties file.
 * @author zinal
 */
public class YcFileAwsCredentials implements AWSCredentialsProvider {

    private static final org.slf4j.Logger LOG
            = org.slf4j.LoggerFactory.getLogger(YcFileAwsCredentials.class);

    public static final String ENTRY_ID = "key-id";
    public static final String ENTRY_SECRET = "key-secret";

    private final AWSCredentials credentials;

    public YcFileAwsCredentials(String fileName) {
        final Properties props = new Properties();
        LOG.debug("Reading credentials from file {}", fileName);
        try (FileInputStream fis = new FileInputStream(fileName)) {
            props.loadFromXML(fis);
        } catch(IOException ix) {
            throw new RuntimeException("Failed to read file " + fileName, ix);
        }
        String keyId = props.getProperty(ENTRY_ID);
        String keySecret = props.getProperty(ENTRY_SECRET);
        if (keyId==null || keySecret==null) {
            LOG.warn("Property file {} does not contain entries {} and {}",
                    fileName, ENTRY_ID, ENTRY_SECRET);
        } else {
            LOG.debug("Working with key id {}", keyId);
        }
        this.credentials = (keyId==null || keySecret==null) ?
                null : new BasicAWSCredentials(keyId, keySecret);
    }

    @Override
    public AWSCredentials getCredentials() {
        return credentials;
    }

    @Override
    public void refresh() {
        /* noop */
    }

}
