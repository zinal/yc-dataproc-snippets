package yandex.cloud.custom.ddb;

import java.time.Duration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import io.grpc.ManagedChannel;
import yandex.cloud.api.lockbox.v1.PayloadServiceGrpc;
import yandex.cloud.api.lockbox.v1.PayloadServiceGrpc.PayloadServiceBlockingStub;
import yandex.cloud.api.lockbox.v1.PayloadServiceOuterClass.GetPayloadRequest;
import yandex.cloud.api.lockbox.v1.PayloadOuterClass.Payload;
import yandex.cloud.sdk.ServiceFactory;
import yandex.cloud.sdk.auth.Auth;

/**
 * This class reads the AWS key id and secret from Yandex Cloud Lockbox entry.
 * @author zinal
 */
public class YcLockboxAwsCredentials implements AWSCredentialsProvider {

    private static final org.slf4j.Logger LOG
            = org.slf4j.LoggerFactory.getLogger(YcLockboxAwsCredentials.class);

    public static final String ENTRY_ID = "key-id";
    public static final String ENTRY_SECRET = "key-secret";

    private final AWSCredentials credentials;

    public YcLockboxAwsCredentials(String lockboxEntryName) {
        ServiceFactory factory = ServiceFactory.builder()
                .credentialProvider(Auth.computeEngineBuilder())
                .requestTimeout(Duration.ofMinutes(1))
                .build();
        PayloadServiceBlockingStub service = factory.create(
                PayloadServiceBlockingStub.class,
                PayloadServiceGrpc::newBlockingStub);
        String keyId = null;
        String keySecret = null;
        try {
            GetPayloadRequest request = GetPayloadRequest.newBuilder()
                    .setSecretId(lockboxEntryName).build();
            Payload response = service.get(request);
            if (response!=null) {
                for (Payload.Entry e : response.getEntriesList()) {
                    LOG.debug("Processing Yandex Cloud Lockbox entry: {}", e.getKey());
                    if ( ENTRY_ID.equalsIgnoreCase(e.getKey()) )
                        keyId = e.getTextValue();
                    else if ( ENTRY_SECRET.equalsIgnoreCase(e.getKey()) )
                        keySecret = e.getTextValue();
                }
                if (keyId==null || keySecret==null) {
                    LOG.warn("Entry {} in the Yandex Cloud Lockbox does not contain entries {} and {}",
                            lockboxEntryName, ENTRY_ID, ENTRY_SECRET);
                } else {
                    LOG.debug("Using AWS access key {}", keyId);
                }
            } else {
                LOG.warn("Missing entry {} in the Yandex Cloud Lockbox", lockboxEntryName);
            }
        } finally {
            // ERROR ManagedChannelOrphanWrapper: *~*~*~
            // Channel ManagedChannelImpl{logId=1, target=payload.lockbox.api.cloud.yandex.net:443} was not shutdown properly!!! ~*~*~*
            // Make sure to call shutdown()/shutdownNow() and wait until awaitTermination() returns true.
            if ( service.getChannel() instanceof ManagedChannel ) {
                ((ManagedChannel)service.getChannel()).shutdown();
            }
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
