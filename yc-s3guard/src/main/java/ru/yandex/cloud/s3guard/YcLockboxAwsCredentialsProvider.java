package ru.yandex.cloud.s3guard;

import java.time.Duration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
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
public class YcLockboxAwsCredentialsProvider implements AWSCredentialsProvider {

    public static final String ENTRY_ID = "key-id";
    public static final String ENTRY_SECRET = "key-secret";

    private final AWSCredentials credentials;

    public YcLockboxAwsCredentialsProvider(String lockboxEntryName) {
        ServiceFactory factory = ServiceFactory.builder()
                .credentialProvider(Auth.computeEngineBuilder())
                .requestTimeout(Duration.ofMinutes(1))
                .build();
        PayloadServiceBlockingStub service = factory.create(
                PayloadServiceBlockingStub.class,
                PayloadServiceGrpc::newBlockingStub);
        GetPayloadRequest request = GetPayloadRequest.newBuilder()
                .setSecretId(lockboxEntryName).build();
        Payload response = service.get(request);
        String keyId = null;
        String keySecret = null;
        if (response!=null) {
            for (Payload.Entry e : response.getEntriesList()) {
                if ( ENTRY_ID.equalsIgnoreCase(e.getKey()) )
                    keyId = e.getTextValue();
                else if ( ENTRY_SECRET.equalsIgnoreCase(e.getKey()) )
                    keySecret = e.getTextValue();
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
