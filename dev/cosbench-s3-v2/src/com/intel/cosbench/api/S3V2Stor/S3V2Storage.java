package com.intel.cosbench.api.S3V2Stor;

import static com.intel.cosbench.client.S3V2Stor.S3V2Constants.*;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.Arrays;

import org.apache.http.HttpStatus;

// Old Imports
//import com.amazonaws.*;
//import com.amazonaws.Protocol;
//import com.amazonaws.auth.*;
//import com.amazonaws.services.s3.*;
//import com.amazonaws.services.s3.model.*;
//import com.amazonaws.services.s3.model.S3Object;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.SdkField;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.http.apache.*;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.*;
import software.amazon.awssdk.services.s3.model.*;

import com.intel.cosbench.api.storage.*;
import com.amazonaws.util.IOUtils;
import com.intel.cosbench.api.context.*;
import com.intel.cosbench.config.Config;
import com.intel.cosbench.log.Logger;

public class S3V2Storage extends NoneStorage {
	// How long to wait (in milliseconds) when establishing a connection
	private int timeout;
	
    private String accessKey;
    private String secretKey;
    private String endpoint;
    
//    private AmazonS3 client;
    private S3Client client;

    @Override
    public void init(Config config, Logger logger) {
    	super.init(config, logger);
    	logger.trace("Begin s3v2 init");
    	
    	timeout = config.getInt(CONN_TIMEOUT_KEY, CONN_TIMEOUT_DEFAULT);
    	Duration timeoutDuration = Duration.ofMillis(timeout);

    	parms.put(CONN_TIMEOUT_KEY, timeoutDuration);
    	
    	endpoint = config.get(ENDPOINT_KEY, ENDPOINT_DEFAULT);
        accessKey = config.get(AUTH_USERNAME_KEY, AUTH_USERNAME_DEFAULT);
        secretKey = config.get(AUTH_PASSWORD_KEY, AUTH_PASSWORD_DEFAULT);

        boolean pathStyleAccess = config.getBoolean(PATH_STYLE_ACCESS_KEY, PATH_STYLE_ACCESS_DEFAULT);
        
		String proxyHost = config.get(PROXY_HOST_KEY, "");
		String proxyPort = config.get(PROXY_PORT_KEY, "");
        
        parms.put(ENDPOINT_KEY, endpoint);
    	parms.put(AUTH_USERNAME_KEY, accessKey);
    	parms.put(AUTH_PASSWORD_KEY, secretKey);
    	parms.put(PATH_STYLE_ACCESS_KEY, pathStyleAccess);
    	parms.put(PROXY_HOST_KEY, proxyHost);
    	parms.put(PROXY_PORT_KEY, proxyPort);

        logger.debug("using storage config: {}", parms);
        
        
        // Old configuration method 
/*
        ClientConfiguration clientConf = new ClientConfiguration();
        
        clientConf.setConnectionTimeout(timeout);
        clientConf.setSocketTimeout(timeout);
        clientConf.withUseExpectContinue(false);
        
        clientConf.withSignerOverride("S3SignerType");
        clientConf.setProtocol(Protocol.HTTP);
        
		if((!proxyHost.equals(""))&&(!proxyPort.equals(""))){
			clientConf.setProxyHost(proxyHost);
			clientConf.setProxyPort(Integer.parseInt(proxyPort));
		} 
//*/

        // New Configuration
    	logger.trace("Create HTTP client builder");
        ApacheHttpClient.Builder httpClientBuilder = ApacheHttpClient.builder()
        		.connectionTimeout(timeoutDuration)
        		.socketTimeout(timeoutDuration)
        		.expectContinueEnabled(false);
        
/*
        // Unable to find v2 equivalent of S3SignerType, assume default is sufficient
        ClientOverrideConfiguration.Builder overrideConfig = ClientOverrideConfiguration.builder()
        		.advancedOption(SdkAdvancedClientOption.SIGNER);//"S3SignerType"
        // protocol is set indirectly, by setting an http endpoint on the client builder
        
        if((!proxyHost.equals(""))&&(!proxyPort.equals(""))){
        	
            ProxyConfiguration.Builder proxyConfig = ProxyConfiguration.builder();
        	int portNum = Integer.parseInt(proxyPort);
        	
        	try {
        		URI proxyEndpointURI = new URI(null,null,proxyHost,portNum,null,null,null);
        		
        		proxyConfig.endpoint(proxyEndpointURI);
        		httpClientBuilder.proxyConfiguration(proxyConfig.build());
        		
			} catch (URISyntaxException e) {
				// Auto-generated catch block
				e.printStackTrace();
			}
		} 
        
        
		// Old client creation
        AWSCredentials myCredentials = new BasicAWSCredentials(accessKey, secretKey);
        client = new AmazonS3Client(myCredentials, clientConf);
        client.setEndpoint(endpoint);
        client.setS3ClientOptions(new S3ClientOptions().withPathStyleAccess(pathStyleAccess));
//*/

    	logger.trace("Create basic AWS credentials");
        AwsBasicCredentials myCredentials = AwsBasicCredentials.create(accessKey, secretKey);
        
        S3Configuration s3config = S3Configuration.builder()
				.pathStyleAccessEnabled(pathStyleAccess)
				.checksumValidationEnabled(false) //TODO: read from configuration
				.build();

        logger.trace("Create s3 client builder");
        S3ClientBuilder clientBuilder = S3Client.builder()
        		.region(Region.AWS_GLOBAL)
        		.credentialsProvider(StaticCredentialsProvider.create(myCredentials))
        		.httpClientBuilder(httpClientBuilder)
        		.serviceConfiguration(s3config);
		try {
			URI endpointURI = new URI(endpoint);
			clientBuilder.endpointOverride(endpointURI);
		} catch (URISyntaxException e) {
	    	logger.error("Unable to parse endpoint URI: {}", endpoint);
			e.printStackTrace();
		}
		
    	logger.trace("Build client");
        client = clientBuilder.build();
		
        logger.debug("S3 client V2 has been initialized");
    }
    
    @Override
    public void setAuthContext(AuthContext info) {
        super.setAuthContext(info);
//        try {
//        	client = (AmazonS3)info.get(S3CLIENT_KEY);
//            logger.debug("s3client=" + client);
//        } catch (Exception e) {
//            throw new StorageException(e);
//        }
    }
    
    private boolean bucketExists(String container) {
    	try {
    		// doesBucketExist replaced by headBucket exception
        	HeadBucketRequest bucketExistsCheck = HeadBucketRequest.builder()
        			.bucket(container)
        			.build();
        	
        	client.headBucket(bucketExistsCheck);
        	return true;
        	
        } catch (NoSuchBucketException e) {
        	// Bucket DNE
        	return false;
        }
    }

    @Override
    public void dispose() {
        super.dispose();
        client = null;
    }

	@Override
    public InputStream getObject(String container, String object, Config config) {
        super.getObject(container, object, config);
//    	PipedOutputStream response = new PipedOutputStream();
//        PipedInputStream stream;// = new PipedInputStream(response);
        InputStream stream;
        try {
        	// Old method
//            S3Object s3Obj = client.getObject(container, object);
//            stream = s3Obj.getObjectContent();

//        	stream = new PipedInputStream(response);
//        	client.getObject(GetObjectRequest.builder()
//            stream = client.getObjectAsBytes(GetObjectRequest.builder()
//        			.bucket(container)
//        			.key(object)
//        			.build())
//            		.asInputStream();//, 
//        			ResponseTransformer.toOutputStream(response));
//        			ResponseTransformer.toInputStream());
        	HeadObjectResponse objectHeader = client.headObject(HeadObjectRequest.builder()
        			.bucket(container)
        			.key(object)
        			.build());
        	logger.trace(objectHeader.toString());
        	
        	GetObjectRequest objectRequest = GetObjectRequest.builder()
        			.bucket(container)
        			.key(object)
        			.build();
        	
        	ResponseInputStream<GetObjectResponse> response = client.getObject(objectRequest);
        	logger.debug("Object: " + response.toString());
        	stream = response;
            
        } catch (Exception e) {
            throw new StorageException(e);
        }
        
        logger.debug("Object retrieved");
        return stream;
    }

    @Override
    public void createContainer(String container, Config config) {
        super.createContainer(container, config);
        // Old Method
//        try {
//        	if(!client.doesBucketExist(container)) {
//	        	
//	            client.createBucket(container);
//        	}
//        } catch (Exception e) {
//            throw new StorageException(e);
//        }
        
        try {
        	if(!bucketExists(container)) {
	        	CreateBucketRequest bucketRequest = CreateBucketRequest.builder()
	                    .bucket(container)
	                    .build();
	        	
	        	client.createBucket(bucketRequest);
        	}
        } catch (Exception e) {
        	throw new StorageException(e);
        }

    }

	@Override
    public void createObject(String container, String object, InputStream data,
            long length, Config config) {
        super.createObject(container, object, data, length, config);
        try {
        	// Old method
//    		ObjectMetadata metadata = new ObjectMetadata();
//    		metadata.setContentLength(length);
//    		metadata.setContentType("application/octet-stream");
//    		
//        	client.putObject(container, object, data, metadata);
        	
        	logger.trace("object info: ");
        	logger.trace("  container: {}  object: {}", container, object);
        	logger.trace("  length: {}  config: {}",length, config);
//        	logger.trace("  data: {}", data);
        	
//        	logger.debug("Length compare: {}, {}", length, data.available());
        	
//        	byte[] dataBytes = new byte[length];
//        	data.reset();
//        	data.mark((int) length);
        	
        	// creating request directly from stream leads to read after EOF exceptions
//        	RequestBody request = RequestBody.fromInputStream(data, length);
        	
        	// convert input stream to byte array and make request from that
        	RequestBody request = RequestBody.fromBytes(IOUtils.toByteArray(data));

        	PutObjectResponse response = client.putObject(PutObjectRequest.builder()
                    .bucket(container)
                    .key(object)
                    .contentLength(length)
                    .contentType("application/octet-stream")
                    .build(),
//                    RequestBody.fromString(container + object));
//            		RequestBody.fromInputStream(data, length/8));
                    request);
        	
        	logger.debug("Object created: " + response.toString());
//        	logger.trace(Arrays.toString(response.sdkFields().toArray()));
//        	for (SdkField field:response.sdkFields()) {
////        		logger.trace("locaton: {}  member: {}", field.locationName(), field.memberName());
//        		logger.trace("name: {}  value: {}", field.memberName(), field.getValueOrDefault(response));
//        	}
        	
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    @Override
    public void deleteContainer(String container, Config config) {
        super.deleteContainer(container, config);
        // Old method
//        try {
//        	if(client.doesBucketExist(container)) {
//        		client.deleteBucket(container);
//        	}
//        } catch(S3Exception awse) {
//        	if(awse.statusCode() != HttpStatus.SC_NOT_FOUND) {
//        		throw new StorageException(awse);
//        	}
//        } catch (Exception e) {
//            throw new StorageException(e);
//        }
        
        try {
	    	if(bucketExists(container)) {
	    		DeleteBucketRequest bucketRequest = DeleteBucketRequest.builder()
                    .bucket(container)
                    .build();
	    		
	    		client.deleteBucket(bucketRequest);
	    	}
	    } catch(S3Exception awse) {
	    	if(awse.statusCode() != HttpStatus.SC_NOT_FOUND) {
	    		throw new StorageException(awse);
	    	}
	    } catch (Exception e) {
	        throw new StorageException(e);
	    }
    }

    @Override
    public void deleteObject(String container, String object, Config config) {
        super.deleteObject(container, object, config);
        try {
        	DeleteObjectRequest objectRequest = DeleteObjectRequest.builder()
        			.bucket(container)
        			.key(object)
        			.build();
        	
            client.deleteObject(objectRequest);
            
        } catch(S3Exception awse) {
        	if(awse.statusCode() != HttpStatus.SC_NOT_FOUND) {
        		throw new StorageException(awse);
        	}
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

}
