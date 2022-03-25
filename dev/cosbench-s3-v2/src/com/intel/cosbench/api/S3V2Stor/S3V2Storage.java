package com.intel.cosbench.api.S3V2Stor;

import static com.intel.cosbench.client.S3V2Stor.S3V2Constants.*;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;

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
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.apache.*;
import software.amazon.awssdk.services.s3.*;
import software.amazon.awssdk.services.s3.model.*;

import com.intel.cosbench.api.storage.*;
import com.intel.cosbench.api.context.*;
import com.intel.cosbench.config.Config;
import com.intel.cosbench.log.Logger;

public class S3V2Storage extends NoneStorage {
	private int timeout;
	private Duration timeoutDuration;
	
    private String accessKey;
    private String secretKey;
    private String endpoint;
    
//    private AmazonS3 client;
    private S3Client client;

    @Override
    public void init(Config config, Logger logger) {
    	super.init(config, logger);
    	
    	timeout = config.getInt(CONN_TIMEOUT_KEY, CONN_TIMEOUT_DEFAULT);

    	parms.put(CONN_TIMEOUT_KEY, timeout);
    	
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
//        ClientConfiguration clientConf = new ClientConfiguration();
        
//        clientConf.setConnectionTimeout(timeout);
//        clientConf.setSocketTimeout(timeout);
//        clientConf.withUseExpectContinue(false);
        
//        clientConf.withSignerOverride("S3SignerType");
//        clientConf.setProtocol(Protocol.HTTP);
        
//		if((!proxyHost.equals(""))&&(!proxyPort.equals(""))){
//			clientConf.setProxyHost(proxyHost);
//			clientConf.setProxyPort(Integer.parseInt(proxyPort));
//		}

        // New Configuration
        ApacheHttpClient.Builder httpClientBuilder = ApacheHttpClient.builder()
        		.connectionTimeout(timeoutDuration)
        		.socketTimeout(timeoutDuration)
        		.expectContinueEnabled(false);
        
        // Unable to find v2 equivalent of S3SignerType, assume default is sufficient
//        ClientOverrideConfiguration.Builder overrideConfig = ClientOverrideConfiguration.builder()
//        		.advancedOption(SdkAdvancedClientOption.SIGNER);//"S3SignerType"
        // protocol is set indirectly, by setting an http endpoint on the client builder
        
        if((!proxyHost.equals(""))&&(!proxyPort.equals(""))){
        	
            ProxyConfiguration.Builder proxyConfig = ProxyConfiguration.builder();
        	int portNum = Integer.parseInt(proxyPort);
        	
        	try {
        		URI proxyEndpointURI = new URI(null,null,proxyHost,portNum,null,null,null);
        		
        		proxyConfig.endpoint(proxyEndpointURI);
        		httpClientBuilder.proxyConfiguration(proxyConfig.build());
        		
			} catch (URISyntaxException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
        
        
		// Old client creation
//        AWSCredentials myCredentials = new BasicAWSCredentials(accessKey, secretKey);
//        client = new AmazonS3Client(myCredentials, clientConf);
//        client.setEndpoint(endpoint);
//        client.setS3ClientOptions(new S3ClientOptions().withPathStyleAccess(pathStyleAccess));

        AwsBasicCredentials myCredentials = AwsBasicCredentials.create(accessKey, secretKey);
        S3ClientBuilder clientBuilder = S3Client.builder()
        		.credentialsProvider(StaticCredentialsProvider.create(myCredentials))
        		.httpClientBuilder(httpClientBuilder)
        		.serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(pathStyleAccess).build());
		try {
			URI endpointURI = new URI(endpoint);
			clientBuilder.endpointOverride(endpointURI);
		} catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
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
        InputStream stream;
        try {
        	// Old method
//            S3Object s3Obj = client.getObject(container, object);
//            stream = s3Obj.getObjectContent();
        	
        	stream = client.getObject(GetObjectRequest.builder().bucket(container).key(object).build());
            
        } catch (Exception e) {
            throw new StorageException(e);
        }
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
        	
        	client.putObject(PutObjectRequest.builder()
                    .bucket(container)
                    .key(object)
                    .build(),
                    RequestBody.fromInputStream(data, length));
        	
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
