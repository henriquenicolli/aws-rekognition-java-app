
package com.henrique.nicolli.test;

import java.awt.List;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.rekognition.AmazonRekognition;
import com.amazonaws.services.rekognition.AmazonRekognitionClientBuilder;
import com.amazonaws.services.rekognition.model.AmazonRekognitionException;
import com.amazonaws.services.rekognition.model.DetectLabelsRequest;
import com.amazonaws.services.rekognition.model.DetectLabelsResult;
import com.amazonaws.services.rekognition.model.Image;
import com.amazonaws.services.rekognition.model.Label;
import com.amazonaws.services.rekognition.model.S3Object;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.event.S3EventNotification;

/**
 * AWS Lambda function with S3 trigger.
 * 
 */
public class App implements RequestHandler<S3EventNotification, String> {
	
	static final Logger log = LoggerFactory.getLogger(App.class);
	String bucket;
	String s3Key;
	

	@Override
	public String handleRequest(S3EventNotification s3EventNotification, Context context) {
		log.info("Lambda function is invoked:" + s3EventNotification.toJson());
		
		AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().build();
		DynamoDB dynamoDB = new DynamoDB(client);

		Table table = dynamoDB.getTable("file_data");
		

		for(S3EventNotification.S3EventNotificationRecord record : s3EventNotification.getRecords()) 
		{	
		  s3Key = record.getS3().getObject().getKey();
		  bucket = record.getS3().getBucket().getName();
			
		  AmazonRekognition rekognitionClient = AmazonRekognitionClientBuilder.defaultClient();

	      DetectLabelsRequest request = new DetectLabelsRequest()
	           .withImage(new Image()
	           .withS3Object(new S3Object()
	           .withName(s3Key).withBucket(bucket)))
	           .withMaxLabels(10)
	           .withMinConfidence(75F);
	      
	      java.util.List <Label> labels = null;
		
	      try {
	          DetectLabelsResult result = rekognitionClient.detectLabels(request);
	          labels = result.getLabels();

	          System.out.println("Detected labels for " + s3Key);
	          for (Label label: labels) 
	          {
	        	 //grava cada label da imagem em um no no banco de dados
	             System.out.println(label.getName() + ": " + label.getConfidence().toString());
	             Item item = new Item()
							.withPrimaryKey("id",new Random().nextInt())
						    .withString("label", label.getConfidence().toString());
			      
			      PutItemOutcome outcome = table.putItem(item);
	             
	          }
	       } catch(AmazonRekognitionException e) {
	          e.printStackTrace();
	       }  		     
		}

		return null;
	}

}
