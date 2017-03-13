package gtfstransformer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.onebusaway.gtfs_transformer.GtfsTransformer;
import org.onebusaway.gtfs_transformer.GtfsTransformerLibrary;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.event.S3EventNotification.S3EventNotificationRecord;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;

public class TransformerHandler implements RequestHandler<S3Event, String> {
	
	private final String ZIP_TYPE = "zip";
	private final String TXT_TYPE = "txt";

    public String handleRequest(S3Event s3event, Context context) {
    	final File tmpFile = new File("/tmp");
		final long TIME = System.currentTimeMillis();
		LambdaLogger _log = context.getLogger();

		// GTFS data file : FILE.zip
		S3EventNotificationRecord record = s3event.getRecords().get(0); 	
		
		_log.log("starting transformer from event " + record.getEventSource() + record.getEventName() 
				+ " with file " +  record.getS3().getObject().getKey());
		
		String srcBucket = record.getS3().getBucket().getName();
		String dstBucket = srcBucket + "-transformed";

		// Object key may have spaces or unicode non-ASCII characters.
		String srcDataKey = record.getS3().getObject().getKey().replace('+', ' ');
		try {
			srcDataKey = URLDecoder.decode(srcDataKey, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		
		// Infer the data file type.
		Matcher dataMatcher = Pattern.compile(".*\\.([^\\.]*)").matcher(srcDataKey);
		if (!dataMatcher.matches()) {
			_log.log("Unable to infer file type for key " + srcDataKey);
			return "";
		}
		String dataType = dataMatcher.group(1).toLowerCase();
		if (!(ZIP_TYPE.equals(dataType))) {
			_log.log("Skipping non-data " + srcDataKey);
			return "";
		}
    	
		// Download the GTFS data zip file from S3 into a stream
		AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
		S3ObjectInputStream s3Stream = s3Client.getObject(new GetObjectRequest(srcBucket, srcDataKey)).getObjectContent();
		
		File inputFile = new File("/tmp/" + TIME + "google_transit.zip");
		Path inputFilePath = inputFile.toPath();
		try {
			Files.copy(s3Stream, inputFilePath);
			s3Stream.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	    List<File> paths = new ArrayList<File>();
	    paths.add(inputFile);
		
	    // Download the command txt file from S3 into a stream
		String srcCommandKey = srcDataKey + ".transform.txt";
		
		S3Object s3CommandObject = s3Client.getObject(new GetObjectRequest(srcBucket, srcCommandKey));
		S3ObjectInputStream is = s3CommandObject.getObjectContent();
		
		
	    File commandFile = new File("/tmp/transform.txt" + TIME);
	    Path commandFilePath = commandFile.toPath();

		try {
			Files.copy(is, commandFilePath);
			is.close();
		} catch (Exception e) {
			_log.log("could not download transform " + srcCommandKey);
			e.printStackTrace();
		} 

		// Infer the command file type.
		Matcher commandMatcher = Pattern.compile(".*\\.([^\\.]*)").matcher(srcCommandKey);
		if (!commandMatcher.matches()) {
			_log.log("Unable to infer file type for key " + srcCommandKey);
			return "";
		}
		String commandType = commandMatcher.group(1).toLowerCase();
		if (!(TXT_TYPE.equals(commandType))) {
			_log.log("Skipping non-data " + srcCommandKey);
			return "";
		}
		
	    GtfsTransformer transformer = new GtfsTransformer();
	    transformer.setGtfsInputDirectories(paths);

	    String outputFilePath = "/tmp/transformedFile" + TIME +".zip";
	    transformer.setOutputDirectory(new File(outputFilePath));
	    _log.log("output path: " + outputFilePath);

        try {
        	_log.log("Call GtfsTransformerLibrary : " +commandFile.getAbsolutePath().toString());
			GtfsTransformerLibrary.configureTransformation(transformer, commandFile.getAbsolutePath().toString());
			transformer.run();
			inputFile.delete();
		} catch (Exception e) {
			e.printStackTrace();
		}
        
		File uploadFile = new File(outputFilePath);
		s3Client.putObject(new PutObjectRequest(dstBucket, srcDataKey, uploadFile));
		
		try {
			s3CommandObject.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		// delete temporary files
		
		deleteFile(tmpFile);
		deleteEmptyDirectory(tmpFile);
				
        return "Done";		
    }
	
    public void printDirectory(File file) {  
        File[] childFiles = file.listFiles();  
        for (File childFile : childFiles) {  
            if (childFile.isDirectory()) {  
                printDirectory(childFile);  
            }  
            System.out.println(childFile.getName());  
        }  
    }  
	
	public void deleteFile(File file) {
	    for(File childFile:file.listFiles()){
	        if(childFile.isDirectory()){
	        	deleteFile(childFile);
	        }
	        else{
	        	if(childFile.getPath().matches(".*\\d{10,}.*")){
	        		childFile.delete();
	        	}
	        }
	    }
	}
	
	public boolean deleteEmptyDirectory(File path) {		
		if( path.exists() ) {  
			File[] files = path.listFiles();  
		     for(int i=0; i<files.length; i++) {  
		    	 if(files[i].isDirectory()) {		 
		    		 deleteEmptyDirectory(files[i]);  
		         }  
		         else { 
		        	 files[i].delete();  
		         }  
		     }
		}  
		return path.delete();  
	} 
}
