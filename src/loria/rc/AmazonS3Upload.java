/*
 *  Replication Benchmarker
 *  https://github.com/score-team/replication-benchmarker/
 *  Copyright (C) 2012 LORIA / Inria / SCORE Team
 * 
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 * 
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 * 
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.

 */
package loria.rc;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Stephane Martin <stephane.martin@loria.fr>
 */
public class AmazonS3Upload {
    public static int BUFFSIZE=1024;
    String bucketName = "swiftcloudbucket-miam";
    AmazonS3 s3;
    AWSCredentials credentials = null;

    private void init() throws Exception {
        credentials = new PropertiesCredentials(
                AmazonMachine.class.getResourceAsStream("AwsCredentials.properties"));

        s3 = new AmazonS3Client(credentials);

    }
    
    
    public void createBucketIfNotPresent() {
        /*for (Bucket bucket : s3.listBuckets()) {
            if (bucket.getName().equals(bucketName)) {
                return;
            }
        }*/
        
        if (!s3.doesBucketExist(bucketName)){
            s3.createBucket(bucketName);
        }
    }

    public void uploadFile(File f,String key) {
        s3.putObject(new PutObjectRequest(bucketName, key, f));
    }
    public void downloadAllFiles(){
            ObjectListing objectListing = s3.listObjects(new ListObjectsRequest()
                    .withBucketName(bucketName));
            for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
                 S3Object object = s3.getObject(new GetObjectRequest(bucketName, objectSummary.getKey()));
                 createFile(object.getObjectContent(),objectSummary.getKey());
       //         displayTextInputStream(object.getObjectContent());
                /*System.out.println(" - " + objectSummary.getKey() + "  " +
                                   "(size = " + objectSummary.getSize() + ")");*/
            }
       
    }
    
    private static void createFile(InputStream input,String name){
        try {
            int v=1;
            int len;
            File f= new File(name);
            while(f.exists()){
                f=new File(name+".v"+v++);
            }
            f.createNewFile();
            OutputStream out=new FileOutputStream(f);
            
            byte buf[]=new byte[BUFFSIZE];
            while((len=input.read(buf))>0){
                 out.write(buf,0,len);
            }
            out.close();
            input.close();
            
        } catch (IOException ex) {
            Logger.getLogger(AmazonS3Upload.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    private static void displayTextInputStream(InputStream input) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
        while (true) {
            String line = reader.readLine();
            if (line == null) break;

            System.out.println("    " + line);
        }
        System.out.println();
    }
    
}
