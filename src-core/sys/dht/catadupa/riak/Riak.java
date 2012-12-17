/*****************************************************************************
 * Copyright 2011-2012 INRIA
 * Copyright 2011-2012 Universidade Nova de Lisboa
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *****************************************************************************/
package sys.dht.catadupa.riak;

import static sys.net.api.Networking.Networking;
import sys.net.api.Serializer;

import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.RiakException;
import com.basho.riak.client.RiakFactory;
import com.basho.riak.client.RiakRetryFailedException;
import com.basho.riak.client.bucket.Bucket;
import com.basho.riak.client.cap.UnresolvedConflictException;
import com.basho.riak.client.convert.ConversionException;

/**
 * 
 * A convenience class to store and load stuff off Riak. Data is stored in riak
 * as raw arrays of bytes after serialization.
 * 
 * @author smd
 * 
 */
public class Riak {

    private static IRiakClient riak;
    private static Serializer serializer;

    public static <T> T load(String bucket, String key) {
        byte[] data = getValue(bucket, key);
        if (data != null) {
            return serializer.readObject(data);
        } else
            return null;
    }

    public static void store(String bucket, String key, Object value) {
        try {
            byte[] data = serializer.writeObject(value);
            getBucket(bucket).store(key, data).execute();
        } catch (RiakRetryFailedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (UnresolvedConflictException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (ConversionException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (RiakException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public static void delete(String bucket, String key) {
        Bucket b;
        try {
            b = getBucket(bucket);
            if (b != null)
                b.delete(key).rw(3).execute();
        } catch (RiakException e) {
            e.printStackTrace();
        }
    }

    static private byte[] getValue(String bucket, String key) {
        try {
            Bucket b = riak.fetchBucket(bucket).execute();
            if (b != null) {
                IRiakObject obj = b.fetch(key).execute();
                if (obj != null)
                    return obj.getValue();
            }
        } catch (RiakRetryFailedException e) {
            e.printStackTrace();
        }
        return null;
    }

    static private Bucket getBucket(String bucket) throws RiakException {
        Bucket res = riak.fetchBucket(bucket).execute();
        if (res == null) {
            return riak.createBucket("bucket").execute();
        } else
            return res;
    }

    static {
        try {
            serializer = Networking.serializer();
            riak = RiakFactory.pbcClient();

        } catch (RiakException e) {
            e.printStackTrace();
        }

    }
}
