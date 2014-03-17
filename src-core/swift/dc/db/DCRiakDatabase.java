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
package swift.dc.db;

import static sys.net.api.Networking.Networking;

import java.io.IOException;
import java.util.Properties;

import swift.crdt.core.CRDTIdentifier;
import swift.dc.CRDTData;
import swift.dc.DCConstants;

import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.builders.RiakObjectBuilder;
import com.basho.riak.client.raw.RawClient;
import com.basho.riak.client.raw.RiakResponse;
import com.basho.riak.client.raw.pbc.PBClientAdapter;
import com.basho.riak.pbc.RiakClient;

public class DCRiakDatabase implements DCNodeDatabase {
    String url;
    int port;
    RawClient riak;

    @Override
    public void init(Properties props) {
        try {
            url = props.getProperty(DCConstants.RIAK_URL);
            port = Integer.parseInt(props.getProperty(DCConstants.RIAK_PORT));
            RiakClient pbcClient;
            pbcClient = new RiakClient(url);
            riak = new PBClientAdapter(pbcClient);
        } catch (IOException e) {
            throw new RuntimeException("Cannot contact Riak servers", e);
        }

    }

    @Override
    public synchronized CRDTData<?> read(CRDTIdentifier id) {
        try {
            RiakResponse response = riak.fetch(id.getTable(), id.getKey());
            DCConstants.DCLogger.info("RIAK.get " + id + ": response:" + response.hasValue());
            if (response.hasValue()) {
                if (response.hasSiblings()) {
                    IRiakObject[] obj = response.getRiakObjects();
                    CRDTData<?> data = (CRDTData<?>) Networking.serializer().readObject(obj[0].getValue());
                    for (int i = 1; i < obj.length; i++) {
                        CRDTData<?> t = (CRDTData<?>) Networking.serializer().readObject(obj[i].getValue());
                        // FIXME: this is an outcome of change to the op-based
                        // model and discussions over e-mail.
                        // It's unclear whether this should ever happen given
                        // reliable DCDataServer.
                        DCConstants.DCLogger
                                .warning("DCRiakDatabase: merging a sibling in op-based model may not work!");
                        data.mergeInto(t);
                    }
                    return data;
                } else {
                    IRiakObject[] obj = response.getRiakObjects();
                    byte[] arr = obj[0].getValue();
                    return (CRDTData<?>) Networking.serializer().readObject(arr);
                }
            } else
                return null;
        } catch (IOException e) {
            DCConstants.DCLogger.throwing("DCRiakDatabase", "read", e);
            return null;
        }
    }

    @Override
    public synchronized boolean write(CRDTIdentifier id, CRDTData<?> data) {
        try {
            DCConstants.DCLogger.info("RIAK.store " + id + ": what:" + data.getId());

            byte[] arr = Networking.serializer().writeObject(data);
            IRiakObject riakObject = RiakObjectBuilder.newBuilder(id.getTable(), id.getKey()).withValue(arr).build();

            riak.store(riakObject);
            return true;
        } catch (IOException e) {
            DCConstants.DCLogger.throwing("DCRiakDatabase", "write", e);
            return false;
        }

    }

    @Override
    public boolean ramOnly() {
        return riak == null;
    }

    @Override
    public synchronized Object readSysData(String table, String key) {
        try {
            DCConstants.DCLogger.info("RIAK.SYSget " + table + ": response:" + key);
            RiakResponse response = riak.fetch(table, key);
            if (response.hasValue()) {
                if (response.hasSiblings()) {
                    IRiakObject[] obj = response.getRiakObjects();
                    Object[] objs = new Object[obj.length];
                    for (int i = 0; i < obj.length; i++)
                        objs[i] = Networking.serializer().readObject(obj[0].getValue());
                    return objs;
                } else {
                    IRiakObject[] obj = response.getRiakObjects();
                    return obj[0].getValue();
                }
            } else
                return null;
        } catch (IOException e) {
            DCConstants.DCLogger.throwing("DCRiakDatabase", "read", e);
            return null;
        }
    }

    @Override
    public synchronized boolean writeSysData(String table, String key, Object data) {
        try {
            DCConstants.DCLogger.info("RIAK.SYSstore " + table + ": what:" + key);

            byte[] arr = Networking.serializer().writeObject(data);
            IRiakObject riakObject = RiakObjectBuilder.newBuilder(table, key).withValue(arr).build();

            riak.store(riakObject);
            return true;
        } catch (IOException e) {
            DCConstants.DCLogger.throwing("DCRiakDatabase", "write", e);
            return false;
        }
    }

}
