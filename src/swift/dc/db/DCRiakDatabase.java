package swift.dc.db;

import static sys.net.api.Networking.Networking;

import java.io.IOException;
import java.util.Properties;

import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.RiakException;
import com.basho.riak.client.builders.RiakObjectBuilder;
import com.basho.riak.client.raw.RawClient;
import com.basho.riak.client.raw.RiakResponse;
import com.basho.riak.client.raw.StoreMeta;
import com.basho.riak.client.raw.pbc.PBClientAdapter;
import com.basho.riak.pbc.RiakClient;

import swift.crdt.CRDTIdentifier;
import swift.dc.CRDTData;
import swift.dc.DCConstants;
import sys.net.impl.KryoSerializer;

public class DCRiakDatabase implements DCNodeDatabase {
    String url;
    int port;
    RawClient riak;

    @Override
    public void init(Properties props) {
        try {
            url = props.getProperty(DCConstants.RIAK_URL);
            port = Integer.parseInt(props.getProperty(DCConstants.RIAK_URL));
            RiakClient pbcClient;
            pbcClient = new RiakClient(url, port);
            riak = new PBClientAdapter(pbcClient);
        } catch (IOException e) {
            throw new RuntimeException("Cannot contact Riak servers", e);
        }

    }

    @Override
    public CRDTData<?> read(CRDTIdentifier id) {
        try {
            RiakResponse response = riak.fetch( id.getTable(), id.getKey());
            if( response.hasValue()) {
                if( response.hasSiblings()) {
                    //TODO: merge CRDTs... this should not happen...
                    IRiakObject[] obj = response.getRiakObjects();
                    byte[] arr = obj[0].getValue();
                    return (CRDTData<?>)Networking.serializer().readObject(arr);
                } else {
                    IRiakObject[] obj = response.getRiakObjects();
                    byte[] arr = obj[0].getValue();
                    return (CRDTData<?>)Networking.serializer().readObject(arr);
                }
            } else
                return null;
        } catch (IOException e) {
            return null;
        }
    }

    @Override
    public boolean write(CRDTIdentifier id, CRDTData<?> data) {
        try {
            byte[] arr = Networking.serializer().writeObject(data);
            IRiakObject riakObject = RiakObjectBuilder.newBuilder(id.getTable(), id.getKey()).withValue( arr).build();
            
            riak.store(riakObject);
            return true;
        } catch (IOException e) {
            return false;
        }

    }

}
