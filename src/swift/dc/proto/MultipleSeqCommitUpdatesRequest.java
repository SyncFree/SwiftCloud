package swift.dc.proto;


import java.util.List;

import swift.dc.DCSequencerServer;
import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;
import sys.net.api.rpc.RpcMessage;

/**
 * Message with committted transaction.
 * <p>
 * All updates use the same timestamp. Updates are organized into atomic groups
 * of updates per each object.
 * 
 * @author preguica
 */
public class MultipleSeqCommitUpdatesRequest implements RpcMessage {
    
	public List<SeqCommitUpdatesRequest> commitRecords;
    
    
    /**
     * Fake constructor for Kryo serialization. Do NOT use.
     */
    MultipleSeqCommitUpdatesRequest() {
    }

    public MultipleSeqCommitUpdatesRequest( List<SeqCommitUpdatesRequest> records ) {
    	this.commitRecords = records;
    }

    @Override
    public void deliverTo(RpcHandle conn, RpcHandler handler) {
        ((DCSequencerServer) handler).onReceive(conn, this);
    }
}
