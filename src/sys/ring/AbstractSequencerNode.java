package sys.ring;

import sys.dht.catadupa.CatadupaNode;
import sys.dht.catadupa.Node;
import sys.net.api.Endpoint;

public class AbstractSequencerNode extends CatadupaNode {


	protected AbstractSequencerNode() {
	}

	@Override
	public void init() {
		super.init();
	}

	protected Endpoint getSequencerFor( String datacenter ) {
        for (Node i : super.db.nodes(0L))
            if (i.isOnline() && datacenter.equals( i.getDatacenter() ) )
                return i.endpoint;

        return self.endpoint; // return null???
    }
}
