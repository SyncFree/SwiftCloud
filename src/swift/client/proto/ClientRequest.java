package swift.client.proto;

import sys.net.api.rpc.RpcMessage;

/**
 * Abstract client to server request, identifying a client by its unique id.
 * 
 * @author mzawirski
 */
public abstract class ClientRequest implements RpcMessage {
    protected String clientId;

    // Fake constructor for Kryo serialization. Do NOT use.
    public ClientRequest() {
    }

    public ClientRequest(final String clientId) {
        this.clientId = clientId;
    }

    /**
     * @return unique client id of originator of this request
     */
    public String getClientId() {
        return clientId;
    }
}
