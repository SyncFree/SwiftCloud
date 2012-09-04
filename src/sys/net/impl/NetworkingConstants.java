package sys.net.impl;

public interface NetworkingConstants {

	static final int KRYOBUFFER_INITIAL_CAPACITY = 1024;
	
	static final int KRYOBUFFERPOOL_SIZE = 4;
	static final int KRYOBUFFERPOOL_TIMEOUT = 100;
	
	
	static final int RPC_MAX_TIMEOUT = 100000;
	static final long RPC_MAX_SERVICE_ID = 1L << 16;

	static final int RPC_GC_STALE_HANDLERS_SWEEP_FREQUENCY = 10;
	static final int RPC_GC_STALE_HANDLERS_PERIOD = 15 * 60 * 1000;

	static final int NIO_CONNECTION_TIMEOUT = 10000;
	static enum NIO_ReadBufferPoolPolicy { POLLING, BLOCKING } ; 	
	static enum NIO_WriteBufferPoolPolicy { POLLING, BLOCKING } ; 	
	static enum NIO_ReadBufferDispatchPolicy { READER_EXECUTES, USE_THREAD_POOL }

	static final int NIO_MAX_POOL_THREADS = 6;
	static final int NIO_CORE_POOL_THREADS = 3;
	static final int NIO_MAX_IDLE_THREAD_IMEOUT = 30;

	
	static final int RPC_CONNECTION_RETRIES = 3;
	static final int RPC_CONNECTION_RETRY_DELAY = 250;
	
	static final int DHT_CLIENT_RETRIES = 3;
	static final int DHT_CLIENT_TIMEOUT = 100;

	
}
