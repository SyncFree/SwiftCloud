package swift.benchmarks.kryo;


import swift.benchmarks.rpc.Reply;
import swift.benchmarks.rpc.Request;
import sys.net.api.Serializer;
import static sys.net.api.Networking.*;

import static sys.Sys.*;

public class KryoBenchmark {

    public static void main(final String[] args) {

        sys.Sys.init();

        Serializer ser = Networking.serializer();
        
        double T0 = Sys.currentTime();
        int n = 0;
        for(;;) {
            ser.readObject(ser.writeObject( new Request(n) ));
            ser.readObject(ser.writeObject( new Reply(n) ));
            n++;
            
            if( (n % 1000 == 0 ) ) {
                System.out.println( n + "  its/sec " + n / (Sys.currentTime() - T0) );
            }
        }
        
    }
}
