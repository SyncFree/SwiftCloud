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
package swift.test.microbenchmark;

import java.util.Random;

import swift.crdt.CRDTIdentifier;
import swift.crdt.IntegerTxnLocal;
import swift.crdt.IntegerVersioned;
import swift.crdt.interfaces.CachePolicy;
import swift.crdt.interfaces.IsolationLevel;
import swift.crdt.interfaces.SwiftSession;
import swift.crdt.interfaces.TxnHandle;
import swift.exceptions.NetworkException;
import swift.exceptions.NoSuchObjectException;
import swift.exceptions.VersionNotFoundException;
import swift.exceptions.WrongTypeException;
import swift.test.microbenchmark.interfaces.MicroBenchmarkWorker;
import swift.test.microbenchmark.interfaces.ResultHandler;
import swift.test.microbenchmark.interfaces.WorkerManager;

public class SwiftInitializerWorker implements MicroBenchmarkWorker {

    private WorkerManager manager;
    private SwiftSession clientServer;
    private Random random;
    private CRDTIdentifier[] identifiers;

    protected long startTime, endTime;

    public SwiftInitializerWorker(WorkerManager manager, CRDTIdentifier[] identifiers, Random random, SwiftSession clientServer) {
        this.manager = manager;
        this.identifiers = identifiers;
        this.random = random;
        this.clientServer = clientServer;
    }

    @Override
    public void run() {
        manager.onWorkerStart(this);
        startTime = System.currentTimeMillis();
        try {
            for (int i = 0; i < identifiers.length;) {

                TxnHandle txh = clientServer.beginTxn(IsolationLevel.SNAPSHOT_ISOLATION, CachePolicy.STRICTLY_MOST_RECENT,
                        false);
                for( int j = 0; j < 50 && i < identifiers.length; j++, i++) {
                IntegerTxnLocal integer = (IntegerTxnLocal) txh.get(identifiers[i], true, IntegerVersioned.class);
                integer.add(0);
                //System.out.println(integer.getValue());
                }
                txh.commit();

            }

        } catch (NetworkException e) {
            e.printStackTrace();
        } catch (WrongTypeException e) {
            e.printStackTrace();
        } catch (NoSuchObjectException e) {
            e.printStackTrace();
        } catch (VersionNotFoundException e) {
            e.printStackTrace();
        }
        endTime = System.currentTimeMillis();
        manager.onWorkerFinish(this);

    }

    @Override
    public ResultHandler getResults() {
        return new DBInitializerResultHandler(this);
    }

    @Override
    public void stop() {

    }

    @Override
    public String getWorkerID() {
        return "INITIALIZER";
    }

    @Override
    public RawDataCollector getRawData() {
        return null;
    }

}

class DBInitializerResultHandler implements ResultHandler {

    double executionTime;

    public DBInitializerResultHandler(SwiftInitializerWorker worker) {
        executionTime = (worker.endTime - worker.startTime) / 1000d;
    }

    @Override
    public String toString() {
        String results = "***DB initializer Results***\n";
        results += "Execution Time:\t" + executionTime + "s";
        return results;
    }

    @Override
    public double getExecutionTime() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getNumExecutedTransactions() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getWriteOps() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getReadOps() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public String getWorkerID() {
        // TODO Auto-generated method stub
        return null;
    }

/*    @Override
    public String getRawResults() {
        return "Initializer";
    }
*/
}
