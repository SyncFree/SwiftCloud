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
package swift.utils;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;

import swift.utils.KryoCRDTUtils.Registerable;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;

/**
 * Durable log using Kryo serialization and disk as a storage.
 * <p>
 * All written objects must by Kryolizable.
 * 
 * @author mzawirski
 */
public class KryoDiskLog implements TransactionsLog {
    private final Kryo kryo;
    private final Output output;

    /**
     * @param fileName
     *            file where objects are written
     * @throws FileNotFoundException
     */
    public KryoDiskLog(final String fileName) throws FileNotFoundException {
        kryo = new Kryo();
        KryoCRDTUtils.registerCRDTClasses(new Registerable() {
            @Override
            public void register(Class<?> cl, int id) {
                kryo.register(cl, id);
            }
        });
        output = new Output(new FileOutputStream(fileName));
    }

    @Override
    public synchronized void writeEntry(final long transactionId, final Object object) {
        kryo.writeObject(output, transactionId);
        kryo.writeObject(output, object);
    }

    @Override
    public synchronized void flush() {
        output.flush();
    }

    @Override
    public synchronized void close() {
        output.close();
    }
}
