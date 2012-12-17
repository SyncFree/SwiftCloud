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

public class SerializerTester {

    /**
     * @param args
     */
    public static void main(String[] args) {
        /*
         * Needs to be converted to Kryo v2
         * 
         * Kryo kryo = new Kryo(); VersionVectorWithExceptions vv = new
         * VersionVectorWithExceptions();
         * 
         * kryo.register(Map.class, new MapSerializer(kryo));
         * kryo.register(HashMap.class, new MapSerializer(kryo));
         * kryo.register(ArrayList.class, new CollectionSerializer(kryo));
         * kryo.register(LinkedHashMap.class, new MapSerializer(kryo));
         * kryo.register(VersionVectorWithExceptions.class);
         * 
         * ByteBuffer bb = ByteBuffer.allocate(65536); kryo.writeObject(bb,
         * "teste");
         */

    }

}
