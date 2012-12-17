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
package sys.net.api;

/**
 * 
 * A generic exception for wrapping assorted serialization errors. Currently
 * unchecked...
 * 
 * @author smd (smd@fct.unl.pt)
 * 
 */
public class SerializerException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public SerializerException(final String cause) {
        super(cause);
    }

    public SerializerException(final Throwable t) {
        super(t);
    }
}
