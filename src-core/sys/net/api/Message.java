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
 * This interface is used for delivering incoming messages.
 * 
 * The underlying communication system upon receiving a message will invoke the
 * deliverTo method in the message object. A Visitor programming pattern can be
 * used to call a specific handler for the message class.
 * 
 * @author smd (smd@fct.unl.pt)
 * 
 */
public interface Message {

	/**
	 * Implement this method by casting the handler to a more specific class,
	 * with an onReceive(...) method for this particular object.
	 * 
	 * @param conn
	 *            The connection that received the message, which may be used to
	 *            sending a reply back.
	 * @param handler
	 *            The handler the message should be delivered to.
	 */
	void deliverTo(final TransportConnection conn, final MessageHandler handler);

	/**
	 * Records the size of the message after being de-or-serialized...Meant for gathering traffic statistics...
	 * @param size - the length of the serialized representation of the message
	 */
	void setSize( int size ) ;
	
	/**
	 * Retrieves the size of the message after being de-or-serialized...Meant for gathering traffic statistics...
	 * @return
	 */
	int getSize() ;
}
