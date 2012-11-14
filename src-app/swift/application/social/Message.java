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
package swift.application.social;

import java.util.Date;

public class Message implements Cloneable, java.io.Serializable, Comparable<Message> {
    private static final long serialVersionUID = 1L;
    private String msg;
    private String sender;
    private long date;

    /** Do not use: This constructor is required for Kryo */
    public Message() {
    }

    public Message(String msg, String sender, long date) {
        this.msg = msg;
        this.sender = sender;
        this.date = date;
    }

    public Object copy() {
        return new Message(msg, sender, date);
    }

    public int hashCode() {
        int result = 17;
        result = 37 * result + msg.hashCode();
        result = 37 * result + sender.hashCode();
        result = 37 * result + (int) (date ^ (date >>> 32));
        return result;
    }

    public boolean equals(Object obj) {
        if (obj == null)
            return false;
        if (obj == this)
            return true;
        if (!(obj instanceof Message))
            return false;
        Message other = (Message) obj;
        return this.date == other.date && this.msg.equals(other.msg) && this.sender.equals(other.sender);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(new Date(date));
        sb.append(", FROM ").append(sender);
        sb.append(": ").append(msg);
        return sb.toString();
    }

    @Override
    public int compareTo(Message other) {
        return new Long(date).compareTo(other.date);
    }

}
