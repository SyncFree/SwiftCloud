package swift.application.social;

import java.util.Date;

public class Message implements Cloneable, java.io.Serializable {
    private static final long serialVersionUID = 1L;
    private String msg;
    private String sender;
    private String receiver;
    private long date;

    /** Do not use: This constructor is required for Kryo */
    public Message() {
    }

    public Message(String msg, String sender, String receiver, long date) {
        this.msg = msg;
        this.sender = sender;
        this.receiver = receiver;
        this.date = date;
    }

    public Object copy() {
        return new Message(msg, sender, receiver, date);
    }

    public int hashCode() {
        int result = 17;
        result = 37 * result + msg.hashCode();
        result = 37 * result + sender.hashCode();
        result = 37 * result + receiver.hashCode();
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
        return this.date == other.date && this.msg.equals(other.msg) && this.receiver.equals(other.receiver)
                && this.sender.equals(other.sender);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(new Date(date));
        sb.append(", FROM ").append(sender).append(" TO ").append(receiver);
        sb.append(": ").append(msg);
        return sb.toString();
    }

}
