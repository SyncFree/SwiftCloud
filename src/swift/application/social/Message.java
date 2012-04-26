package swift.application.social;

import java.util.Date;

import swift.crdt.interfaces.Copyable;

public class Message implements Copyable, java.io.Serializable {
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

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(new Date(date));
        sb.append(", FROM ");
        sb.append(sender);
        sb.append(" TO ");
        sb.append(receiver);
        sb.append(": ");
        sb.append(msg);
        return sb.toString();

    }
}
