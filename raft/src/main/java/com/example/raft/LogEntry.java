
package com.example.raft;
import java.io.Serializable;

public class LogEntry implements Serializable {
    public int term;
    public int origin;
    public String op;
    public int amount;

    public LogEntry(int term, int origin, String op, int amount) {
        this.term = term;
        this.origin = origin;
        this.op = op;
        this.amount = amount;
    }

    public String toString() {
        return "[term=" + term +
               ", origin=" + origin +
               ", op=" + op +
               ", amount=" + amount + "]";
    }
}
