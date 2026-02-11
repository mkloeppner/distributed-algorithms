
package com.example.raft;
import java.io.Serializable;

public class LogEntry implements Serializable {
    public int term;
    public String op;
    public int amount;

    public LogEntry(int term, String op, int amount) {
        this.term = term;
        this.op = op;
        this.amount = amount;
    }

    public String toString() {
        return "[" + term + " " + op + " " + amount + "]";
    }
}
