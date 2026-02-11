
package com.example.raft;
import java.io.Serializable;
import java.util.List;

public class RpcMessage implements Serializable {

    public enum Type {
        REQUEST_VOTE,
        VOTE_RESPONSE,
        APPEND_ENTRIES,
        APPEND_RESPONSE
    }

    public Type type;
    public int term;
    public int senderId;

    public int lastLogIndex;
    public int lastLogTerm;
    public boolean voteGranted;

    public int prevLogIndex;
    public int prevLogTerm;
    public List<LogEntry> entries;
    public int leaderCommit;
    public boolean success;
}
