package com.example.raft

import java.io.Serializable

class RpcMessage : Serializable {
    enum class Type {
        REQUEST_VOTE,
        VOTE_RESPONSE,
        APPEND_ENTRIES,
        APPEND_RESPONSE,
        CLIENT_REQUEST
    }

    var type: Type? = null
    var term: Int = 0
    var senderId: Int = 0

    var lastLogIndex: Int = 0
    var lastLogTerm: Int = 0
    var voteGranted: Boolean = false

    var prevLogIndex: Int = 0
    var prevLogTerm: Int = 0
    var entries: List<LogEntry> = emptyList<LogEntry>()
    var leaderCommit: Int = 0
    var success: Boolean = false
}
