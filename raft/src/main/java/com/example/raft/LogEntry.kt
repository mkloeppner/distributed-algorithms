package com.example.raft

import java.io.Serializable

class LogEntry(var term: Int, var origin: Int, var op: String?, var amount: Int) : Serializable {
    override fun toString(): String {
        return "[term=" + term +
                ", origin=" + origin +
                ", op=" + op +
                ", amount=" + amount + "]"
    }
}
