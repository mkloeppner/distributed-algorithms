package com.example.raft

class WalletStateMachine {
    var balance: Int = 0
        private set

    @Synchronized
    fun apply(entry: LogEntry): Boolean {
        when (entry.op) {
            "ADD" -> {
                balance += entry.amount
                return true
            }

            "CONSUME" -> {
                if (balance - entry.amount < 0) return false
                balance -= entry.amount
                return true
            }
        }
        return false
    }
}
