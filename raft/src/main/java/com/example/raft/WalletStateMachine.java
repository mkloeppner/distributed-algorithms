
package com.example.raft;

public class WalletStateMachine {

    private int balance = 0;

    public synchronized boolean apply(LogEntry entry) {
        switch (entry.op) {
            case "ADD":
                balance += entry.amount;
                return true;
            case "CONSUME":
                if (balance - entry.amount < 0) return false;
                balance -= entry.amount;
                return true;
        }
        return false;
    }

    public int getBalance() {
        return balance;
    }
}
