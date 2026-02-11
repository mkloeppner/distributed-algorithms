
package com.example.raft;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

public class RaftNode {

    enum State { FOLLOWER, CANDIDATE, LEADER }

    private final int id;
    private final int port;
    private final Map<Integer,Integer> peers;

    private volatile State state = State.FOLLOWER;
    private volatile Integer leaderId = null;

    private int currentTerm = 0;
    private Integer votedFor = null;

    private final List<LogEntry> log = new ArrayList<>();
    private int commitIndex = -1;
    private int lastApplied = -1;

    private Map<Integer,Integer> nextIndex = new ConcurrentHashMap<>();
    private Map<Integer,Integer> matchIndex = new ConcurrentHashMap<>();

    private final WalletStateMachine stateMachine = new WalletStateMachine();
    private final ScheduledExecutorService scheduler =
            Executors.newScheduledThreadPool(4);

    private volatile long lastHeartbeat = System.currentTimeMillis();
    private final Random random = new Random();

    public RaftNode(int id, int port, Map<Integer,Integer> peers) {
        this.id = id;
        this.port = port;
        this.peers = peers;
        startServer();
        startElectionTimer();
        startClientLoad();
    }

    private void logState(String msg) {
        System.out.println("Node " + id +
                " | term=" + currentTerm +
                " | state=" + state +
                " | commit=" + commitIndex +
                " | applied=" + lastApplied +
                " | " + msg);
    }

    private void startServer() {
        new Thread(() -> {
            try (ServerSocket server = new ServerSocket(port)) {
                logState("listening on " + port);
                while (true) {
                    Socket socket = server.accept();
                    new Thread(() -> handle(socket)).start();
                }
            } catch (Exception e) { e.printStackTrace(); }
        }).start();
    }

    private void handle(Socket socket) {
        try (ObjectInputStream in = new ObjectInputStream(socket.getInputStream())) {
            RpcMessage msg = (RpcMessage) in.readObject();
            RpcMessage resp = process(msg);
            if (resp != null) {
                ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                out.writeObject(resp);
                out.flush();
            }
        } catch (Exception ignored) {}
    }

    private void startElectionTimer() {
        scheduler.scheduleAtFixedRate(() -> {
            long timeout = 600 + random.nextInt(600);
            if (state != State.LEADER &&
                System.currentTimeMillis() - lastHeartbeat > timeout) {
                startElection();
            }
        }, 0, 100, TimeUnit.MILLISECONDS);
    }

    private synchronized void startElection() {
        state = State.CANDIDATE;
        currentTerm++;
        votedFor = id;
        int votes = 1;

        for (var peer : peers.values()) {
            if (requestVote(peer)) votes++;
        }

        if (votes > (peers.size()+1)/2) {
            becomeLeader();
        }
    }

    private boolean requestVote(int peerPort) {
        try (Socket socket = new Socket("localhost", peerPort)) {
            RpcMessage msg = new RpcMessage();
            msg.type = RpcMessage.Type.REQUEST_VOTE;
            msg.term = currentTerm;
            msg.senderId = id;
            msg.lastLogIndex = log.size()-1;
            msg.lastLogTerm = log.isEmpty()?0:log.get(log.size()-1).term;

            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
            out.writeObject(msg);
            out.flush();

            ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
            RpcMessage resp = (RpcMessage) in.readObject();

            return resp.voteGranted;
        } catch (Exception e) { return false; }
    }

    private void becomeLeader() {
        state = State.LEADER;
        leaderId = id;
        logState("BECAME LEADER");
        for (var peer : peers.keySet()) {
            nextIndex.put(peer, log.size());
            matchIndex.put(peer, -1);
        }
        startHeartbeat();
    }

    private void startHeartbeat() {
        scheduler.scheduleAtFixedRate(() -> {
            if (state == State.LEADER) {
                for (var peer : peers.entrySet()) {
                    replicate(peer.getKey(), peer.getValue());
                }
            }
        }, 0, 100, TimeUnit.MILLISECONDS);
    }

    private void replicate(int peerId, int peerPort) {
        int ni = nextIndex.get(peerId);
        int prevIndex = ni - 1;
        int prevTerm = (prevIndex >= 0) ? log.get(prevIndex).term : 0;

        List<LogEntry> entries = new ArrayList<>();
        if (ni < log.size()) {
            entries.add(log.get(ni));
        }

        try (Socket socket = new Socket("localhost", peerPort)) {

            RpcMessage msg = new RpcMessage();
            msg.type = RpcMessage.Type.APPEND_ENTRIES;
            msg.term = currentTerm;
            msg.senderId = id;
            msg.prevLogIndex = prevIndex;
            msg.prevLogTerm = prevTerm;
            msg.entries = entries;
            msg.leaderCommit = commitIndex;

            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
            out.writeObject(msg);
            out.flush();

            ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
            RpcMessage resp = (RpcMessage) in.readObject();

            if (resp.success) {
                nextIndex.put(peerId, ni + entries.size());
                matchIndex.put(peerId, ni + entries.size() - 1);
                advanceCommit();
            } else {
                nextIndex.put(peerId, Math.max(0, ni-1));
            }

        } catch (Exception ignored) {}
    }

    private void advanceCommit() {
        for (int i = log.size()-1; i > commitIndex; i--) {
            int count = 1;
            for (int peer : matchIndex.keySet()) {
                if (matchIndex.get(peer) >= i) count++;
            }
            if (count > (peers.size()+1)/2 &&
                log.get(i).term == currentTerm) {
                commitIndex = i;
                applyLog();
                break;
            }
        }
    }

    private void applyLog() {
        while (lastApplied < commitIndex) {
            lastApplied++;
            LogEntry entry = log.get(lastApplied);
            boolean ok = stateMachine.apply(entry);
            logState("APPLIED index=" + lastApplied +
                    " entry=" + entry +
                    " balance=" + stateMachine.getBalance() +
                    " ok=" + ok);
        }
    }

    private synchronized RpcMessage process(RpcMessage msg) {

        if (msg.term > currentTerm) {
            currentTerm = msg.term;
            state = State.FOLLOWER;
            votedFor = null;
        }

        switch (msg.type) {

            case REQUEST_VOTE:
                boolean grant = false;
                int lastTerm = log.isEmpty()?0:log.get(log.size()-1).term;
                int lastIndex = log.size()-1;

                boolean upToDate =
                        msg.lastLogTerm > lastTerm ||
                        (msg.lastLogTerm == lastTerm &&
                         msg.lastLogIndex >= lastIndex);

                if ((votedFor == null || votedFor == msg.senderId) &&
                        upToDate) {
                    grant = true;
                    votedFor = msg.senderId;
                    lastHeartbeat = System.currentTimeMillis();
                }

                RpcMessage voteResp = new RpcMessage();
                voteResp.type = RpcMessage.Type.VOTE_RESPONSE;
                voteResp.term = currentTerm;
                voteResp.voteGranted = grant;
                return voteResp;

            case APPEND_ENTRIES:
                lastHeartbeat = System.currentTimeMillis();
                leaderId = msg.senderId;
                state = State.FOLLOWER;

                if (msg.prevLogIndex >= 0) {
                    if (msg.prevLogIndex >= log.size() ||
                        log.get(msg.prevLogIndex).term != msg.prevLogTerm) {
                        RpcMessage fail = new RpcMessage();
                        fail.type = RpcMessage.Type.APPEND_RESPONSE;
                        fail.success = false;
                        fail.term = currentTerm;
                        return fail;
                    }
                }

                if (msg.entries != null && !msg.entries.isEmpty()) {
                    int index = msg.prevLogIndex + 1;
                    while (log.size() > index) {
                        log.remove(log.size()-1);
                    }
                    log.addAll(msg.entries);
                }

                if (msg.leaderCommit > commitIndex) {
                    commitIndex = Math.min(msg.leaderCommit, log.size()-1);
                    applyLog();
                }

                RpcMessage ok = new RpcMessage();
                ok.type = RpcMessage.Type.APPEND_RESPONSE;
                ok.success = true;
                ok.term = currentTerm;
                return ok;

            case CLIENT_REQUEST:
                if (state == State.LEADER) {
                    log.add(msg.entries.get(0));
                }
                break;
        }
        return null;
    }

    public void clientRequest(String op, int amount) {
        if (state == State.LEADER) {
            log.add(new LogEntry(currentTerm, id, op, amount));
        } else if (leaderId != null) {
            forwardToLeader(op, amount);
        }
    }

    private void forwardToLeader(String op, int amount) {
        try (Socket socket = new Socket("localhost", peers.get(leaderId))) {
            RpcMessage msg = new RpcMessage();
            msg.type = RpcMessage.Type.CLIENT_REQUEST;
            msg.term = currentTerm;
            msg.senderId = id;
            msg.entries = List.of(
                    new LogEntry(currentTerm, id, op, amount)
            );

            ObjectOutputStream out =
                    new ObjectOutputStream(socket.getOutputStream());
            out.writeObject(msg);
            out.flush();
        } catch (Exception ignored) {}
    }

    private void startClientLoad() {
        scheduler.scheduleAtFixedRate(() -> {
            boolean add = random.nextBoolean();
            int amount = 1 + random.nextInt(100);
            clientRequest(add ? "ADD" : "CONSUME", amount);
        }, 5, 2, TimeUnit.SECONDS);
    }

    public static void main(String[] args) {

        int id = Integer.parseInt(args[0]);
        int port = Integer.parseInt(args[1]);

        Map<Integer,Integer> peers = new HashMap<>();
        for (int i = 2; i < args.length; i+=2) {
            peers.put(Integer.parseInt(args[i]),
                      Integer.parseInt(args[i+1]));
        }

        new RaftNode(id, port, peers);
    }
}
