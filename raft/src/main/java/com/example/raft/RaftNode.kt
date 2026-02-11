package com.example.raft

import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import java.net.ServerSocket
import java.net.Socket
import java.util.*
import java.util.List
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import kotlin.concurrent.Volatile
import kotlin.math.max
import kotlin.math.min

class RaftNode(private val id: Int, private val port: Int, private val peers: MutableMap<Int?, Int>) {
    internal enum class State {
        FOLLOWER, CANDIDATE, LEADER
    }

    @Volatile
    private var state = State.FOLLOWER

    @Volatile
    private var leaderId: Int? = null

    private var currentTerm = 0
    private var votedFor: Int? = null

    private val log: MutableList<LogEntry> = ArrayList<LogEntry>()
    private var commitIndex = -1
    private var lastApplied = -1

    private val nextIndex: MutableMap<Int?, Int?> = ConcurrentHashMap<Int?, Int?>()
    private val matchIndex: MutableMap<Int?, Int?> = ConcurrentHashMap<Int?, Int?>()

    private val stateMachine = WalletStateMachine()
    private val scheduler: ScheduledExecutorService = Executors.newScheduledThreadPool(4)

    @Volatile
    private var lastHeartbeat = System.currentTimeMillis()
    private val random = Random()

    init {
        startServer()
        startElectionTimer()
        startClientLoad()
    }

    private fun logState(msg: String?) {
        println(
            "Node " + id +
                    " | term=" + currentTerm +
                    " | state=" + state +
                    " | commit=" + commitIndex +
                    " | applied=" + lastApplied +
                    " | " + msg
        )
    }

    private fun startServer() {
        Thread(Runnable {
            try {
                ServerSocket(port).use { server ->
                    logState("listening on " + port)
                    while (true) {
                        val socket = server.accept()
                        Thread(Runnable { handle(socket) }).start()
                    }
                }
            } catch (e: Exception) {
                e.printStackTrace()
            }
        }).start()
    }

    private fun handle(socket: Socket) {
        try {
            ObjectInputStream(socket.getInputStream()).use { `in` ->
                val msg = `in`.readObject() as RpcMessage
                val resp = process(msg)
                if (resp != null) {
                    val out = ObjectOutputStream(socket.getOutputStream())
                    out.writeObject(resp)
                    out.flush()
                }
            }
        } catch (ignored: Exception) {
        }
    }

    private fun startElectionTimer() {
        scheduler.scheduleAtFixedRate(Runnable {
            val timeout = (600 + random.nextInt(600)).toLong()
            if (state != State.LEADER &&
                System.currentTimeMillis() - lastHeartbeat > timeout
            ) {
                startElection()
            }
        }, 0, 100, TimeUnit.MILLISECONDS)
    }

    @Synchronized
    private fun startElection() {
        state = State.CANDIDATE
        currentTerm++
        votedFor = id
        var votes = 1

        for (peer in peers.values) {
            if (requestVote(peer)) votes++
        }

        if (votes > (peers.size + 1) / 2) {
            becomeLeader()
        }
    }

    private fun requestVote(peerPort: Int): Boolean {
        try {
            Socket("localhost", peerPort).use { socket ->
                val msg = RpcMessage()
                msg.type = RpcMessage.Type.REQUEST_VOTE
                msg.term = currentTerm
                msg.senderId = id
                msg.lastLogIndex = log.size - 1
                msg.lastLogTerm = if (log.isEmpty()) 0 else log.get(log.size - 1).term

                val out = ObjectOutputStream(socket.getOutputStream())
                out.writeObject(msg)
                out.flush()

                val `in` = ObjectInputStream(socket.getInputStream())
                val resp = `in`.readObject() as RpcMessage
                return resp.voteGranted
            }
        } catch (e: Exception) {
            return false
        }
    }

    private fun becomeLeader() {
        state = State.LEADER
        leaderId = id
        logState("BECAME LEADER")
        for (peer in peers.keys) {
            nextIndex.put(peer, log.size)
            matchIndex.put(peer, -1)
        }
        startHeartbeat()
    }

    private fun startHeartbeat() {
        scheduler.scheduleAtFixedRate(Runnable {
            if (state == State.LEADER) {
                for (peer in peers.entries) {
                    replicate(peer.key!!, peer.value)
                }
            }
        }, 0, 100, TimeUnit.MILLISECONDS)
    }

    private fun replicate(peerId: Int, peerPort: Int) {
        val ni: Int = nextIndex[peerId]!!
        val prevIndex = ni - 1
        val prevTerm = if (prevIndex >= 0) log[prevIndex].term else 0

        val entries: MutableList<LogEntry> = ArrayList<LogEntry>()
        if (ni < log.size) {
            entries.add(log[ni])
        }

        try {
            Socket("localhost", peerPort).use { socket ->
                val msg = RpcMessage()
                msg.type = RpcMessage.Type.APPEND_ENTRIES
                msg.term = currentTerm
                msg.senderId = id
                msg.prevLogIndex = prevIndex
                msg.prevLogTerm = prevTerm
                msg.entries = entries
                msg.leaderCommit = commitIndex

                val out = ObjectOutputStream(socket.getOutputStream())
                out.writeObject(msg)
                out.flush()

                val `in` = ObjectInputStream(socket.getInputStream())
                val resp = `in`.readObject() as RpcMessage
                if (resp.success) {
                    nextIndex[peerId] = ni + entries.size
                    matchIndex[peerId] = ni + entries.size - 1
                    advanceCommit()
                } else {
                    nextIndex.put(peerId, max(0, ni - 1))
                }
            }
        } catch (ignored: Exception) {
        }
    }

    private fun advanceCommit() {
        for (i in log.size - 1 downTo commitIndex + 1) {
            var count = 1
            for (peer in matchIndex.keys) {
                if (matchIndex[peer]!! >= i) count++
            }
            if (count > (peers.size + 1) / 2 &&
                log[i].term == currentTerm
            ) {
                commitIndex = i
                applyLog()
                break
            }
        }
    }

    private fun applyLog() {
        while (lastApplied < commitIndex) {
            lastApplied++
            val entry = log[lastApplied]
            val ok = stateMachine.apply(entry)
            logState(
                "APPLIED index=" + lastApplied +
                        " entry=" + entry +
                        " balance=" + stateMachine.balance +
                        " ok=" + ok
            )
        }
    }

    @Synchronized
    private fun process(msg: RpcMessage): RpcMessage? {
        if (msg.term > currentTerm) {
            currentTerm = msg.term
            state = State.FOLLOWER
            votedFor = null
        }

        when (msg.type) {
            RpcMessage.Type.REQUEST_VOTE -> {
                var grant = false
                val lastTerm = if (log.isEmpty()) 0 else log.get(log.size - 1).term
                val lastIndex = log.size - 1

                val upToDate =
                    msg.lastLogTerm > lastTerm ||
                            (msg.lastLogTerm == lastTerm &&
                                    msg.lastLogIndex >= lastIndex)

                if ((votedFor == null || votedFor == msg.senderId) &&
                    upToDate
                ) {
                    grant = true
                    votedFor = msg.senderId
                    lastHeartbeat = System.currentTimeMillis()
                }

                val voteResp = RpcMessage()
                voteResp.type = RpcMessage.Type.VOTE_RESPONSE
                voteResp.term = currentTerm
                voteResp.voteGranted = grant
                return voteResp
            }

            RpcMessage.Type.APPEND_ENTRIES -> {
                lastHeartbeat = System.currentTimeMillis()
                leaderId = msg.senderId
                state = State.FOLLOWER

                if (msg.prevLogIndex >= 0) {
                    if (msg.prevLogIndex >= log.size ||
                        log.get(msg.prevLogIndex).term != msg.prevLogTerm
                    ) {
                        val fail = RpcMessage()
                        fail.type = RpcMessage.Type.APPEND_RESPONSE
                        fail.success = false
                        fail.term = currentTerm
                        return fail
                    }
                }

                if (!(msg.entries.isEmpty())) {
                    val index = msg.prevLogIndex + 1
                    while (log.size > index) {
                        log.removeAt(log.size - 1)
                    }
                    log.addAll(msg.entries)
                }

                if (msg.leaderCommit > commitIndex) {
                    commitIndex = min(msg.leaderCommit, log.size - 1)
                    applyLog()
                }

                val ok = RpcMessage()
                ok.type = RpcMessage.Type.APPEND_RESPONSE
                ok.success = true
                ok.term = currentTerm
                return ok
            }

            RpcMessage.Type.CLIENT_REQUEST -> if (state == State.LEADER) {
                log.add(msg.entries.get(0))
            }

            else -> {}
        }
        return null
    }

    fun clientRequest(op: String?, amount: Int) {
        if (state == State.LEADER) {
            log.add(LogEntry(currentTerm, id, op, amount))
        } else if (leaderId != null) {
            forwardToLeader(op, amount)
        }
    }

    private fun forwardToLeader(op: String?, amount: Int) {
        try {
            Socket("localhost", peers.get(leaderId)!!).use { socket ->
                val msg = RpcMessage()
                msg.type = RpcMessage.Type.CLIENT_REQUEST
                msg.term = currentTerm
                msg.senderId = id
                msg.entries = listOf<LogEntry>(
                    LogEntry(currentTerm, id, op, amount)
                )

                val out =
                    ObjectOutputStream(socket.getOutputStream())
                out.writeObject(msg)
                out.flush()
            }
        } catch (ignored: Exception) {
        }
    }

    private fun startClientLoad() {
        scheduler.scheduleAtFixedRate(Runnable {
            val add = random.nextBoolean()
            val amount = 1 + random.nextInt(100)
            clientRequest(if (add) "ADD" else "CONSUME", amount)
        }, 5, 2, TimeUnit.SECONDS)
    }

    companion object {
        @JvmStatic
        fun main(args: Array<String>) {
            val id = args[0].toInt()
            val port = args[1].toInt()

            val peers: MutableMap<Int?, Int> = HashMap<Int?, Int>()
            var i = 2
            while (i < args.size) {
                peers.put(
                    args[i].toInt(),
                    args[i + 1].toInt()
                )
                i += 2
            }

            RaftNode(id, port, peers)
        }
    }
}
