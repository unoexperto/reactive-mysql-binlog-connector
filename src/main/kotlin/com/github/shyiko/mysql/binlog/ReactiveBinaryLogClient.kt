/*
* Copyright 2013 Stanley Shyiko
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
@file:Suppress("SameParameterValue")

package com.github.shyiko.mysql.binlog

import com.github.shyiko.mysql.binlog.BinaryLogClient
import com.github.shyiko.mysql.binlog.event.*
import com.github.shyiko.mysql.binlog.event.deserialization.*
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer.EventDataWrapper
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer.EventDataWrapper.Deserializer
import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream
import com.github.shyiko.mysql.binlog.network.*
import com.github.shyiko.mysql.binlog.network.protocol.*
import com.github.shyiko.mysql.binlog.network.protocol.command.*
import reactor.core.publisher.Flux
import reactor.core.publisher.cast
import java.io.EOFException
import java.io.IOException
import java.net.InetSocketAddress
import java.net.Socket
import java.security.GeneralSecurityException
import java.security.cert.CertificateException
import java.security.cert.X509Certificate
import java.time.Duration
import java.util.Arrays
import java.util.LinkedList
import java.util.Locale
import java.util.concurrent.*
import java.util.concurrent.atomic.AtomicBoolean
import java.util.logging.Level
import java.util.logging.Logger
import javax.net.ssl.SSLContext
import javax.net.ssl.TrustManager
import javax.net.ssl.X509TrustManager

/**
 * MySQL replication stream client.
 *
 * @author [Stanley Shyiko](mailto:stanley.shyiko@gmail.com)
 */

/**
 * @param hostname mysql server hostname
 * @param port mysql server port
 * @param schema database name, nullable. Note that this parameter has nothing to do with event filtering. It's
 * used only during the authentication.
 * @param username login name
 * @param password password
 */
class ReactiveBinaryLogClient(private val hostname: String, private val port: Int, private val schema: String?, private val username: String?, private val password: String?) {

    companion object {

        private val DEFAULT_REQUIRED_SSL_MODE_SOCKET_FACTORY: SSLSocketFactory = object : DefaultSSLSocketFactory() {
            @Throws(GeneralSecurityException::class)
            override fun initSSLContext(sc: SSLContext) {
                sc.init(null, arrayOf<TrustManager>(
                    object : X509TrustManager {
                        @Throws(CertificateException::class)
                        override fun checkClientTrusted(x509Certificates: Array<X509Certificate>, s: String) {
                        }

                        @Throws(CertificateException::class)
                        override fun checkServerTrusted(x509Certificates: Array<X509Certificate>, s: String) {
                        }

                        override fun getAcceptedIssuers(): Array<X509Certificate> {
                            return emptyArray()
                        }
                    }
                ), null)
            }
        }

        private val DEFAULT_VERIFY_CA_SSL_MODE_SOCKET_FACTORY: SSLSocketFactory = DefaultSSLSocketFactory()

        // https://dev.mysql.com/doc/internals/en/sending-more-than-16mbyte.html
        private const val MAX_PACKET_LENGTH = 16777215
    }

    private val logger = Logger.getLogger(javaClass.name)

    /**
     * @param blocking blocking mode. If set to false - BinaryLogClient will disconnect after the last event.
     */
    var isBlocking = true

    /**
     * @return server id (65535 by default)
     * @see .setServerId
     */
    /**
     * @param serverId server id (in the range from 1 to 2^32 - 1). This value MUST be unique across whole replication
     * group (that is, different from any other server id being used by any master or slave). Keep in mind that each
     * binary log client (mysql-binlog-connector-java/BinaryLogClient, mysqlbinlog, etc) should be treated as a
     * simplified slave and thus MUST also use a different server id.
     * @see .getServerId
     */
    var serverId: Long = 65535

    @Volatile
    private var binlogFilename: String? = null

    @Volatile
    private var binlogPosition: Long = 4

    /**
     * @return thread id
     */
    @Volatile
    var connectionId: Long = 0
        private set
    private var gtidSet: GtidSet? = null
    private val gtidSetAccessLock = Any()

    /**
     * @see .setGtidSetFallbackToPurged
     * @return whether gtid_purged is used as a fallback
     */
    /**
     * @param gtidSetFallbackToPurged true if gtid_purged should be used as a fallback when gtidSet is set to "" and
     * MySQL server has purged some of the binary logs, false otherwise (default).
     */
    var isGtidSetFallbackToPurged = false

    /**
     * @see .setUseBinlogFilenamePositionInGtidMode
     * @return value of useBinlogFilenamePostionInGtidMode
     */
    /**
     * @param useBinlogFilenamePositionInGtidMode true if MySQL server should start streaming events from a given
     * [.getBinlogFilename] and [.getBinlogPosition] instead of "the oldest known binlog" when
     * [.getGtidSet] is set, false otherwise (default).
     */
    var isUseBinlogFilenamePositionInGtidMode = false

    private var gtid: String? = null
    private var tx = false
    private var eventDeserializer = EventDeserializer()
    private var socketFactory: SocketFactory? = null
    private var sslSocketFactory: SSLSocketFactory? = null

    @Volatile
    private var channel: PacketChannel? = null

    @Volatile
    private var connected = false

    @Volatile
    var masterServerId: Long = -1
        private set

    /**
     * @return heartbeat period in milliseconds (0 if not set (default)).
     * @see .setHeartbeatInterval
     */
    /**
     * @param heartbeatInterval heartbeat period in milliseconds.
     *
     *
     * If set (recommended)
     *
     *  *  HEARTBEAT event will be emitted every "heartbeatInterval".
     *  *  if [.setKeepAlive] is on then keepAlive thread will attempt to reconnect if no
     * HEARTBEAT events were received within [.setKeepAliveInterval] (instead of trying to send
     * PING every [.setKeepAliveInterval], which is fundamentally flawed -
     * https://github.com/shyiko/mysql-binlog-connector-java/issues/118).
     *
     * Note that when used together with keepAlive heartbeatInterval MUST be set less than keepAliveInterval.
     *
     * @see .getHeartbeatInterval
     */
    var heartbeatInterval: Long = 0

    @Volatile
    private var eventLastSeen: Long = 0

    /**
     * @return connect timeout in milliseconds, 3 seconds by default.
     * @see .setConnectTimeout
     */
    /**
     * @param connectTimeout connect timeout in milliseconds.
     * @see .getConnectTimeout
     */
    /**
     * @return "keep alive" connect timeout in milliseconds.
     * @see .setKeepAliveConnectTimeout
     */
    /**
     * @param connectTimeout "keep alive" connect timeout in milliseconds.
     * @see .getKeepAliveConnectTimeout
     */
    var connectTimeout = TimeUnit.SECONDS.toMillis(3)

    /**
     * Alias for BinaryLogClient("localhost", 3306, &lt;no schema&gt; = null, username, password).
     * @see BinaryLogClient.BinaryLogClient
     * @param username login name
     * @param password password
     */
    constructor(username: String?, password: String?) : this("localhost", 3306, null, username, password) {}

    /**
     * Alias for BinaryLogClient("localhost", 3306, schema, username, password).
     * @see BinaryLogClient.BinaryLogClient
     * @param schema database name, nullable
     * @param username login name
     * @param password password
     */
    constructor(schema: String?, username: String?, password: String?) : this("localhost", 3306, schema, username, password) {}

    /**
     * Alias for BinaryLogClient(hostname, port, &lt;no schema&gt; = null, username, password).
     * @see BinaryLogClient.BinaryLogClient
     * @param hostname mysql server hostname
     * @param port mysql server port
     * @param username login name
     * @param password password
     */
    constructor(hostname: String, port: Int, username: String?, password: String?) : this(hostname, port, null, username, password) {}

    private var _sslMode = SSLMode.DISABLED // FIXME: can be replaced with 'get() = field'
    var sSLMode: SSLMode
        get() = _sslMode
        set(sslMode) {
            this._sslMode = sslMode
        }

    /**
     * @return binary log filename, nullable (and null be default). Note that this value is automatically tracked by
     * the client and thus is subject to change (in response to [EventType.ROTATE], for example).
     * @see .setBinlogFilename
     */
//    override
    fun getBinlogFilename(): String {
        return binlogFilename!!
    }

    /**
     * @param binlogFilename binary log filename.
     * Special values are:
     *
     *  * null, which turns on automatic resolution (resulting in the last known binlog and position). This is what
     * happens by default when you don't specify binary log filename explicitly.
     *  * "" (empty string), which instructs server to stream events starting from the oldest known binlog.
     *
     * @see .getBinlogFilename
     */
//    override
    fun setBinlogFilename(binlogFilename: String) {
        this.binlogFilename = binlogFilename
    }

    /**
     * @return binary log position of the next event, 4 by default (which is a position of first event). Note that this
     * value changes with each incoming event.
     * @see .setBinlogPosition
     */
//    override
    fun getBinlogPosition(): Long {
        return binlogPosition
    }

    /**
     * @param binlogPosition binary log position. Any value less than 4 gets automatically adjusted to 4 on connect.
     * @see .getBinlogPosition
     */
//    override
    fun setBinlogPosition(binlogPosition: Long) {
        this.binlogPosition = binlogPosition
    }

    /**
     * @return GTID set. Note that this value changes with each received GTID event (provided client is in GTID mode).
     * @see .setGtidSet
     */
    fun getGtidSet(): String? {
        synchronized(gtidSetAccessLock) { return if (gtidSet != null) gtidSet.toString() else null }
    }

    /**
     * @param gtidSet GTID set (can be an empty string).
     *
     * NOTE #1: Any value but null will switch BinaryLogClient into a GTID mode (this will also set binlogFilename
     * to "" (provided it's null) forcing MySQL to send events starting from the oldest known binlog (keep in mind
     * that connection will fail if gtid_purged is anything but empty (unless
     * [.setGtidSetFallbackToPurged] is set to true))).
     *
     * NOTE #2: GTID set is automatically updated with each incoming GTID event (provided GTID mode is on).
     * @see .getGtidSet
     * @see .setGtidSetFallbackToPurged
     */
    fun setGtidSet(gtidSet: String?) {
        if (gtidSet != null && binlogFilename == null) {
            binlogFilename = ""
        }
        synchronized(gtidSetAccessLock) { this.gtidSet = gtidSet?.let { GtidSet(it) } }
    }

    /**
     * @param eventDeserializer custom event deserializer
     */
    fun setEventDeserializer(eventDeserializer: EventDeserializer?) {
        requireNotNull(eventDeserializer) { "Event deserializer cannot be NULL" }
        this.eventDeserializer = eventDeserializer
    }

    /**
     * @param socketFactory custom socket factory. If not provided, socket will be created with "new Socket()".
     */
    fun setSocketFactory(socketFactory: SocketFactory?) {
        this.socketFactory = socketFactory
    }

    /**
     * @param sslSocketFactory custom ssl socket factory
     */
    fun setSslSocketFactory(sslSocketFactory: SSLSocketFactory?) {
        this.sslSocketFactory = sslSocketFactory
    }

    /**
     * Connect to the replication stream. Note that this method blocks until disconnected.
     * @throws AuthenticationException if authentication fails
     * @throws ServerException if MySQL server responds with an error
     * @throws IOException if anything goes wrong while trying to connect
     * @throws IllegalStateException if binary log client is already connected
     */
    fun createBinLogStream(pingInterval: Duration?): Flux<Event> {
        return Flux.using({
            val notifyWhenDisconnected = AtomicBoolean(false)
            check(channel == null) { "BinaryLogClient is already connected" }

            try {
                channel = openChannel()
                if (channel!!.inputStream.peek() == -1) {
                    throw EOFException()
                }
            } catch (e: IOException) {
                throw IOException(
                    "Failed to connect to MySQL on " + hostname + ":" + port +
                            ". Please make sure it's running.", e
                )
            }
            val greetingPacket = receiveGreeting()
            tryUpgradeToSSL(greetingPacket)
            Authenticator(greetingPacket, channel, schema, username, password).authenticate()
            channel!!.authenticationComplete()
            connectionId = greetingPacket.threadId
            if ("" == binlogFilename) {
                synchronized(gtidSetAccessLock) {
                    if (gtidSet != null && "" == gtidSet.toString() && isGtidSetFallbackToPurged) {
                        gtidSet = GtidSet(fetchGtidPurged())
                    }
                }
            }
            if (binlogFilename == null) {
                fetchBinlogFilenameAndPosition()
            }
            if (binlogPosition < 4) {
                if (logger.isLoggable(Level.WARNING)) {
                    logger.warning("Binary log position adjusted from $binlogPosition to 4")
                }
                binlogPosition = 4
            }
            setupConnection()
            gtid = null
            tx = false
            requestBinaryLogStream()

            connected = true
            notifyWhenDisconnected.set(true)
            if (logger.isLoggable(Level.INFO)) {
                var position: String
                synchronized(gtidSetAccessLock) { position = if (gtidSet != null) gtidSet.toString() else "$binlogFilename/$binlogPosition" }
                logger.info("Connected to $hostname:$port at $position (${if (isBlocking) "sid:$serverId, " else ""}cid:$connectionId)")
            }

            ensureEventDataDeserializer(EventType.ROTATE, RotateEventDataDeserializer::class.java)
            synchronized(gtidSetAccessLock) {
                if (gtidSet != null) {
                    ensureEventDataDeserializer(EventType.GTID, GtidEventDataDeserializer::class.java)
                    ensureEventDataDeserializer(EventType.QUERY, QueryEventDataDeserializer::class.java)
                }
            }
            notifyWhenDisconnected
        }, { s: AtomicBoolean? ->

            val pingFlux = pingInterval?.let { Flux.interval(it).doOnNext {
                channel?.write(PingCommand())
            }.ignoreElements().flux().cast<Event>() }

            pingFlux?.let { Flux.merge(it, eventPacketsStream(channel!!)) } ?: eventPacketsStream(channel!!)
        }) {
            disconnectChannel()
        }
    }

    /**
     * Apply additional options for connection before requesting binlog stream.
     */
    @Throws(IOException::class)
    private fun setupConnection() {
        val checksumType = fetchBinlogChecksum()
        if (checksumType != ChecksumType.NONE) {
            confirmSupportOfChecksum(checksumType)
        }
        setMasterServerId()
        if (heartbeatInterval > 0) {
            enableHeartbeat()
        }
    }

    @Throws(IOException::class)
    private fun openChannel(): PacketChannel {
        val socket = if (socketFactory != null) socketFactory!!.createSocket() else Socket()
        socket.connect(InetSocketAddress(hostname, port), connectTimeout.toInt())
        return PacketChannel(socket)
    }

    @Throws(IOException::class)
    private fun checkError(packet: ByteArray) {
        if (packet[0] == 0xFF.toByte() /* error */) {
            val bytes = Arrays.copyOfRange(packet, 1, packet.size)
            val errorPacket = ErrorPacket(bytes)
            throw ServerException(
                errorPacket.errorMessage, errorPacket.errorCode,
                errorPacket.sqlState
            )
        }
    }

    @Throws(IOException::class)
    private fun receiveGreeting(): GreetingPacket {
        val initialHandshakePacket = channel!!.read()
        checkError(initialHandshakePacket)
        return GreetingPacket(initialHandshakePacket)
    }

    @Throws(IOException::class)
    private fun tryUpgradeToSSL(greetingPacket: GreetingPacket): Boolean {
        val collation = greetingPacket.serverCollation
        if (_sslMode != SSLMode.DISABLED) {
            val serverSupportsSSL = greetingPacket.serverCapabilities and ClientCapabilities.SSL != 0
            if (!serverSupportsSSL && (_sslMode == SSLMode.REQUIRED || _sslMode == SSLMode.VERIFY_CA || _sslMode == SSLMode.VERIFY_IDENTITY)) {
                throw IOException("MySQL server does not support SSL")
            }
            if (serverSupportsSSL) {
                val sslRequestCommand = SSLRequestCommand()
                sslRequestCommand.setCollation(collation)
                channel!!.write(sslRequestCommand)
                val sslSocketFactory =
                    if (sslSocketFactory != null) sslSocketFactory else if (_sslMode == SSLMode.REQUIRED || _sslMode == SSLMode.PREFERRED) DEFAULT_REQUIRED_SSL_MODE_SOCKET_FACTORY else DEFAULT_VERIFY_CA_SSL_MODE_SOCKET_FACTORY
                channel!!.upgradeToSSL(
                    sslSocketFactory,
                    if (_sslMode == SSLMode.VERIFY_IDENTITY) TLSHostnameVerifier() else null
                )
                logger.info("SSL enabled")
                return true
            }
        }
        return false
    }

    @Throws(IOException::class)
    private fun enableHeartbeat() {
        channel!!.write(QueryCommand("set @master_heartbeat_period=" + heartbeatInterval * 1000000))
        val statementResult = channel!!.read()
        checkError(statementResult)
    }

    @Throws(IOException::class)
    private fun setMasterServerId() {
        channel!!.write(QueryCommand("select @@server_id"))
        val resultSet = readResultSet()
        if (resultSet.size >= 0) {
            masterServerId = resultSet[0].getValue(0).toLong()
        }
    }

    @Throws(IOException::class)
    private fun requestBinaryLogStream() {
        val serverId = if (isBlocking) serverId else 0 // http://bugs.mysql.com/bug.php?id=71178
        var dumpBinaryLogCommand: Command
        synchronized(gtidSetAccessLock) {
            dumpBinaryLogCommand = if (gtidSet != null) {
                DumpBinaryLogGtidCommand(
                    serverId,
                    if (isUseBinlogFilenamePositionInGtidMode) binlogFilename else "",
                    if (isUseBinlogFilenamePositionInGtidMode) binlogPosition else 4,
                    gtidSet
                )
            } else {
                DumpBinaryLogCommand(serverId, binlogFilename, binlogPosition)
            }
        }
        channel!!.write(dumpBinaryLogCommand)
    }

    private fun ensureEventDataDeserializer(eventType: EventType, eventDataDeserializerClass: Class<out EventDataDeserializer<*>>) {
        val eventDataDeserializer = eventDeserializer.getEventDataDeserializer(eventType)
        if (eventDataDeserializer.javaClass != eventDataDeserializerClass &&
            eventDataDeserializer.javaClass != Deserializer::class.java
        ) {
            val internalEventDataDeserializer: EventDataDeserializer<*> = eventDataDeserializerClass.getDeclaredConstructor().newInstance()
            eventDeserializer.setEventDataDeserializer(
                eventType,
                Deserializer(
                    internalEventDataDeserializer,
                    eventDataDeserializer
                )
            )
        }
    }

    @Throws(IOException::class)
    private fun fetchGtidPurged(): String {
        channel!!.write(QueryCommand("show global variables like 'gtid_purged'"))
        val resultSet = readResultSet()
        return if (resultSet.isNotEmpty()) resultSet[0].getValue(1).uppercase(Locale.getDefault()) else ""
    }

    @Throws(IOException::class)
    private fun fetchBinlogFilenameAndPosition() {
        val resultSet: Array<ResultSetRowPacket>
        channel!!.write(QueryCommand("show master status"))
        resultSet = readResultSet()
        if (resultSet.isEmpty()) {
            throw IOException("Failed to determine binlog filename/position")
        }
        val resultSetRow = resultSet[0]
        binlogFilename = resultSetRow.getValue(0)
        binlogPosition = resultSetRow.getValue(1).toLong()
    }

    @Throws(IOException::class)
    private fun fetchBinlogChecksum(): ChecksumType {
        channel!!.write(QueryCommand("show global variables like 'binlog_checksum'"))
        val resultSet = readResultSet()
        return if (resultSet.isEmpty()) {
            ChecksumType.NONE
        } else ChecksumType.valueOf(resultSet[0].getValue(1).uppercase(Locale.getDefault()))
    }

    @Throws(IOException::class)
    private fun confirmSupportOfChecksum(checksumType: ChecksumType) {
        channel!!.write(QueryCommand("set @master_binlog_checksum= @@global.binlog_checksum"))
        val statementResult = channel!!.read()
        checkError(statementResult)
        eventDeserializer.setChecksumType(checksumType)
    }

    private fun eventPacketsStream(channel: PacketChannel): Flux<Event> {
        return Flux.using({ channel.inputStream }, { inputStream ->
            Flux.generate { sink ->
                try {
                    if (inputStream.peek() != -1) {
                        val packetLength = inputStream.readInteger(3)
                        inputStream.skip(1) // 1 byte for sequence
                        val marker = inputStream.read()
                        if (marker == 0xFF) {
                            val errorPacket = ErrorPacket(inputStream.read(packetLength - 1))
                            sink.error(ServerException(errorPacket.errorMessage, errorPacket.errorCode, errorPacket.sqlState))
                        } else if (marker == 0xFE && !isBlocking) {
                            sink.complete()
                        } else {
                            try {
                                var event = eventDeserializer.nextEvent(
                                    if (packetLength == MAX_PACKET_LENGTH)
                                        ByteArrayInputStream(readPacketSplitInChunks(inputStream, packetLength - 1))
                                    else
                                        inputStream
                                )
                                if (event == null) {
                                    sink.error(EOFException("Event parser returned null."))
                                } else {
                                    eventLastSeen = System.currentTimeMillis()
                                    updateGtidSet(event)
                                    if (event.getData<EventData>() is EventDataWrapper) {
                                        event = Event(event.getHeader(), (event.getData<EventData>() as EventDataWrapper).external)
                                    }
                                    sink.next(event)
                                    updateClientBinlogFilenameAndPosition(event)
                                }
                            } catch (e: Exception) {
                                sink.error(e)
                            }
                        }
                    } else sink.complete()
                } catch (ex: Throwable) {
                    sink.error(ex)
                }
            }
        }) { it.close() }
    }

    @Throws(IOException::class)
    private fun readPacketSplitInChunks(inputStream: ByteArrayInputStream, packetLength: Int): ByteArray {
        var result = inputStream.read(packetLength)
        var chunkLength: Int
        do {
            chunkLength = inputStream.readInteger(3)
            inputStream.skip(1) // 1 byte for sequence
            result = Arrays.copyOf(result, result.size + chunkLength)
            inputStream.fill(result, result.size - chunkLength, chunkLength)
        } while (chunkLength == Packet.MAX_LENGTH)
        return result
    }

    private fun updateClientBinlogFilenameAndPosition(event: Event) {
        val eventHeader = event.getHeader<EventHeader>()
        val eventType = eventHeader.eventType
        if (eventType == EventType.ROTATE) {
            val rotateEventData = EventDataWrapper.internal(event.getData()) as RotateEventData
            binlogFilename = rotateEventData.binlogFilename
            binlogPosition = rotateEventData.binlogPosition
        } else  // do not update binlogPosition on TABLE_MAP so that in case of reconnect (using a different instance of
        // client) table mapping cache could be reconstructed before hitting row mutation event
            if (eventType != EventType.TABLE_MAP && eventHeader is EventHeaderV4) {
                val nextBinlogPosition = eventHeader.nextPosition
                if (nextBinlogPosition > 0) {
                    binlogPosition = nextBinlogPosition
                }
            }
    }

    private fun updateGtidSet(event: Event) {
        synchronized(gtidSetAccessLock) {
            if (gtidSet == null)
                return
        }
        val eventHeader = event.getHeader<EventHeader>()
        when (eventHeader.eventType) {
            EventType.GTID -> {
                val gtidEventData = EventDataWrapper.internal(event.getData()) as GtidEventData
                gtid = gtidEventData.gtid
            }
            EventType.XID -> {
                commitGtid()
                tx = false
            }
            EventType.QUERY -> {
                val queryEventData = EventDataWrapper.internal(event.getData()) as QueryEventData
                if (queryEventData.sql != null) {
                    if ("BEGIN" == queryEventData.sql) {
                        tx = true
                    } else if ("COMMIT" == queryEventData.sql || "ROLLBACK" == queryEventData.sql) {
                        commitGtid()
                        tx = false
                    } else if (!tx) {
                        // auto-commit query, likely DDL
                        commitGtid()
                    }
                }
            }
            else -> {}
        }
    }

    private fun commitGtid() {
        if (gtid != null) {
            synchronized(gtidSetAccessLock) { gtidSet!!.add(gtid) }
        }
    }

    @Throws(IOException::class)
    private fun readResultSet(): Array<ResultSetRowPacket> {
        val resultSet: MutableList<ResultSetRowPacket> = LinkedList()
        val statementResult = channel!!.read()
        checkError(statementResult)
        while (channel!!.read()[0] != 0xFE.toByte() /* eof */) {
            /* skip */
        }
        var bytes: ByteArray
        while (channel!!.read().also { bytes = it }[0] != 0xFE.toByte() /* eof */) {
            checkError(bytes)
            resultSet.add(ResultSetRowPacket(bytes))
        }
        return resultSet.toTypedArray()
    }

    @Throws(IOException::class)
    private fun disconnectChannel() {
        connected = false
        if (channel != null && channel!!.isOpen) {
            channel?.close()
            channel = null
        }
    }
}
