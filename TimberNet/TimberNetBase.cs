using Newtonsoft.Json.Linq;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Text;

namespace TimberNet
{
    // TODO: I should create a method here that attempts to operate on a steam,
    // and handles errors uniformly if it fails.
    // Pretty much any error means the session is over, but the game should show
    // the error rather than crashing.
    public abstract class TimberNetBase
    {
        public const int HEADER_SIZE = 4;
        public const string TICKS_KEY = "ticksSinceLoad";
        public const string TYPE_KEY = "type";
        public const string SET_STATE_EVENT = "SetState";
        public const string HEARTBEAT_EVENT = "Heartbeat";
        public const int MAX_BUFFER_SIZE = 8192 * 4; // 32K
    // --- Binary frame types (first byte of raw (uncompressed) payload when length header high bit set) ---
    // 0x01 Heartbeat ABS (absolute tick varint)
    // 0x02 Heartbeat DELTA (varint delta-1)
    // 0x12 Events Container (pure binary inner events; may hold RawJson fallback blobs)
        protected const byte FRAME_HEARTBEAT_ABS = 0x01;
        protected const byte FRAME_HEARTBEAT_DELTA = 0x02;
    // 0x11 previously used for mixed JSON container (removed)
    protected const byte FRAME_EVENTS_BIN = 0x12; // pure binary inner events (includes RawJson fallback)
        private int _lastSentHeartbeatTick = -1;
        private int _lastRecvHeartbeatTick = -1;
        private int _lastSentEventsTick = -1;
        private int _lastRecvEventsTick = -1;
        // Event type ID registry (stable alphabetical ordering)
    private static Dictionary<string,int>? _eventTypeToId = null;
    private static string[]? _eventIdToType = null;
        protected static void EnsureEventTypeRegistry()
        {
            if (_eventTypeToId != null) return;
            Type replayBase = null;
            foreach (var asm in AppDomain.CurrentDomain.GetAssemblies())
            {
                try
                {
                    replayBase = asm.GetTypes().FirstOrDefault(t => t.Name == "ReplayEvent" && t.IsClass);
                    if (replayBase != null) break;
                }
                catch { }
            }
            if (replayBase == null)
            {
                _eventTypeToId = new Dictionary<string, int>();
                _eventIdToType = Array.Empty<string>();
                return;
            }
            var allTypes = AppDomain.CurrentDomain.GetAssemblies()
                .SelectMany(a => { try { return a.GetTypes(); } catch { return Array.Empty<Type>(); } })
                .Where(t => !t.IsAbstract && replayBase.IsAssignableFrom(t))
                .Select(t => t.Name)
                .Distinct()
                .OrderBy(n => n, StringComparer.Ordinal)
                .ToList();
            _eventTypeToId = new Dictionary<string, int>(allTypes.Count);
            _eventIdToType = new string[allTypes.Count];
            for (int i = 0; i < allTypes.Count; i++) { _eventTypeToId[allTypes[i]] = i; _eventIdToType[i] = allTypes[i]; }
        }

        private static int SkipVarInt(byte[] buffer, int offset)
        {
            int adv = 0; byte bVal;
            do
            {
                if (offset + adv >= buffer.Length) break;
                bVal = buffer[offset + adv];
                adv++;
            } while ((bVal & 0x80) != 0 && adv < 5);
            return adv;
        }

        // Varint helper (7-bit encoding) used for tick delta
        protected static int WriteVarUInt32(uint value, Span<byte> buffer)
        {
            int index = 0;
            while (value >= 0x80)
            {
                buffer[index++] = (byte)(value | 0x80);
                value >>= 7;
            }
            buffer[index++] = (byte)value;
            return index;
        }
        protected static int ReadVarUInt32(byte[] buffer, int offset, out uint value)
        {
            int shift = 0;
            uint result = 0;
            int index = offset;
            while (true)
            {
                if (index >= buffer.Length) throw new Exception("VarInt truncated");
                byte b = buffer[index++];
                result |= (uint)(b & 0x7F) << shift;
                if ((b & 0x80) == 0) break;
                shift += 7;
                if (shift > 35) throw new Exception("VarInt too long");
            }
            value = result;
            return index - offset;
        }

        // Compute number of bytes a uint32 would take when varint-encoded
        protected static int GetVarUInt32Length(uint value)
        {
            int len = 1;
            while (value >= 0x80)
            {
                value >>= 7;
                len++;
            }
            return len;
        }

        public delegate void MessageReceived(string message);
        public delegate void MapReceived(byte[] mapBytes);

        public event MessageReceived? OnLog;
        public event MessageReceived? OnError;
        public event MapReceived? OnMapReceived;

        private readonly ConcurrentQueue<string> receivedEventQueue = new ConcurrentQueue<string>();
        private readonly ConcurrentQueue<string> logQueue = new ConcurrentQueue<string>();
        private byte[]? mapBytes = null;

        public bool IsStopped { get; private set; } = false;

        public int Hash { get; private set; } = 17;

        public int TickCount { get; private set; }

    // --- Instrumentation counters (baseline) ---
    private long _totalBytesSent = 0;
    private long _totalPacketsSent = 0;
    private long _totalBytesRecv = 0;
    private long _totalPacketsRecv = 0;
    private long _serializeMicrosAccum = 0;
    private int _framesMeasured = 0;
    private int _lastMetricsLogTick = 0;
    // Rolling window (simple) – reset every MetricsLogIntervalTicks
    private long _windowBytesSent = 0;
    private long _windowPacketsSent = 0;
    private long _windowBytesRecv = 0;
    private long _windowPacketsRecv = 0;
    private long _windowSerializeMicros = 0;
    protected virtual int MetricsLogIntervalTicks => 200; // configurable via override if needed
    // Per-event type counters (compressed bytes)
    private readonly Dictionary<string, (long count, long bytes)> _sentEventType = new Dictionary<string, (long count, long bytes)>();
    private readonly Dictionary<string, (long count, long bytes)> _recvEventType = new Dictionary<string, (long count, long bytes)>();

        public int TicksBehind
        {
            get
            {
                if (receivedEvents.Count == 0)
                    return 0;
                return Math.Max(0, GetTick(receivedEvents.Last()) - TickCount);
            }
        }

        public bool Started { get; private set; }

        public virtual bool ShouldTick => Started;

        protected List<JObject> receivedEvents = new List<JObject>();

        public virtual void Close()
        {
            IsStopped = true;
        }

        public TimberNetBase()
        {
            Log("Started");
        }

        protected void Log(string message)
        {
            Log(message, TickCount, Hash);
        }

        protected void Log(string message, int ticks, int hash)
        {
            // Should be threadsafe
            OnLog?.Invoke($"T{ticks.ToString("D4")} [{hash.ToString("X8")}] : {message}");
            //logQueue.Enqueue($"T{ticks.ToString("D4")} [{hash.ToString("X8")}] : {message}");
        }

        public virtual void Start()
        {
            Started = true;
        }

        public static int GetTick(JObject message)
        {
            if (message[TICKS_KEY] == null)
                throw new Exception($"Message does not contain {TICKS_KEY} key");
            return message[TICKS_KEY]!.ToObject<int>();
        }

        public static string GetType(JObject message)
        {
            var type = message["type"];
            if (type == null)
                throw new Exception($"Message does not contain type key");
            return type.ToObject<string>()!;
        }

        protected void InsertInScript(JObject message, List<JObject> script)
        {
            int tick = GetTick(message);
            int index = script.FindIndex(m => GetTick(m) > tick);

            if (index == -1)
                script.Add(message);
            else
                script.Insert(index, message);
        }

        public static List<T> PopEventsForTick<T>(int tick, List<T> events, Func<T, int> getTick)
        {
            List<T> list = new List<T>();
            while (events.Count > 0)
            {
                T message = events[0];
                int delay = getTick(message);
                if (delay > tick)
                    break;

                events.RemoveAt(0);
                list.Add(message);
            }
            return list;
        }

        private List<JObject> PopEventsToProcess(List<JObject> events)
        {
            if (events.Count == 0) return new List<JObject>();
            JObject firstEvent = events[0];
            int firstEventTick = GetTick(firstEvent);
            if (firstEventTick < TickCount)
                Log($"Warning: late event {GetType(firstEvent)}: {firstEventTick} < {TickCount}");

            return PopEventsForTick(TickCount, events, GetTick);
        }

        /**
         * Process an event that the user initiated.
         */
        public virtual void DoUserInitiatedEvent(JObject message)
        {
            AddEventToHash(message);
        }

        /**
        * Process a validated event from a peer that is ready to happen on
        * the Update() thread.
        */
        protected virtual void ProcessReceivedEvent(JObject message)
        {
        }

        protected void AddEventToHash(JObject message)
        {
            if (GetType(message) == SET_STATE_EVENT)
            {
                Hash = message["hash"]!.ToObject<int>();
            }
            else
            {
                AddToHash(message.ToString());
            }
            Log($"Event: {GetType(message)}");
        }

        protected void SendLength(ISocketStream stream, int length)
        {
            byte[] buffer = BitConverter.GetBytes(length);
            if (BitConverter.IsLittleEndian)
                Array.Reverse(buffer);
            stream.Write(buffer, 0, buffer.Length);
        }

    protected void SendEvent(ISocketStream client, JObject message)
    {
        Log($"Sending: {GetType(message)} for tick {GetTick(message)}");

            try
            {
                // If this is a heartbeat event and very small, substitute with a binary heartbeat frame
                var msgType = GetType(message);
                if (msgType == HEARTBEAT_EVENT || msgType == "H")
                {
                    int tick = GetTick(message);
                    int delta = (_lastSentHeartbeatTick < 0) ? -1 : tick - _lastSentHeartbeatTick;
                    bool useDelta = delta > 0 && delta < (1 << 29);
                    Span<byte> payload = stackalloc byte[1 + 5];
                    int p = 0;
                    payload[p++] = useDelta ? FRAME_HEARTBEAT_DELTA : FRAME_HEARTBEAT_ABS;
                    uint varintVal = useDelta ? (uint)(delta - 1) : (uint)tick;
                    p += WriteVarUInt32(varintVal, payload.Slice(p));
                    int frameSize = p;
                    int headerValue = frameSize | unchecked((int)0x80000000); // high bit set marks raw frame (no compression)
                    byte[] outBuf = new byte[HEADER_SIZE + frameSize];
                    byte[] headerBytes = BitConverter.GetBytes(headerValue);
                    if (BitConverter.IsLittleEndian) Array.Reverse(headerBytes);
                    Array.Copy(headerBytes, 0, outBuf, 0, HEADER_SIZE);
                    for (int i = 0; i < frameSize; i++) outBuf[HEADER_SIZE + i] = payload[i];
                    client.Write(outBuf, 0, outBuf.Length);
                    RegisterSend(outBuf.Length); // already includes header
                    TryCountEvent(_sentEventType, (msgType == "H" ? "H" : HEARTBEAT_EVENT) + "(bin)", outBuf.Length);
                    _lastSentHeartbeatTick = tick;
                }
                else
                {
                    // Route all other single events through binary container path for uniform binary handling.
            var start = System.Diagnostics.Stopwatch.GetTimestamp();
                    SendEventsContainerForTick(client, GetTick(message), new System.Collections.Generic.List<JObject>{ message });
            var end = System.Diagnostics.Stopwatch.GetTimestamp();
            AccumSerializeTime(start, end);
                }
            } catch (Exception e)
            {
                Log($"Error sending event: {e.Message}");
            }
        }

        // Allow higher layers (NetIOBase) to send multiple events as one binary container.
        protected internal void SendEventsContainerForTick(ISocketStream client, int tick, List<JObject> events)
        {
            // Re-use existing SendEventsContainer helper if we are server/client side
            // Find underlying socket stream; for now only direct call sites will pass correct client
            try
            {
                EnsureEventTypeRegistry();
                EnsureBinarySupported();
                // Build container bytes directly similar to BuildEventsContainer but inline to avoid duplication
                int delta = (_lastSentEventsTick < 0) ? 0 : tick - _lastSentEventsTick;
                if (delta < 0) delta = 0;
                var payloads = new List<(int typeId, byte[] bytes, string typeName)>();
                foreach (var e in events)
                {
                    string typeName = e[TYPE_KEY]?.ToString();
                    if (typeName == null || !_eventTypeToId.TryGetValue(typeName, out int typeId)) continue;
                    if (_binSchemas == null || !_binSchemas.ContainsKey(typeName))
                    {
                        // Should not happen because EnsureBinarySupported provides RawJson fallback, but guard anyway.
                        EnsureBinarySupported();
                    }
                    byte[] innerBytes = EncodeBinaryInner(typeName, e);
                    payloads.Add((typeId, innerBytes, typeName));
                }
                using var ms = new MemoryStream();
                ms.WriteByte(FRAME_EVENTS_BIN); // always pure binary now
                Span<byte> tmp = stackalloc byte[10];
                int wrote = WriteVarUInt32((uint)delta, tmp); ms.Write(tmp.Slice(0, wrote));
                wrote = WriteVarUInt32((uint)payloads.Count, tmp); ms.Write(tmp.Slice(0, wrote));
                foreach (var (typeId, bytes, _) in payloads)
                {
                    wrote = WriteVarUInt32((uint)typeId, tmp); ms.Write(tmp.Slice(0, wrote));
                    wrote = WriteVarUInt32((uint)bytes.Length, tmp); ms.Write(tmp.Slice(0, wrote));
                    ms.Write(bytes, 0, bytes.Length);
                }
                byte[] body = ms.ToArray();
                int headerValue = body.Length | unchecked((int)0x80000000);
                byte[] headerBytes = BitConverter.GetBytes(headerValue);
                if (BitConverter.IsLittleEndian) Array.Reverse(headerBytes);
                client.Write(headerBytes, 0, headerBytes.Length);
                client.Write(body, 0, body.Length);
                _lastSentEventsTick = tick;
                RegisterSend(body.Length + HEADER_SIZE);
                // Count aggregate container once under synthetic label (always pure now)
                TryCountEvent(_sentEventType, "Events(bin-pure)", body.Length + HEADER_SIZE);
                foreach (var (typeId, bytes, typeName) in payloads)
                {
                    int typeIdLen = GetVarUInt32Length((uint)typeId);
                    int lenLen = GetVarUInt32Length((uint)bytes.Length);
                    int eventBytes = typeIdLen + lenLen + bytes.Length;
                    TryCountEvent(_sentEventType, typeName + "(b)", eventBytes);
                }
            }
            catch (Exception ex)
            {
                Log($"Error SendEventsContainerForTick: {ex.Message}");
            }
        }

        protected bool TryReadLength(ISocketStream stream, out int length)
        {
            byte[] headerBuffer;
            try
            {
                headerBuffer = stream.ReadUntilComplete(HEADER_SIZE);
            }
            catch
            {
                length = 0;
                return false;
            }
            if (BitConverter.IsLittleEndian)
                Array.Reverse(headerBuffer);
            length = BitConverter.ToInt32(headerBuffer, 0);
            return true;
        }

        protected void StartListening(ISocketStream client, bool isClient)
        {
            //Log("Client connected");
            int messageCount = 0;
            while (client.Connected && !IsStopped)
            {
                if (!TryReadLength(client, out int messageLength)) break;

                // First message is always the file
                if (messageCount == 0 && isClient)
                {
                    if (messageLength == 0)
                    {
                        ReadErrorMessage(client);
                        return;
                    }

                    ReceiveFile(client, messageLength);
                    messageCount++;
                    continue;
                }

                if (messageLength == 0)
                {
                    Log("Received message of length 0; aborting listen");
                    break;
                }

                //Log($"Starting to read {messageLength} bytes");
                // TODO: How should this fail and not hang if map stops sending?
                bool rawFrame = (messageLength & unchecked((int)0x80000000)) != 0;
                int payloadLength = messageLength & 0x7FFFFFFF;
                byte[] buffer = client.ReadUntilComplete(payloadLength);
                if (rawFrame)
                {
                    // Interpret raw binary frame
                    if (payloadLength == 0) { Log("Empty raw frame"); continue; }
                    byte frameType = buffer[0];
                    if (frameType == FRAME_HEARTBEAT_ABS || frameType == FRAME_HEARTBEAT_DELTA)
                    {
                        int offset = 1;
                        ReadVarUInt32(buffer, offset, out uint val);
                        int producedTick;
                        if (frameType == FRAME_HEARTBEAT_ABS)
                        {
                            producedTick = (int)val;
                        }
                        else
                        {
                            if (_lastRecvHeartbeatTick < 0) { Log("Delta heartbeat without baseline"); goto endRaw; }
                            producedTick = _lastRecvHeartbeatTick + 1 + (int)val;
                        }
                        _lastRecvHeartbeatTick = producedTick;
                        var hb = new JObject { [TICKS_KEY] = producedTick, [TYPE_KEY] = HEARTBEAT_EVENT };
                        receivedEventQueue.Enqueue(hb.ToString(Newtonsoft.Json.Formatting.None));
                        RegisterRecv(payloadLength + HEADER_SIZE);
                        TryCountEvent(_recvEventType, HEARTBEAT_EVENT + "(bin)", payloadLength + HEADER_SIZE);
                    }
                    else if (frameType == FRAME_EVENTS_BIN)
                    {
                        EnsureEventTypeRegistry();
                        EnsureBinarySupported();
                        int idx = 1;
                        ReadVarUInt32(buffer, idx, out uint deltaVal);
                        idx += SkipVarInt(buffer, idx);
                        int baseTick = _lastRecvEventsTick >= 0 ? _lastRecvEventsTick : (_lastRecvHeartbeatTick >= 0 ? _lastRecvHeartbeatTick : 0);
                        int tick = baseTick + (int)deltaVal;
                        ReadVarUInt32(buffer, idx, out uint evCount);
                        idx += SkipVarInt(buffer, idx);
                        for (int i = 0; i < evCount; i++)
                        {
                            int eventStart = idx;
                            ReadVarUInt32(buffer, idx, out uint typeId);
                            idx += SkipVarInt(buffer, idx);
                            ReadVarUInt32(buffer, idx, out uint len);
                            idx += SkipVarInt(buffer, idx);
                            if (idx + len > buffer.Length) { Log("Bin container truncated"); break; }
                            string typeName = (_eventIdToType != null && typeId < _eventIdToType.Length) ? _eventIdToType[(int)typeId] : null;
                            byte[] innerBytes = new byte[len];
                            Array.Copy(buffer, idx, innerBytes, 0, (int)len);
                            idx += (int)len;
                            if (typeName == null) continue;
                            JObject obj = DecodeBinaryInner(typeName, innerBytes);
                            obj[TICKS_KEY] = tick;
                            obj[TYPE_KEY] = typeName;
                            receivedEventQueue.Enqueue(obj.ToString(Newtonsoft.Json.Formatting.None));
                            int eventConsumed = idx - eventStart;
                            TryCountEvent(_recvEventType, typeName+"(b)", eventConsumed);
                        }
                        RegisterRecv(payloadLength + HEADER_SIZE);
                        TryCountEvent(_recvEventType, "Events(bin-pure)", payloadLength + HEADER_SIZE);
                        _lastRecvEventsTick = tick;
                    }
                    else
                    {
                        Log($"Unknown frame type {frameType}; skipping");
                    }
                    endRaw: ;
                }
                else
                {
                    string message = BufferToStringMessage(buffer);
                    receivedEventQueue.Enqueue(message);
                    RegisterRecv(payloadLength + HEADER_SIZE);
                    try
                    {
                        var obj = Newtonsoft.Json.Linq.JObject.Parse(message);
                        TryCountEvent(_recvEventType, GetType(obj), payloadLength + HEADER_SIZE);
                    }
                    catch { }
                }
                messageCount++;
            }
        }

        protected byte[] MessageToBuffer(JObject message)
        {
            string json = message.ToString(Newtonsoft.Json.Formatting.None);
            return MessageToBuffer(json);
        }

        protected byte[] MessageToBuffer(string message)
        {
            return CompressionUtils.Compress(message);
        }

        protected string BufferToStringMessage(byte[] buffer)
        {
            return CompressionUtils.Decompress(buffer);
        }

        private void ReadErrorMessage(ISocketStream stream)
        {
            if (TryReadLength(stream, out int length))
            {
                byte[] bytes = stream.ReadUntilComplete(length);
                string message = BufferToStringMessage(bytes);
                if (OnError != null)
                {
                    OnError(message);
                }
            }
        }

        public static int CombineHash(int h1, int h2)
        {
            return h1 * 31 + h2;
        }

        private void AddToHash(string str)
        {
            AddToHash(Encoding.UTF8.GetBytes(str));
        }

        private void AddToHash(byte[] bytes)
        {
            Hash = CombineHash(Hash, GetHashCode(bytes));
        }

        public static int GetHashCode(byte[] bytes)
        {
            int code = 0;
            foreach (byte b in bytes)
            {
                code = CombineHash(code, b);
            }
            return code;
        }

        private void AddFileToHash(byte[] bytes)
        {
            AddToHash(bytes);
        }

        private void ReceiveFile(ISocketStream stream, int messageLength)
        {
            byte[] mapBytes = stream.ReadUntilComplete(messageLength);
            AddFileToHash(mapBytes);
            Log($"Received map with length {mapBytes.Length} and Hash: {GetHashCode(mapBytes).ToString("X8")}");
            this.mapBytes = mapBytes;
        }

        private void ProcessReceivedEventsQueue()
        {
            while (receivedEventQueue.TryDequeue(out string? message))
            {
                try
                {
                    ReceiveEvent(JObject.Parse(message));
                } catch (Exception e)
                {
                    Log($"Error receiving event: {e.Message}");
                }
            }
        }

        /**
         * Called when an event is received from a connected Net
         * and ready to be added to the queue for processing.
         */
        protected virtual void ReceiveEvent(JObject message)
        {
            InsertInScript(message, receivedEvents);
        }

        private void ProcessLogs()
        {
            while (logQueue.TryDequeue(out string? log))
            {
                OnLog?.Invoke(log);
            }
        }

        private void ProcessReceivedMap()
        {
            if (mapBytes == null) return;
            OnMapReceived?.Invoke(mapBytes);
            mapBytes = null;
        }

        /**
         * Updates, processing queued logs, maps and events.
         */
        public void Update()
        {
            ProcessLogs();
            if (!Started) return;
            ProcessReceivedMap();
            ProcessReceivedEventsQueue();
            MaybeLogMetrics();

        }

        private List<JObject> FilterEvents(List<JObject> events)
        {
            return events.Where(ShouldReadEvent).ToList();
        }

        private bool ShouldReadEvent(JObject message)
        {
            string type = GetType(message);
            return !(type == SET_STATE_EVENT || type == HEARTBEAT_EVENT);
        }

        /**
         * Reads received events that should be processed by the game
         * and deletes and returns.
         * Will call update before processing events.
         */
        public virtual List<JObject> ReadEvents(int ticksSinceLoad)
        {
            //if (ticksSinceLoad != TickCount) Log($"Setting ticks from {TickCount} to {ticksSinceLoad}");
            TickCount = ticksSinceLoad;
            Update();
            List<JObject> toProcess = PopEventsToProcess(receivedEvents);
            toProcess.ForEach(e => ProcessReceivedEvent(e));
            return FilterEvents(toProcess);
        }

        // --- Instrumentation helpers ---
        private void RegisterSend(int bytes)
        {
            _totalBytesSent += bytes;
            _totalPacketsSent++;
            _windowBytesSent += bytes;
            _windowPacketsSent++;
        }

        private void RegisterRecv(int bytes)
        {
            _totalBytesRecv += bytes;
            _totalPacketsRecv++;
            _windowBytesRecv += bytes;
            _windowPacketsRecv++;
        }

        private void AccumSerializeTime(long startTimestamp, long endTimestamp)
        {
            // Stopwatch timestamp to microseconds
            long freq = System.Diagnostics.Stopwatch.Frequency;
            long deltaTicks = endTimestamp - startTimestamp;
            long micros = (long)(deltaTicks * 1_000_000L / freq);
            _serializeMicrosAccum += micros;
            _windowSerializeMicros += micros;
            _framesMeasured++;
        }

        private void MaybeLogMetrics()
        {
            if (TickCount - _lastMetricsLogTick < MetricsLogIntervalTicks) return;
            _lastMetricsLogTick = TickCount;
            if (MetricsLogIntervalTicks <= 0) return;
            double avgPktSizeSent = _windowPacketsSent == 0 ? 0 : (double)_windowBytesSent / _windowPacketsSent;
            double avgPktSizeRecv = _windowPacketsRecv == 0 ? 0 : (double)_windowBytesRecv / _windowPacketsRecv;
            double avgSerializeMicros = _framesMeasured == 0 ? 0 : (double)_windowSerializeMicros / _framesMeasured;
            Log($"NET METRICS window={MetricsLogIntervalTicks} ticks | sent={_windowBytesSent}B/{_windowPacketsSent}pkts (avg {avgPktSizeSent:F1} B) | recv={_windowBytesRecv}B/{_windowPacketsRecv}pkts (avg {avgPktSizeRecv:F1} B) | serializeAvg={avgSerializeMicros:F1} us | totals sent={_totalBytesSent}B recv={_totalBytesRecv}B");
            Log(TopEventsSummary());
            // Explicit cumulative totals line for easier external parsing
            double totalSerializePerPkt = _totalPacketsSent == 0 ? 0 : (double)_serializeMicrosAccum / _totalPacketsSent;
            Log($"NET TOTALS sent={_totalBytesSent}B/{_totalPacketsSent}pkts recv={_totalBytesRecv}B/{_totalPacketsRecv}pkts serializeAvgPerSentPkt={totalSerializePerPkt:F1}us");
            _windowBytesSent = _windowPacketsSent = 0;
            _windowBytesRecv = _windowPacketsRecv = 0;
            _windowSerializeMicros = 0;
            _framesMeasured = 0;
        }

        private static void TryCountEvent(Dictionary<string,(long count,long bytes)> dict, string type, int bytes)
        {
            if (string.IsNullOrEmpty(type)) return;
            if (dict.TryGetValue(type, out var v))
                dict[type] = (v.count + 1, v.bytes + bytes);
            else
                dict[type] = (1, bytes);
        }

        private string TopEventsSummary(int topN = 5)
        {
            string Format(Dictionary<string,(long count,long bytes)> d) => string.Join(", ", d.OrderByDescending(kv => kv.Value.bytes).Take(topN)
                .Select(kv => $"{kv.Key}:{kv.Value.count}c/{kv.Value.bytes}B"));
            return $"EVENT TYPES sent[{Format(_sentEventType)}] recv[{Format(_recvEventType)}]";
        }

        public bool HasEventsForTick(int tickSinceLoad)
        {
            Update();
            return receivedEvents.Any(e => GetTick(e) == tickSinceLoad);
        }

        // --- Binary inner helpers ---
        private enum BinFieldKind : byte {
            Int = 1,
            Float = 2,
            Bool = 3,
            String = 4,
            Enum = 5,
            Vector3Int = 6,
            StringList = 7,
            Vector3 = 8,
            Vector3IntList = 9,
            GuidList = 10,
            Ray = 11,
            RawJson = 250 // fallback: full event (minus meta) JSON blob so we can still treat as binary
        }
        private class BinSchema { public (string name, BinFieldKind kind)[] fields = Array.Empty<(string,BinFieldKind)>(); }
        private static Dictionary<string, BinSchema>? _binSchemas = null;

        private static bool IsSimpleFieldType(Type t)
        {
            if (t == typeof(int) || t == typeof(float) || t == typeof(bool) || t == typeof(string)) return true;
            if (t.IsEnum) return true;
            // Unity Vector3Int without referencing assembly directly: check FullName
            if (t.FullName == "UnityEngine.Vector3Int") return true;
            if (t.FullName == "UnityEngine.Vector3") return true;
            if (t.FullName == "UnityEngine.Ray") return true;
            // Generic lists
            if (t.IsGenericType && t.GetGenericTypeDefinition() == typeof(List<>))
            {
                var ga = t.GetGenericArguments()[0];
                if (ga == typeof(string)) return true; // StringList
                if (ga.FullName == "UnityEngine.Vector3Int") return true; // Vector3IntList
                if (ga == typeof(Guid)) return true; // GuidList
            }
            return false;
        }

        private static BinFieldKind KindFor(Type t)
        {
            if (t == typeof(int)) return BinFieldKind.Int;
            if (t == typeof(float)) return BinFieldKind.Float;
            if (t == typeof(bool)) return BinFieldKind.Bool;
            if (t == typeof(string)) return BinFieldKind.String;
            if (t.IsEnum) return BinFieldKind.Enum;
            if (t.FullName == "UnityEngine.Vector3Int") return BinFieldKind.Vector3Int;
            if (t.FullName == "UnityEngine.Vector3") return BinFieldKind.Vector3;
            if (t.FullName == "UnityEngine.Ray") return BinFieldKind.Ray;
            if (t.IsGenericType && t.GetGenericTypeDefinition() == typeof(List<>))
            {
                var ga = t.GetGenericArguments()[0];
                if (ga == typeof(string)) return BinFieldKind.StringList;
                if (ga.FullName == "UnityEngine.Vector3Int") return BinFieldKind.Vector3IntList;
                if (ga == typeof(Guid)) return BinFieldKind.GuidList;
            }
            throw new Exception("Unsupported field type for binary schema: " + t);
        }

        private static void EnsureBinarySupported()
        {
            if (_binSchemas != null) return;
            EnsureEventTypeRegistry();
            _binSchemas = new Dictionary<string, BinSchema>(StringComparer.Ordinal);
            // Discover all ReplayEvent subclasses
            Type replayBase = null;
            foreach (var asm in AppDomain.CurrentDomain.GetAssemblies())
            {
                try { replayBase = asm.GetTypes().FirstOrDefault(t => t.Name == "ReplayEvent" && t.IsClass); if (replayBase!=null) break; } catch { }
            }
            if (replayBase == null) return;
            var types = AppDomain.CurrentDomain.GetAssemblies()
                .SelectMany(a => { try { return a.GetTypes(); } catch { return Array.Empty<Type>(); } })
                .Where(t => !t.IsAbstract && replayBase.IsAssignableFrom(t)).ToList();
            foreach (var t in types)
            {
                string tn = t.Name;
                try
                {
                    var fields = t.GetFields(System.Reflection.BindingFlags.Public|System.Reflection.BindingFlags.Instance);
                    var simple = new List<(string,BinFieldKind)>();
                    bool unsupported = false;
                    foreach (var f in fields)
                    {
                        if (f.Name == "ticksSinceLoad" || f.Name == "randomS0Before" || f.Name == "type") continue;
                        if (!IsSimpleFieldType(f.FieldType)) { unsupported = true; break; }
                        simple.Add((f.Name, KindFor(f.FieldType)));
                    }
                    if (unsupported || simple.Count == 0)
                    {
                        // Provide RawJson fallback schema so event still goes through pure binary path.
                        _binSchemas[tn] = new BinSchema { fields = new[]{ ("__raw", BinFieldKind.RawJson) } };
                    }
                    else
                    {
                        simple.Sort((a,b)=> string.CompareOrdinal(a.Item1,b.Item1));
                        _binSchemas[tn] = new BinSchema{ fields = simple.ToArray() };
                    }
                }
                catch { }
            }
        }

        // --- Binary inner event helpers ---
        private static void WriteVarUInt32(MemoryStream ms, uint value)
        {
            Span<byte> tmp = stackalloc byte[5];
            int len = WriteVarUInt32(value, tmp);
            ms.Write(tmp.Slice(0, len));
        }

        private static void WriteVarInt32(MemoryStream ms, int value)
        {
            uint zigzag = (uint)((value << 1) ^ (value >> 31));
            WriteVarUInt32(ms, zigzag);
        }

        private static int ReadVarInt32(byte[] buffer, ref int idx)
        {
            uint result = 0; int shift = 0;
            while (true)
            {
                if (idx >= buffer.Length) throw new Exception("Truncated varint");
                byte b = buffer[idx++];
                result |= (uint)(b & 0x7F) << shift;
                if ((b & 0x80) == 0) break;
                shift += 7;
                if (shift > 35) throw new Exception("Varint too long");
            }
            int val = (int)(result >> 1) ^ -((int)result & 1);
            return val;
        }

        private static void WriteString(MemoryStream ms, string? s)
        {
            if (s == null) s = string.Empty;
            byte[] data = Encoding.UTF8.GetBytes(s);
            WriteVarUInt32(ms, (uint)data.Length);
            ms.Write(data, 0, data.Length);
        }

        private static void WriteFloat(MemoryStream ms, float v)
        {
            byte[] b = BitConverter.GetBytes(v);
            if (BitConverter.IsLittleEndian) Array.Reverse(b); // store big-endian for consistency
            ms.Write(b, 0, 4);
        }

    private static byte[] EncodeBinaryInner(string typeName, JObject obj)
        {
            using var ms = new MemoryStream();
            if (_binSchemas != null && _binSchemas.TryGetValue(typeName, out var schema))
            {
                foreach (var (name, kind) in schema.fields)
                {
                    switch (kind)
                    {
                        case BinFieldKind.String:
                            WriteString(ms, obj[name]?.ToString());
                            break;
                        case BinFieldKind.Int:
                            WriteVarInt32(ms, (int?)obj[name]?.ToObject<int>() ?? 0);
                            break;
                        case BinFieldKind.Float:
                            WriteFloat(ms, (float?)obj[name]?.ToObject<float>() ?? 0f);
                            break;
                        case BinFieldKind.Bool:
                            ms.WriteByte((byte)((bool?)obj[name]?.ToObject<bool>() == true ? 1 : 0));
                            break;
                        case BinFieldKind.Enum:
                            int enumVal = 0; if (obj[name] != null) int.TryParse(obj[name]!.ToString(), out enumVal);
                            WriteVarInt32(ms, enumVal);
                            break;
                        case BinFieldKind.Vector3:
                            {
                                // Expect object with x,y,z OR array [x,y,z]
                                float x=0,y=0,z=0;
                                var token = obj[name];
                                if (token is JArray va && va.Count >=3)
                                {
                                    x = va[0]!.ToObject<float>(); y = va[1]!.ToObject<float>(); z = va[2]!.ToObject<float>();
                                }
                                else if (token is JObject vo)
                                {
                                    x = vo["x"]?.ToObject<float>() ?? 0;
                                    y = vo["y"]?.ToObject<float>() ?? 0;
                                    z = vo["z"]?.ToObject<float>() ?? 0;
                                }
                                WriteFloat(ms, x); WriteFloat(ms, y); WriteFloat(ms, z);
                            }
                            break;
                        case BinFieldKind.Vector3IntList:
                            {
                                var arr = obj[name] as JArray; int count = arr?.Count ?? 0; WriteVarUInt32(ms, (uint)count);
                                if (arr != null)
                                {
                                    foreach (var el in arr)
                                    {
                                        int x=0,y=0,z=0;
                                        if (el is JArray ea && ea.Count>=3)
                                        { x=ea[0]!.ToObject<int>(); y=ea[1]!.ToObject<int>(); z=ea[2]!.ToObject<int>(); }
                                        else if (el is JObject eo)
                                        { x=eo["x"]?.ToObject<int>()??0; y=eo["y"]?.ToObject<int>()??0; z=eo["z"]?.ToObject<int>()??0; }
                                        WriteVarInt32(ms,x); WriteVarInt32(ms,y); WriteVarInt32(ms,z);
                                    }
                                }
                            }
                            break;
                        case BinFieldKind.GuidList:
                            {
                                var arr = obj[name] as JArray; int count = arr?.Count ?? 0; WriteVarUInt32(ms, (uint)count);
                                if (arr != null)
                                {
                                    foreach (var el in arr)
                                    {
                                        Guid g;
                                        if (el != null && Guid.TryParse(el.ToString(), out g))
                                        {
                                            var bytes = g.ToByteArray(); ms.Write(bytes,0,16);
                                        }
                                        else
                                        {
                                            Span<byte> zero = stackalloc byte[16]; ms.Write(zero);
                                        }
                                    }
                                }
                            }
                            break;
                        case BinFieldKind.Ray:
                            {
                                // Encode origin(x,y,z), direction(x,y,z)
                                float ox=0,oy=0,oz=0,dx=0,dy=0,dz=0;
                                var token = obj[name] as JObject;
                                if (token != null)
                                {
                                    var origin = token["origin"] as JObject;
                                    var direction = token["direction"] as JObject;
                                    if (origin!=null)
                                    { ox=origin["x"]?.ToObject<float>()??0; oy=origin["y"]?.ToObject<float>()??0; oz=origin["z"]?.ToObject<float>()??0; }
                                    if (direction!=null)
                                    { dx=direction["x"]?.ToObject<float>()??0; dy=direction["y"]?.ToObject<float>()??0; dz=direction["z"]?.ToObject<float>()??0; }
                                }
                                WriteFloat(ms,ox); WriteFloat(ms,oy); WriteFloat(ms,oz); WriteFloat(ms,dx); WriteFloat(ms,dy); WriteFloat(ms,dz);
                            }
                            break;
                        case BinFieldKind.Vector3Int:
                            {
                                // Accept either JArray [x,y,z] or JObject {x:..,y:..,z:..}
                                int x = 0, y = 0, z = 0;
                                var token = obj[name];
                                if (token is JArray arr)
                                {
                                    if (arr.Count > 0) x = arr[0]!.ToObject<int>();
                                    if (arr.Count > 1) y = arr[1]!.ToObject<int>();
                                    if (arr.Count > 2) z = arr[2]!.ToObject<int>();
                                }
                                else if (token is JObject vo)
                                {
                                    x = vo["x"]?.ToObject<int>() ?? 0;
                                    y = vo["y"]?.ToObject<int>() ?? 0;
                                    z = vo["z"]?.ToObject<int>() ?? 0;
                                }
                                WriteVarInt32(ms, x);
                                WriteVarInt32(ms, y);
                                WriteVarInt32(ms, z);
                            }
                            break;
                        case BinFieldKind.StringList:
                            {
                                var arr = obj[name] as JArray;
                                int count = arr?.Count ?? 0;
                                WriteVarUInt32(ms, (uint)count);
                                if (arr != null)
                                {
                                    foreach (var el in arr)
                                    {
                                        WriteString(ms, el?.ToString());
                                    }
                                }
                            }
                            break;
                        case BinFieldKind.RawJson:
                            {
                                // Serialize full object (minus meta) as compact JSON string
                                var clone = new JObject();
                                foreach (var prop in obj.Properties())
                                {
                                    if (prop.Name == TYPE_KEY || prop.Name == TICKS_KEY) continue;
                                    clone[prop.Name] = prop.Value;
                                }
                                string jsonInner = clone.ToString(Newtonsoft.Json.Formatting.None);
                                WriteString(ms, jsonInner);
                            }
                            break;
                    }
                }
            }
            // If no schema entry was found (unexpected) or schema wrote zero bytes, emit a 1-byte sentinel (0)
            // so that the receiver never sees an empty payload (which could hide missing schema bugs).
            if (ms.Length == 0)
            {
                ms.WriteByte(0);
            }
            return ms.ToArray();
        }

        private static JObject DecodeBinaryInner(string typeName, byte[] bytes)
        {
            var obj = new JObject();
            int idx = 0;
            if (_binSchemas != null && _binSchemas.TryGetValue(typeName, out var schema))
            {
                foreach (var (name, kind) in schema.fields)
                {
                    switch (kind)
                    {
                        case BinFieldKind.String:
                            obj[name] = ReadString(bytes, ref idx);
                            break;
                        case BinFieldKind.Int:
                            obj[name] = ReadVarInt32(bytes, ref idx);
                            break;
                        case BinFieldKind.Float:
                            obj[name] = ReadFloat(bytes, ref idx);
                            break;
                        case BinFieldKind.Bool:
                            obj[name] = (idx < bytes.Length && bytes[idx++] != 0);
                            break;
                        case BinFieldKind.Enum:
                            obj[name] = ReadVarInt32(bytes, ref idx);
                            break;
                        case BinFieldKind.Vector3:
                            {
                                float x = ReadFloat(bytes, ref idx);
                                float y = ReadFloat(bytes, ref idx);
                                float z = ReadFloat(bytes, ref idx);
                                obj[name] = new JObject { ["x"] = x, ["y"] = y, ["z"] = z };
                            }
                            break;
                        case BinFieldKind.Vector3IntList:
                            {
                                uint count = ReadVarUInt(bytes, ref idx);
                                var arr = new JArray();
                                for (int i=0;i<count;i++)
                                {
                                    int x = ReadVarInt32(bytes, ref idx);
                                    int y = ReadVarInt32(bytes, ref idx);
                                    int z = ReadVarInt32(bytes, ref idx);
                                    // Use object form so Json.NET can populate List<Vector3Int>
                                    arr.Add(new JObject { ["x"] = x, ["y"] = y, ["z"] = z });
                                }
                                obj[name] = arr;
                            }
                            break;
                        case BinFieldKind.GuidList:
                            {
                                uint count = ReadVarUInt(bytes, ref idx);
                                var arr = new JArray();
                                for (int i=0;i<count;i++)
                                {
                                    if (idx + 16 > bytes.Length) { idx = bytes.Length; arr.Add(Guid.Empty.ToString()); break; }
                                    byte[] gb = new byte[16]; Array.Copy(bytes, idx, gb, 0, 16); idx += 16; Guid g = new Guid(gb); arr.Add(g.ToString());
                                }
                                obj[name] = arr;
                            }
                            break;
                        case BinFieldKind.Ray:
                            {
                                float ox = ReadFloat(bytes, ref idx);
                                float oy = ReadFloat(bytes, ref idx);
                                float oz = ReadFloat(bytes, ref idx);
                                float dx = ReadFloat(bytes, ref idx);
                                float dy = ReadFloat(bytes, ref idx);
                                float dz = ReadFloat(bytes, ref idx);
                                obj[name] = new JObject
                                {
                                    ["origin"] = new JObject { ["x"] = ox, ["y"] = oy, ["z"] = oz },
                                    ["direction"] = new JObject { ["x"] = dx, ["y"] = dy, ["z"] = dz },
                                };
                            }
                            break;
                        case BinFieldKind.Vector3Int:
                            {
                                int x = ReadVarInt32(bytes, ref idx);
                                int y = ReadVarInt32(bytes, ref idx);
                                int z = ReadVarInt32(bytes, ref idx);
                                // Use JObject with x,y,z to match original JSON serialization of Vector3Int
                                obj[name] = new JObject { ["x"] = x, ["y"] = y, ["z"] = z };
                            }
                            break;
                        case BinFieldKind.StringList:
                            {
                                uint count = ReadVarUInt(bytes, ref idx);
                                var arr = new JArray();
                                for (int i = 0; i < count; i++) arr.Add(ReadString(bytes, ref idx));
                                obj[name] = arr;
                            }
                            break;
                        case BinFieldKind.RawJson:
                            {
                                string json = ReadString(bytes, ref idx) ?? string.Empty;
                                if (!string.IsNullOrEmpty(json))
                                {
                                    try
                                    {
                                        var inner = JObject.Parse(json);
                                        foreach (var p in inner.Properties()) obj[p.Name] = p.Value;
                                    }
                                    catch { }
                                }
                            }
                            break;
                    }
                }
            }
            return obj;
        }

        private static string ReadString(byte[] bytes, ref int idx)
        {
            uint len = ReadVarUInt(bytes, ref idx);
            int l = (int)len;
            if (idx + l > bytes.Length) l = Math.Max(0, bytes.Length - idx);
            string s = Encoding.UTF8.GetString(bytes, idx, l);
            idx += l;
            return s;
        }

        private static uint ReadVarUInt(byte[] buffer, ref int idx)
        {
            uint result = 0; int shift = 0;
            while (true)
            {
                if (idx >= buffer.Length) throw new Exception("Truncated varuint");
                byte b = buffer[idx++];
                result |= (uint)(b & 0x7F) << shift;
                if ((b & 0x80) == 0) break;
                shift += 7; if (shift > 35) throw new Exception("Varuint too long");
            }
            return result;
        }

        private static float ReadFloat(byte[] bytes, ref int idx)
        {
            if (idx + 4 > bytes.Length) { idx = bytes.Length; return 0f; }
            byte[] tmp = new byte[4]; Array.Copy(bytes, idx, tmp, 0, 4); if (BitConverter.IsLittleEndian) Array.Reverse(tmp); idx += 4; return BitConverter.ToSingle(tmp, 0);
        }
    }
}
