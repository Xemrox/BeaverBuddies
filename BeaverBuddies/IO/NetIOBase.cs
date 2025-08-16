using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using BeaverBuddies.Events;
using TimberNet;
using BeaverBuddies.Steam;

namespace BeaverBuddies.IO
{
    public abstract class NetIOBase<T> : EventIO where T : TimberNetBase
    {

        public T NetBase { get; protected set; }
        public abstract bool RecordReplayedEvents { get; }
        public abstract bool ShouldSendHeartbeat { get; }
        public abstract UserEventBehavior UserEventBehavior { get; }
        public bool IsOutOfEvents => NetBase == null ? true : !NetBase.ShouldTick;
        public int TicksBehind => NetBase == null ? 0 : NetBase.TicksBehind;

        private SteamPacketListener steamPacketListener = null;

        public void Close()
        {
            if (NetBase == null) return;
            NetBase.Close();
        }

        public void Update()
        {
            if (NetBase == null) return;
            NetBase.Update();
            steamPacketListener?.Update();
        }

        private static ReplayEvent ToEvent(JObject obj)
        {
            // Fast path for minimal heartbeat objects (custom short form {"type":"H","ticksSinceLoad":N})
            var typeToken = obj["type"];
            if (typeToken != null && typeToken.Type == JTokenType.String && typeToken.Value<string>() == "H")
            {
                // Minimal construction without JSON polymorphism
                return new HeartbeatEvent() { ticksSinceLoad = obj[TimberNetBase.TICKS_KEY]?.Value<int>() ?? 0 };
            }
            try
            {
                return NetworkEventSerializer.Deserialize(obj);
            }
            catch (Exception ex)
            {
                Plugin.Log(ex.ToString());
                return null;
            }
        }

        public List<ReplayEvent> ReadEvents(int ticksSinceLoad)
        {
            if (NetBase == null) return new List<ReplayEvent>();
            return NetBase.ReadEvents(ticksSinceLoad)
                .Select(ToEvent)
                .Where(e => e != null)
                .ToList();
        }

        public virtual void WriteEvents(params ReplayEvent[] events)
        {
            if (NetBase == null) return;
            // If only a single heartbeat, keep existing fast path.
            if (events.Length == 1 && events[0] is HeartbeatEvent hbOnly)
            {
                var hb = new JObject
                {
                    [TimberNetBase.TICKS_KEY] = hbOnly.ticksSinceLoad,
                    [TimberNetBase.TYPE_KEY] = "H"
                };
                NetBase.DoUserInitiatedEvent(hb);
                return;
            }
            // If more than one event (or a single non-heartbeat), build a binary container.
            List<JObject> serialized = new List<JObject>();
            foreach (var e in events)
            {
                if (e is HeartbeatEvent hb)
                {
                    var obj = new JObject
                    {
                        [TimberNetBase.TICKS_KEY] = hb.ticksSinceLoad,
                        [TimberNetBase.TYPE_KEY] = "H"
                    };
                    serialized.Add(obj);
                }
                else
                {
                    serialized.Add(NetworkEventSerializer.Serialize(e));
                }
            }
            // Use underlying server/client socket(s). For client, NetBase is TimberClient (single socket).
            // We call SendEventsContainerForTick once per target; for server we still rely on DoUserInitiatedEvent path to broadcast.
            // Simpler: if server, fall back to existing per-event path (will broadcast), else container.
            if (NetBase is TimberNet.TimberServer)
            {
                // Fallback: send individually (server broadcast logic lives there). Future: implement server-side container broadcast.
                foreach (var obj in serialized)
                {
                    NetBase.DoUserInitiatedEvent(obj);
                }
            }
            else
            {
                // Client: send as single binary container for this tick.
                int tick = events.Last().ticksSinceLoad; // all set above
                var client = NetBase as TimberNet.TimberClient;
                if (client != null)
                {
                    client.SendEventsContainer(tick, serialized);
                }
                else
                {
                    // Fallback (should not happen): individual
                    foreach (var obj in serialized)
                        NetBase.DoUserInitiatedEvent(obj);
                }
            }
        }

        public bool HasEventsForTick(int tick)
        {
            if (NetBase == null) return false;
            return NetBase.HasEventsForTick(tick);
        }

        protected void TryRegisterSteamPacketReceiver(object receiver)
        {
            if (!(receiver is ISteamPacketReceiver)) return;
            if (steamPacketListener == null)
            {
                Plugin.Log("Creating SteamPacketListener");
                steamPacketListener = new SteamPacketListener();
            }
            ((ISteamPacketReceiver)receiver).RegisterSteamPacketListener(steamPacketListener);
        }
    }
}
