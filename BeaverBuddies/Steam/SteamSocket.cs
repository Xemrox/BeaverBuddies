using Steamworks;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Timberborn.BuildingsUI;
using Timberborn.Workshops;
using TimberNet;
using UnityEngine.PlayerLoop;

namespace BeaverBuddies.Steam
{
    public class SteamSocket : ISocketStream, ISteamPacketReceiver
    {

        public bool Connected { get; private set; }

        public string Name { get; private set; }

        public readonly CSteamID friendID;
        //public readonly CSteamID lobbyID;

        private readonly ConcurrentQueueWithWait<byte[]> readBuffer = new ConcurrentQueueWithWait<byte[]>();
    // removed unused readOffset

        private SteamPacketListener packetListener;

        public SteamSocket(CSteamID friendID, bool autoconnect = false)
        {
            this.friendID = friendID;
            Name = SteamFriends.GetFriendPersonaName(friendID);
            Connected = autoconnect;
        }

        public void RegisterSteamPacketListener(SteamPacketListener listener)
        {
            packetListener = listener;
            listener.RegisterSocket(this);
        }

        public Task ConnectAsync()
        {
            // This is the client joining, and this only gets called when
            // we've already joined the lobby. It automatically closes
            // the prior client (I think).
            Connected = true;
            Plugin.Log("SteamSocket requested to connect!");
            return Task.CompletedTask;
        }

        public void Close()
        {
            Connected = false;
            packetListener?.UnregisterSocket(this);
        }

        private byte[] _currentRead = null;
        private int _currentReadOffset = 0;
        public int Read(byte[] buffer, int offset, int count)
        {
            // Ensure we have a current buffer with remaining bytes
            while (_currentRead == null || _currentReadOffset >= _currentRead.Length)
            {
                if (!readBuffer.WaitAndTryDequeue(out _currentRead)) continue; // block until data
                _currentReadOffset = 0;
            }
            int remaining = _currentRead.Length - _currentReadOffset;
            int bytesToCopy = Math.Min(count, remaining);
            Array.Copy(_currentRead, _currentReadOffset, buffer, offset, bytesToCopy);
            _currentReadOffset += bytesToCopy;
            if (_currentReadOffset >= _currentRead.Length)
            {
                // Finished this packet
                _currentRead = null;
                _currentReadOffset = 0;
            }
            return bytesToCopy;
        }

        public void Write(byte[] buffer, int offset, int count)
        {
            if (offset > 0)
            {
                byte[] newBuffer = new byte[count];
                Array.Copy(buffer, offset, newBuffer, 0, count);
                buffer = newBuffer;
            }
            Plugin.Log($"SteamSocket sending {count} bytes");
            SteamNetworking.SendP2PPacket(friendID, buffer, (uint)count, EP2PSend.k_EP2PSendReliable);
        }

        public void ReceiveData(byte[] data)
        {
            readBuffer.Enqueue(data);
        }
    }
}
