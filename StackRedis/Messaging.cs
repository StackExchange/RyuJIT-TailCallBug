using System;
using System.Collections.Generic;
using StackExchange.Redis;

namespace StackRedis
{
    public class MessageTupleEqualityComparer : IEqualityComparer<Tuple<string, byte[]>>
    {
        public static MessageTupleEqualityComparer Singleton = new MessageTupleEqualityComparer();

        public bool Equals(Tuple<string, byte[]> x, Tuple<string, byte[]> y)
        {
            // Quick (and cheap!) reference checks
            if (object.ReferenceEquals(x, y)) return true;
            if (object.ReferenceEquals(x, null)) return false;
            if (object.ReferenceEquals(y, null)) return false;

            // If one byte array is null and the other's not, obviously not equal (cheap!)
            if ((x.Item2 == null && y.Item2 != null) || (y.Item2 == null && x.Item2 != null)) return false;
            
            // If both byte arrays are null, only the string comparison matters
            if (x.Item2 == null && y.Item2 == null) return x.Item1 == y.Item1;

            // If both byte arrays are non-null, they have to be the same length
            if (x.Item2.Length != y.Item1.Length) return false;

            // string check is potentially cheaper than a full byte array comparison
            if (x.Item1 != y.Item1) return false;

            // Slow byte array comparison
            for (int i = 0; i < x.Item2.Length; i++)
                if (x.Item2[i] != y.Item2[i]) return false;

            return true;
        }

        public int GetHashCode(Tuple<string, byte[]> obj)
        {
            int raw = 0;
            for (int i = 0; i < obj.Item2.Length; i++)
                raw ^= (obj.Item2[i] + i);

            return raw ^ obj.Item1.GetHashCode();
        }
    }

    public interface IMessaging
    {
        bool Send(string channel, RedisValue message); 
        void SubscribeForAsync(string channel, Action<RedisChannel, RedisValue> callback);
        void UnsubscribeForAsync(string channel, Action<RedisChannel, RedisValue> callback);
    }

    /// <summary>
    /// Dummy standin for when we need a messaging interface, but don't
    /// want it to actually do anything
    /// </summary>
    public class NullMessaging : IMessaging
    {
        public bool Send(string channel, RedisValue message) { return true; }

        public void SubscribeForAsync(string channel, Action<RedisChannel, RedisValue> callback) { }
        
        public void UnsubscribeForAsync(string channel, Action<RedisChannel, RedisValue> callback) { }
    }

    /// <summary>
    /// Interface for Redis messaging
    /// </summary>
    public class Messaging : IMessaging
    {
        // Messages aren't getting where they're supposed to, logging all messages and dispatches
        private List<string> _log = new List<string>(1000);
        internal List<string> DebuggingLog { get { lock (_log) { return new List<string>(_log); } } }

        private readonly IRedisLite _gateway;
        /// <summary>
        /// Initialize with a common sync object (for access to the redis connection) and the redis connection
        /// </summary>
        public Messaging(IRedisConnection connection)
        {
            _gateway = connection.GetForMessaging();
        }

        bool IMessaging.Send(string channel, RedisValue message)
        {
            return SendImpl(channel, message);
        }
        private bool SendImpl(string channel, RedisValue message)
        {
            try
            {
                _gateway.Publish(channel, message);
                return true;
            }
            catch (Exception e)
            {
                StackRedisError.FireExceptionOccurredImpl(e);
            }
            return false;
        }

        /// <summary>
        /// Send a message on the given channel
        /// </summary>
        public bool Send(string channel, string message)
        {
            return SendImpl(channel, message);
        }
        /// <summary>
        /// Send a message on the given channel
        /// </summary>
        public bool Send(string channel, byte[] message)
        {
            return SendImpl(channel, message);
        }

        /// <summary>
        /// Subscribes to a pub/sub channel, but on the proviso that messages will come back **without a site context**.
        /// If you care about the specific site, *capture it*. This allows you to obtain messages more promptly and
        /// with much less overhead on the overall system.
        /// </summary>
        /// <param name="channel">The name of the **tier specific** (not site specific) channel to listen to.</param>
        /// <param name="handler">The operaion to perform when a message is received.</param>
        public void SubscribeForAsync(string channel, Action<RedisChannel, RedisValue> handler)
        {
            if(handler == null) return; // nothing to do
            
            _gateway.Subscribe(channel, handler);
        }

        //public void UnsubscribeLastHandler(string channel)
        //{
        //    lock (asyncCallbacks)
        //    {
        //        Action<string, byte[]> existing;
        //        if (asyncCallbacks.TryGetValue(channel, out existing))
        //        {
        //            var lastHandler = (from h in existing.GetInvocationList()
        //                   select (Action<string, byte[]>) h).Last();

        //            UnsubscribeForAsync(channel, lastHandler);
        //        }
        //    }            
        //}

        public void UnsubscribeForAsync(string channel, Action<RedisChannel, RedisValue> handler)
        {
            if (handler == null) return;
            
            _gateway.Unsubscribe(channel, handler);
        }

        public void Ping()
        {
            _gateway.Ping();
        }
    }
}