using System;
using StackExchange.Redis;

namespace StackRedis
{
    public interface IRedisConnection
    {
        bool CanWrite { get; }
        bool WaitingOnRecovery { get;}
        DateTime? NextRecoveryAttempt { get; }
        void RegisterRecoveryCallback(Action<StackRedis.IRedisConnection> callback);
        IRedisLite GetForMessaging();
    }
    public interface IRedisLite
    {
        void Subscribe(string channel, Action<RedisChannel, RedisValue> handler);
        void Unsubscribe(string channel, Action<RedisChannel, RedisValue> handler);
        void Publish(string channel, RedisValue message);
        
        string GetInfo(bool allowTalkToServer = false);
        void Ping();
    }
}
