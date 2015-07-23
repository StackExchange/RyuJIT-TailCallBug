using AsyncRedis;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using StackExchange.Redis.KeyspaceIsolation;
using System.Collections;
using System.Runtime.CompilerServices;

namespace StackRedis
{
    /// <summary>
    /// Common base class for all redis caches.
    /// </summary>
    public sealed class AsyncDelayedRedisCache : IRestorableCache
    {
        private readonly IDatabase _cache;
        private readonly ICache _l1;
        private readonly IFallbackCache _fallback;
        private readonly IMessaging _messaging;

        private readonly int db = -1;
        /// <summary>
        /// Mung a key into a form we can shove into redis
        /// </summary>
        private RedisKey CleanKey(string key)
        {
            return MakeRedisSafe(key);
        }

        bool ICache.CanWrite
        {
            get { return _cache.IsConnected(default(RedisKey), CommandFlags.DemandMaster); }
        }

        /// <summary>
        /// Makes any key passed in "safe" for direct redis use.
        /// 
        /// Discards some uniqueness potentially, be careful.
        /// </summary>
        public static string MakeRedisSafe(string key)
        {
            return _cacheKeyFilter.Replace(key.Trim(), "-").Replace("\r", "").Replace("\n", "");
        }
        private static Regex _cacheKeyFilter = new Regex(@"[\s*?{}]", RegexOptions.IgnoreCase | RegexOptions.Compiled);

        private void Subscribe()
        {
            _messaging.SubscribeForAsync("db_" + Db.ToString(),
                (channel, message) => {
                    string key = message;
                    _l1.Remove(key);
                    _fallback.Remove(key);
                });
        }
        private readonly IProfilerProvider profilerProvider;
        public int Db { get { return db; } }
        private readonly string cacheName;

        [Obsolete("Please supply a meaningful name for this instance")]
        public AsyncDelayedRedisCache(IMessaging messaging, AsyncRedisConnection connection, ICache local, IFallbackCache fallback, int db, string tier, IProfilerProvider profilerProvider = null)
            : this((string)null, messaging, connection,local, fallback, db, tier, profilerProvider)
        {}
        public AsyncDelayedRedisCache(string name, IMessaging messaging, AsyncRedisConnection connection, ICache local, IFallbackCache fallback, int db, string tier, IProfilerProvider profilerProvider = null)
        {
            this.cacheName = name;
            this.db = db;
            this.profilerProvider = profilerProvider;
            if (local.Db != db) throw new InvalidOperationException(string.Format("L1 db:{0} does not match async-redis db:{1}", local.Db, db));
            if (fallback.Db != db) throw new InvalidOperationException(string.Format("Fallback db:{0} does not match async-redis db:{1}", fallback.Db, db));
            _l1 = local;
            _fallback = fallback;
            _messaging = messaging;

            _cache = connection.GetDatabase(db);
            if (tier.HasValue() && db == 0) // global
            {
                keyPrefix = tier + "-";
                _cache = _cache.WithKeyPrefix(keyPrefix);
            }
            _fallback.SetPushTarget(this);
            Subscribe();
        }
        private readonly string keyPrefix;

        /// <summary>
        /// Broadcast a message instructing all nodes to flush the given value from cache
        /// </summary>
        public void BroadcastRemoveFromCache(string key)
        {
            _messaging.Send("db_" + db.ToString(), key);
        }

        /// <summary>
        /// Get info on the common cache
        /// </summary>
        public Dictionary<string, string> Info()
        {
            var result = new Dictionary<string, string>();
            try
            {
                var server = AsyncRedisConnection.TryGetServer(_cache);
                if (server != null)
                {
                    IGrouping<string, KeyValuePair<string, string>>[] info;
                    using (Profile("Info", ""))
                    {
                        info = server.Info();
                    }

                    foreach (var section in info)
                    {
                        foreach (var pair in section)
                        {
                            result[pair.Key] = pair.Value;
                        }
                    }
                }
            }
            catch (Exception e)
            {
                FireExceptionOccurred(new CacheException("Info", e));
            }
            return result;
        }


        [Obsolete("Check you didn't mean to use a RedisKey")]
        private bool CanRead(string key)
        {
            return _cache.IsConnected(key, CommandFlags.PreferMaster);
        }
        public bool CanRead(RedisKey key)
        {
            return _cache.IsConnected(key, CommandFlags.PreferMaster);
        }
        public bool CanRead(RedisKey[] keys)
        {
            RedisKey key = (keys == null || keys.Length == 0) ? default(RedisKey) : keys[0];
            return _cache.IsConnected(key, CommandFlags.PreferMaster);
        }
        [Obsolete("Check you didn't mean to use a RedisKey")]
        private bool CanWrite(string key)
        {
            return _cache.IsConnected(key, CommandFlags.DemandMaster);
        }
        public bool CanWrite(RedisKey key)
        {
            return _cache.IsConnected(key, CommandFlags.DemandMaster);
        }
        public IDisposable Profile(string caller, string key)
        {
            var profiler = profilerProvider == null ? null : profilerProvider.GetProfiler();
            return profiler == null ? null : profiler.Profile(cacheName, caller, key);
        }
        public IDisposable Profile(string caller, string[] keys)
        {
            var profiler = profilerProvider == null ? null : profilerProvider.GetProfiler();
            return profiler == null ? null : profiler.Profile(cacheName, caller, ((keys != null && keys.Length == 1) ? keys[0] : null));
        }
        public int GetMaxLatency()
        {
            //try
            //{
            //    return _connection.GetMaxLatency();
            //}catch
            //{ // gulp
            //}
            return -1;
        }

        void ICache.DetectError()
        {
            //try
            //{
            //    _connection.DetectError();
            //}
            //catch { }
        }

        public bool LockSync(string key, string value, int expirySeconds)
        {
            var cacheKey = CleanKey(key);

            try
            {
                if (!CanWrite(cacheKey))
                {
                    return _fallback.LockSync(key, value, expirySeconds);
                }
                
                using (Profile("LockSync", key))
                {
                    return _cache.LockTake(cacheKey, value, TimeSpan.FromSeconds(expirySeconds));
                }
            }
            catch (Exception e)
            {
                FireExceptionOccurred(new CacheException("LockSync", e, key));

            }

            return false;
        }

        public string[] GetLockOwners(string[] keys)
        {

            try
            {
                if (!CanWrite(default(RedisKey)))
                {
                    return _fallback.GetLockOwners(keys);
                }
                var cacheKeys = CleanKeys(keys);
                using (Profile("ExtendLockSync", keys))
                {
                    return Array.ConvertAll(_cache.StringGet(cacheKeys), x => (string)x);
                }
            }
            catch (Exception e)
            {
                FireExceptionOccurred(new CacheException("ExtendLockSync", e, keys));

            }

            return new string[keys.Length];
        }
        public bool ExtendLockSync(string key, string value, int expirySeconds)
        {
            var cacheKey = CleanKey(key);

            try
            {
                if(!CanWrite(cacheKey))
                {
                    return _fallback.ExtendLockSync(key, value, expirySeconds);
                }
                using (Profile("ExtendLockSync", key))
                {
                    return _cache.LockExtend(cacheKey, value, TimeSpan.FromSeconds(expirySeconds));
                }
            }
            catch (Exception e)
            {
                FireExceptionOccurred(new CacheException("ExtendLockSync", e, key));

            }

            return false;
        }
        public bool ReleaseLockSync(string key, string value)
        {
            var cacheKey = CleanKey(key);

            try
            {
                if (!CanWrite(cacheKey))
                {
                    return _fallback.ReleaseLockSync(key, value);
                }
                using (Profile("ExtendLockSync", key))
                {
                    return _cache.LockRelease(cacheKey, value);
                }
            }
            catch (Exception e)
            {
                FireExceptionOccurred(new CacheException("ReleaseLockSync", e, key));
            }

            return false;
        }

        public bool Exists(string key, bool queueJump, Server server)
        {
            var cacheKey = CleanKey(key);

            try
            {
                
                if (!CanRead(cacheKey))
                {
                    return _fallback.Exists(key, queueJump, server);
                }
                using (Profile("Exists", key))
                {
                    return _cache.KeyExists(cacheKey, FlagsFor(server, queueJump));
                }
            }
            catch (Exception e)
            {
                FireExceptionOccurred(new CacheException("Exists", e, key));

            }

            return false;
        }

        public TimeSpan? ExpiresIn(string key, Server server)
        {
            var cacheKey = CleanKey(key);

            try
            {
                
                if (!CanRead(cacheKey))
                {
                    // There is no fallback for this method
                    return null;
                }

                using (Profile("ExpiresIn", key))
                {
                    return _cache.KeyTimeToLive(cacheKey, FlagsFor(server));
                }
            }
            catch (Exception e)
            {
                FireExceptionOccurred(new CacheException("ExpiresIn", e, key));

            }

            return null;
        }

        public void Expire(string key, int ttlSeconds, bool queueJump = false)
        {
            var cacheKey = CleanKey(key);

            try
            {

                if (!CanRead(cacheKey))
                {
                    // There is no fallback for this method
                    return;
                }
                using (Profile("Expire", key))
                {
                    _cache.KeyExpire(cacheKey, TimeSpan.FromSeconds(ttlSeconds), QueueJump(queueJump) | CommandFlags.FireAndForget);
                }
            }
            catch (Exception e)
            {
                FireExceptionOccurred(new CacheException("Expire", e, key, ttlSeconds.ToString(), queueJump.ToString()));

            }

        }

        public long GetInt(string key, bool queueJump, Server server)
        {
            var cacheKey = CleanKey(key);

            try
            {
                
                if (!CanRead(cacheKey))
                {
                    return _fallback.GetInt(key, queueJump, server);
                }

                using (Profile("GetInt", key))
                {
                    return (long)_cache.StringGet(cacheKey, FlagsFor(server, true));
                }
            }
            catch (Exception e)
            {
                FireExceptionOccurred(new CacheException("GetInt", e, key));

            }

            return default(long);
        }
        public void Read(bool reset, out long hitl1, out long hitl2, out long miss, out long error)
        {
            if (reset)
            {
                hitl1 = Interlocked.Exchange(ref this.hitl1, 0);
                hitl2 = Interlocked.Exchange(ref this.hitl2, 0);
                miss = Interlocked.Exchange(ref this.miss, 0);
                error = Interlocked.Exchange(ref this.error, 0);
            }
            else
            {
                hitl1 = Interlocked.Read(ref this.hitl1);
                hitl2 = Interlocked.Read(ref this.hitl2);
                miss = Interlocked.Read(ref this.miss);
                error = Interlocked.Read(ref this.error);
            }
        }
        private void HitL1() { Interlocked.Increment(ref this.hitl1); }
        private void HitL2() { Interlocked.Increment(ref this.hitl2); }
        private void Miss() { Interlocked.Increment(ref this.miss); }
        private long hitl1, hitl2, miss, error;

        private void FireExceptionOccurred(CacheException ex)
        {
            if (ex != null)
            {
                Interlocked.Increment(ref error);
                try
                {
                    var tmp = ex.InnerException;
                    var data = tmp == null ? null : tmp.Data;
                    if (data != null && data.Contains("Redis-Busy-Workers"))
                    {
                        int count = Convert.ToInt32(data["Redis-Busy-Workers"]);
                        if(count > 10) TriggerDump(count);
                    }
                }
                catch { }
                StackRedisError.FireExceptionOccurredImpl(ex);
            }
        }
        void TriggerDump(int count)
        {
            try
            {
                var instanceName = this.cacheName;
                if (string.IsNullOrWhiteSpace(instanceName)) instanceName = "anon";
                using (var counter = new PerformanceCounter("Stack Exchange", "ItsNotYouItsMe", instanceName, false))
                {
                    counter.Increment();
                }
            }
            catch { }
        }

        public void SetInt(string key, long val)
        {            
            var cacheKey = CleanKey(key);

            try
            {
                
                if (!CanWrite(cacheKey))
                {
                    _fallback.SetInt(key, val);
                    return;
                }
                using (Profile("SetInt", key))
                {
                    _cache.StringSet(cacheKey, val, flags: CommandFlags.FireAndForget);
                }
            }
            catch (Exception e)
            {
                FireExceptionOccurred(new CacheException("SetInt", e, key));

            }
        }

        static CommandFlags QueueJump(bool queueJump)
        {
            return queueJump ? CommandFlags.HighPriority : CommandFlags.None;
        }
        public long IncrementIntSync(string key, int val, bool queueJump = false)
        {
            var cacheKey = CleanKey(key);

            try
            {
                
                if (!CanWrite(cacheKey))
                {
                    return _fallback.IncrementIntSync(key, val, queueJump);
                }
                using (Profile("IncrementInt", key))
                {
                    return _cache.StringIncrement(cacheKey, val, QueueJump(queueJump));
                }
            }
            catch (Exception e)
            {
                FireExceptionOccurred(new CacheException("IncrementInt", e, key));

            }

            return default(long);
        }

        public void IncrementInt(string key, int val, bool queueJump = false)
        {
            IncrementInt(key, val, queueJump, null);
        }
        public void IncrementInt(string key, int val, Action<long> callback)
        {
            IncrementInt(key, val, false, callback);
        }
        private void IncrementInt(string key, int val, bool queueJump, Action<long> callback)
        {
            var cacheKey = CleanKey(key);

            try
            {
                
                if (!CanWrite(cacheKey))
                {
                    _fallback.IncrementInt(key, val, queueJump);
                }
                else
                {
                    using (Profile("IncrementInt", key))
                    {
                        if (callback == null)
                        {
                            _cache.StringIncrement(cacheKey, val, QueueJump(queueJump) | CommandFlags.FireAndForget);
                        }
                        else
                        {
                            var task = _cache.StringIncrementAsync(cacheKey, val, QueueJump(queueJump));
                            task.ContinueWith(t =>
                            {
                                try
                                {
                                    callback(t.Result);
                                }
                                catch
                                {/* gulp!! */}
                            }, TaskContinuationOptions.OnlyOnRanToCompletion);
                        }
                    }
                }
            }
            catch (Exception e)
            {
                FireExceptionOccurred(new CacheException("IncrementInt", e, key));

            }
        }

        public bool AddToSetSync(string key, byte[] val)
        {
            
            var cacheKey = CleanKey(key);

            try
            {
                
                if (!CanWrite(cacheKey))
                {
                    return _fallback.AddToSetSync(key, val);
                }
                using (Profile("AddToSet", key))
                {
                    return _cache.SetAdd(cacheKey, val);
                }
            }
            catch (Exception e)
            {
                FireExceptionOccurred(new CacheException("AddToSet", e, key));

            }

            return false;
        }
        public Task<bool> AddToSet(string key, byte[] val)
        {
            var cacheKey = CleanKey(key);

            try
            {
                
                if (!CanWrite(cacheKey))
                {
                    return _fallback.AddToSet(key, val);
                }
                else
                {
                    using (Profile("AddToSet", key))
                    {
                        return _cache.SetAddAsync(cacheKey, val);
                    }
                }
            }
            catch (Exception e)
            {
                FireExceptionOccurred(new CacheException("AddToSet", e, key));

            }

            return Task.FromResult(false);
        }

        public void AddMultipleToSet(string key, IEnumerable<byte[]> values)
        {
            var cacheKey = CleanKey(key);

            try
            {

                if (!CanWrite(cacheKey))
                {
                    _fallback.AddMultipleToSet(key, values);
                }
                else
                {
                    // note: there is a version of Sets.Add that takes byte[][], but: that would
                    // be one single op; we want to send multiple ops and let other threads have
                    // access to the connection, hence the keepTogether: false
                    foreach (var value in values)
                    {
                        using (Profile("AddToSet", key))
                        {
                            _cache.SetAdd(cacheKey, value, CommandFlags.FireAndForget);
                        }
                    }
                }
            }
            catch (Exception e)
            {
                FireExceptionOccurred(new CacheException("AddToSet", e, key));

            }
        }

        public int GetSetSize(string key, Server server)
        {           
            var cacheKey = CleanKey(key);

            try
            {
                
                if (!CanRead(cacheKey))
                {
                    return _fallback.GetSetSize(key, server);
                }

                using (Profile("GetSetSize", key))
                {
                    return (int)_cache.SetLength(cacheKey, FlagsFor(server));
                }
            }
            catch (Exception e)
            {
                FireExceptionOccurred(new CacheException("GetSetSize", e, key));

            }

            return 0;
        }

        public IEnumerable<byte[]> EnumerateSet(string key, Server server)
        {
            
            var cacheKey = CleanKey(key);

            try
            {
                
                if (!CanRead(cacheKey))
                {
                    return _fallback.EnumerateSet(key, server);
                }
                using (Profile("EnumerateSet", key))
                {
                    return _cache.SetScan(cacheKey, flags: FlagsFor(server), pageSize: 200).Select(x => (byte[])x);
                }
            }
            catch (Exception e)
            {
                FireExceptionOccurred(new CacheException("EnumerateSet", e, key));
            }

            return Enumerable.Empty<byte[]>();
        }

        public IEnumerable<byte[]> GetRangeInList(string key, int start, int stop, Server server)
        {
            var cacheKey = CleanKey(key);

            try
            {
                if (!CanRead(cacheKey))
                {
                    return _fallback.GetRangeInList(key, start, stop, server);
                }
                using (Profile("GetRangeInList", key))
                {
                    return _cache.ListRange(cacheKey, start, stop, FlagsFor(server)).Select(x => (byte[])x);
                }
            }
            catch (Exception e)
            {
                FireExceptionOccurred(new CacheException("GetRangeInList", e, key));

            }

            return Enumerable.Empty<byte[]>();
        }

        public bool RemoveFromSetSync(string key, byte[] val)
        {          
            var cacheKey = CleanKey(key);

            try
            {
                
                if (!CanWrite(cacheKey))
                {
                    return _fallback.RemoveFromSetSync(key, val);
                }

                using (Profile("RemoveFromSet", key))
                {
                    return _cache.SetRemove(cacheKey, val);
                }
            }
            catch (Exception e)
            {
                FireExceptionOccurred(new CacheException("RemoveFromSet", e, key));

            }

            return false;
        }
        public void RemoveFromSet(string key, byte[] val)
        {
         
            var cacheKey = CleanKey(key);

            try
            {
                
                if (!CanWrite(cacheKey))
                {
                    _fallback.RemoveFromSet(key, val);
                }
                else
                {
                    using (Profile("RemoveFromSet", key))
                    {
                        _cache.SetRemove(cacheKey, val, CommandFlags.FireAndForget);
                    }
                }
            }
            catch (Exception e)
            {
                FireExceptionOccurred(new CacheException("RemoveFromSet", e, key));

            }
        }
        static readonly RedisKey[] EmptyKeys = new RedisKey[0];
        private RedisKey[] CleanKeys(IList<string> keys)
        {
            if (keys == null) return null;
            if (keys.Count == 0) return EmptyKeys;
            var cpy = new RedisKey[keys.Count];
            int index = 0;
            foreach(var key in keys)
            {
                cpy[index++] = CleanKey(key);
            }
            return cpy;
        }
        public int UnionAndStore(string toKey, string[] fromKeys)
        {
          
            var toCacheKey = CleanKey(toKey);
            var fromCacheKeys = CleanKeys(fromKeys);

            try
            {
                if (!CanWrite(toCacheKey))
                {
                    _fallback.UnionAndStore(toKey, fromKeys);
                    return _fallback.GetSetSize(toKey, Server.DemandMaster);
                }

                using (Profile("UnionAndStore", fromKeys))
                {
                    return (int)_cache.SetCombineAndStore(SetOperation.Union, toCacheKey, fromCacheKeys);
                }

            }
            catch (Exception e)
            {
                FireExceptionOccurred(new CacheException("UnionAndStore", e, fromKeys.ToList().Union(new string[] { toKey }).ToArray()));

            }

            return -1;
        }

        public IEnumerable<byte[]> Union(string[] fromKeys, Server server)
        {

            var fromCacheKeys = CleanKeys(fromKeys);

            try
            {
                if (!CanRead(fromCacheKeys))
                {
                    return _fallback.Union(fromKeys, server);
                }

                using (Profile("Union", fromKeys))
                {
                    return Array.ConvertAll(_cache.SetCombine(SetOperation.Union, fromCacheKeys, FlagsFor(server)), x => (byte[])x);
                }
            }
            catch (Exception e)
            {
                FireExceptionOccurred(new CacheException("Union", e, fromKeys));

            }

            return Enumerable.Empty<byte[]>();
        }

        public int IntersectAndStore(string toKey, string[] fromKeys)
        {

            var toCacheKey = CleanKey(toKey);
            var fromCacheKeys = CleanKeys(fromKeys);

            try
            {
                
                if (!CanWrite(toCacheKey))
                {
                    _fallback.IntersectAndStore(toKey, fromKeys);
                    return _fallback.GetSetSize(toKey, Server.DemandMaster);
                }
                using (Profile("IntersectAndStore", fromKeys))
                {
                    return (int)_cache.SetCombineAndStore(SetOperation.Intersect, toCacheKey, fromCacheKeys);
                }
            }
            catch (Exception e)
            {
                FireExceptionOccurred(new CacheException("IntersectAndStore", e, fromKeys.ToList().Union(new string[] { toKey }).ToArray()));

            }

            return -1;
        }

        public IEnumerable<byte[]> Intersect(string[] fromKeys, Server server)
        {

            var fromCacheKeys = CleanKeys(fromKeys);

            try
            {
                if (!CanRead(fromCacheKeys))
                {
                    return _fallback.Intersect(fromKeys, server);
                }
                using (Profile("Intersect", fromKeys))
                {
                    return Array.ConvertAll(_cache.SetCombine(SetOperation.Intersect, fromCacheKeys, FlagsFor(server)), x => (byte[])x);
                }
            }
            catch (Exception e)
            {
                FireExceptionOccurred(new CacheException("Intersect", e, fromKeys));

            }

            return Enumerable.Empty<byte[]>();
        }

        public bool SetContains(string key, byte[] value, Server server)
        {
           
            var cacheKey = CleanKey(key);

            try
            {
                
                if (!CanRead(cacheKey))
                {
                    return _fallback.SetContains(key, value, server);
                }
                using (Profile("SetContains", key))
                {
                    return _cache.SetContains(cacheKey, value, FlagsFor(server));
                }
            }
            catch (Exception e)
            {
                FireExceptionOccurred(new CacheException("SetContains", e, key));

            }

            return false;
        }

        public long AppendSync(string key, string toAppend)
        {
            var cacheKey = CleanKey(key);

            try
            {
                
                if (!CanWrite(cacheKey))
                {
                    return _fallback.AppendSync(key, toAppend);
                }
                using (Profile("Append", key))
                {
                    return _cache.StringAppend(cacheKey, toAppend);
                }
            }
            catch (Exception e)
            {
                FireExceptionOccurred(new CacheException("Append", e, key));

            }

            return 0;
        }
        public void Append(string key, string toAppend)
        {
            var cacheKey = CleanKey(key);

            try
            {
                
                if (!CanWrite(cacheKey))
                {
                    _fallback.Append(key, toAppend);
                }
                else
                {
                    using (Profile("Append", key))
                    {
                        _cache.StringAppend(cacheKey, toAppend, CommandFlags.FireAndForget);
                    }
                }
            }
            catch (Exception e)
            {
                FireExceptionOccurred(new CacheException("Append", e, key));

            }
        }

        /// <summary>
        /// Special Redis only Get() that always goes to the network,
        /// completely bypasses local cache (including priming it for subsequent requests).
        /// </summary>
        public T DirectGet<T>(string key, Server server)
        {
           
            var cacheKey = CleanKey(key);

            try
            {
                if (!CanRead(cacheKey))
                {
                    return default(T);
                }

                byte[] bytes;
                using (Profile("DirectGet", key))
                {
                    bytes = _cache.StringGet(cacheKey, FlagsFor(server));
                }
                if (bytes == null) return default(T);

                LogSize(server, key, bytes);
                var wrapper = RedisWrapper.FromCacheArray(bytes);
                if (wrapper == null) return default(T);


                return wrapper.GetValue<T>();
            }
            catch (Exception e)
            {
                FireExceptionOccurred(new CacheException("DirectGet", e, key));

            }

            return default(T);
        }
        
        public void DirectSet<T>(string key, T val)
        {
            var cacheKey = CleanKey(key);

            try
            {
                Exception ex;
                var wrapped = RedisWrapper.For(val, null, RedisWrapper.Format.Automatic, out ex);

                // Can't put this in redis
                if (wrapped == null)
                {
                    throw CreateNonSerializable<T>(ex);
                }

                var bytes = wrapped.ToCacheArray();
                LogSize(Server.DemandMaster, key, bytes);
                if (!CanWrite(cacheKey))
                {
                    return;
                }

                using (Profile("DirectSet", key))
                {
                    _cache.StringSet(cacheKey, bytes, flags: CommandFlags.FireAndForget);
                }
            }
            catch (Exception e)
            {
                FireExceptionOccurred(new CacheException("DirectSet", e, key));

            }
        }

        public void DirectSetSync<T>(string key, T val, int? durationSecs = null)
        {
            var cacheKey = CleanKey(key);
            try
            {
                Exception ex;
                var wrapped = RedisWrapper.For(val, null, RedisWrapper.Format.Automatic, out ex);

                // Can't put this in redis
                if (wrapped == null)
                {
                    throw CreateNonSerializable<T>(ex);
                }

                var bytes = wrapped.ToCacheArray();
                LogSize(Server.DemandMaster, key, bytes);

                TimeSpan? expiry = durationSecs.HasValue ? TimeSpan.FromSeconds(durationSecs.Value) : (TimeSpan?)null;
                if (!CanWrite(cacheKey)) return;
                
                using (Profile("DirectSet", key))
                {
                    _cache.StringSet(cacheKey, bytes, expiry);
                }
            }
            catch (Exception e)
            {
                FireExceptionOccurred(new CacheException("DirectSet", e, key));

            }
        }
        public string Describe(string key)
        {
            var cacheKey = CleanKey(key);
            var l1 = _l1.Describe(key);
            if (!CanRead(cacheKey))
            {
                return l1;
            }
            return l1 + " => " + (string)cacheKey + ": " + ((string)_cache.DebugObject(cacheKey) ?? "(none)");
        }
        public T Get<T>(string key, Server server)
        {
            try
            {
                // Check the (pure) local cache first
                // Disallowing value types, they muck things up
                if (!typeof(T).IsValueType && _l1.Exists(key, false, server))
                {
                    var o = _l1.Get<T>(key, server);
                    if (o != null)
                    {
                        HitL1();
                        return (T)o;
                    }
                }

                var cacheKey = CleanKey(key);

                // Check for local cache use as an "L1" data store
                var wrapper = _l1.Get<RedisWrapper>(key, server);
                if (wrapper != null)
                {
                    HitL1();
                    return wrapper.GetValue<T>();
                }

                if(!CanRead(cacheKey))
                {
                    return _fallback.Get<T>(key, server);
                }

                // Now check redis; note we will pipeline a GET and a TTL, since we want both
                byte[] bytes;
                TimeSpan? ttl;
                using (Profile("Get", key))
                {
                    var valueWithExpiry = _cache.StringGetWithExpiry(cacheKey, FlagsFor(server, true));

                    bytes = valueWithExpiry.Value;
                    ttl = valueWithExpiry.Expiry;
                    if (bytes == null)
                    {
                        Miss();
                        return default(T);
                    }
                    LogSize(server, key, bytes);
                    wrapper = RedisWrapper.FromCacheArray(bytes);
                    if (wrapper == null)
                    {
                        Miss();
                        return default(T);
                    }
                }
                
                if (ttl != null && ttl.Value.TotalSeconds > 0)
                {   // push into L1
                    _l1.Set<RedisWrapper>(key, wrapper, (int)ttl.Value.TotalSeconds, false);
                }
                // Keeping sliding values in the cache manually
                if (wrapper.SlidingWindow.HasValue && CanWrite(cacheKey))
                {   // ^^^ note we can't slide at the server if it is not writeable
                    int seconds = (int)wrapper.SlidingWindow.Value.TotalSeconds;
                    using (Profile("Get", key))
                    {
                        // on later versions you can expire a volatile key to change the expiry
                        _cache.KeyExpire(cacheKey, TimeSpan.FromSeconds(seconds), CommandFlags.FireAndForget);
                    }
                }
                HitL2();
                return wrapper.GetValue<T>();
            }
            catch (AggregateException ex)
            {
                Exception e = ex.InnerExceptions.Count == 1 ? ex.InnerExceptions[0] : ex;
                FireExceptionOccurred(new CacheException("Get", e, key));
            }
            catch (Exception e)
            {
                FireExceptionOccurred(new CacheException("Get", e, key));
            }
            return default(T);
        }

        public bool SetNXSync<T>(string key, T val)
        {
            var cacheKey = CleanKey(key);
            var wrapped = RedisWrapper.For(val,RedisWrapper.Format.Automatic);

            try
            {
                if (!CanWrite(cacheKey))
                {
                    return _fallback.SetNXSync(key, val);
                }

                var bytes = wrapped.ToCacheArray();
                LogSize(Server.DemandMaster, key, bytes);
                using (Profile("SetNXSync", key))
                {
                    return _cache.StringSet(cacheKey, bytes, when: When.NotExists);
                }
            }
            catch (Exception e)
            {
                FireExceptionOccurred(new CacheException("SetNXSync", e, key));

            }

            return false;
        }
        public void SetNX<T>(string key, T val)
        {            
            var cacheKey = CleanKey(key);
            var wrapped = RedisWrapper.For(val, RedisWrapper.Format.Automatic);

            try
            {
                if (!CanWrite(cacheKey))
                {
                    _fallback.SetNX<T>(key, val);
                }
                else
                {
                    var bytes = wrapped.ToCacheArray();
                    LogSize(Server.DemandMaster, key, bytes);
                    using (Profile("SetNX", key))
                    {
                        _cache.StringSet(cacheKey, bytes, when: When.NotExists, flags: CommandFlags.FireAndForget);
                    }
                }
            }
            catch (Exception e)
            {
                FireExceptionOccurred(new CacheException("SetNX", e, key));

            }
        }

        private struct CacheSize : IComparable<CacheSize>, IEquatable<CacheSize>
        {
            private readonly int db;
            private readonly string key;
            private readonly int size;

            public int Db { get { return db; } }
            public string Key { get { return key;}}
            public int Size { get { return size;}}
            public override string ToString()
            {
                return db.ToString().PadLeft(3) + ":" + key + " = " + size.ToString("###,###,###,##0") + " bytes";
            }
            public CacheSize(int db, string key, int size)
            {
                this.db = db;
                this.key = key;
                this.size = size;
            }

            int IComparable<CacheSize>.CompareTo(CacheSize other)
            {
                int result = this.size.CompareTo(other.size);
                if(result == 0) 
                {
                    result = string.Compare(this.key, other.key);
                    if (result == 0)
                    {
                        result = this.db.CompareTo(other.db);
                    }
                }
                return result;
            }


            bool IEquatable<CacheSize>.Equals(CacheSize other)
            {
                return this.size == other.size && this.key == other.key && this.db == other.db;
            }
        }
        public static string GetExpensiveQueries()
        {
            StringBuilder builder = new StringBuilder();
            lock (expensiveQueries)
            {
                foreach (var item in expensiveQueries)
                {
                    builder.AppendLine(item.ToString());
                }
            }
            return builder.ToString();
        }
        private static readonly SortedSet<CacheSize> expensiveQueries = new SortedSet<CacheSize>();

        private const int MaxExpensiveQueryCount = 20;

        private void LogSize(Server server, string key, byte[] bytes)
        {
            switch(server)
            {
                case Server.PreferSlave:
                case Server.DemandSlave:
                    return; // no logging for you
            }
            int len = bytes == null ? 0 : bytes.Length;
            if (len > 0)
            {
                lock (expensiveQueries)
                {
                    if (expensiveQueries.Count < MaxExpensiveQueryCount)
                    {
                        expensiveQueries.Add(new CacheSize(db, key, len));
                    }
                    else if(len >= expensiveQueries.First().Size)
                    {
                        if (expensiveQueries.Add(new CacheSize(db, key, len)))
                        {
                            expensiveQueries.Remove(expensiveQueries.First());
                        }
                    }
                }
            }
        }
        internal static Exception CreateNonSerializable<T>(Exception ex)
        {
            string fullName = typeof(T).Name;
            try // be a little cautious here
            {
                if (typeof(T).IsGenericType)
                {
                    var args = typeof(T).GetGenericArguments();
                    fullName = fullName + ":" + string.Join(",", args.Select(x => x.Name));
                }
            }
            catch { }
            string message = "Not serializable, value of type " + fullName;
            return ex == null ? new Exception(message) : new Exception(message, ex);
        }
        [Conditional("DEBUG")]
        public static void ExposeSerializationErrorsInDebug()
        {
            ExposeSerializationErrors = true;
        }

        public static bool ExposeSerializationErrors { get;set; }

        public void Set<T>(string key, T val, int? durationSecs, bool sliding, bool broadcastRemoveFromCache = false)
        {
            LocalCache.OnLogDuration(key, durationSecs, "Redis.Set<T>");
            bool wasSerializing = false;
            try
            {
                Exception ex;

                wasSerializing = true;
                var wrapped = RedisWrapper.For(val, sliding ? durationSecs : null, RedisWrapper.Format.Automatic, out ex);

                // Can't put this in redis
                if (wrapped == null)
                {
                    throw CreateNonSerializable<T>(ex);
                }

                // Stash in "L1" cache
                
                var bytes = wrapped.ToCacheArray();
                wasSerializing = false;
                LogSize(Server.DemandMaster, key, bytes);
                // Don't cache things that don't expire in "L1"
                //   Things could get very ill, so override it to 1 hr
                durationSecs = durationSecs ?? 60 * 60;
                _l1.Set<RedisWrapper>(key, wrapped, durationSecs, sliding);

                var cacheKey = CleanKey(key);
                
                if (!CanWrite(cacheKey))
                {
                    _fallback.Set<T>(key, val, durationSecs, sliding);
                    return;
                }
                TimeSpan? expiry = null;
                if (durationSecs.HasValue)
                {
                    expiry = TimeSpan.FromSeconds(durationSecs.Value);
                }
                else
                {
                    broadcastRemoveFromCache = true;
                }
                using (Profile("Set", key))
                {
                    _cache.StringSet(cacheKey, bytes, TimeSpan.FromSeconds(durationSecs.Value), flags: CommandFlags.FireAndForget);
                }
                if (broadcastRemoveFromCache)
                {
                    BroadcastRemoveFromCache(key);
                }
            }
            catch (Exception e)
            {
                if (wasSerializing && ExposeSerializationErrors) throw;
                FireExceptionOccurred(new CacheException("Set", e, key));
            }
        }

        public bool RequiresSerialization { get { return true; } }

        public bool RemoveSync(string key)
        {
            var cacheKey = CleanKey(key);

            try
            {
                if (!CanWrite(cacheKey))
                {
                    // Tricky: if we've lost a connection between a set/remove we need to nuke it from both L1 & fallback to be sure
                    //   Being in either is an indication that it was written *sometime*
                    return _fallback.RemoveSync(key) | _l1.RemoveSync(key);  // Intentionally *doesn't* short-circuit
                }

                BroadcastRemoveFromCache(key);

                // Also remove from "L1" & fallback
                _l1.RemoveSync(key);
                _fallback.RemoveSync(key);
                using (Profile("Remove", key))
                {
                    return _cache.KeyDelete(cacheKey);
                }
            }
            catch (Exception e)
            {
                FireExceptionOccurred(new CacheException("Remove", e, key));

            }
            return false;
        }
        public void Remove(string key)
        {
            var cacheKey = CleanKey(key);

            try
            {
                if (!CanWrite(cacheKey))
                {
                    // Tricky: if we've lost a connection between a set/remove we need to nuke it from both L1 & fallback to be sure
                    //   Being in either is an indication that it was written *sometime*
                    _fallback.Remove(key);
                    _l1.Remove(key);  // Intentionally *doesn't* short-circuit
                    return;
                }

                BroadcastRemoveFromCache(key);

                // Also remove from "L1" & fallback
                _l1.Remove(key);
                _fallback.Remove(key);
                using (Profile("Remove", key))
                {
                    _cache.KeyDelete(cacheKey, CommandFlags.FireAndForget);
                }
            }
            catch (Exception e)
            {
                FireExceptionOccurred(new CacheException("Remove", e, key));

            }
        }


        public void AddToSortedSet<T>(string key, T member, int score)
        {
            AddToSortedSet(key, new[] { member }, new[] { score });
        }

        public void AddToSortedSet<T>(string key, T[] members, int[] scores)
        {
            if (members.Length != scores.Length)
            {
                throw new ArgumentException("members and scores must be the same length");
            }

            var cacheKey = CleanKey(key);
            try
            {
                if (!CanWrite(cacheKey))
                {
                    _fallback.AddToSortedSet(key, members, scores);
                    return;
                }
                using (Profile("AddToSortedSet", key))
                {
                    var entries = new SortedSetEntry[members.Length];
                    for (var i = 0; i < members.Length; i++)
                    {
                        var member = members[i];
                        var score = scores[i];
                        var wrapper = RedisWrapper.For<T>(member, RedisWrapper.Format.Uncompressed);
                        var blob = wrapper.ToCacheArray();

                        entries[i] = new SortedSetEntry(blob, score);
                    }
                    
                    _cache.SortedSetAdd(cacheKey, entries, CommandFlags.FireAndForget);
                }
            }
            catch (Exception e)
            {
                FireExceptionOccurred(new CacheException("AddToSortedSet", e, key));
            }
        }

        public void AddToSortedSet(string key, string member, int score)
        {
            AddToSortedSet(key, new[] { member }, new[] { score });
        }

        public void AddToSortedSet(string key, string[] members, int[] scores)
        {
            if(members.Length != scores.Length)
            {
                throw new ArgumentException("members and scores must be the same length");
            }

            var cacheKey = CleanKey(key);

            try
            {
                if (!CanWrite(cacheKey))
                {
                    _fallback.AddToSortedSet(key, members, scores);
                    return;
                }
                using (Profile("AddToSortedSet", key))
                {
                    var entries = new SortedSetEntry[members.Length];
                    for(var i = 0; i < members.Length; i++)
                    {
                        entries[i] = new SortedSetEntry(members[i], scores[i]);
                    }

                    _cache.SortedSetAdd(cacheKey, entries, CommandFlags.FireAndForget);
                }
            }
            catch (Exception e)
            {
                FireExceptionOccurred(new CacheException("AddToSortedSet", e, new[] { key }.Concat(members).ToArray()));
            }
        }

        public void IncrementMemberOfSortedSet(string key, string[] members, int by, bool queueJump = false)
        {
            var cacheKey = CleanKey(key);

            try
            {
                if (!CanWrite(cacheKey))
                {
                    _fallback.IncrementMemberOfSortedSet(key, members, by, queueJump);
                }
                else
                {
                    using (Profile("IncrementMemberOfSortedSet", key))
                    {
                        var flags = QueueJump(queueJump) | CommandFlags.FireAndForget;
                        foreach (var member in members)
                        {
                            _cache.SortedSetIncrement(cacheKey, member, by, flags);
                        }
                        
                    }
                }
            }
            catch (Exception e)
            {
                FireExceptionOccurred(new CacheException("IncrementMemberOfSortedSet", e, key, string.Join(",", members), by.ToString()));

            }
        }

        public IOrderedEnumerable<Tuple<string, int>> GetRangeFromSortedSet(string key, int at, int length, Server server)
        {
            var cacheKey = CleanKey(key);

            try
            {
                if (!CanRead(cacheKey))
                {
                    return _fallback.GetRangeFromSortedSet(key, at, length, server);
                }

                SortedSetEntry[] pairs;
                using (Profile("GetRangeFromSortedSet", key))
                {
                    pairs = _cache.SortedSetRangeByRankWithScores(cacheKey, at, length, Order.Ascending, FlagsFor(server));
                }

                var array = Array.ConvertAll(
                    pairs,
                    pair => Tuple.Create((string)pair.Element, (int)pair.Score));
                var order = new List<Tuple<string, int>>(array);

                return array.OrderBy(a => order.IndexOf(a));
            }
            catch (Exception e)
            {
                FireExceptionOccurred(new CacheException("GetRangeFromSortedSet", e, key, at.ToString(), length.ToString()));

            }

            return Enumerable.Empty<Tuple<string, int>>().OrderBy(a => 0);
        }

        public Task<IOrderedEnumerable<Tuple<string, int>>> GetRangeFromSortedSetAsync(string key, int at, int length, Server server)
        {
            var cacheKey = CleanKey(key);

            try
            {
                if (!CanRead(cacheKey))
                {
                    return _fallback.GetRangeFromSortedSetAsync(key, at, length, server);
                }

                Task<SortedSetEntry[]> pairs;
                using (Profile("GetRangeFromSortedSetAsync", key))
                {
                    pairs = _cache.SortedSetRangeByRankWithScoresAsync(cacheKey, (long)at, (long)length, Order.Ascending, FlagsFor(server));
                }

                return 
                    pairs.ContinueWith(
                        task =>
                        {
                            var array = Array.ConvertAll(
                                task.Result,
                                pair => Tuple.Create((string)pair.Element, (int)pair.Score));
                            var order = new List<Tuple<string, int>>(array);

                            return array.OrderBy(a => order.IndexOf(a));
                        }
                    );
            }
            catch (Exception e)
            {
                FireExceptionOccurred(new CacheException("GetRangeFromSortedSetAsync", e, key, at.ToString(), length.ToString()));

            }

            return Task.FromResult(Enumerable.Empty<Tuple<string, int>>().OrderBy(a => 0));
        }

        public IOrderedEnumerable<Tuple<string, int>> GetRangeFromSortedSetDescending(string key, int at, int length, Server server)
        {           
            var cacheKey = CleanKey(key);

            try
            {
                
                if (!CanRead(cacheKey))
                {
                    return _fallback.GetRangeFromSortedSetDescending(key, at, length, server);
                }

                SortedSetEntry[] pairs;
                using (Profile("GetRangeFromSortedSetDescending", key))
                {
                    pairs = _cache.SortedSetRangeByRankWithScores(cacheKey, at, length, Order.Descending, FlagsFor(server));
                }
                var array = Array.ConvertAll(
                    pairs,
                    pair => Tuple.Create((string)pair.Element, (int)pair.Score));
                var order = new List<Tuple<string, int>>(array);

                return array.OrderBy(a => order.IndexOf(a));
            }
            catch (Exception e)
            {
                FireExceptionOccurred(new CacheException("GetRangeFromSortedSetDescending", e, key, at.ToString(), length.ToString()));

            }

            return Enumerable.Empty<Tuple<string, int>>().OrderBy(a => 0);
        }

        public int GetSortedSetSize(string key, Server server)
        {
            var cacheKey = CleanKey(key);

            try
            {
                
                if (!CanRead(cacheKey))
                {
                    return _fallback.GetSortedSetSize(key, server);
                }
                using (Profile("GetSortedSetSize", key))
                {
                    return (int)_cache.SortedSetLength(cacheKey, flags: FlagsFor(server));
                }
            }
            catch (Exception e)
            {
                FireExceptionOccurred(new CacheException("GetSortedSetSize", e, key));

            }

            return 0;
        }

        public Task<int> GetSortedSetSizeAsync(string key, Server server)
        {
            var cacheKey = CleanKey(key);

            try
            {

                if (!CanRead(cacheKey))
                {
                    return _fallback.GetSortedSetSizeAsync(key, server);
                }
                using (Profile("GetSortedSetSizeAsync", key))
                {
                    return _cache.SortedSetLengthAsync(cacheKey).ContinueWith(task => (int)task.Result);
                }
            }
            catch (Exception e)
            {
                FireExceptionOccurred(new CacheException("GetSortedSetSizeAsync", e, key));

            }

            return Task.FromResult(0);
        }

        public int CountInSortedSetByScore(string key, int from, int to, Server server)
        {
            var cacheKey = CleanKey(key);

            try
            {

                if (!CanRead(cacheKey))
                {
                    return _fallback.CountInSortedSetByScore(key, from, to, server);
                }
                using (Profile("RemoveFromSortedSetByScore", key))
                {
                    return (int)_cache.SortedSetLength(cacheKey, (double)from, (double)to, flags: FlagsFor(server));
                }
            }
            catch (Exception e)
            {
                FireExceptionOccurred(new CacheException("RemoveFromSortedSetByScore", e, key, from.ToString(), to.ToString()));

            }

            return 0;
        }

        public int RemoveFromSortedSetByScore(string key, int from, int to)
        {
            var cacheKey = CleanKey(key);

            try
            {
                
                if (!CanWrite(cacheKey))
                {
                    return _fallback.RemoveFromSortedSetByScore(key, from, to);
                }
                using (Profile("RemoveFromSortedSetByScore", key))
                {
                    return (int)_cache.SortedSetRemoveRangeByScore(cacheKey, (double)from, (double)to);
                }
            }
            catch (Exception e)
            {
                FireExceptionOccurred(new CacheException("RemoveFromSortedSetByScore", e, key, from.ToString(), to.ToString()));

            }

            return 0;
        }

        public void RemoveFromSortedSetByRank(string key, int from, int to)
        {
            var cacheKey = CleanKey(key);

            try
            {
                if (!CanWrite(cacheKey))
                {
                    _fallback.RemoveFromSortedSetByRank(key, from, to);
                    return;
                }
                
                using (Profile("RemoveFromSortedSetByRank", key))
                {
                    _cache.SortedSetRemoveRangeByRank(cacheKey, (long)from, (long)to, CommandFlags.FireAndForget);
                }
            }
            catch (Exception e)
            {
                FireExceptionOccurred(new CacheException("RemoveFromSortedSetByRank", e, key, from.ToString(), to.ToString()));

            }
        }

        public Task<bool> RemoveFromSortedSetAsync(string set, string value)
        {
            var t = RemoveFromSortedSetAsync(set, new[] { value });

            var ret = t.ContinueWith(x => x.Result > 0);

            return ret;
        }

        public Task<long> RemoveFromSortedSetAsync(string set, string[] values)
        {
            var cacheKey = CleanKey(set);

            try
            {

                if (!CanWrite(cacheKey))
                {
                    return _fallback.RemoveFromSortedSetAsync(set, values);
                }
                using (Profile("RemoveFromSortedSetAsync", set))
                {
                    var members = new RedisValue[values.Length];
                    for(var i =0; i < values.Length; i++)
                    {
                        members[i] = values[i];
                    }

                    return _cache.SortedSetRemoveAsync(cacheKey, members);
                }
            }
            catch (Exception e)
            {
                FireExceptionOccurred(new CacheException("RemoveFromSortedSetAsync", e, new[] { set }.Concat(values).ToArray()));

            }

            return Task.FromResult(0L);
        }

        public void RemoveFromSortedSet(string set, string value)
        {
            RemoveFromSortedSet(set, new[] { value });
        }

        public void RemoveFromSortedSet(string set, string[] values)
        {
            var cacheKey = CleanKey(set);
            try
            {
                if (!CanWrite(cacheKey))
                {
                    return;
                }

                using (Profile("RemoveFromSortedSet", set))
                {
                    var members = new RedisValue[values.Length];
                    for(var i =0; i < members.Length; i++)
                    {
                        members[i] = values[i];
                    }

                    _cache.SortedSetRemove(cacheKey, members, CommandFlags.FireAndForget);
                }
            }
            catch (Exception e)
            {
                FireExceptionOccurred(new CacheException("RemoveFromSortedSet", e, new[] { set }.Concat(values).ToArray()));
            }
        }

        public void TrimSortedSet(string set, int start, int stop)
        {
            var cacheKey = CleanKey(set);

            try
            {

                if (!CanWrite(cacheKey))
                {
                    _fallback.TrimSortedSet(set, start, stop);
                    return;
                }
                using (Profile("TrimSortedSet", set))
                {
                    _cache.SortedSetRemoveRangeByRank(cacheKey, start, stop, CommandFlags.FireAndForget);
                }
            }
            catch (Exception e)
            {
                FireExceptionOccurred(new CacheException("TrimSortedSet", e, set, start.ToString(), stop.ToString()));

            }
        }

        IEnumerable<string> ICache.AllKeys(string pattern, Server server, int pageSize)
        {
            try
            {
                var serverApi = AsyncRedisConnection.TryGetServer(_cache);
                if(serverApi == null)
                {
                    // No fallback, we don't want to enumerate local
                    Enumerable.Empty<string>();
                }
                using (Profile("AllKeys", pattern))
                {
                    // automatically uses SCAN when available
                    if (string.IsNullOrEmpty(keyPrefix))
                    {
                        return serverApi.Keys(db, pattern, pageSize, FlagsFor(server)).Select(x => (string)x);
                    } else
                    {
                        return RemoveKeyPrefix(serverApi.Keys(db, keyPrefix + pattern, pageSize, FlagsFor(server)));
                    }
                }
            }
            catch (Exception e)
            {
                FireExceptionOccurred(new CacheException("AllKeys", e));
            }

            return Enumerable.Empty<string>();
        }
        private IEnumerable<string> RemoveKeyPrefix(IEnumerable<RedisKey> source)
        {
            if (source == null) yield break;

            foreach(var typedKey in source)
            {
                string key = (string)typedKey;
                if (key != null && key.StartsWith(keyPrefix))
                {
                    yield return key.Substring(keyPrefix.Length);
                }
            }
        }

        public void AppendToList(string key, byte[] value)
        {
            var cacheKey = CleanKey(key);

            try
            {
                if (!CanWrite(cacheKey))
                {
                    return;
                }

                using (Profile("AppendToList", key))
                {
                    _cache.ListRightPush(cacheKey, value, flags: CommandFlags.FireAndForget);
                }
            }
            catch (Exception e)
            {
                FireExceptionOccurred(new CacheException("AppendToList", e, key));

            }
        }

        public int AppendToListSync(string key, byte[] value)
        {
            var cacheKey = CleanKey(key);

            try
            {
                if (!CanWrite(cacheKey))
                {
                    return 0;
                }

                using (Profile("AppendToListSync", key))
                {
                    return (int)_cache.ListRightPush(cacheKey, value);
                }
            }
            catch (Exception e)
            {
                FireExceptionOccurred(new CacheException("AppendToListSync", e, key));
                return 0;
            }
        }

        public void PrependToList(string key, byte[] value)
        {
           
            var cacheKey = CleanKey(key);

            try
            {
                if (!CanWrite(cacheKey))
                {
                    return;
                }

                using (Profile("PrependToList", key))
                {
                    _cache.ListLeftPush(cacheKey, value, flags: CommandFlags.FireAndForget);
                }
            }
            catch (Exception e)
            {
                FireExceptionOccurred(new CacheException("PrependToList", e, key));

            }
        }

        public byte[] PopFromList(string key)
        {
            var cacheKey = CleanKey(key);

            try
            {
                if (!CanWrite(cacheKey))
                {
                    return null;
                }
                using (Profile("PopFromList", key))
                {
                    return _cache.ListRightPop(cacheKey);
                }
            }
            catch (Exception e)
            {
                FireExceptionOccurred(new CacheException("PopFromList", e, key));

            }

            return null;
        }

        public byte[] PopFromListPushToList(string from, string to)
        {
            var fromKey = CleanKey(from);
            var toKey = CleanKey(to);

            try
            {
                if (!CanWrite(toKey))
                {
                    return null;
                }

                using (Profile("PopFromListPushToList", from))
                {
                    return _cache.ListRightPopLeftPush(fromKey, toKey);
                }
            }
            catch (Exception e)
            {
                FireExceptionOccurred(new CacheException("PopFromListPushToList", e, from, to));

            }

            return null;
        }

        public IEnumerable<byte[]> GetList(string key, Server server)
        {
          
            var cacheKey = CleanKey(key);

            try
            {
                if (!CanRead(cacheKey))
                {
                    return Enumerable.Empty<byte[]>();
                }

                using (Profile("GetList", key))
                {
                    return Array.ConvertAll(_cache.ListRange(cacheKey, flags: FlagsFor(server)), x => (byte[])x);
                }
            }
            catch (Exception e)
            {
                FireExceptionOccurred(new CacheException("GetList", e, key));

            }

            return Enumerable.Empty<byte[]>();
        }

        public int GetListLength(string key, Server server)
        {
            var cacheKey = CleanKey(key);

            try
            {
                if (!CanRead(cacheKey))
                {
                    return 0;
                }

                using (Profile("GetListLength", key))
                {
                    return (int)_cache.ListLength(cacheKey, FlagsFor(server));
                }
            }
            catch (Exception e)
            {
                FireExceptionOccurred(new CacheException("GetListLength", e, key));

            }

            return 0;
        }

        public int GetHashLength(string key, Server server = Server.PreferMaster)
        {
            var cacheKey = CleanKey(key);

            try
            {
                if (!CanRead(cacheKey))
                {
                    return 0;
                }

                using (Profile("GetHashLength", key))
                {
                    return (int)_cache.HashLength(cacheKey, FlagsFor(server));
                }
            }
            catch (Exception e)
            {
                FireExceptionOccurred(new CacheException("GetHashLength", e, key));
            }

            return 0;
        }


        public void TrimList(string key, int start, int stop)
        {
            var cacheKey = CleanKey(key);

            try
            {
                if (!CanWrite(cacheKey))
                {
                    return;
                }

                using (Profile("TrimList", key))
                {
                    _cache.ListTrim(cacheKey, start, stop, CommandFlags.FireAndForget);
                }
            }
            catch (Exception e)
            {
                FireExceptionOccurred(new CacheException("TrimList", e, key, start.ToString(), stop.ToString()));

            }
        }

        public void IncrementHash(string key, string field, int by = 1, bool queueJump = false)
        {
            var cacheKey = CleanKey(key);

            try
            {
                if (!CanWrite(cacheKey))
                {
                    _fallback.IncrementHash(key, field, by, queueJump);
                }
                else
                {
                    using (Profile("IncrementHash", key))
                    {
                        _cache.HashIncrement(cacheKey, field, by, QueueJump(queueJump) | CommandFlags.FireAndForget);
                    }
                }
            }
            catch (Exception e)
            {
                FireExceptionOccurred(new CacheException("IncrementHash", e, key, by.ToString()));

            }
        }

        public Dictionary<string, long> GetIntHash(string key, bool queueJump, Server server)
        {
            var cacheKey = CleanKey(key);

            try
            {
                if (!CanRead(cacheKey))
                {
                    return new Dictionary<string, long>();
                }

                HashEntry[] inRedis;
                using (Profile("GetIntHash", key))
                {
                    inRedis = _cache.HashGetAll(cacheKey, FlagsFor(server, queueJump));
                }
                var ret = new Dictionary<string, long>();
                foreach (var pair in inRedis)
                {
                    try
                    {
                        ret.Add(pair.Name, (long)pair.Value);
                    } catch { }                    
                        
                }
                return ret;
            }
            catch (Exception e)
            {
                FireExceptionOccurred(new CacheException("GetIntHash", e, key));

            }

            return new Dictionary<string, long>();
        }

        public long GetIntFromHash(string key, string field, bool queueJump, Server server)
        {
            var cacheKey = CleanKey(key);

            try
            {
                if (!CanRead(cacheKey))
                {
                    return 0;
                }

                RedisValue ret;
                using (Profile("GetIntFromHash", key))
                {
                    ret = _cache.HashGet(cacheKey, field, FlagsFor(server, queueJump));
                }
                return (long)ret;
            }
            catch (Exception e)
            {
                FireExceptionOccurred(new CacheException("GetIntFromHash", e, key, field));

            }

            return 0;
        }

        public void Rename(string oldName, string newName)
        {
            var oldCacheKey = CleanKey(oldName);
            var newCacheKey = CleanKey(newName);

            try
            {
                if (!CanWrite(newCacheKey))
                {
                    return;
                }

                using (Profile("Rename", oldName))
                {
                    // Jump, so anything that tries to *read* from that key won't run until it does
                    _cache.KeyRename(oldCacheKey, newCacheKey, flags: CommandFlags.HighPriority | CommandFlags.FireAndForget); 
                }
            }
            catch (Exception e)
            {
                FireExceptionOccurred(new CacheException("Rename", e, oldName, newName));

            }
        }

        public Dictionary<string, T> MultiGet<T>(IEnumerable<string> keys, Server server)
        {
            var ret = new Dictionary<string, T>();

            // Try to hydrate what we can from local cache, simulating the "get for N keys" behavior
            // you'd sort of expect
            var queryForKeys = new List<string>();
            foreach (var key in keys)
            {
                // Check for local cache use as an "L1" data store
                var wrapper = _l1.Get<RedisWrapper>(key, server);
                if (wrapper != null)
                {
                    ret[key] = wrapper.GetValue<T>();
                }
                else
                {
                    ret[key] = default(T);
                    queryForKeys.Add(key);
                }
            }

            if (queryForKeys.Count == 0) return ret;


            try
            {
                var cacheKeys = CleanKeys(queryForKeys);
                if (!CanRead(cacheKeys))
                {
                    var nonCacheRet = new Dictionary<string, T>();

                    foreach (var key in keys)
                    {
                        nonCacheRet[key] = default(T);
                    }

                    return nonCacheRet;
                }

                RedisValue[] fromCache;
                using (Profile("MultiGet", queryForKeys.Count == 1 ? queryForKeys[0] : null))
                {
                    fromCache = _cache.StringGet(cacheKeys, FlagsFor(server));
                }
                for (int i = 0; i < queryForKeys.Count; i++)
                {
                    byte[] val = fromCache[i];
                    if (val != null)
                    {
                        ret[queryForKeys[i]] = RedisWrapper.FromCacheArray(val).GetValue<T>();
                    }
                }

                return ret;
            }
            catch (Exception e)
            {
                FireExceptionOccurred(new CacheException("MultiGet", e, keys.ToArray()));

            }

            var badRet = new Dictionary<string, T>();

            foreach (var key in keys)
            {
                badRet[key] = default(T);
            }

            return badRet;
        }

        byte[] ICache.GetHash(string key, string field, bool queueJump, Server server)
        {
            var cacheKey = CleanKey(key);

            try
            {
                if (!CanRead(cacheKey))
                {
                    return null;
                }
                using (Profile("GetHash", key))
                {
                    return _cache.HashGet(cacheKey, field, FlagsFor(server, queueJump));
                }
            }
            catch (Exception e)
            {
                FireExceptionOccurred(new CacheException("GetHash", e, key));

                return null;
            }
        }
        void ICache.SetHash<T>(string key, string field, T value) {
            try {
                var wrapper = RedisWrapper.For<T>(value, RedisWrapper.Format.Uncompressed);
                ((ICache)this).SetHash(key, field, wrapper.ToCacheArray());
            }
            catch (Exception e)
            {
                FireExceptionOccurred(new CacheException("SetHash", e, key));
            }
        }
        Dictionary<string, T> ICache.GetHashAll<T>(string key, bool queueJump, Server server)
        {
            try
            {
                var raw = ((ICache)this).GetHashAll(key, queueJump, server);
                if (raw == null) return null;
                var result = new Dictionary<string, T>(raw.Count, raw.Comparer);
                foreach(var pair in raw)
                {
                    var wrapper = RedisWrapper.FromCacheArray(pair.Value);
                    result[pair.Key] = wrapper.GetValue<T>();
                }
                return result;
            }
            catch (Exception e)
            {
                FireExceptionOccurred(new CacheException("GetHashAll", e, key));
                return new Dictionary<string, T>();
            }
        }
        T ICache.GetHash<T>(string key, string field, bool queueJump, Server server)
        {
            try
            {
                var bytes = ((ICache)this).GetHash(key, field, queueJump, server);
                var wrapper = RedisWrapper.FromCacheArray(bytes);
                if (wrapper == null)
                {
                    return default(T);
                }
                return wrapper.GetValue<T>();
            }
            catch (Exception e)
            {
                FireExceptionOccurred(new CacheException("GetHash", e, key));

                return default(T);
            }
        }
        static CommandFlags FlagsFor(Server server, bool queueJump = false)
        {
            var flags = ((CommandFlags)server)
                & (CommandFlags.DemandMaster | CommandFlags.DemandSlave | CommandFlags.PreferSlave | CommandFlags.PreferMaster);

            return queueJump ? (flags | CommandFlags.HighPriority) : flags;
        }
        static CommandFlags FlagsFor(Server server)
        {
            return ((CommandFlags)server)
                & (CommandFlags.DemandMaster | CommandFlags.DemandSlave | CommandFlags.PreferSlave | CommandFlags.PreferMaster);
        }
        Dictionary<string, byte[]> ICache.GetHashAll(string key, bool queueJump, Server server)
        {
            var cacheKey = CleanKey(key);

            try
            {
                if (!CanRead(cacheKey))
                {
                    return null;
                }

                using (Profile("GetHashAll", key))
                {
                    return _cache.HashGetAll(cacheKey, FlagsFor(server) | QueueJump(queueJump)).ToDictionary(x => (string)x.Name, x => (byte[])x.Value);
                }
            }
            catch (Exception e)
            {
                FireExceptionOccurred(new CacheException("GetHashAll", e, key));

                return null;
            }
        }
        void ICache.SetHash(string key, string field, byte[] value)
        {
            var cacheKey = CleanKey(key);

            try
            {
                if (!CanWrite(cacheKey))
                {
                    return;
                }

                using (Profile("SetHash", key))
                {
                    _cache.HashSet(cacheKey, field, value, flags: CommandFlags.FireAndForget);
                }
            }
            catch (Exception e)
            {
                FireExceptionOccurred(new CacheException("SetHash", e, key, field));

            }
        }
        bool ICache.SetHashSync(string key, string field, byte[] value)
        {
            var cacheKey = CleanKey(key);

            try
            {
                if (!CanWrite(cacheKey))
                {
                    return false;
                }

                using (Profile("SetHashSync", key))
                {
                    return _cache.HashSet(cacheKey, field, value);
                }
            }
            catch (Exception e)
            {
                FireExceptionOccurred(new CacheException("SetHashSync", e, key, field));
                return false;
            }
        }

        void ICache.DeleteFromHash(string key, string field)
        {
            var cacheKey = CleanKey(key);

            try
            {
                if (!CanWrite(cacheKey))
                {
                    return;
                }

                using (Profile("DeleteFromHash", key))
                {
                    _cache.HashDelete(cacheKey, field, CommandFlags.FireAndForget);
                }
            }
            catch (Exception e)
            {
                FireExceptionOccurred(new CacheException("DeleteFromHash", e, key, field));

            }
        }




        public Tuple<T, int>[] GetRangeFromSortedSet<T>(string key, int at, int length, Server server = Server.PreferMaster)
        {
            var cacheKey = CleanKey(key);

            try
            {
                if (!CanRead(cacheKey))
                {
                    return _fallback.GetRangeFromSortedSet<T>(key, at, length, server);
                }

                SortedSetEntry[] pairs;
                using (Profile("GetRangeFromBlobSortedSet", key))
                {
                    pairs = _cache.SortedSetRangeByRankWithScores(cacheKey, at, length, Order.Ascending, FlagsFor(server));
                }

                return Array.ConvertAll(
                    pairs,
                    pair => Tuple.Create(
                        RedisWrapper.FromCacheArray(pair.Element).GetValue<T>(),
                        (int)pair.Score));
            }
            catch (Exception e)
            {
                FireExceptionOccurred(new CacheException("GetRangeFromSortedSet", e, key, at.ToString(), length.ToString()));

            }
            return LocalCache.Array<Tuple<T, int>>.Empty;
        }

        public Tuple<T, int>[] GetRangeFromSortedSetDescending<T>(string key, int at, int length, Server server = Server.PreferMaster)
        {
            var cacheKey = CleanKey(key);

            try
            {
                if (!CanRead(cacheKey))
                {
                    return _fallback.GetRangeFromSortedSetDescending<T>(key, at, length, server);
                }

                SortedSetEntry[] pairs;
                using (Profile("GetRangeFromBlobSortedSetDescending", key))
                {
                    pairs = _cache.SortedSetRangeByRankWithScores(cacheKey, at, length, Order.Descending, FlagsFor(server));
                }

                return Array.ConvertAll(
                    pairs,
                    pair => Tuple.Create(
                        RedisWrapper.FromCacheArray(pair.Element).GetValue<T>(),
                        (int)pair.Score));
            }
            catch (Exception e)
            {
                FireExceptionOccurred(new CacheException("GetRangeFromSortedSet", e, key, at.ToString(), length.ToString()));

            }
            return LocalCache.Array<Tuple<T, int>>.Empty;
        }
        static RedisKey dummyKey = "dummy";
        RedisResult ICache.ExecuteScript(string script, object args, Server server)
        {
            if (CanRead(dummyKey))
            {
                LuaScript lua = LuaScript.Prepare(script);
                return lua.Evaluate(_cache, args, flags: FlagsFor(server));
            }
            return null;
        }


        byte[] IRestorableCache.Dump(string key, out DateTime? expiry, Server server)
        {
            var cacheKey = CleanKey(key);

            try
            {
                if (!CanRead(cacheKey))
                {
                    throw new InvalidOperationException();
                }

                using (Profile("Dump", key))
                {
                    var flags = FlagsFor(server);
                    var ttl = _cache.KeyTimeToLive(cacheKey, flags);
                    var body = _cache.KeyDump(cacheKey, flags);

                    if(ttl == null)
                    {
                        expiry = null;
                    } else
                    {
                        expiry = DateTime.UtcNow.Add(ttl.Value);
                    }
                    return body;
                }
                
            }
            catch (Exception e)
            {
                FireExceptionOccurred(new CacheException("Dump", e, key));
                throw; // no hiding here
            }
        }
        void IRestorableCache.Restore(string key, byte[] body, DateTime? expiry)
        {
            var cacheKey = CleanKey(key);

            try
            {
                if (!CanWrite(cacheKey))
                {
                    throw new InvalidOperationException();
                }

                using (Profile("Restore", key))
                {
                    var now = DateTime.UtcNow;
                    TimeSpan? ttl = null;
                    bool purge = false;
                    if(body == null)
                    {
                        purge = true;
                    }
                    else if(expiry != null)
                    {
                        if(now < expiry)
                        {
                            ttl = expiry.Value - now;   
                        }
                        else
                        {
                            purge = true;
                        }
                    }
                    if (purge)
                    {
                        _cache.KeyDelete(cacheKey);
                    }
                    else
                    {
                        var tran = _cache.CreateTransaction();
                        tran.KeyDeleteAsync(cacheKey, CommandFlags.FireAndForget);
                        tran.KeyRestoreAsync(cacheKey, body, ttl, CommandFlags.FireAndForget);
                        if (!tran.Execute()) throw new InvalidOperationException("Restore transaction failed");
                        BroadcastRemoveFromCache(key);
                        _l1.RemoveSync(key);
                        _fallback.RemoveSync(key);
                    }
                }
            }
            catch (Exception e)
            {
                FireExceptionOccurred(new CacheException("Restore", e, key));
                throw; // no hiding here
            }
        }
    }
}