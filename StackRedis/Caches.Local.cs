using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using System.Web;
using System.Web.Caching;

namespace StackRedis
{
    /// <summary>
    /// Wraps HttpRuntime.Cache in the IDelayedCache interface.
    /// 
    /// Note that not all functions are implemented
    /// </summary>
    public class LocalCache : ICache
    {
        [Obsolete("Don't use this unless you're debugging  insanity")]
        public static CacheItemRemovedCallback Removed { get; set; }

        [Obsolete("Don't use this unless you're debugging  insanity")]
        public static event Action<string, object, DateTime, TimeSpan, CacheItemPriority, int?, bool> Added;
        [Obsolete("Don't use this unless you're debugging  insanity")]

        public static event Action<string, string, int?> LogDuration;

        internal static void OnLogDuration(string key, int? duration, string caller)
        {
            var evt = LogDuration;
            if (evt != null) evt(key, caller ?? "(unknown)", duration);
        }
        RedisResult ICache.ExecuteScript(string script, object args, Server server) { return null; }
        void ICache.DetectError() {}
        public bool CanWrite { get { return true; } }
        public virtual int GetMaxLatency() { return -1; }
        protected object _lock { get; private set; }
        private readonly int db = -1;
        public int Db { get { return db; } }
        public LocalCache(object syncRoot, int db)
        {
            this.db = db;
            _lock = syncRoot;
        }
        public Dictionary<string, string> Info() { return new Dictionary<string, string>(); }
        public virtual string KeyInContext(string key)
        {
            return db == 0 ? key : KeyUniquifier.UniquePerSiteString(db, key);
        }

        public IEnumerable<string> AllKeys(string pattern, Server server, int pageSize)
        {
            if (pattern != "*") throw new NotImplementedException("Only '*' pattern valid for LocalCache");

            var ret = new List<string>();

            lock (_lock)
            {
                var e = HttpRuntime.Cache.GetEnumerator();

                while (e.MoveNext())
                {
                    var key = e.Key.ToString();

                    if (KeyUniquifier.IsPerSiteString(key, db))
                        ret.Add(KeyUniquifier.StripPerSiteString(key));
                }
            }

            return ret;
        }

        public long AppendSync(string key, string append)
        {
            lock (_lock)
            {
                var cur = Get<string>(key, Server.DemandMaster);
                if (cur == null) cur = "";
                cur += append;
                Set<string>(key, cur, null, false);

                return cur.Length;
            }
        }
        public void Append(string key, string append)
        {
            lock (_lock)
            {
                var cur = Get<string>(key, Server.DemandMaster);
                if (cur == null) cur = "";
                cur += append;
                Set<string>(key, cur, null, false);
            }
        }

        public  bool RemoveFromSetSync(string key, byte[] value)
        {
            lock (_lock)
            {
                var s = Get<HashSet<byte[]>>(key, Server.DemandMaster);
                if (s == null) return false;

                return s.Remove(value);
            }
        }
        public void RemoveFromSet(string key, byte[] value)
        {
            lock (_lock)
            {
                var s = Get<HashSet<byte[]>>(key, Server.DemandMaster);
                if (s == null) return;

                s.Remove(value);
            }
        }

        public bool RequiresSerialization {  get { return false; } }

        public IEnumerable<byte[]> EnumerateSet(string key, Server server)
        {
            List<byte[]> ret;

            lock (_lock)
            {
                var s = Get<HashSet<byte[]>>(key, server);
                ret = new List<byte[]>(s ?? new HashSet<byte[]>());
            }

            return ret;
        }

        public int GetSetSize(string key, Server server)
        {
            lock (_lock)
            {
                var r = Get<HashSet<byte[]>>(key, server);

                if (r == null) return 0;
                return r.Count;
            }
        }

        public bool AddToSetSync(string key, byte[] value)
        {
            lock (_lock)
            {
                HashSet<byte[]> s = Get<HashSet<byte[]>>(key, Server.DemandMaster);
                if (s == null)
                {
                    // Need an explicit Comparator, because byte[] default equality is all sorts of jacked
                    //   once you get to some arbitrary length
                    s = new HashSet<byte[]>(ByteArrayEqualityComparer.Singleton);
                    Set<HashSet<byte[]>>(key, s, null, false);
                }

                return s.Add(value);
            }
        }
        public void AddMultipleToSet(string key, IEnumerable<byte[]> values)
        {
            foreach (var val in values)
            {
                AddToSet(key, val);
            }
        }

        public Task<bool> AddToSet(string key, byte[] value)
        {
            lock (_lock)
            {
                HashSet<byte[]> s = Get<HashSet<byte[]>>(key, Server.DemandMaster);
                if (s == null)
                {
                    // Need an explicit Comparator, because byte[] default equality is all sorts of jacked
                    //   once you get to some arbitrary length
                    s = new HashSet<byte[]>(ByteArrayEqualityComparer.Singleton);
                    Set<HashSet<byte[]>>(key, s, null, false);
                }

                s.Add(value);
            }

            return Task.FromResult(true);
        }

        public bool SetContains(string key, byte[] value, Server server)
        {            
            var s = Get<HashSet<byte[]>>(key, server);

            if (s == null) return false;

            return s.Contains(value);
        }

        public IEnumerable<byte[]> Union(string[] keys, Server server)
        {            
            var ret = new HashSet<byte[]>(ByteArrayEqualityComparer.Singleton);

            lock (_lock)
            {
                foreach (var key in keys)
                    ret.UnionWith(Get<HashSet<byte[]>>(key, server));

                return ret;
            }
        }

        public int UnionAndStore(string to, string[] from)
        {
            // Bit of a cheat, but we know what Union *actually* returns for LocalCache
            var ret = (HashSet<byte[]>)Union(from, Server.DemandMaster);
            Set<HashSet<byte[]>>(to, ret, null, false);

            return ret.Count();
        }

        public IEnumerable<byte[]> Intersect(string[] keys, Server server)
        {

            if(keys.Length == 0) return new List<byte[]>();

            lock (_lock)
            {
                var ret = Get<HashSet<byte[]>>(keys[0], server);

                foreach (var key in keys.Skip(1))
                    ret.IntersectWith(Get<HashSet<byte[]>>(key, server));

                return ret;
            }
        }

        public int IntersectAndStore(string to, string[] from)
        {
            // Bit of a cheat, but we know what Union *actually* returns for LocalCache
            var ret = (HashSet<byte[]>)Intersect(from, Server.DemandMaster);
            Set<HashSet<byte[]>>(to, ret, null, false);

            return ret.Count();
        }

        public void IncrementInt(string key, int val, Action<long> callback)
        {
            var newVal = IncrementIntSync(key, val, false);
            try
            {
                callback(newVal);
            }
            catch { }
        }
        public void IncrementInt(string key, int val, bool queueJump = false)
        {
            lock (_lock)
            {
                var i = GetInt(key, queueJump, Server.DemandMaster);
                i += val;
                SetInt(key, i);
            }
        }
        public long IncrementIntSync(string key, int val, bool queueJump = false)
        {
            lock (_lock)
            {
                var i = GetInt(key, queueJump, Server.DemandMaster);
                i += val;
                SetInt(key, i);

                return i;
            }
        }

        public void SetInt(string key, long value)
        {
            Set<long>(key, value, null, false);
        }

        public long GetInt(string key, bool queueJump, Server server)
        {
            key = KeyInContext(key);

            lock (_lock)
            {
                object x = HttpRuntime.Cache[key];

                if (x is long) return (long)x;

                return default(long);
            }
        }

        public TimeSpan? ExpiresIn(string key, Server server)
        {
            throw new NotSupportedException();
        }


        public void Expire(string key, int ttlSeconds, bool queueJump = false)
        {
            throw new NotSupportedException();
        }

        public bool Exists(string key, bool queueJump, Server server)
        {
            
            key = KeyInContext(key);

            return HttpRuntime.Cache[key] != null;
        }

        public bool RemoveSync(string key)
        {
            key = KeyInContext(key);
            lock (_lock)
            {
                return HttpRuntime.Cache.Remove(key) != null;
            }
        }
        public bool LockSync(string key, string value, int expirySeconds)
        {
            lock(_lock)
            {
                string old = Get<string>(key, Server.DemandMaster);
                if (old.HasValue()) return false;
                SetWithPriority<string>(key, value, expirySeconds, false, CacheItemPriority.Normal);
                return true;
            }
        }
        public bool ExtendLockSync(string key, string value, int expirySeconds)
        {
            lock (_lock)
            {
                string old = Get<string>(key, Server.DemandMaster);
                if (old == value)
                {
                    SetWithPriority<string>(key, value, expirySeconds, false, CacheItemPriority.Normal);
                    return true;
                }
                return false;
            }
        }
        public bool ReleaseLockSync(string key, string value)
        {
            lock (_lock)
            {
                string old = Get<string>(key, Server.DemandMaster);
                if (old == value)
                {
                    Remove(key);
                    return true;
                }
                return false;
            }
        }

        public string[] GetLockOwners(string[] keys)
        {
            string[] owners = new string[keys.Length];
            lock (_lock)
            {
                for (int i = 0; i < keys.Length; i++)
                {
                    owners[i] = Get<string>(keys[i], Server.DemandMaster);
                }
            }
            return owners;
        }

        public void Remove(string key)
        {
            key = KeyInContext(key);

            lock (_lock)
            {
                HttpRuntime.Cache.Remove(key);
            }
        }

        public bool SetNXSync<T>(string key, T val)
        {
            lock (_lock)
            {
                if (Get<T>(key, Server.DemandMaster).Equals(default(T)))
                {
                    Set<T>(key, val, null, false);

                    return true;
                }

                return false;
            }
        }
        public void SetNX<T>(string key, T val)
        {
            lock (_lock)
            {
                if (Get<T>(key, Server.DemandMaster).Equals(default(T)))
                {
                    Set<T>(key, val, null, false);
                }
            }
        }

        public void Set<T>(string key, T value, int? durationSecs, bool sliding, bool broadcastRemoveFromCache = false)
        {
            LocalCache.OnLogDuration(key, durationSecs, "LocalCache.Set");
            SetWithPriority<T>(key, value, durationSecs, sliding, System.Web.Caching.CacheItemPriority.Default);
        }

        /// <summary>
        /// Non-standard set for when we want to be absolutely explicit about cache priorities
        /// </summary>
        public void SetWithPriority<T>(string key, T value, int? durationSecs, bool isSliding, System.Web.Caching.CacheItemPriority priority)
        {
            LocalCache.OnLogDuration(key, durationSecs, "LocalCache.SetWithPriority");
            key = KeyInContext(key);

            RawSet(key, value, durationSecs, isSliding, priority);
        }

        /// <summary>
        /// Only call with already munged (KeyInContext style) keys.
        /// </summary>
        private void RawSet(string cacheKey, object value, int? durationSecs, bool isSliding, System.Web.Caching.CacheItemPriority priority)
        {
            LocalCache.OnLogDuration(cacheKey, durationSecs, "RawSet");
            var absolute = !isSliding && durationSecs.HasValue ? DateTime.UtcNow.AddSeconds(durationSecs.Value) : Cache.NoAbsoluteExpiration;
            var sliding = isSliding && durationSecs.HasValue ? TimeSpan.FromSeconds(durationSecs.Value) : Cache.NoSlidingExpiration;

            HttpRuntime.Cache.Insert(cacheKey, value, null, absolute, sliding, priority, Removed);
            var evt = Added;
            if(evt != null) evt(cacheKey, value, absolute, sliding, priority, durationSecs, isSliding);
        }
        public T DirectGet<T>(string key, Server server) { return Get<T>(key, server); }
        public void DirectSet<T>(string key, T value) { Set<T>(key, value, null, false); }
        public void DirectSetSync<T>(string key, T val, int? durationSecs = null) { Set<T>(key, val, durationSecs, false); }

        /// <summary>
        /// Do not use directly, and only pass in already munged (KeyInContext style) keys.
        /// </summary>
        private object RawGet(string key)
        {
            return HttpRuntime.Cache[key];
        }
        public string Describe(string key) {
            key = KeyInContext(key);
            var o = RawGet(key);
            if (o == null) return key + ": (none)";
            return key + ":  " + o.GetType().FullName + ": " + o.ToString();
        }
        public T Get<T>(string key, Server server)
        {
            key = KeyInContext(key);

            var o = RawGet(key);

            if (o == null) return default(T);

            if(o is T)
                return (T)o;

            return default(T);
        }

        public void AddToSortedSet(string key, string name, int score)
        {
            AddToSortedSet(key, new[] { name }, new[] { score });
        }

        public void AddToSortedSet(string key, string[] names, int[] scores)
        {
            if(names.Length != scores.Length)
            {
                throw new ArgumentException("names and scores must be of the same size");
            }

            lock (_lock)
            {
                var sorted = Get<Dictionary<string, int>>(key, Server.DemandMaster);
                if (sorted == null)
                {
                    sorted = new Dictionary<string, int>();
                    Set<Dictionary<string, int>>(key, sorted, null, false);
                }

                for (var i = 0; i < names.Length; i++)
                {
                    var name = names[i];
                    var score = scores[i];
                    sorted[name] = score;
                }
            }
        }

        public void AddToSortedSet<T>(string key, T value, int score)
        {
        }

        public void AddToSortedSet<T>(string key, T[] values, int[] scores)
        {
        }
        

        public IOrderedEnumerable<Tuple<string, int>> GetRangeFromSortedSet(string key, int start, int length, Server server)
        {
            var sorted = Get<Dictionary<string, int>>(key, server);

            if (sorted == null) return Enumerable.Empty<Tuple<string, int>>().OrderBy(o => 0);

            IEnumerable<KeyValuePair<string,int>> ret = sorted.OrderBy(k => k.Value);

            // start == 0, length == -1 means "all"
            if(!(start == 0 && length == -1)) ret = ret.Skip(start).Take(length);

            return ret.Select(k => new Tuple<string, int>(k.Key, k.Value)).ToList().OrderBy(k => sorted[k.Item1]);
        }

        public Task<IOrderedEnumerable<Tuple<string, int>>> GetRangeFromSortedSetAsync(string key, int start, int length, Server server)
        {
            return Task.FromResult(GetRangeFromSortedSet(key, start, length, server));
        }

        public IOrderedEnumerable<Tuple<string, int>> GetRangeFromSortedSetDescending(string key, int start, int length, Server server)
        {
            var sorted = Get<Dictionary<string, int>>(key, server);

            if (sorted == null) return Enumerable.Empty<Tuple<string, int>>().OrderBy(o => 0);

            var ret = sorted.OrderByDescending(k => k.Value).Skip(start).Take(length);

            return ret.Select(k => new Tuple<string, int>(k.Key, k.Value)).ToList().OrderByDescending(k => sorted[k.Item1]);
        }

        public void IncrementMemberOfSortedSet(string key, string[] members, int by, bool queueJump = false)
        {
            lock (_lock)
            {
                var sorted = Get<Dictionary<string, int>>(key, Server.DemandMaster);
                if (sorted == null)
                {
                    sorted = new Dictionary<string, int>();
                    Set<Dictionary<string, int>>(key, sorted, null, false);
                }

                //int[] ret = new int[members.Length];

                int p = 0;
                foreach (var member in members)
                {
                    if (!sorted.ContainsKey(member)) sorted[member] = 0;

                    sorted[member] += by;
                            
                    //ret[p] = sorted[member];
                    p++;
                }
                        
                //return ret;
            }
        }

        public int GetSortedSetSize(string key, Server server)
        {
            lock (_lock)
            {
                var sorted = Get<Dictionary<string, int>>(key, server);

                if (sorted == null) return 0;

                return sorted.Count;
            }
        }

        public Task<int> GetSortedSetSizeAsync(string key, Server server)
        {
            return Task.FromResult(GetSortedSetSize(key, server));
        }

        public int RemoveFromSortedSetByScore(string key, int from, int to)
        {
            lock (_lock)
            {
                var sorted = Get<Dictionary<string, int>>(key, Server.DemandMaster);

                if (sorted == null || sorted.Count == 0) return 0;

                var toRemove = sorted.Where(u => u.Value >= from && u.Value <= to).ToList();

                foreach (var remove in toRemove)
                {
                    sorted.Remove(remove.Key);
                }

                return toRemove.Count;
            }
        }

        public void RemoveFromSortedSetByRank(string key, int from, int to)
        {
            lock (_lock)
            {
                var dict = Get<Dictionary<string, int>>(key, Server.DemandMaster);
                var sorted = dict.OrderBy(x => x.Value).ToList();

                if (sorted == null || sorted.Count == 0) return;

                // -1 is last, -2 is second-to-last, etc (see ZREMRANGEBYRANK)
                if (from < 0) from = sorted.Count + from;
                if (to < 0) to = sorted.Count + to;

                if (to < from || to < 0 || from >= sorted.Count) return; // nothing to do

                if (from < 0) from = 0;
                if (to > sorted.Count) to = sorted.Count;

                for (int i = from; i < to; i++)
                {
                    dict.Remove(sorted[i].Key);
                }
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
            lock (_lock)
            {
                var sorted = Get<Dictionary<string, int>>(set, Server.DemandMaster);

                if (sorted == null) return Task.FromResult(0L);

                long ret = 0;

                for (var i = 0; i < values.Length; i++)
                {
                    var value = values[i];
                    if (sorted.Remove(value)) ret++;
                }

                return Task.FromResult(ret);
            }
        }

        public void RemoveFromSortedSet(string set, string value)
        {
            RemoveFromSortedSet(set, new[] { value });
        }

        public void RemoveFromSortedSet(string set, string[] values)
        {
            var task = RemoveFromSortedSetAsync(set, values);

            task.Wait();
        }

        public void TrimSortedSet(string set, int start, int stop)
        {
            lock (_lock)
            {
                var sorted = Get<Dictionary<string, int>>(set, Server.DemandMaster);

                if (sorted == null) return;

                start = start >= 0 ? start : start + sorted.Count;
                stop = stop >= 0 ? stop : stop + sorted.Count;

                var toRemove = sorted.OrderBy(o => o.Value).Where((_, ix) => ix >= start && ix <= stop).Select(v => v.Key).ToList();

                foreach (var key in toRemove)
                {
                    sorted.Remove(key);
                }

                return;
            }
        }

        public int CountInSortedSetByScore(string key, int from, int to, Server server)
        {
            lock (_lock)
            {
                var sorted = Get<Dictionary<string, int>>(key, server);

                if (sorted == null) return 0;

                return sorted.Count(u => u.Value >= from && u.Value <= to);
            }
        }

        public void AppendToList(string key, byte[] value)
        {
            AppendToListSync(key, value);
        }

        public int AppendToListSync(string key, byte[] value)
        {
            lock (_lock)
            {
                var list = Get<List<byte[]>>(key, Server.DemandMaster);
                if (list == null)
                {
                    list = new List<byte[]>();
                    Set<List<byte[]>>(key, list, null, false);
                }

                list.Add(value);
                return list.Count;
            }
        }

        public void PrependToList(string key, byte[] value)
        {
            lock (_lock)
            {
                var list = Get<List<byte[]>>(key, Server.DemandMaster);
                if (list == null)
                {
                    list = new List<byte[]>();
                    Set<List<byte[]>>(key, list, null, false);
                }

                list.Insert(0, value);
            }
        }

        public byte[] PopFromList(string key)
        {
            lock (_lock)
            {
                var list = Get<List<byte[]>>(key, Server.DemandMaster);

                if (list == null) return null;

                byte[] ret = null;

                if (list.Count > 0)
                {
                    ret = list[0];
                    list.RemoveAt(0);
                }

                if (list.Count == 0)
                {
                    Remove(key);
                }

                return ret;
            }
        }

        public byte[] PopFromListPushToList(string from, string to) { throw new NotSupportedException(); }

        public IEnumerable<byte[]> GetList(string key, Server server)
        {
            lock (_lock)
            {
                var list = Get<List<byte[]>>(key, server);

                if (list != null) return list.ToList();

                return new byte[0][];
            }
        }

        public IEnumerable<byte[]> GetRangeInList(string key, int start, int stop, Server server)
        {
            var list = GetList(key, server);

            if (list != null)
            {
                return list.Skip(start).Take(stop - start + 1).ToList();
            }

            return Enumerable.Empty<byte[]>();
        }

        public void IncrementHash(string key, string subKey, int by = 1, bool queueJump = false)
        {
            lock (_lock)
            {
                var hash = Get<Dictionary<string, long>>(key, Server.DemandMaster);

                if (hash == null)
                {
                    hash = new Dictionary<string, long>();
                    Set(key, hash, null, false);
                }

                long newVal;
                if (!hash.TryGetValue(subKey, out newVal)) newVal = 0;

                newVal += by;

                hash[subKey] = newVal;
            }
        }
        byte[] ICache.GetHash(string key, string field, bool queueJump, Server server) { return null; }

        T ICache.GetHash<T>(string key, string field, bool queueJump, Server server) { return default(T); }
        Dictionary<string, byte[]> ICache.GetHashAll(string key, bool queueJump, Server server) { return new Dictionary<string, byte[]>(); }

        Dictionary<string, T> ICache.GetHashAll<T>(string key, bool queueJump, Server server) { return new Dictionary<string, T>(); }
        void ICache.SetHash(string key, string field, byte[] value) { }
        void ICache.SetHash<T>(string key, string field, T value) { }

        bool ICache.SetHashSync(string key, string field, byte[] value) { return false; }
        void ICache.DeleteFromHash(string key, string field) { }

        public Dictionary<string, long> GetIntHash(string key, bool queueJump, Server server)
        {
            return Get<Dictionary<string, long>>(key, server);
        }

        public long GetIntFromHash(string key, string field, bool queueJump, Server server)
        {
            long ret;
            if(!GetIntHash(key, queueJump, server).TryGetValue(field, out ret)) ret = 0;

            return ret;
        }

        public void Rename(string oldName, string newName)
        {
            lock (_lock)
            {
                var old = RawGet(KeyInContext(oldName));
                Remove(oldName);
                RawSet(KeyInContext(newName), old, null, false, CacheItemPriority.Default);
            }
        }

        public int GetListLength(string key, Server server)
        {
            var list = Get<List<byte[]>>(key,server);

            if (list == null) return 0;

            return list.Count;
        }

        public int GetHashLength(string key, Server server)
        {
            var hash = Get <Dictionary<string, byte[]>>(key, server);

            if (hash == null) return 0;

            return hash.Keys.Count;
        }


        public void TrimList(string key, int start, int stop)
        {
            lock (_lock)
            {
                var list = Get<List<byte[]>>(key, Server.DemandMaster);

                if (list == null) return;

                if (list.Count > 0)
                {
                    list = list.Skip(start).Take(stop - start + 1).ToList();
                }

                if (list.Count == 0)
                {
                    Remove(key);
                }
            }
        }

        public Dictionary<string, T> MultiGet<T>(IEnumerable<string> keys, Server server)
        {
            var ret = new Dictionary<string, T>();

            foreach (var key in keys)
            {
                ret[key] = Get<T>(key, server);
            }

            return ret;
        }


        public static class Array<T>
        {
            public static T[] Empty = new T[0];
        }
        public Tuple<T, int>[] GetRangeFromSortedSet<T>(string key, int at, int length, Server server = Server.PreferMaster)
        {
            return Array<Tuple<T, int>>.Empty;
        }

        public Tuple<T, int>[] GetRangeFromSortedSetDescending<T>(string key, int at, int length, Server server = Server.PreferMaster)
        {
            return Array<Tuple<T, int>>.Empty;
        }
    }
}