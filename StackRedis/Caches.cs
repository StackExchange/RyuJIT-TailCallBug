using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using ProtoBuf;
using System.Threading.Tasks;
using StackExchange.Redis;
using System.Runtime.CompilerServices;

namespace StackRedis
{
    class CacheException : Exception
    {
        public CacheException(string message, Exception e, params string[] keys)
            : base(
                keys != null ?
                    string.Format(
                        "{0}: {1} with keys: {2}",
                        message,
                        e == null ? "(No Exception Message)" : e.Message,
                        string.Join(",", keys)) :
                    "",
                e)
        {
            var from = e == null ? null : e.Data;
            if (from != null)
            {
                var iter = from.GetEnumerator();
                using (iter as IDisposable)
                {
                    while (iter.MoveNext())
                    {
                        string key = iter.Key as string;
                        if (key != null && key.StartsWith("redis-"))
                        {   // propegate this upwards
                            Data.Add(key, iter.Value);
                        }
                    }
                }
            }
        }
    }

    /// <summary>
    /// Byte[] equality is tricky, we need to this to make it actually work for all lengths.
    /// </summary>
    class ByteArrayEqualityComparer : IEqualityComparer<byte[]>
    {
        public static readonly ByteArrayEqualityComparer Singleton = new ByteArrayEqualityComparer();

        public ByteArrayEqualityComparer() { }

        public bool Equals(byte[] x, byte[] y)
        {
            if (object.ReferenceEquals(x, y)) return true;
            if (object.ReferenceEquals(x, null)) return false;
            if (object.ReferenceEquals(y, null)) return false;

            if (x.Length != y.Length) return false;

            for (int i = 0; i < x.Length; i++)
                if (x[i] != y[i]) return false;

            return true;
        }

        public int GetHashCode(byte[] obj)
        {
            int ret = 0;

            for (int i = 0; i < obj.Length; i++)
                ret ^= (obj[i] + i).GetHashCode();

            return ret;
        }
    }

    /// <summary>
    /// Common interface for all "kinds" of caches
    /// 
    /// Delayed access only, converted to synchronous access in
    /// ImmediateCache
    /// </summary>
    public interface ICache
    {
        void DetectError();
        bool CanWrite { get; }
        int GetMaxLatency();
        int Db { get; }
        bool RequiresSerialization { get; }

        long GetInt(string key, bool queueJump = false, Server server = Server.PreferMaster);
        void SetInt(string key, long value);
        void IncrementInt(string key, int value, bool queueJump = false);
        long IncrementIntSync(string key, int value, bool queueJump = false);
        void IncrementInt(string key, int val, Action<long> callback);

        bool Exists(string key, bool queueJump = false, Server server = Server.PreferMaster);
        TimeSpan? ExpiresIn(string key, Server server = Server.PreferMaster);
        void Expire(string key, int ttlSeconds, bool queueJump = false);

        bool LockSync(string key, string value, int expirySeconds);
        bool ExtendLockSync(string key, string value, int expirySeconds);
        bool ReleaseLockSync(string key, string value);
        string[] GetLockOwners(params string[] keys);
        long AppendSync(string key, string value);
        void Append(string key, string value);
        T Get<T>(string key, Server server = Server.PreferMaster);

        string Describe(string key);

        bool SetNXSync<T>(string key, T value);
        void SetNX<T>(string key, T value);
        void Set<T>(string key, T val, int? durationSecs, bool sliding, bool broadcastRemoveFromCache = false);
        bool RemoveSync(string key);
        void Remove(string key);

        T DirectGet<T>(string key, Server server = Server.PreferMaster);
        void DirectSet<T>(string key, T value);
        void DirectSetSync<T>(string key, T val, int? durationSecs = null);
        Dictionary<string, string> Info();

        bool AddToSetSync(string key, byte[] value);
        Task<bool> AddToSet(string key, byte[] value);
        void AddMultipleToSet(string key, IEnumerable<byte[]> values);
        int GetSetSize(string key, Server server = Server.PreferMaster);
        IEnumerable<byte[]> EnumerateSet(string key, Server server = Server.PreferMaster);
        bool RemoveFromSetSync(string key, byte[] value);
        void RemoveFromSet(string key, byte[] value);
        int IntersectAndStore(string toKey, string[] fromKeys);
        IEnumerable<byte[]> Intersect(string[] keys, Server server = Server.PreferMaster);
        int UnionAndStore(string toKey, string[] fromKeys);
        IEnumerable<byte[]> Union(string[] keys, Server server = Server.PreferMaster);
        bool SetContains(string key, byte[] value, Server server = Server.PreferMaster);

        void AppendToList(string key, byte[] value);
        int AppendToListSync(string key, byte[] vale);
        void PrependToList(string key, byte[] value);
        byte[] PopFromList(string key);
        byte[] PopFromListPushToList(string from, string to);
        IEnumerable<byte[]> GetList(string key, Server server = Server.PreferMaster);
        int GetListLength(string key, Server server = Server.PreferMaster);
        IEnumerable<byte[]> GetRangeInList(string key, int start, int stop, Server server = Server.PreferMaster);
        void TrimList(string key, int start, int stop);

        void AddToSortedSet(string key, string value, int score);
        void AddToSortedSet(string key, string[] values, int[] scores);
        void AddToSortedSet<T>(string key, T value, int score);
        void AddToSortedSet<T>(string key, T[] values, int[] scores);
        void IncrementMemberOfSortedSet(string key, string[] values, int score, bool queueJump = false);
        IOrderedEnumerable<Tuple<string, int>> GetRangeFromSortedSet(string key, int at, int length, Server server = Server.PreferMaster);
        IOrderedEnumerable<Tuple<string, int>> GetRangeFromSortedSetDescending(string key, int at, int length, Server server = Server.PreferMaster);

        Tuple<T, int>[] GetRangeFromSortedSet<T>(string key, int at, int length, Server server = Server.PreferMaster);
        Tuple<T, int>[] GetRangeFromSortedSetDescending<T>(string key, int at, int length, Server server = Server.PreferMaster);

        int RemoveFromSortedSetByScore(string key, int from, int to);
        void RemoveFromSortedSetByRank(string key, int from, int to);
        int CountInSortedSetByScore(string key, int from, int to, Server server = Server.PreferMaster);
        int GetSortedSetSize(string key, Server server = Server.PreferMaster);
        Task<bool> RemoveFromSortedSetAsync(string set, string value);
        Task<long> RemoveFromSortedSetAsync(string set, string[] values);
        void RemoveFromSortedSet(string set, string value);
        void RemoveFromSortedSet(string set, string[] values);
        void TrimSortedSet(string set, int start, int stop);

        Task<int> GetSortedSetSizeAsync(string key, Server server = Server.PreferMaster);
        Task<IOrderedEnumerable<Tuple<string, int>>> GetRangeFromSortedSetAsync(string key, int at, int length, Server server = Server.PreferMaster);

        void IncrementHash(string key, string field, int by = 1, bool queueJump = false);
        Dictionary<string, long> GetIntHash(string key, bool queueJump = false, Server server = Server.PreferMaster);
        long GetIntFromHash(string key, string field, bool queueJump = false, Server server = Server.PreferMaster);
        byte[] GetHash(string key, string field, bool queueJump = false, Server server = Server.PreferMaster);
        int GetHashLength(string key, Server server = Server.PreferMaster);

        T GetHash<T>(string key, string field, bool queueJump = false, Server server = Server.PreferMaster);
        Dictionary<string, byte[]> GetHashAll(string key, bool queueJump = false, Server server = Server.PreferMaster);

        Dictionary<string, T> GetHashAll<T>(string key, bool queueJump = false, Server server = Server.PreferMaster);
        void SetHash(string key, string field, byte[] value);

        void SetHash<T>(string key, string field, T value);

        bool SetHashSync(string key, string field, byte[] value);
        void DeleteFromHash(string key, string field);

        void Rename(string oldName, string newName);

        Dictionary<string, T> MultiGet<T>(IEnumerable<string> keys, Server server = Server.PreferMaster);

        IEnumerable<string> AllKeys(string pattern, Server server = Server.PreferSlave, int pageSize = ImmediateCache.DefaultPageSize);

        RedisResult ExecuteScript(string script, object args = null, Server server = Server.PreferMaster);
    }
    public interface IRestorableCache: ICache
    {
        byte[] Dump(string key, out DateTime? expiry, Server server = Server.PreferMaster);
        void Restore(string key, byte[] dump, DateTime? expiry);
    }

    /// <summary>
    /// Standin for an IDelayedCache that does *nothing* but looks like it works.
    /// </summary>
    public class NullCache : ICache, IFallbackCache
    {

        public string Describe(string key) { return "(nil cache)"; }
        RedisResult ICache.ExecuteScript(string script, object args, Server server) { return null; }
        public bool RequiresSerialization { get { return false; } }

        void ICache.DetectError() { }
        public bool CanWrite { get { return false; } }
        public int GetMaxLatency() { return -1; }
        public void SetPushTarget(ICache cache) { }
        private readonly int db = -1;
        public int Db { get { return db; } }
        public NullCache(int db)
        {
            this.db = db;
        }
        public Dictionary<string, string> Info() { return new Dictionary<string, string>(); }
        public bool Exists(string key, bool queueJump, Server server) { return false; }
        public TimeSpan? ExpiresIn(string key, Server server) { return null; }
        public void Expire(string key, int ttlSeconds, bool queueJump = false) { }
        public long GetInt(string key, bool queueJump, Server server) { return 0; }
        public void SetInt(string key, long value) { }
        public long IncrementIntSync(string key, int value, bool queueJump = false) { return 0; }
        public void IncrementInt(string key, int value, bool queueJump = false) { }
        public void IncrementInt(string key, int val, Action<long> callback) { }

        public bool LockSync(string key, string value, int expirySeconds) { return false; }
        public string[] GetLockOwners(string[] keys)
        {
            return new string[keys.Length];
        }
        public bool ExtendLockSync(string key, string value, int expirySeconds) { return false; }
        public bool ReleaseLockSync(string key, string value) { return false; }
        
        public bool AddToSetSync(string key, byte[] value){ return false;  }
        public Task<bool> AddToSet(string key, byte[] value) { return Task.FromResult(false); }
        public void AddMultipleToSet(string key, IEnumerable<byte[]> values) { }
        public int GetSetSize(string key, Server server) { return 0; }
        public IEnumerable<byte[]> EnumerateSet(string key, Server server) { return Enumerable.Empty<byte[]>(); }
        public bool RemoveFromSetSync(string key, byte[] value) { return false; }
        public void RemoveFromSet(string key, byte[] value) { }
        public int IntersectAndStore(string toKey, string[] fromKeys) { return 0; }
        public IEnumerable<byte[]> Intersect(string[] keys, Server server) { return Enumerable.Empty<byte[]>(); }
        public int UnionAndStore(string toKey, string[] fromKeys) { return 0; }
        public IEnumerable<byte[]> Union(string[] keys, Server server) { return Enumerable.Empty<byte[]>(); }
        public bool SetContains(string key, byte[] value, Server server) { return false; }

        public long AppendSync(string key, string value) { return 0; }
        public void Append(string key, string value) { }
        public T Get<T>(string key, Server server) { return default(T); }

        public bool SetNXSync<T>(string key, T value) { return false; }
        public void SetNX<T>(string key, T value) { }
        public void Set<T>(string key, T val, int? durationSecs, bool sliding, bool broadcastRemoveFromCache = false) { }
        public bool RemoveSync(string key) { return false; }
        public void Remove(string key) { }

        public T DirectGet<T>(string key, Server server) { return default(T); }
        public void DirectSet<T>(string key, T value) { }
        public void DirectSetSync<T>(string key, T val, int? durationSecs = null) { }

        public void AddToSortedSet(string key, string value, int score) {  }
        public void AddToSortedSet(string key, string[] values, int[] scores) { }
        public void AddToSortedSet<T>(string key, T value, int score) { }
        public void AddToSortedSet<T>(string key, T[] values, int[] scores) { }
        public void IncrementMemberOfSortedSet(string key, string[] values, int score, bool queueJump = false) { }
        public IOrderedEnumerable<Tuple<string, int>> GetRangeFromSortedSet(string key, int at, int length, Server server) { return Enumerable.Empty<Tuple<string, int>>().OrderBy(s => 0); }
        public Task<IOrderedEnumerable<Tuple<string, int>>> GetRangeFromSortedSetAsync(string key, int at, int length, Server server) { return Task.FromResult(Enumerable.Empty<Tuple<string, int>>().OrderBy(s => 0)); }
        public IOrderedEnumerable<Tuple<string, int>> GetRangeFromSortedSetDescending(string key, int at, int length, Server server) { return Enumerable.Empty<Tuple<string, int>>().OrderBy(s => 0); }
        public int RemoveFromSortedSetByScore(string key, int from, int to){ return 0; }
        public void RemoveFromSortedSetByRank(string key, int from, int to) { }
        public int CountInSortedSetByScore(string key, int from, int to, Server server) { return 0; }
        public int GetSortedSetSize(string key, Server server) { return 0; }
        public Task<int> GetSortedSetSizeAsync(string key, Server server) { return Task.FromResult(0); }
        public Task<bool> RemoveFromSortedSetAsync(string set, string value) { return Task.FromResult(false); }
        public Task<long> RemoveFromSortedSetAsync(string set, string[] values) { return Task.FromResult(0L); }
        public void RemoveFromSortedSet(string set, string value) { }
        public void RemoveFromSortedSet(string set, string[] values) { }
        public void TrimSortedSet(string set, int from, int to) { }

        public void AppendToList(string key, byte[] value) { }
        public int AppendToListSync(string key, byte[] value) { return 0; }
        public void PrependToList(string key, byte[] value) { }
        public byte[] PopFromList(string key) { return null; }
        public byte[] PopFromListPushToList(string from, string to)  { return null; }
        public IEnumerable<byte[]> GetList(string key, Server server) { return Enumerable.Empty<byte[]>();}
        public int GetListLength(string key, Server server) { return 0; }
        public IEnumerable<byte[]> GetRangeInList(string key, int start, int stop, Server server) { return Enumerable.Empty<byte[]>(); }
        public void TrimList(string key, int start, int stop) { }

        public void IncrementHash(string key, string field, int by, bool queueJump) { }
        public Dictionary<string, long> GetIntHash(string key, bool queueJump, Server server) { return new Dictionary<string, long>(); }
        public long GetIntFromHash(string key, string field, bool queueJump, Server server) { return 0; }
        void ICache.DeleteFromHash(string key, string field) { }

        public void Rename(string oldName, string newName) { }

        public Dictionary<string, T> MultiGet<T>(IEnumerable<string> keys, Server server)
        {
            var ret = new Dictionary<string, T>();
            foreach (var key in keys)
            {
                ret[key] = default(T);
            }

            return ret;
        }

        public IEnumerable<string> AllKeys(string pattern, Server server, int pageSize) { return Enumerable.Empty<string>();}

        byte[] ICache.GetHash(string key, string field, bool queueJump, Server server) { return null; }
        public int GetHashLength(string key, Server server) { return 0; }

        T ICache.GetHash<T>(string key, string field, bool queueJump, Server server) { return default(T); }
        Dictionary<string, byte[]> ICache.GetHashAll(string key, bool queueJump, Server server) { return new Dictionary<string, byte[]>(); }
        Dictionary<string, T> ICache.GetHashAll<T>(string key, bool queueJump, Server server) { return new Dictionary<string, T>(); }
        void ICache.SetHash(string key, string field, byte[] value) { }
        void ICache.SetHash<T>(string key, string field, T value) { }

        bool ICache.SetHashSync(string key, string field, byte[] value) { return false; }



        
        public Tuple<T, int>[] GetRangeFromSortedSet<T>(string key, int at, int length, Server server = Server.PreferMaster)
        {
            return LocalCache.Array<Tuple<T,int>>.Empty;
        }

        public Tuple<T, int>[] GetRangeFromSortedSetDescending<T>(string key, int at, int length, Server server = Server.PreferMaster)
        {
            return LocalCache.Array<Tuple<T, int>>.Empty;
        }
    }

    /// <summary>
    /// Implements some friendly "do this right now" methods on a cache.
    /// 
    /// Also exposes the inner "delayed" version of a cache, for those
    /// rare occasions where you need it
    /// </summary>
    public class ImmediateCache
    {
        public bool RequiresSerialization
        {
            get { return tail.RequiresSerialization; }
        }
        public string ValidateSet<T>(string key, Server server = Server.PreferMaster)
        {
            var sb = new StringBuilder();
            using (var sw = new StringWriter(sb))
            {
                sw.WriteLine();
                sw.WriteLine("Set format is: {0}", setFormat);
                sw.WriteLine();
                int pass = 0, fail = 0, total = 0;
                foreach (var raw in tail.EnumerateSet(key, server))
                {
                    total++;
                    try
                    {
                        var obj = RedisWrapper.FromCacheArray(raw).GetValue<T>();
                        var calc = RedisWrapper.For<T>(obj, setFormat).ToCacheArray();
                        string s1 = Convert.ToBase64String(raw), s2 = Convert.ToBase64String(calc);
                        
                        if (s1 == s2)
                        {
                            pass++;
                        }
                        else
                        {
                            fail++;
                            sw.WriteLine("fail: {0} vs {1}", s1, s2);
                        }
                    }
                    catch (Exception ex)
                    {
                        fail++;
                        sw.WriteLine(ex.Message);
                    }
                }
                sw.WriteLine();
                sw.WriteLine("items: {2}, pass: {0}, fail: {1}", pass, fail, total);
            }
            return sb.ToString();
        }
        
        /// <summary>
        /// Construct a "right-now" version of this delayed access cache
        /// </summary>
        public ImmediateCache(ICache delayed, int db)
        {
            this.db = db;
            if (delayed.Db != db) throw new InvalidOperationException(string.Format("Delayed db:{0} does not match immediate db:{1}", delayed.Db, db));
            tail = delayed;
        }

        public bool CanWrite { get { return tail.CanWrite; } }

        private readonly ICache tail;
        public ICache Tail { get { return tail; } }
        private readonly int db = -1;
        public int Db { get { return db; } }
        private static byte[] StringBytes(string value)
        {
            return Encoding.UTF8.GetBytes(value);
        }

        /// <summary>
        /// Does this key exist in the cache?
        /// </summary>
        public bool Exists(string key, bool queueJump = false, Server server = Server.PreferMaster) { return tail.Exists(key, queueJump, server); }

        /// <summary>
        /// How long until this key expires (if it does)?
        /// </summary>
        public TimeSpan? ExpiresIn(string key, Server server = Server.PreferMaster) { return tail.ExpiresIn(key, server); }
        
        /// <summary>
        /// Set expiration of key
        /// </summary>
        public void Expire(string key, int ttlSeconds, bool queueJump = false)
        {
            tail.Expire(key, ttlSeconds, queueJump);
        }

        /// <summary>
        /// Get the (incrementable) integer stored under key
        /// </summary>
        public long GetInt(string key, bool queueJump = false, Server server = Server.PreferMaster) { return tail.GetInt(key, queueJump, server); }

        /// <summary>
        /// Set an (incrementable) integer under the given key
        /// </summary>
        public void SetInt(string key, long i) { tail.SetInt(key, i); }

        /// <summary>
        /// Increment the (empty or SetInt'ified) integer under key by some number
        /// </summary>
        public long IncrementIntSync(string key, int by = 1, bool queueJump = false) { return tail.IncrementIntSync(key, by, queueJump); }

        /// <summary>
        /// Increment the (empty or SetInt'ified) integer under key by some number
        /// </summary>
        public void IncrementInt(string key, int by = 1, bool queueJump = false) { tail.IncrementInt(key, by, queueJump); }

        /// <summary>
        /// Add a byte[] to the set (creating if necessary) under the given key
        /// </summary>
        public bool AddToSetSync(string set, byte[] value) { return tail.AddToSetSync(set, value); }

        /// <summary>
        /// Add a byte[] to the set (creating if necessary) under the given key
        /// </summary>
        public Task<bool> AddToSet(string set, byte[] value) { return tail.AddToSet(set, value); }

        /// <summary>
        /// Add a byte[] to the set (creating if necessary) under the given key
        /// </summary>
        public void AddMultipleToSet(string set, IEnumerable<byte[]> values) { tail.AddMultipleToSet(set, values); }

        /// <summary>
        /// Add a serializable item to the set
        /// </summary>
        /// <typeparam name="T"></typeparam>
        public bool AddToSetSync<T>(string set, T value) { return AddToSetSync(set, RedisWrapper.For(value, setFormat).ToCacheArray()); }

        /// <summary>
        /// Extend the specified lock
        /// </summary>
        public bool ExtendLockSync(string key, string value, int expirySeconds) { return tail.ExtendLockSync(key, value, expirySeconds); }

        /// <summary>
        /// Add a serializable item to the set
        /// </summary>
        /// <typeparam name="T"></typeparam>
        public Task<bool> AddToSet<T>(string set, T value) { return AddToSet(set, RedisWrapper.For(value, setFormat).ToCacheArray()); }

        /// <summary>
        /// Add a serializable item to the set
        /// </summary>
        /// <typeparam name="T"></typeparam>
        public void AddMultipleToSet<T>(string set, IEnumerable<T> values) {
            AddMultipleToSet(set, values.Select(value => RedisWrapper.For(value, setFormat).ToCacheArray()));
        }

        private RedisWrapper.Format setFormat = DefaultSetFormat;
        /// <summary>
        /// What format should set-based operations use? This is mainly to allow testing during changes to DefaultSetFormat
        /// </summary>
        public RedisWrapper.Format SetFormat { get { return setFormat; } set { setFormat = value; } }

        /// <summary>
        /// What format should set-based operations use BY DEFAULT?
        /// </summary>
        public const RedisWrapper.Format DefaultSetFormat = RedisWrapper.Format.Uncompressed;

        /// <summary>
        /// Get the size of the given set
        /// </summary>
        public int GetSetSize(string set, Server server = Server.PreferMaster) { return tail.GetSetSize(set, server); }

        /// <summary>
        /// Enumerate an entire set as byte[]'s
        /// </summary>
        public IEnumerable<byte[]> EnumerateSet(string set, Server server = Server.PreferMaster) { return tail.EnumerateSet(set, server); }

        private static IEnumerable<T> Deserialize<T>(IEnumerable<byte[]> raw)
        {
            var toRet = new List<T>();

            foreach (var r in raw)
            {
                try
                {
                    toRet.Add(RedisWrapper.FromCacheArray(r).GetValue<T>());
                }
                catch (Exception)
                {
                    byte[] buff = new byte[1024];

                    // Old set items could be protobuf byte[] inside a gzip stream
                    using (var copyTo = new MemoryStream())
                    {
                        using (var rs = new MemoryStream(r))
                        using (var gzip = new System.IO.Compression.GZipStream(rs, System.IO.Compression.CompressionMode.Decompress))
                        {
                            int i;
                            while ((i = gzip.Read(buff, 0, buff.Length)) > 0)
                                copyTo.Write(buff, 0, i);
                        }

                        if (typeof(T) == typeof(string))
                            toRet.Add((T)(object)Encoding.UTF8.GetString(copyTo.ToArray()));
                        else
                            toRet.Add(Serializer.Deserialize<T>(new MemoryStream(copyTo.ToArray())));
                    }
                }
            }

            return toRet;
        }

        /// <summary>
        /// Enumerate an entire set as some serializable type
        /// </summary>
        public IEnumerable<T> EnumerateSet<T>(string set, Server server = Server.PreferMaster) { return Deserialize<T>(EnumerateSet(set, server)); }


        /// <summary>
        /// Remove a string value from a set, returning if it was a member
        /// </summary>
        public bool RemoveFromSet<T>(string set, T value)
        {
            byte[] oldValue = null;

            using (var mem = new MemoryStream())
            {
                using (var gzip = new System.IO.Compression.GZipStream(mem, System.IO.Compression.CompressionMode.Compress))
                {
                    Serializer.Serialize<T>(gzip, value);
                    gzip.Close();
                }
                oldValue = mem.ToArray();
            }

            byte[] stringValue = null;

            if (typeof(T) == typeof(string))
            {
                var strBs = Encoding.UTF8.GetBytes(((string)(object)value));

                using (var mem = new MemoryStream())
                {
                    using (var gzip = new System.IO.Compression.GZipStream(mem, System.IO.Compression.CompressionMode.Compress))
                    {
                        gzip.Write(strBs, 0, strBs.Length);
                        gzip.Close();
                    }
                    stringValue = mem.ToArray();
                }
            }
            
            return RemoveFromSetSync(set, RedisWrapper.For(value, RedisWrapper.Format.Uncompressed).ToCacheArray())
                   || RemoveFromSetSync(set, RedisWrapper.For(value, RedisWrapper.Format.Compressed).ToCacheArray())
                   || RemoveFromSetSync(set, oldValue)
                   || (stringValue != null ? RemoveFromSetSync(set, stringValue) : false);
        }

        /// <summary>
        /// Remove a byte[] value from a set, returning if it was a member
        /// </summary>
        public bool RemoveFromSetSync(string set, byte[] value) { return tail.RemoveFromSetSync(set, value); }

        /// <summary>
        /// Remove a byte[] value from a set, returning if it was a member
        /// </summary>
        public void RemoveFromSet(string set, byte[] value) { tail.RemoveFromSet(set, value); }

        /// <summary>
        /// Append to a string (creating if empty) and return the resultant string
        /// </summary>
        public long AppendSync(string key, string value) { return tail.AppendSync(key, value); }

        /// <summary>
        /// Append to a string (creating if empty) and return the resultant string
        /// </summary>
        public void Append(string key, string value) { tail.Append(key, value); }

        /// <summary>
        /// Union some sets and store the result under a new key.
        /// 
        /// Returns the size of the new set.
        /// </summary>
        public int UnionAndStore(string key, params string[] sets) { return tail.UnionAndStore(key, sets); }

        /// <summary>
        /// Intersect some sets and store the result under a new key.
        /// 
        /// Returns the size of the new set.
        /// </summary>
        public int IntersectAndStore(string key, params string[] sets) { return tail.IntersectAndStore(key, sets); }

        /// <summary>
        /// Union some sets and return the results as a collection of byte[]s
        /// </summary>
        public IEnumerable<byte[]> Union(string[] sets, Server server = Server.PreferMaster) { return tail.Union(sets, server); }

        /// <summary>
        /// Intersect some sets and return the result as a collection of byte[]s
        /// </summary>
        public IEnumerable<byte[]> Intersect(string[] sets, Server server = Server.PreferMaster) { return tail.Intersect(sets, server); }

        /// <summary>
        /// Union some sets and return the results as a collection of byte[]s
        /// </summary>
        public IEnumerable<T> Union<T>(params string[] sets) { return Deserialize<T>(Union(sets)); }

        /// <summary>
        /// Intersect some sets and return the result as a collection of byte[]s
        /// </summary>
        public IEnumerable<T> Intersect<T>(params string[] sets) { return Deserialize<T>(Intersect(sets)); }
        
        /// <summary>
        /// Get a T from the cache under key
        /// </summary>
        public T Get<T>(string key, Server server = Server.PreferMaster)
        {
            return tail.Get<T>(key, server);
        }

        /// <summary>
        /// Sets a value T under the key that expires, but only if the key is not already set.
        /// 
        /// Return true if the key was set.
        /// </summary>
        public bool SetNXSync<T>(string key, T value) { return tail.SetNXSync<T>(key, value); }

        /// <summary>
        /// Sets a value T under the key that expires, but only if the key is not already set.
        /// </summary>
        public void SetNX<T>(string key, T value) { tail.SetNX<T>(key, value); }

        /// <summary>
        /// Set a value T under key that expires in durationSeconds, optionally as a sliding expiration
        /// </summary>
        [MethodImpl(MethodImplOptions.NoInlining)]
        public void Set<T>(string key, T value, int? durationSeconds = null, bool sliding = false, bool broadcastRefresh = false) {
            LocalCache.OnLogDuration(key, durationSeconds, "Immediate.Set<T>");
            tail.Set<T>(key, value, durationSeconds, sliding, broadcastRemoveFromCache: broadcastRefresh);
        }

        /// <summary>
        /// Remove the value under key from the cache, returns true if it was a member
        /// </summary>
        public bool RemoveSync(string key) { return tail.RemoveSync(key); }

        /// <summary>
        /// Remove the value under key from the cache
        /// </summary>
        public void Remove(string key) { tail.Remove(key); }

        internal const int DefaultPageSize = 500;

        /// <summary>
        /// Enumerate all the keys in the cache
        /// </summary>
        public IEnumerable<string> AllKeys() { return AllKeys("*", Server.PreferSlave, DefaultPageSize); }


        /// <summary>
        /// Enumerate all the keys in the cache that match the given pattern.
        /// 
        /// pattern can contain * and ? wildcards
        /// </summary>
        public IEnumerable<string> AllKeys(string matching, Server server)
        {
            return AllKeys(matching, server, DefaultPageSize);
        }
        /// <summary>
        /// Enumerate all the keys in the cache that match the given pattern.
        /// 
        /// pattern can contain * and ? wildcards
        /// </summary>
        public IEnumerable<string> AllKeys(string matching, Server server = Server.PreferSlave, int pageSize = DefaultPageSize)
        {
#pragma warning disable 0618
            return tail.AllKeys(matching, server, pageSize);
#pragma warning restore 0618
        }

        /// <summary>
        /// Add a member to a sorted set with the given rank.
        /// 
        /// If the member is already in the set, its ranked will be reset to the new value.
        /// 
        /// If the sorted set does not exist, it will be created.
        /// </summary>
        public void AddToSortedSet(string set, string member, int rank) { tail.AddToSortedSet(set, member, rank); }

        /// <summary>
        /// Adds members to a sorted set with the given ranks.
        /// 
        /// If a member is already in the set, its ranked will be reset to the new value.
        /// 
        /// If the sorted set does not exist, it will be created.
        /// </summary>
        public void AddToSortedSet(string set, string[] members, int[] ranks) { tail.AddToSortedSet(set, members, ranks); }

        /// <summary>
        /// Add a member to a sorted set with the given rank.
        /// 
        /// If the member is already in the set, its ranked will be reset to the new value.
        /// 
        /// If the sorted set does not exist, it will be created.
        /// </summary>
        public void AddToSortedSet<T>(string set, T member, int rank) { tail.AddToSortedSet<T>(set, member, rank); }

        /// <summary>
        /// Adds members to a sorted set with the given ranks.
        /// 
        /// If a member is already in the set, its ranked will be reset to the new value.
        /// 
        /// If the sorted set does not exist, it will be created.
        /// </summary>
        public void AddToSortedSet<T>(string set, T[] members, int[] ranks) { tail.AddToSortedSet(set, members, ranks); }

        /// <summary>
        /// Increment the rank of a given member of a sorted set.
        /// 
        /// If the member is not in the set, it is added with the given score.
        /// 
        /// If the set does not exist, it is created with the single provided entry.
        /// </summary>
        public void IncrementMemberOfSortedSet(string set, string member, int by, bool queueJump = false)
        {
            IncrementMembersOfSortedSet(set, by, new[]{member}, queueJump);
        }

        /// <summary>
        /// Batch up a set of increments on a sorted set.
        /// 
        /// In theory, this should yield significant savings.  Given that updates
        /// are the slowest part of using a sorted set, this is highly recommented
        /// over multiple calls to IncrementMemberOfSortedSet
        /// </summary>
        public void IncrementMembersOfSortedSet(string set, int by, params string[] members) { tail.IncrementMemberOfSortedSet(set, members, by); }

        /// <summary>
        /// Batch up a set of increments on a sorted set.
        /// 
        /// In theory, this should yield significant savings.  Given that updates
        /// are the slowest part of using a sorted set, this is highly recommented
        /// over multiple calls to IncrementMemberOfSortedSet
        /// </summary>
        public void IncrementMembersOfSortedSet(string set, int by, string[] members, bool queueJump = false) { tail.IncrementMemberOfSortedSet(set, members, by, queueJump); }

        /// <summary>
        /// Get a subset of the members in a given set.
        /// 
        /// This method is 0-indexed, and returns ordered ascending.
        /// </summary>
        public IOrderedEnumerable<Tuple<string, int>> GetRangeFromSortedSet(string set, int at, int count, Server server = Server.PreferMaster) { return tail.GetRangeFromSortedSet(set, at, count, server); }

        /// <summary>
        /// Get a subset of the members in a given set.
        /// 
        /// This method is 0-indexed, and returns ordered descending.
        /// </summary>
        public IOrderedEnumerable<Tuple<string, int>> GetRangeFromSortedSetDescending(string set, int at, int count, Server server = Server.PreferMaster) { return tail.GetRangeFromSortedSetDescending(set, at, count, server); }

        /// <summary>
        /// Get a subset of the members in a given set.
        /// 
        /// This method is 0-indexed, and returns ordered ascending.
        /// </summary>
        public Tuple<T, int>[] GetRangeFromSortedSet<T>(string set, int at, int count, Server server = Server.PreferMaster) { return tail.GetRangeFromSortedSet<T>(set, at, count, server); }

        /// <summary>
        /// Get a subset of the members in a given set.
        /// 
        /// This method is 0-indexed, and returns ordered descending.
        /// </summary>
        public Tuple<T, int>[] GetRangeFromSortedSetDescending<T>(string set, int at, int count, Server server = Server.PreferMaster) { return tail.GetRangeFromSortedSetDescending<T>(set, at, count, server); }

        /// <summary>
        /// Gets all of a sorted set.
        /// 
        /// If you just need a subset, its much more efficient to use GetRangeFromSortedSet.
        /// </summary>
        public IOrderedEnumerable<Tuple<string, int>> GetSortedSet(string set)
        {
            return GetRangeFromSortedSet(set, 0, GetSortedSetSize(set));
        }

        /// <summary>
        /// Gets all of a sorted set.
        /// 
        /// If you just need a subset, its much more efficient to use GetRangeFromSortedSet.
        /// 
        /// </summary>
        public Task<IOrderedEnumerable<Tuple<string, int>>> GetSortedSetAsync(string set, Server server = Server.PreferMaster)
        {
            return tail.GetRangeFromSortedSetAsync(set, 0, -1, server);
        }

        /// <summary>
        /// Get the size of a given sorted set.
        /// </summary>
        public int GetSortedSetSize(string set, Server server = Server.PreferMaster) { return tail.GetSortedSetSize(set, server); }

        /// <summary>
        /// Remove all keys with scores of `from` to `to`, inclusive.
        /// 
        /// Returns the number of items removed (which will be 0 if the set does not exist).
        /// </summary>
        public int RemoveFromSortedSetByScore(string set, int from, int to) { return tail.RemoveFromSortedSetByScore(set, from, to); }

        /// <summary>
        /// Remove all keys by rank
        /// </summary>
        public void RemoveFromSortedSetByRank(string set, int from, int to) { tail.RemoveFromSortedSetByRank(set, from, to); }

        /// <summary>
        /// Remove the given value from the given sorted set.
        /// </summary>
        public void RemoveFromSortedSet(string set, string value) { tail.RemoveFromSortedSet(set, value); }
        
        /// <summary>
        /// Remove the given values from the given sorted set.
        /// </summary>
        public void RemoveFromSortedSet(string set, string[] values) { tail.RemoveFromSortedSet(set, values); }

        /// <summary>
        /// Remove the given value from the given sorted set, does not wait for completion before returning.
        /// </summary>
        public Task<bool> RemoveFromSortedSetAsync(string set, string value) { return tail.RemoveFromSortedSetAsync(set, value); }

        public Task<long> RemoveFromSortedSetAsync(string set, string[] values) { return tail.RemoveFromSortedSetAsync(set, values); }

        /// <summary>
        /// Removes a range of items from the sorted set.
        /// 
        /// start and stop are 0-indexed.
        /// </summary>
        public void TrimSortedSet(string set, int start, int stop) { tail.TrimSortedSet(set, start, stop); }

        /// <summary>
        /// Remove all keys with scores of `from` to `to`, inclusive.
        /// 
        /// Returns the number of items removed (which will be 0 if the set does not exist).
        /// </summary>
        public int CountInSortedSetByScore(string set, int from, int to, Server server = Server.PreferMaster) { return tail.CountInSortedSetByScore(set, from, to, server); }

        /// <summary>
        /// Just serialize this thing using protobuf.
        /// 
        /// Assumes that there aren't any fields that would benefit from compression (so no strings).
        /// </summary>
        private static byte[] SimpleSerialize<T>(T value)
        {
            using (var mem = new MemoryStream())
            {
                Serializer.Serialize<T>(mem, value);
                mem.Flush();

                return mem.ToArray();
            }
        }

        /// <summary>
        /// Deserializes using protobuf.
        /// 
        /// Inverse of SimpleSerialize.
        /// </summary>
        private static T SimpleDeserialize<T>(byte[] encoded)
        {
            using (var mem = new MemoryStream(encoded))
                return Serializer.Deserialize<T>(mem);
        }

        /// <summary>
        /// Append an item to a redis list.
        /// 
        /// Returns the new size of the list
        /// </summary>
        public void AppendToList<T>(string key, T value)
        {
            tail.AppendToList(key, SimpleSerialize(value));
        }

        /// <summary>
        /// Synchronous equivalent of AppendToList.
        /// </summary>
        public int AppendToListSync<T>(string key, T value)
        {
            return tail.AppendToListSync(key, SimpleSerialize(value));
        }

        /// <summary>
        /// Prepend an item to a redis list.
        /// 
        /// Returns the new size of the list.
        /// </summary>
        public void PrependToList<T>(string key, T value)
        { 
            tail.PrependToList(key, SimpleSerialize(value)); 
        }

        /// <summary>
        /// Remove the first item a list, and return it.
        /// 
        /// Returns null if the list is empty.
        /// </summary>
        public T PopFromList<T>(string key)
        {
            var fromRedis = tail.PopFromList(key);

            if (fromRedis == null) return default(T);

            return SimpleDeserialize<T>(fromRedis);
        }

        /// <summary>
        /// Blocks until `from` has an item, then removes it, returns it, and pushes it to `to`.
        /// </summary>
        public T PopFromListPushToList<T>(string from, string to)
        {
            var fromRedis = tail.PopFromListPushToList(from, to);

            if (fromRedis == null) return default(T);

            return SimpleDeserialize<T>(fromRedis);
        }

        /// <summary>
        /// Enumerates the entire list identified by `key`.
        /// </summary>
        public IEnumerable<T> GetList<T>(string key, Server server = Server.PreferMaster)
        {
            var ret = new List<T>();
            var raw = tail.GetList(key, server);

            foreach (var r in raw)
                ret.Add(SimpleDeserialize<T>(r));

            return ret;
        }

        /// <summary>
        /// Get a subset of a list identified by `key`.
        /// </summary>
        public IEnumerable<T> GetRangeInList<T>(string key, int start, int stop, Server server = Server.PreferMaster)
        {
            var raw = tail.GetRangeInList(key, start, stop, server);

            return raw.Select(s => SimpleDeserialize<T>(s)).ToList();
        }

        /// <summary>
        /// Return the size of a list.
        /// </summary>
        public int GetListLength(string key, Server server = Server.PreferMaster) { return tail.GetListLength(key, server); }

        /// <summary>
        /// Return the size of a hash.
        /// </summary>
        public int GetHashLength(string key, Server server = Server.PreferMaster) { return tail.GetHashLength(key, server); }

        /// <summary>
        /// Trim a list to the specified range.
        /// </summary>
        public void TrimList(string key, int start, int stop) { tail.TrimList(key, start, stop); }

        /// <summary>
        /// Increment a field on a hash.  Creates the hash if it does not exist.
        /// </summary>
        public void IncrementHash(string key, string field, int by = 1, bool queueJump = false) { tail.IncrementHash(key, field, by, queueJump); }

        /// <summary>
        /// Get all the fields on a hash.
        /// 
        /// Will only work if *every* field on that has is a number.
        /// </summary>
        public Dictionary<string, long> GetIntHash(string key, bool queueJump = false, Server server = Server.PreferMaster) { return tail.GetIntHash(key, queueJump, server); }

        /// <summary>
        /// Get a single field on a hash as a number.
        /// 
        /// Expects this field to have been created via IncrementHash()
        /// </summary>
        public long GetIntFromHash(string key, string field, bool queueJump = false, Server server = Server.PreferMaster) { return tail.GetIntFromHash(key, field, queueJump, server); }

        /// <summary>
        /// Copy and destroy (atomically) the value at oldName to newName.
        /// 
        /// Only call if you're sure oldName exists (check via Exists()), otherwise you'll encounter an error.
        /// </summary>
        public void Rename(string oldName, string newName) { tail.Rename(oldName, newName); }

        /// <summary>
        /// Get an individual field from a hash
        /// </summary>
        public byte[] GetHash(string key, string field, bool queueJump = false, Server server = Server.PreferMaster) { return tail.GetHash(key, field, queueJump, server); }
        /// <summary>
        /// Get all fields from a hash
        /// </summary>
        public Dictionary<string, byte[]> GetHashAll(string key, bool queueJump = false, Server server = Server.PreferMaster) { return tail.GetHashAll(key, queueJump, server); }
        /// <summary>
        /// Set an individual field from a hash
        /// </summary>
        public void SetHash(string key, string field, byte[] value) { tail.SetHash(key, field, value); }

        /// <summary>
        /// Set an individual field from a hash
        /// </summary>
        /// <returns>true if this is a new field in the hash; false if the field already existed</returns>
        public bool SetHashSync(string key, string field, byte[] value) { return tail.SetHashSync(key, field, value); }
        /// <summary>
        /// Delete a field from the hash
        /// </summary>
        public void DeleteFromHash(string key, string field) { tail.DeleteFromHash(key, field); }

        /// <summary>
        /// Self-checking
        /// </summary>
        public void DetectError() { tail.DetectError(); }
        /// <summary>
        /// Gets a set of keys in one batch request.
        /// </summary>
        public Dictionary<string, T> MultiGet<T>(string[] keys, Server server = Server.PreferMaster) { return tail.MultiGet<T>(keys, server); }
        /// <summary>
        /// Gets a set of keys in one batch.
        /// </summary>
        public Dictionary<string, T> MultiGet<T>(IEnumerable<string> keys, Server server = Server.PreferMaster) { return tail.MultiGet<T>(keys, server); }
        
        /// <summary>
        /// Try to take an exclusve lock by setting a key for a given duration
        /// </summary>
        public bool LockSync(string key, string value, int expirySeconds) { return tail.LockSync(key, value, expirySeconds); }
        /// <summary>
        /// How long are things taking?
        /// </summary>
        public int GetMaxLatency() { return tail.GetMaxLatency(); }
    }

    public enum Server
    {
        DemandMaster = StackExchange.Redis.CommandFlags.DemandMaster,
        PreferMaster = StackExchange.Redis.CommandFlags.PreferMaster,
        PreferSlave = StackExchange.Redis.CommandFlags.PreferSlave,
        DemandSlave = StackExchange.Redis.CommandFlags.DemandSlave
    }
}