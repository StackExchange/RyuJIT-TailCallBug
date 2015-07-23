using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace StackRedis
{
    public interface IFallbackCache : ICache
    {
        void SetPushTarget(ICache cache);
    }

    public class FallbackCache : LocalCache, IFallbackCache
    {
        public override int GetMaxLatency() { return -1; }
        private char _marker;
        private ICache _target;

        public FallbackCache(char marker, object syncRoot, IRedisConnection connection, int db)
            : base(syncRoot, db)
        {
            _marker = marker;
            connection.RegisterRecoveryCallback(Push);
        }
        /// <summary>
        /// Get all keys (in a form such that Get() will get them) set by this cache
        /// </summary>
        /// <returns></returns>
        protected IEnumerable<string> FallbackKeys()
        {
            var ret = new List<string>();

            lock (_lock)
            {
                var e = HttpRuntime.Cache.GetEnumerator();
                while (e.MoveNext())
                {
                    if (!(e.Key is string)) continue;

                    var key = (string)e.Key;

                    if (key.StartsWith(_marker.ToString()))
                        ret.Add(key.Substring(1));
                }
            }

            return ret;
        }

        public override string KeyInContext(string key)
        {
            return _marker + base.KeyInContext(key);
        }

        public void SetPushTarget(ICache target)
        {
            if (target != null && target.Db != Db) throw new InvalidOperationException(string.Format("Push-target db:{0} does not match fallback db:{1}", target.Db, Db));
            _target = target;
        }

        private void Push(IRedisConnection connection)
        {
            if (!connection.CanWrite) return; // just don't bother... yet
            // sets
            var toPush = new List<Tuple<string, HashSet<byte[]>>>();

            // ints
            var toIncrement = new List<Tuple<string, long>>();

            // Everything else can take a hike, as they're hard to merge

            var keys = FallbackKeys();

            lock (_lock)
            {
                foreach (var e in keys)
                {
                    var o = Get<object>(e, Server.DemandMaster);

                    var s = o as HashSet<byte[]>;
                    long? i = o is long ? (long?)((long)o) : null;

                    if (s != null)
                        toPush.Add(new Tuple<string, HashSet<byte[]>>(e, new HashSet<byte[]>(s)));  // a copy so we don't get boned when we release the lock

                    if (i.HasValue)
                        toIncrement.Add(new Tuple<string, long>(e, i.Value));
                }
            }

            int id, db = Db;
            foreach (var set in toPush)
            {
                var setName = set.Item1.Substring(1);

                if (KeyUniquifier.IsPerSiteString(setName, db))
                {
                    setName = KeyUniquifier.StripPerSiteString(setName, out id);
                }
                else
                {
                    id = 0;
                }

                lock (_lock)
                {
                    foreach (var i in set.Item2)
                        _target.AddToSet(setName, i);
                }
                Remove(setName);
            }

            foreach (var i in toIncrement)
            {
                var key = i.Item1.Substring(1);

                if (KeyUniquifier.IsPerSiteString(key, db))
                {
                    key = KeyUniquifier.StripPerSiteString(key, out id);
                }
                else
                {
                    id = 0;
                }

                lock (_lock)
                {
                    _target.IncrementInt(key, (int)i.Item2);
                }
                Remove(key);
            }
        }
    }
}