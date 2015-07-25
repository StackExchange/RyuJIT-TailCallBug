using System;
using AsyncRedis;
using CorruptionRepro.Controllers;
using StackRedis;

namespace CorruptionRepro
{
    public class Current
    {
        public static ImmediateCache SiteCache { get; private set; }

        public static HomeController.TestInfo CurrentTest { get; set; }

        static Current()
        {
            var connection = new AsyncRedisConnection(null, "127.0.0.1:6379,connectTimeout=10", () => false, "", "ryujit-debug");
            var syncRoot = new object();
            var localCache = new LocalCache(syncRoot, 0);
            var redis = new AsyncDelayedRedisCache("connect", new Messaging(connection), connection, localCache, new NullCache(0), 0, "dev");
            SiteCache = new ImmediateCache(redis, 0);

#pragma warning disable 618
            LocalCache.LogDuration += (string key, string caller, int? duration) =>
#pragma warning restore 618
            {
                if (key.Contains("guid-test"))
                {
                    switch (caller)
                    {
                        case "LocalCache.Set":
                            CurrentTest.LocalCacheSetDuration = duration;
                            break;
                        case "LocalCache.SetWithPriority":
                            CurrentTest.LocalCacheSetWithPriorityDuration = duration;
                            break;
                        case "RawSet":
                            CurrentTest.RawSetDuration = duration;
                            break;
                    }
                }
            };
        }
    }
}