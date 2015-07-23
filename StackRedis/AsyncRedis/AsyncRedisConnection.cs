using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using StackExchange.Redis;
using StackRedis;

namespace AsyncRedis
{
    public class ConnectionException : Exception
    {
        public ConnectionException(string message) : base(message) { }
        public ConnectionException(string message, Exception innerException) : base(message, innerException) { }
    }
    /// <summary>
    /// Wrap a single redis connection in use by multiple caches.
    /// </summary>
    public class AsyncRedisConnection : StackRedis.IRedisConnection, IRedisLite
    {

        public void ResetStormLog()
        {
            muxer.ResetStormLog();
        }

        public string GetStormLog()
        {
            return muxer.GetStormLog();
        }

        public int StormLogThreshold
        {
            get {  return muxer.StormLogThreshold; }
            set {  muxer.StormLogThreshold = value; }
        }

        /// <summary>
        /// Time (in milliseconds) that should be spent trying to connect to redis before giving up
        /// </summary>
        public static int ConnectTimeout { get; set; }
        public event Action<string> MasterChanged;
        private void OnMasterChanged(object sender, EndPointEventArgs args)
        {
            var handler = MasterChanged;
            if(handler != null)
            {
                handler(EndPointCollection.ToString(args.EndPoint));
            }
        }

        bool IRedisConnection.CanWrite
        {
            get {  return muxer.GetDatabase().IsConnected(default(RedisKey), CommandFlags.DemandMaster);  }
        }

        internal static IServer TryGetServer(IRedis redis, bool checkConnected = true)
        {
            return redis == null ? null : TryGetServer(redis.Multiplexer, checkConnected);
        }
        internal static IServer TryGetServer(ConnectionMultiplexer muxer, bool checkConnected = true)
        {
            if (muxer != null)
            {
                var eps = muxer.GetEndPoints(true);
                foreach (var ep in eps)
                {
                    var server = muxer.GetServer(ep);
                    if (server.IsSlave) continue;
                    if (checkConnected && !server.IsConnected) continue;

                    // that'll do
                    return server;
                }
            }
            return null;
        }
#if DEBUG // if you expose this in production, I WILL CUT YOU
        public void FlushDb(int db)
        {
            var server = TryGetServer(muxer);
            server.FlushDatabase(db);
        }
#endif
        public void BroadcastReconnectMessage()
        {
            muxer.PublishReconfigure(CommandFlags.FireAndForget);
        }
        public string GetConfigurationOverview(out string[] availableEndpoints)
        {
            using (var log = new StringWriter())
            {
                muxer.Configure(log);

                var eps = muxer.GetEndPoints(true);
                availableEndpoints = new string[eps.Length];
                for (int i = 0; i < eps.Length; i++)
                {
                    availableEndpoints[i] = EndPointCollection.ToString(eps[i]);
                }
                return log.ToString();
            }
        }
        void StackRedis.IRedisConnection.RegisterRecoveryCallback(Action<StackRedis.IRedisConnection> callback)
        {
            ConnectionRestored += callback;
        }
        public string SwitchMaster(string newMaster)
        {
            var ep = EndPointCollection.TryParse(newMaster);
            if (ep == null) throw new ArgumentException("newMaster");

            var server = muxer.GetServer(ep);
            using (StringWriter log = new StringWriter())
            {
                server.MakeMaster(ReplicationChangeOptions.SetTiebreaker | ReplicationChangeOptions.EnslaveSubordinates | ReplicationChangeOptions.Broadcast, log);
                return log.ToString();
            }
        }
        private static Regex _cacheKeyFilter = new Regex(@"[\s*?{}]", RegexOptions.IgnoreCase | RegexOptions.Compiled);

        public void BreakConnection(bool reconnectImmediately = true)
        {
            muxer.Configure();
        }
        //public void Reconfigure(string delimitedOptions)
        //{
        //    redundantConnection.Reconfigure(delimitedOptions);
        //}

        private readonly object syncLock = new object();
        public event Action<StackRedis.IRedisConnection> ConnectionRestored;

        /// <summary>
        /// Returns true if this connection is stalled, waiting for a failure condition to clear.
        /// </summary>
        public bool WaitingOnRecovery
        {
            get { return !muxer.IsConnected; }
        }

        /// <summary>
        /// Returns true if this connection is healthy enough
        /// </summary>
        public bool IsConnected
        {
            get { return muxer.IsConnected; }
        }
        /// <summary>
        /// Indicates the total amount of outstanding work against all servers
        /// </summary>
        public long TotalOutstanding
        {
            get { return muxer.GetCounters().TotalOutstanding; }
        }

        /// <summary>
        /// Construct a new redis connection (which will be actually setup on demand)
        /// with an object to synchronize access on.
        /// </summary>
        public AsyncRedisConnection(string tier, string delimitedConfiguration, Func<bool> shouldPromoteSlaveToMaster, string tieBreakerKey, string name, bool preserveAsyncOrder, bool shareSocketManager)
            : this(tier, delimitedConfiguration, shouldPromoteSlaveToMaster, tieBreakerKey, name, preserveAsyncOrder, shareSocketManager, null)
        {   
        }

        /// <summary>
        /// Construct a new redis connection (which will be actually setup on demand)
        /// with an object to synchronize access on.
        /// </summary>
        public AsyncRedisConnection(string tier, string delimitedConfiguration, Func<bool> shouldPromoteSlaveToMaster, string tieBreakerKey, string name, bool preserveAsyncOrder = false, bool shareSocketManager = false, TextWriter log = null)
        {
            if (log != null) log.WriteLine("{0} > AsyncRedisConnection", DateTime.UtcNow.ToShortDateString());
            var options = ConfigurationOptions.Parse(delimitedConfiguration, true);
            muxer = Create(tier, options, tieBreakerKey, name, preserveAsyncOrder, shareSocketManager, log, out subscriber);
            if (log != null) log.WriteLine("{0} < AsyncRedisConnection", DateTime.UtcNow.ToShortDateString());
        }

        static SocketManager sharedSocketManager;
        private static SocketManager GetSharedSocketManager()
        {
            var manager = Interlocked.CompareExchange(ref sharedSocketManager, null, null);
            if (manager != null) return manager;

            SocketManager killMe = null;
            try
            {
                killMe = new SocketManager();
                manager = Interlocked.CompareExchange(ref sharedSocketManager, killMe, null);
                if(manager == null)
                { // we won the race
                    manager = killMe;
                    killMe = null; // don't dispose
                    return manager;
                }
                else
                { // we lost the race; the newly created one will be disposed
                    return manager;
                }
            }
            finally
            {
                try { if (killMe != null) killMe.Dispose(); } catch { }
            }
        }
        ConnectionMultiplexer Create(string tier, ConfigurationOptions options, string tieBreaker, string clientName, bool preserveAsyncOrder, bool shareSocketManager, TextWriter log, out ISubscriber subscriber)
        {
            if (tieBreaker.HasValue()) options.TieBreaker = tieBreaker;
            if (clientName.HasValue()) options.ClientName = clientName;
            options.ChannelPrefix = tier + ":";
            options.AllowAdmin = true;
            options.AbortOnConnectFail = false;
            if(shareSocketManager)
            {
                options.SocketManager = GetSharedSocketManager();
            }
            var tmp = ConnectionMultiplexer.Connect(options, log);
            tmp.PreserveAsyncOrder = preserveAsyncOrder;
            tmp.ConfigurationChangedBroadcast += OnMasterChanged;
            tmp.ConnectionRestored += (sender, args) => OnConnectionRestored();
            tmp.ConnectionFailed += (sender, args) =>
            {
                if(args.Exception == null)
                {
                    OnError("CFAIL:" + args.FailureType.ToString(), new InvalidOperationException(EndPointCollection.ToString(args.EndPoint)));
                }
                else
                {
                    OnError("CFAIL:" + args.Exception.Message, args.Exception);
                }
            };
            tmp.InternalError += (sender, args) =>
            {
                string message = args.EndPoint == null ? args.Origin : (args.Origin + ", " + EndPointCollection.ToString(args.EndPoint)
                    + "/" + args.ConnectionType);
                OnError(message, args.Exception ?? new InvalidOperationException(message));
            };
            subscriber = tmp.GetSubscriber();
            return tmp;
        }

        private readonly ConnectionMultiplexer muxer;
        private readonly ISubscriber subscriber;


        public DateTime? NextRecoveryAttempt { get { return DateTime.UtcNow; } }


        public event Action<string, Exception> Error;

        private void OnError(string cause, Exception exception)
        {
            var handler = Error;
            if (handler != null) handler(cause, exception);
        }

        public IDatabase GetDatabase(int database)
        {
            return muxer.GetDatabase(database);
        }

        public int GetMaxLatency()
        {
            return -1;
            //var snapshot = GetSnapshot();
            //return snapshot == null ? -1 : snapshot.MaxLatency;
        }

        
        static readonly Regex dbLine = new Regex("^db[0-9]+:", RegexOptions.Compiled | RegexOptions.IgnoreCase);
        public object cache;

        public static Dictionary<string, string> ParseInfo(string all)
        {
            string[] lines = all.Split(new[] { "\r\n" }, StringSplitOptions.RemoveEmptyEntries);
            var data = new Dictionary<string, string>();
            for (int i = 0; i < lines.Length; i++)
            {
                string line = lines[i];
                int idx = line.IndexOf(':');
                if (idx >= 0 && !dbLine.IsMatch(line))
                {
                    data.Add(line.Substring(0, idx), line.Substring(idx + 1));
                }
            }
            return data;
        }

        StackRedis.IRedisLite StackRedis.IRedisConnection.GetForMessaging()
        {
            return this;
        }
        
        
        void IRedisLite.Subscribe(string channel, Action<RedisChannel, RedisValue> handler)
        {
            subscriber.Subscribe(channel, handler, flags: CommandFlags.FireAndForget);
        }            
        void IRedisLite.Unsubscribe(string channel, Action<RedisChannel, RedisValue> handler)
        {
            subscriber.Unsubscribe(channel, handler, flags: CommandFlags.FireAndForget);
        }

        void IRedisLite.Publish(string channel, RedisValue message)
        {
            subscriber.Publish(channel, message, flags: CommandFlags.FireAndForget);
        }
            
        private string GetFormattedInfo()
        {
            var server = AsyncRedisConnection.TryGetServer(subscriber);
            if (server == null) return "";
            var all = server.InfoRaw();
            var data = ParseInfo(all);
            return string.Join(Environment.NewLine, data.Select(pair => pair.Key + "\t" + pair.Value));
        }
        string IRedisLite.GetInfo(bool allowTalkToServer)
        {
            var server = AsyncRedisConnection.TryGetServer(subscriber, false);
            if (server == null) return "";
            return "AsyncRedis; " + EndPointCollection.ToString(server.EndPoint) + Environment.NewLine
                + "Server version: " + server.Version + Environment.NewLine + Environment.NewLine
                + server.Features + Environment.NewLine
                + server.GetCounters().ToString() + Environment.NewLine + Environment.NewLine
                + (allowTalkToServer ? (GetFormattedInfo() + Environment.NewLine + Environment.NewLine) : "")
                + StackRedis.AsyncDelayedRedisCache.GetExpensiveQueries();
        }

        void IRedisLite.Ping()
        {
            subscriber.Ping();
        }


        static AsyncRedisConnection()
        {
            ConnectTimeout = 1500;
            TaskScheduler.UnobservedTaskException += (sender, args) =>
            {
                StackRedisError.FireExceptionOccurredImpl(args.Exception);
                args.SetObserved();
            };
        }

        private void OnConnectionRestored()
        {
            var handler = ConnectionRestored;
            if (handler != null)
            {
                // process each individually; any fail, fine, but run all of them
                var subHandler = handler.GetInvocationList(); // (local var so I can see how many in debug)
                foreach (Action<AsyncRedisConnection> child in subHandler)
                {
                    try
                    {
                        child(this);
                    }
                    catch (Exception e)
                    {
                        StackRedisError.FireExceptionOccurredImpl(e);
                    }
                }
            }
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
    }
}