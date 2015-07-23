using System;
using System.IO;
using System.IO.Compression;

using ProtoBuf;
using System.Text;

namespace StackRedis
{
    /// <summary>
    /// Redis doesn't do sliding expirations,
    /// wrapper lets us make it happen
    /// </summary>
    [ProtoContract]
    public class RedisWrapper
    {
        private byte[] m_cachable_array;
        private object m_cached_value;

        /// <summary>
        /// Wrapped value, protobuf encoded
        /// </summary>
        [ProtoMember(1)]
        public byte[] CompressedValue { get; private set; }

        /// <summary>
        /// Wrapped value, protobuf encoded
        /// </summary>
        [ProtoMember(3)]
        public byte[] UncompressedValue { get; private set; }

        /// <summary>
        /// Window in which accessing the value will cause it to 
        /// </summary>
        [ProtoMember(2)]
        public TimeSpan? SlidingWindow { get; set; }

        public override string ToString()
        {
            var sb = new StringBuilder("RedisWrapper");
            var arr = CompressedValue;
            if (arr != null) sb.Append("; compressed: ").Append(arr.Length).Append(" bytes");
            arr = UncompressedValue;
            if (arr != null) sb.Append("; uncompressed: ").Append(arr.Length).Append(" bytes");

            var obj = m_cached_value;
            if(obj != null)
            {
                sb.Append("unwrapped: ").Append(obj.GetType().FullName).Append(" / ").Append(obj.ToString());
            }
            return sb.ToString();
        }
        /// <summary>
        /// Deserialize the object stored in Value as T
        /// </summary>
        public T GetValue<T>()
        {
            if (m_cached_value == null)
            {
                if (CompressedValue != null)
                {
                    using (var mem = new MemoryStream(CompressedValue))
                    using (var gzip = new GZipStream(mem, CompressionMode.Decompress))
                    {
                        m_cached_value = Serializer.Deserialize<T>(gzip);
                    }
                }
                else
                {
                    using (var mem = new MemoryStream(UncompressedValue ?? nix))
                    {
                        m_cached_value = Serializer.Deserialize<T>(mem);
                    }
                }

            }

            return (T)m_cached_value;
        }
        private static readonly byte[] nix = new byte[0];

        /// <summary>
        /// Create a wrapper, with no sliding expiration
        /// </summary>
        public static RedisWrapper For<T>(T o, Format format)
        {
            return For<T>(o, null, format);
        }

        static readonly MicroPool<byte[]> buffers = new MicroPool<byte[]>(10);
        /// <summary>
        /// The internal format to use when serializing contents to a byte[]
        /// </summary>
        public enum Format
        {
            /// <summary>
            /// This is the legacy behaviour; data is always compressed (with gzip)
            /// </summary>
            Compressed,
            /// <summary>
            /// Data is not compressed if it is very small, or if compressing the data resulted in a size increase; otherwise, it is compressed (with gzip)
            /// </summary>
            Automatic,
            /// <summary>
            /// Data is never compressed; this is handy when repeatability trumps all other concerns
            /// </summary>
            Uncompressed
        }

        /// <summary>
        /// Required to enable Format.Automatic
        /// </summary>
        public static bool AutomaticCompressionEnabled { get; set; }

        /// <summary>
        /// Create a wrapper, with a sliding expiration window
        /// </summary>
        public static RedisWrapper For<T>(T o, int? durationSeconds, Format format)
        {
            Exception ex;
            return For<T>(o, durationSeconds, format, out ex);
        }


        /// <summary>
        /// Create a wrapper, with a sliding expiration window
        /// </summary>
        public static RedisWrapper For<T>(T o, int? durationSeconds, Format format, out Exception exception)
        {
            if (format == Format.Automatic && !AutomaticCompressionEnabled) format = Format.Compressed; // legacy behaviour

            try
            {
                byte[] compressed, uncompressed;

                using (var mem = new MemoryStream())
                {
                    switch (format)
                    {
                        case Format.Automatic:
                            Serializer.Serialize<T>(mem, o);
                            if (mem.Length < 256)
                            { // small output; just don't bother trying to compress it
                                uncompressed = mem.ToArray();
                                compressed = null;
                            }
                            else
                            {
                                byte[] buffer = null;
                                try
                                {
                                    buffer = buffers.TryGet() ?? new byte[4096];
                                    mem.Position = 0;
                                    int read;
                                    using (var compressedMem = new MemoryStream())
                                    { // try compressing it
                                        using (var gzip = new GZipStream(compressedMem, CompressionMode.Compress, true))
                                        {
                                            while ((read = mem.Read(buffer, 0, buffer.Length)) > 0)
                                            {
                                                gzip.Write(buffer, 0, read);
                                                if (compressedMem.Length >= mem.Length) break; // longer; give up already!
                                            }
                                            gzip.Close();
                                        }
                                        if (compressedMem.Length >= mem.Length)
                                        { // compression made it no better; use the uncompressed version
                                            uncompressed = mem.ToArray();
                                            compressed = null;
                                        }
                                        else
                                        { // use the compressed version
                                            uncompressed = null;
                                            compressed = compressedMem.ToArray();
                                        }
                                    }
                                }
                                finally
                                {
                                    buffers.Release(buffer);
                                }
                            }
                            break;
                        case Format.Uncompressed:
                            Serializer.Serialize<T>(mem, o);
                            compressed = null;
                            uncompressed = mem.ToArray();
                            break;
                        case Format.Compressed:
                            using (var gzip = new GZipStream(mem, CompressionMode.Compress, true))
                            {
                                Serializer.Serialize<T>(gzip, o);
                                gzip.Close();
                            }
                            uncompressed = null;
                            compressed = mem.ToArray();
                            break;
                        default:
                            throw new ArgumentOutOfRangeException("format");
                    }
                }
                exception = null;
                return new RedisWrapper
                {
                    CompressedValue = compressed,
                    UncompressedValue = uncompressed,
                    SlidingWindow = durationSeconds.HasValue ? (TimeSpan?)TimeSpan.FromSeconds(durationSeconds.Value) : null
                };
            }
            catch (Exception ex)
            {
                // Not serializable
                exception = ex;
                return null;
            }
        }

        /// <summary>
        /// Convert this wrapper to a byte[] that 
        /// can be persisted in a cache.
        /// </summary>
        public byte[] ToCacheArray()
        {
            if (m_cachable_array == null)
            {
                using (var mem = new MemoryStream())
                {
                    Serializer.Serialize(mem, this);
                    mem.Flush();

                    m_cachable_array = mem.ToArray();
                }
            }

            return m_cachable_array;
        }

        /// <summary>
        /// Reverse the operation of ToCacheArray
        /// </summary>
        public static RedisWrapper FromCacheArray(byte[] cacheArray)
        {
            return Serializer.Deserialize<RedisWrapper>(new MemoryStream(cacheArray ?? nix));
        }
    }
}