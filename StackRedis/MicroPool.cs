using System;
using System.Threading;

namespace StackRedis
{
    internal sealed class MicroPool<T> where T : class
    {
        private readonly T[] pool;
        public MicroPool(int size) { pool = new T[size]; }
        public T TryGet()
        {
            T value;
            for (int i = 0; i < pool.Length; i++)
                if ((value = Interlocked.Exchange(ref pool[i], null)) != null) return value;
            return null;
        }
        public void Release(T value)
        {
            if (value != null)
            {
                for (int i = 0; i < pool.Length; i++)
                    if (Interlocked.CompareExchange(ref pool[i], value, null) == null) return; // stored
                // no space; clean-up and drop it
                var disp = value as IDisposable;
                if (disp != null) disp.Dispose();
            }
        }
        public void Clear()
        {
            IDisposable disp;
            for (int i = 0; i < pool.Length; i++)
                if ((disp = Interlocked.Exchange(ref pool[i], null) as IDisposable) != null) disp.Dispose();
        }
    }
}
