
using System;
namespace StackRedis
{
    /// <summary>
    /// Provides basic profiling support
    /// </summary>
    public interface IProfiler
    {
        /// <summary>
        /// Records a profiled operation; the Dispose is called once the operation has completed
        /// </summary>
        IDisposable Profile(string name, string operation, string key);
    }

    /// <summary>
    /// Allows (if desired) the implementor to provide support for contextual profiling
    /// </summary>
    public interface IProfilerProvider
    {
        /// <summary>
        /// Obtains a profiler from the (caller-defined) context
        /// </summary>
        IProfiler GetProfiler();
    }
}
