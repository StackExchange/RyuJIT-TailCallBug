using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;

namespace StackRedis
{
    public static class StackRedisError
    {
        public static event Action<Exception> ExceptionOccurred;

        internal static void FireExceptionOccurredImpl(Exception ex)
        {
            // Don't care about these
            if (ex is ThreadAbortException || (ex.InnerException != null && ex.InnerException is ThreadAbortException)) return; // not much we can do

            var handler = ExceptionOccurred;
            if(handler != null) handler(ex);
        }
    }
}
