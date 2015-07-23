using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;

namespace StackRedis
{
    /// <summary>
    /// Provides a centralized place for common functionality exposed via extension methods.
    /// </summary>
    public static class ExtensionMethods
    {
        /// <summary>
        /// Answers true if this String is either null or empty.
        /// </summary>
        /// <remarks>I'm so tired of typing String.IsNullOrEmpty(s)</remarks>
        public static bool IsNullOrEmpty(this string s)
        {
            return string.IsNullOrEmpty(s);
        }

        /// <summary>
        /// Answers true if this String is neither null or empty.
        /// </summary>
        /// <remarks>I'm also tired of typing !String.IsNullOrEmpty(s)</remarks>
        public static bool HasValue(this string s)
        {
            return !string.IsNullOrEmpty(s);
        }

        /// <summary>
        /// Not actually an extension method; obtains a field value in a thread-safe
        /// way, initializing it and disosing again if we lost the thread-race
        /// </summary>
        public static T FieldInit<T>(ref T field, Func<T> factory) where T : class
        {
            if (field != null) return field;
            T newValue = null;
            try
            {
                newValue = factory();
                if (Interlocked.CompareExchange(ref field, newValue, null) == null)
                {
                    newValue = null; // we swapped it
                }
            }
            finally
            {
                using(newValue as IDisposable) // failed to swap it; kill this one
                {
                }
            }
            return Interlocked.CompareExchange(ref field, null, null);
        }
    }
}
