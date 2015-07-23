using System;

namespace StackRedis
{
    public static class KeyUniquifier
    {
        /// <summary>
        /// Returns true for strings formatted as by UniquePerSiteString, for the given site.
        /// 
        /// Doesn't guard against collisions
        /// </summary>
        public static bool IsPerSiteString(string key, int siteId)
        {
            return key.HasValue() && key.StartsWith(siteId + "-");
        }

        /// <summary>
        /// The converse of UniquePerSiteString
        /// </summary>
        public static string StripPerSiteString(string key)
        {
            int ignored;

            return StripPerSiteString(key, out ignored);
        }

        /// <summary>
        /// The converse of UniquePerSiteString, which also returns the site that was specified
        /// </summary>
        public static string StripPerSiteString(string key, out int siteId)
        {
            siteId = -1;

            var i = key.IndexOf('-');
            if (i == -1) return null;

            siteId = int.Parse(key.Substring(0, i));

            return key.Substring(i + 1);
        }

        /// <summary>
        /// Make a key uniquely tied to a site.
        /// 
        /// Repeatable.
        /// 
        /// Backs multi-site tenancy caching
        /// </summary>
        internal static string UniquePerSiteString(int siteId, string key)
        {
            return siteId + "-" + key;
        } 
    }
}
