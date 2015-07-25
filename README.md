This repository is for demonstrating a .Net 4.6 tail call issue in RyuJIT when running on x64. We initially noticed the issue with MiniProfiler in Stack Overflow when testing locally (a Visual Studio 2015/.Net 4.6 environment for all developers). The issue does not present on our production tier (which is still .Net 4.5.2, though it is built with Roslyn 1.0.0-rc2 compilers).

Fixes
----
Let's start off with ways to fix this up front. There are a few options.

 - If you haven't installed .Net 4.6, don't. Hold off until this serious issue is resolved.
 - If you have or must install .Net 4.6, there are 2 workaround options:
  - Disable RyuJIT, via a registry key:  
    `Set-ItemProperty -Path HKLM:\Software\Microsoft\.NETFramework -Name useLegacyJit -Type DWord -Value 1`
  - Disable RyuJIT, via an environmental variable:  
    `COMPLUS_useLegacyJit=1`



Reproducing the issue
----

Requirements
----
- **This must be run in IIS**, we cannot reproduce the bug while in visual studio. Though attaching a debugger is feasible, using the build-in VS hosting does not reproduce.  
- **StackRedis performance optimizations must be enabled**, they are by default when you pull. The `DEBUG` solution builds a `RELEASE` StackRedis with optimizations.

Problem Description
---
While there is a lot of code here to repro, the actual problem area is narrowly constrained. Here's the affected area in this repro case:

    public void Set<T>(string key, T value, int? durationSecs, bool sliding, bool broadcastRemoveFromCache = false)
    {
        LocalCache.OnLogDuration(key, durationSecs, "LocalCache.Set");
        SetWithPriority<T>(key, value, durationSecs, sliding, System.Web.Caching.CacheItemPriority.Default);
    }
    
    public void SetWithPriority<T>(string key, T value, int? durationSecs, bool isSliding, System.Web.Caching.CacheItemPriority priority)
    {
        LocalCache.OnLogDuration(key, durationSecs, "LocalCache.SetWithPriority");
        key = KeyInContext(key);

        RawSet(key, value, durationSecs, isSliding, priority);
    }
    
    private void RawSet(string cacheKey, object value, int? durationSecs, bool isSliding, System.Web.Caching.CacheItemPriority priority)
    {
        LocalCache.OnLogDuration(cacheKey, durationSecs, "RawSet");
        var absolute = !isSliding && durationSecs.HasValue ? DateTime.UtcNow.AddSeconds(durationSecs.Value) : Cache.NoAbsoluteExpiration;
        var sliding = isSliding && durationSecs.HasValue ? TimeSpan.FromSeconds(durationSecs.Value) : Cache.NoSlidingExpiration;

        HttpRuntime.Cache.Insert(cacheKey, value, null, absolute, sliding, priority, Removed);
        var evt = Added;
        if(evt != null) evt(cacheKey, value, absolute, sliding, priority, durationSecs, isSliding);

The `LocalCache.OnLogDuration` event handlers (added for this test) are logging the value of `durationSeconds` as it is passed down the stack. Each of these 3 handlers corresponds to the same-name column in the output of the homepage:

[![Results Window](http://i.stack.imgur.com/PMZ0G.png)](http://i.stack.imgur.com/PMZ0G.png)

The bug manifests at the last method call here. While `SetWithPriority<T>` has the correct value for `durationSeconds`, `RawSet` does not. Its value is...well, we don't know where it's coming from.

For example, what you would expect in a call stack is this:

 - `cache.Set<Guid>(key, guid, durationSeconds: 60);`
 - `SetWithPriority<T>(key, value, 60, false, CacheItemPriority.Default);`
 - `RawSet(key, value, 60, false, CacheItemPriority.Default);`

What actually happens is the last method call does not get the `60` (or any matching value) passed in from above. Instead it gets *some other value* as seen in the screenshot above. It appears to be a consistent shared value per test run, but we cannot readily explain what's getting substituted in here.

Additional Notes
---
We can reproduce this on the following platforms:
 - Windows 8.1 with VS 2015 RTM, .Net 4.6, IIS 8.5
 - Windows 10 with VS 2015 RTM, .Net 4.6, IIS 10
