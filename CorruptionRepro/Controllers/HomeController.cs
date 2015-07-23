using System;
using System.Collections.Generic;
using System.Web.Mvc;

namespace CorruptionRepro.Controllers
{
    public class HomeController : Controller
    {
        private static readonly Random Gen = new Random();

        public ActionResult Index()
        {
            var results = new List<TestInfo> { TestFetch() };
            for (var i = 0; i < 50; i++)
            {
                results.Add(TestFetch());
            }
            return View(results);
        }

        private TestInfo TestFetch()
        {
            var durationSeconds = Gen.Next(10, 60*60); //  60 *60;
            var guid = Guid.NewGuid();
            var key = "guid-test";
            var cache = Current.SiteCache;


            var testResult = Current.CurrentTest =  new TestInfo
            {
                PassedDurationSeconds = durationSeconds,
                Inserted = guid
            };

            cache.Set<Guid>(key, guid, durationSeconds: durationSeconds);

            testResult.Fetched = cache.Get<Guid?>(key);

            return testResult;
        }

        public class TestInfo
        {
            public int PassedDurationSeconds { get; set; }
            public Guid Inserted { get; set; }
            public Guid? Fetched { get; set; }


            public int? LocalCacheSetDuration { get; set; }
            public int? LocalCacheSetWithPriorityDuration { get; set; }
            public int? RawSetDuration { get; set; }

            public bool Passed => Inserted == Fetched && PassedDurationSeconds == RawSetDuration;
        }
    }
}