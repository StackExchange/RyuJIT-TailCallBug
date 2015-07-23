using CorruptionRepro;
using Microsoft.Owin;
using Owin;

[assembly: OwinStartup(typeof(Startup))]
namespace CorruptionRepro
{
    public partial class Startup
    {
        public void Configuration(IAppBuilder app)
        {
        }
    }
}
