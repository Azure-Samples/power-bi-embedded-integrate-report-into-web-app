using Microsoft.Owin;
using Owin;

[assembly: OwinStartupAttribute(typeof(paas_demo.Startup))]
namespace paas_demo
{
    public partial class Startup
    {
        public void Configuration(IAppBuilder app)
        {
        }
    }
}
