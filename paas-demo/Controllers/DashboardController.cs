using Microsoft.PowerBI.Api.Beta;
using Microsoft.PowerBI.Security;
using Microsoft.Rest;
using paas_demo.Models;
using System;
using System.Configuration;
using System.Linq;
using System.Threading.Tasks;
using System.Web.Mvc;

namespace paas_demo.Controllers
{
    public class DashboardController : Controller
    {
        private readonly string workspaceCollection;
        private readonly string workspaceId;
        private readonly string accessKey;
        private readonly string apiUrl;

        public DashboardController()
        {
            this.workspaceCollection = ConfigurationManager.AppSettings["powerbi:WorkspaceCollection"];
            this.workspaceId = ConfigurationManager.AppSettings["powerbi:WorkspaceId"];
            this.accessKey = ConfigurationManager.AppSettings["powerbi:AccessKey"];
            this.apiUrl = ConfigurationManager.AppSettings["powerbi:ApiUrl"];
        }

        public ActionResult Index()
        {
            return View();
        }

        [ChildActionOnly]
        public ActionResult Reports()
        {
            var devToken = PowerBIToken.CreateDevToken(this.workspaceCollection, this.workspaceId);
            using (var client = this.CreatePowerBIClient(devToken))
            {
                var reportsResponse = client.Reports.GetReports(this.workspaceCollection, this.workspaceId);

                var viewModel = new ReportsViewModel
                {
                    Reports = reportsResponse.Value.ToList()
                };

                return PartialView(viewModel);
            }
        }

        public async Task<ActionResult> Report(string reportId)
        {
            var devToken = PowerBIToken.CreateDevToken(this.workspaceCollection, this.workspaceId);
            using (var client = this.CreatePowerBIClient(devToken))
            {
                var reportsResponse = await client.Reports.GetReportsAsync(this.workspaceCollection, this.workspaceId);
                var report = reportsResponse.Value.FirstOrDefault(r => r.Id == reportId);
                var embedToken = PowerBIToken.CreateReportEmbedToken(this.workspaceCollection, this.workspaceId, report.Id);

                var viewModel = new ReportViewModel
                {
                    Report = report,
                    AccessToken = embedToken.Generate(this.accessKey)
                };

                return View(viewModel);
            }
        }

        private IPowerBIClient CreatePowerBIClient(PowerBIToken token)
        {
            var jwt = token.Generate(accessKey);
            var credentials = new TokenCredentials(jwt, "AppToken");
            var client = new PowerBIClient(credentials)
            {
                BaseUri = new Uri(apiUrl)
            };

            return client;
        }
    }
}