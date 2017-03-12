using ApiHost.Models;
using Microsoft.PowerBI.Api.V1;
using Microsoft.PowerBI.Api.V1.Models;
using Microsoft.PowerBI.Security;
using Microsoft.Rest;
using Microsoft.Rest.Serialization;
using Microsoft.Threading;
using ProvisionSample.Models;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ProvisionSample
{
    partial class Program
    {
        static string defaultRegion = ConfigurationManager.AppSettings["defaultRegion"];
        static string version = ConfigurationManager.AppSettings["apiVersion"];
        static string armResource = ConfigurationManager.AppSettings["armResource"];
        static string apiEndpointUri = ConfigurationManager.AppSettings["powerBiApiEndpoint"];
        static string azureEndpointUri = ConfigurationManager.AppSettings["azureApiEndpoint"];
        static string defaultEmbedUrl = ConfigurationManager.AppSettings["defaultEmbedUrl"];
        static string subscriptionId = ConfigurationManager.AppSettings["subscriptionId"];
        static string resourceGroup = ConfigurationManager.AppSettings["resourceGroup"];

        static string workspaceCollectionName = ConfigurationManager.AppSettings["workspaceCollectionName"];
        static string username = ConfigurationManager.AppSettings["username"];
        static string password = ConfigurationManager.AppSettings["password"];
        static string accessKey = ConfigurationManager.AppSettings["accessKey"];
        static string workspaceId = ConfigurationManager.AppSettings["workspaceId"];

        static string datasetId = null;
        static string reportId = null;

        static string collectionLocation = defaultRegion;
        static Groups groups = new Groups();
        static Commands flatCommands = new Commands();
        static bool flatDisplay = false;
        static WorkspaceCollectionKeys accessKeys = null;
        static UserInput userInput = null;

        enum EmbedMode: int
        {
            // Start from 1 to match user input.
            View = 1,
            EditAndSave,
            ViewAndSaveAs,
            EditAndSaveAs,
            CreateMode,
        }

        [STAThread]
        static void Main(string[] args)
        {
            if (!string.IsNullOrWhiteSpace(accessKey))
            {
                accessKeys = new WorkspaceCollectionKeys
                {
                    Key1 = accessKey
                };
            }

            SetupCommands();
            userInput = new UserInput();
            AsyncPump.Run(async delegate
            {
                bool execute = true;
                while (execute)
                {
                    execute = await Run();
                }
            });
            Console.WriteLine("Enter any key to terminate: ");
            Console.ReadKey(true);
        }

        static void SetupCommands()
        {
            var commands = new Commands();
            commands.RegisterCommand("Get Workspace Collections", ListWorkspaceCollections);
            commands.RegisterCommand("Get metadata for a Workspace Collection", GetWorkspaceCollectionMetadata);
            commands.RegisterCommand("Get API keys for a Workspace Collection", ListWorkspaceCollectionApiKeys);
            commands.RegisterCommand("Provision a new Workspace Collection", ProvisionNewWorkspaceCollection);

            commands.RegisterCommand("Get Workspaces within a collection", ListWorkspacesInCollection);
            commands.RegisterCommand("Provision a new Workspace", ProvisionNewWorkspace);
            //commands.RegisterCommand("Delete Workspace by id", DeleteWorkspace);
            groups.AddGroup("Collection management", commands);

            commands = new Commands();
            commands.RegisterCommand("Get Datasets in a workspace", ListDatasetsInWorkspace);
            commands.RegisterCommand("Get Reports in a workspace", ListReportsInWorkspace);
            commands.RegisterCommand("Import PBIX Desktop file into a workspace", ImportPBIX);
            commands.RegisterCommand("Get status of PBIX import", GetImportStatus);
            commands.RegisterCommand("Delete an imported Dataset", DeleteDataset);
            commands.RegisterCommand("Update the Connection String (Cloud only)", UpdateConnetionString);
            commands.RegisterCommand("Update the Connection Credentials (Cloud only)", UpdateConnetionCredentials);
            commands.RegisterCommand("Generate embed details", GetEmbedInfo);
            commands.RegisterCommand("Clone report", CloneReport);
            commands.RegisterCommand("Rebind report to another dataset", RebindReport);
            commands.RegisterCommand("Delete report", DeleteReport);
            groups.AddGroup("Report management", commands);

            commands = new Commands();
            commands.RegisterCommand("Get billing info", GetBillingInfo);
            commands.RegisterCommand("Generate Push Json from PBI Desktop Template", GetPushFromTemplate);
            groups.AddGroup("Misc.", commands);

            commands = new Commands();
            commands.RegisterCommand("Display Settings", ShowCachedMetadata);
            commands.RegisterCommand("Manage Settings", () => ManageCachedMetadata(forceReset: false));
            commands.RegisterCommand("Clear Settings", () => ManageCachedMetadata(forceReset: true));
            commands.RegisterCommand("Toggle display mode",  ToggleFlatDisplay);
            commands.RegisterCommand("Exit", Exit);
            groups.AddGroup("Settings", commands);
            flatCommands = groups.ToFlatCommands();
        }

        static async Task<bool> Run()
        {
            Console.ResetColor();
            try
            {
                if (!flatDisplay)
                    ConsoleHelper.PrintCommands(groups);
                else
                    ConsoleHelper.PrintCommands(flatCommands);

                int? numericCommand;
                bool switchGroup;
                userInput.GetUserCommandSelection(out switchGroup, out numericCommand);
                if (numericCommand.HasValue)
                {
                    int index = numericCommand.Value - 1;
                    Func<Task> operation = null;
                    if (index >= 0)
                    {
                        operation = flatDisplay ? flatCommands .GetCommand(index) : groups.GetCommand(switchGroup, index);
                    }

                    if (operation != null)
                    {
                        await operation();
                    }
                    else
                    {
                        Console.WriteLine("Numeric value {0} does not have a valid operation", numericCommand.Value);
                    }
                }
                else
                    Console.WriteLine("Missing command");
            }
            catch (HttpOperationException ex)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine("Ooops, something broke: {0}", ex.Message);

                var error = SafeJsonConvert.DeserializeObject<PBIExceptionBody>(ex.Response.Content);
                if (error != null && error.Error != null)
                {
                    if (error.Error.Details != null && error.Error.Details.FirstOrDefault() != null)
                    {
                        Console.WriteLine(error.Error.Details.FirstOrDefault().Message);
                    }
                    else if (error.Error.Message != null)
                    {
                        Console.WriteLine(error.Error.Message);
                    }
                    else if (error.Error.Code != null)
                    {
                        Console.WriteLine(error.Error.Code);
                    }
                }

                IEnumerable<string> requestIds;
                ex.Response.Headers.TryGetValue("RequestId", out requestIds);
                if (requestIds != null && !string.IsNullOrEmpty(requestIds.FirstOrDefault()))
                {
                    Console.WriteLine("RequestId : {0}", requestIds.FirstOrDefault());
                }
                Console.WriteLine();
            }
            catch (Exception ex)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine("Ooops, something broke: {0}", ex.Message);
                Console.WriteLine();
            }
            return true;
        }

        [Flags]
        enum EnsureExtras
        {
            None = 0,
            WorkspaceCollection = 0x1,
            WorspaceId = 0x2,
            DatasetId = 0x4,
            Azure = 0x8,
            CollectionLocation = 0x10
        }

        static void EnsureBasicParams(EnsureExtras extras, EnsureExtras forceEntering = EnsureExtras.None)
        {
            if ((extras & EnsureExtras.WorkspaceCollection) == EnsureExtras.WorkspaceCollection)
            {
                var newWorkspaceCollectionName = userInput.EnsureParam(workspaceCollectionName, "Workspace Collection Name", forceReEnter: ((forceEntering & EnsureExtras.WorkspaceCollection) == EnsureExtras.WorkspaceCollection));
                if (!newWorkspaceCollectionName.Equals(workspaceCollectionName))
                {
                    accessKeys = null;
                    accessKey = null;
                }

                workspaceCollectionName = newWorkspaceCollectionName;
            }
            if ((extras & EnsureExtras.WorspaceId) == EnsureExtras.WorspaceId)
                workspaceId = userInput.EnsureParam(workspaceId, "Workspace Id", forceReEnter: ((forceEntering & EnsureExtras.WorspaceId) == EnsureExtras.WorspaceId));

            if ((extras & EnsureExtras.DatasetId) == EnsureExtras.DatasetId)
                datasetId = userInput.EnsureParam(datasetId, "Dataset Id", forceReEnter: ((forceEntering & EnsureExtras.DatasetId) == EnsureExtras.DatasetId));

            if ((extras & EnsureExtras.Azure) == EnsureExtras.Azure)
            {
                subscriptionId = userInput.EnsureParam(subscriptionId, "Azure Subscription Id", onlyFillIfEmpty: true);
                resourceGroup = userInput.EnsureParam(resourceGroup, "Azure Resource Group", onlyFillIfEmpty: true);
            }

            if ((extras & EnsureExtras.CollectionLocation) == EnsureExtras.CollectionLocation)
                collectionLocation = userInput.EnsureParam(collectionLocation, "Collection location", forceReEnter: ((forceEntering & EnsureExtras.CollectionLocation) == EnsureExtras.CollectionLocation));
        }

        static async Task ProvisionNewWorkspaceCollection()
        {
            // force new workspaceCollectionName, but if collectionLocation, set to the default, as users may be unaware of options
            workspaceCollectionName = null;
            collectionLocation = collectionLocation ?? defaultRegion;
            EnsureBasicParams(EnsureExtras.WorkspaceCollection | EnsureExtras.Azure | EnsureExtras.CollectionLocation);

            await CreateWorkspaceCollection(subscriptionId, resourceGroup, workspaceCollectionName, collectionLocation);
            accessKeys = await ListWorkspaceCollectionKeys(subscriptionId, resourceGroup, workspaceCollectionName);
            Console.ForegroundColor = ConsoleColor.Cyan;
            Console.WriteLine("Workspace collection created successfully");
        }

        static async Task ListWorkspaceCollections()
        {
            EnsureBasicParams(EnsureExtras.Azure);

            var workspaceCollections = await GetWorkspaceCollectionsList(subscriptionId, resourceGroup);
            Console.ForegroundColor = ConsoleColor.Cyan;

            foreach (WorkspaceCollection instance in workspaceCollections)
            {
                Console.WriteLine("Collection: {0}", instance.Name);
            }

            if (workspaceCollections.Any())
            {
                workspaceCollectionName = workspaceCollections.Last().Name;
            }
            else
            {
                Console.WriteLine("No Workspace Collections found");
            }
        }

        static async Task GetWorkspaceCollectionMetadata()
        {
            EnsureBasicParams(EnsureExtras.WorkspaceCollection | EnsureExtras.Azure);

            var metadata = await GetWorkspaceCollectionMetadata(subscriptionId, resourceGroup, workspaceCollectionName);
            Console.ForegroundColor = ConsoleColor.Cyan;
            Console.WriteLine(metadata);
        }

        static async Task ListWorkspaceCollectionApiKeys()
        {
            EnsureBasicParams(EnsureExtras.WorkspaceCollection | EnsureExtras.Azure);

            accessKeys = await ListWorkspaceCollectionKeys(subscriptionId, resourceGroup, workspaceCollectionName);
            Console.ForegroundColor = ConsoleColor.Cyan;
            Console.WriteLine("Key1: {0}", accessKeys.Key1);
            Console.WriteLine("===============================");
            Console.WriteLine("Key2: {0}", accessKeys.Key2);
        }

        static async Task ListWorkspacesInCollection()
        {
            EnsureBasicParams(EnsureExtras.WorkspaceCollection);

            var workspaces = await GetWorkspaces(workspaceCollectionName);
            Console.ForegroundColor = ConsoleColor.Cyan;

            foreach (var instance in workspaces)
            {
                Console.WriteLine("Collection: {0}, Id: {1}, Display Name: {2}", instance.WorkspaceCollectionName, instance.WorkspaceId, instance.DisplayName);
            }
            if (workspaces.Any())
            {
                workspaceId = workspaces.Last().WorkspaceId;
            }
            else
            {
                Console.WriteLine("No workspaces found in collection {0}", workspaceCollectionName);
            }
        }

        static async Task ProvisionNewWorkspace()
        {
            EnsureBasicParams(EnsureExtras.WorkspaceCollection);
            var workspaceName = userInput.EnsureParam(null, "Workspace Name");

            var workspace = await CreateWorkspace(workspaceCollectionName, workspaceName);
            workspaceId = workspace.WorkspaceId;
            Console.ForegroundColor = ConsoleColor.Cyan;
            Console.WriteLine("Workspace Id: {0}", workspaceId);
        }

        //static async Task DeleteWorkspace()
        //{
        //    EnsureBasicParams(EnsureExtras.WorkspaceCollection | EnsureExtras.WorspaceId, forceEntering:EnsureExtras.WorspaceId);

        //    var result = await DeleteWorkspace(workspaceCollectionName, workspaceId);
        //    Console.ForegroundColor = ConsoleColor.Cyan;
        //    Console.WriteLine("Result: {0}", result);
        //}

        static async Task ImportPBIX()
        {
            EnsureBasicParams(EnsureExtras.WorkspaceCollection | EnsureExtras.WorspaceId);
            var datasetName = userInput.EnsureParam(null, "Dataset Name");
            var filePath = userInput.EnsureParam(null, "File Path");

            var import = await ImportPbix(workspaceCollectionName, workspaceId, datasetName, filePath);
            Console.ForegroundColor = ConsoleColor.Cyan;
            Console.WriteLine("Import: {0}", import.Id);
        }

        static async Task UpdateConnetionString()
        {
            EnsureBasicParams(EnsureExtras.WorkspaceCollection | EnsureExtras.WorspaceId | EnsureExtras.DatasetId, forceEntering: EnsureExtras.DatasetId);
            string connectionString = userInput.EnsureParam(null, "Connection String", onlyFillIfEmpty: false);

            if (!string.IsNullOrWhiteSpace(connectionString))
            {
                await UpdateConnectionString(workspaceCollectionName, workspaceId, datasetId, connectionString);
                Console.ForegroundColor = ConsoleColor.Cyan;
                Console.WriteLine("Connection string information updated successfully.");
            }
            else
            {
                Console.ForegroundColor = ConsoleColor.Cyan;
                Console.WriteLine("Empty Connection string. Request ignored");
            }
        }

        static async Task UpdateConnetionCredentials()
        {
            EnsureBasicParams(EnsureExtras.WorkspaceCollection | EnsureExtras.WorspaceId | EnsureExtras.DatasetId, forceEntering: EnsureExtras.DatasetId);
            var chachedUsername = username;
            username = userInput.EnsureParam(username, "Username", onlyFillIfEmpty: false);
            if (username != chachedUsername)
            {
                password = userInput.EnsureParam(null, "Password", onlyFillIfEmpty: false, isPassword: true);
            }

            if (!string.IsNullOrWhiteSpace(username) && !string.IsNullOrWhiteSpace(password))
            {
                var executionReport = await UpdateConnectionCredentials(workspaceCollectionName, workspaceId, datasetId, username, password);
                Console.ForegroundColor = ConsoleColor.Cyan;
                Console.WriteLine(executionReport.ToString());
            }
            else
            {
                Console.ForegroundColor = ConsoleColor.Cyan;
                Console.WriteLine("Connection credentials not updated (Empty data entered).");
            }
        }

        static async Task GetEmbedInfo()
        {
            EnsureBasicParams(EnsureExtras.WorkspaceCollection | EnsureExtras.WorspaceId);
            int? index = 1;

            Console.WriteLine("Select Embed Mode:");
            Console.WriteLine("1) View Mode:            Report.Read");
            Console.WriteLine("2) Edit & Save Mode:     Report.ReadWrite");
            Console.WriteLine("3) View & Save As Mode:  Report.ReadWrite Workspace.Report.Create");
            Console.WriteLine("4) Edit & Save As Mode:  Report.Read Workspace.Report.Create");
            Console.WriteLine("5) Create Report Mode:   Dataset.Read Workspace.Report.Create");

            int? mode = userInput.EnsureIntParam((int)EmbedMode.View, "Embed mode");
            if (!mode.HasValue || mode.Value <= 0 || mode.Value > 5)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine("selected mode is out of range.");
                return;
            }

            var embedMode = (EmbedMode)mode.Value;
            string reportId = null;
            string datasetId = null;
            string embedUrl = null;

            if (embedMode == EmbedMode.View || embedMode == EmbedMode.ViewAndSaveAs || embedMode == EmbedMode.EditAndSaveAs || embedMode == EmbedMode.EditAndSave)
            {
                // For these modes user need to select a report to embed
                var reports = await GetReports(workspaceCollectionName, workspaceId);
                if (!PrintReports(reports))
                {
                    return;
                }

                index = userInput.EnsureIntParam(index, "Index of report to use");
                if (!index.HasValue || index.Value <= 0 || index.Value > reports.Count)
                {
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine("Report index is out of range.");
                    return;
                }

                var report = reports[index.Value - 1];
                reportId = report.Id;
                embedUrl = report.EmbedUrl;
            }
            else
            {
                // For these modes user need to select a dataset to create a report with
                var datasets = await GetDatasets(workspaceCollectionName, workspaceId);
                PrintDatasets(datasets);

                index = userInput.EnsureIntParam(index, "Index of dataset to create a report with");
                if (!index.HasValue || index.Value <= 0 || index.Value > datasets.Count)
                {
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine("Dataset index is out of range.");
                    return;
                }

                var dataset = datasets[index.Value - 1];
                datasetId = dataset.Id;
                embedUrl = defaultEmbedUrl;
            }

            var scopes = string.Empty;
            switch (embedMode)
            {
                case EmbedMode.View:
                    scopes = "Report.Read";
                    break;

                case EmbedMode.EditAndSave:
                    scopes = "Report.ReadWrite";
                    break;

                case EmbedMode.ViewAndSaveAs:
                    scopes = "Report.Read Workspace.Report.Create";
                    break;

                case EmbedMode.EditAndSaveAs:
                    scopes = "Report.ReadWrite Workspace.Report.Create";
                    break;

                case EmbedMode.CreateMode:
                    scopes = "Dataset.Read Workspace.Report.Create";
                    break;

                default:
                    scopes = string.Empty;
                    break;
            }

            // RLS
            var rlsUsername = userInput.EnsureParam(null, "RLS - limit to specific user: (Keep empty to create without RLS)");
            var rlsRoles = userInput.EnsureParam(null, "RLS - limit to specific roles: (comma separated)");
            var roles = string.IsNullOrEmpty(rlsRoles) ? null : rlsRoles.Split(',');

            Console.ForegroundColor = ConsoleColor.Cyan;
            Console.WriteLine("Embed Url: {0}", embedUrl);

            PowerBIToken embedToken = null;
            if (!string.IsNullOrEmpty(reportId))
            {
                embedToken = PowerBIToken.CreateReportEmbedToken(workspaceCollectionName, workspaceId, reportId, rlsUsername, roles, scopes);
            }
            else if (!string.IsNullOrEmpty(datasetId))
            {
                embedToken = PowerBIToken.CreateReportEmbedTokenForCreation(workspaceCollectionName, workspaceId, datasetId, rlsUsername, roles, scopes);
            }

            var token = embedToken.Generate(accessKeys.Key1);
            Console.WriteLine("Embed Token: {0}", token);
                        
            var copy = userInput.EnsureParam(null, "Copy embed token to clipboard? (Y)/(N) ");
            if (copy.Equals("y", StringComparison.CurrentCultureIgnoreCase))
            {
                try
                {
                    // Save access token to Clipboard
                    System.Windows.Forms.Clipboard.SetText(token);

                    Console.ForegroundColor = ConsoleColor.Green;
                    Console.WriteLine("Embed Token saved to clipboard.");
                }
                catch (Exception ex)
                {
                    Console.ForegroundColor = ConsoleColor.Yellow;
                    Console.WriteLine("Embed Token could not be saved to clipboard.");
                    Console.WriteLine(ex.ToString());
                }
            }
        }

        /// <summary>
        /// Print a list of reports to user.
        /// </summary>
        /// <param name="reports"></param>
        /// <returns>True if reports exist, and false otherwise.</returns>
        private static bool PrintReports(IList<Report> reports)
        {
            if (reports == null || !reports.Any())
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine("No report found in Workspace {0} from WorkspaceCollection {1}", workspaceId, workspaceCollectionName);
                return false;
            }

            Console.WriteLine("Existing reports:");
            for (int i = 0; i < reports.Count; i++)
            {
                ConsoleHelper.WriteColoredStringLine(string.Format("{0} report name:{1}, Id:{2}", i + 1, reports[i].Name, reports[i].Id), ConsoleColor.Green, 2);
            }

            Console.WriteLine();
            return true;
        }

        /// <summary>
        /// Print a list of datasets to user.
        /// </summary>
        /// <param name="datasets"></param>
        /// <returns>True if datasets exist, and false otherwise.</returns>
        private static bool PrintDatasets(IList<Dataset> datasets)
        {
            if (datasets == null || !datasets.Any())
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine("No datasets found in Workspace {0} from WorkspaceCollection {1}", workspaceId, workspaceCollectionName);
                return false;
            }

            Console.WriteLine("Existing datasets:");
            for (int i = 0; i < datasets.Count; i++)
            {
                ConsoleHelper.WriteColoredStringLine(string.Format("{0} dataset name:{1}, Id:{2}", i + 1, datasets[i].Name, datasets[i].Id), ConsoleColor.Green, 2);
            }

            Console.WriteLine();
            return true;
        }

        static async Task GetBillingInfo()
        {
            EnsureBasicParams(EnsureExtras.WorkspaceCollection | EnsureExtras.Azure);

            var billInfo = await GetBillingUsage(subscriptionId, resourceGroup, workspaceCollectionName);
            Console.ForegroundColor = ConsoleColor.Cyan;
            Console.WriteLine("Renders: {0}", billInfo.Renders);
        }

        static async Task GetPushFromTemplate()
        {
            Console.Write("pbit File Path:");
            var path = Console.ReadLine();
            Console.Write("Dataset Name:");
            var datasetName = Console.ReadLine();
            await JsonConvertor.Convert(datasetName, path);
        }

        static async Task ListDatasetsInWorkspace()
        {
            EnsureBasicParams(EnsureExtras.WorkspaceCollection | EnsureExtras.WorspaceId);

            var datasets = await ListDatasets(workspaceCollectionName, workspaceId);

            Console.ForegroundColor = ConsoleColor.Cyan;
            if (!datasets.Any())
            {
                Console.WriteLine("No Datasets found in this workspace");
                return;
            }

            foreach (Dataset dataset in datasets)
            {
                Console.WriteLine("Id       | {0}", dataset.Id);
                Console.WriteLine("Name     | {0}", dataset.Name);
                Console.WriteLine();
            }

            datasetId = datasets.Last().Id;
        }

        static async Task ListReportsInWorkspace()
        {
            EnsureBasicParams(EnsureExtras.WorkspaceCollection | EnsureExtras.WorspaceId);

            var reports = await ListReports(workspaceCollectionName, workspaceId);

            Console.ForegroundColor = ConsoleColor.Cyan;
            if (!reports.Any())
            {
                Console.WriteLine("No Reports found in this workspace");
                return;
            }

            foreach (var report in reports)
            {
                Console.WriteLine("Id       | {0}", report.Id);
                Console.WriteLine("Name     | {0}", report.Name);
                Console.WriteLine("EmbedUrl | {0}", report.EmbedUrl);
                Console.WriteLine();
            }

            reportId = reports.Last().Id;
        }

        static async Task DeleteDataset()
        {
            // reset datasetId to force assignment 
            EnsureBasicParams(EnsureExtras.WorkspaceCollection | EnsureExtras.WorspaceId | EnsureExtras.DatasetId, forceEntering: EnsureExtras.DatasetId);
            await DeleteDataset(workspaceCollectionName, workspaceId, datasetId);
            Console.ForegroundColor = ConsoleColor.Cyan;
            Console.WriteLine("Dataset deleted successfully.");
            datasetId = null;
        }

        private static async Task RebindReport()
        {
            EnsureBasicParams(EnsureExtras.WorkspaceCollection | EnsureExtras.WorspaceId);
            reportId = userInput.EnsureParam(reportId, "Report Id");
            datasetId = userInput.EnsureParam(datasetId, "Dataset Id to rebind to");

            var rebindReportRequest = new RebindReportRequest(datasetId);

            using (var client = await CreateClient())
            {
                await client.Reports.RebindReportAsync(
                        workspaceCollectionName,
                        workspaceId,
                        reportId,
                        rebindReportRequest);
            }
        }

        private static async Task DeleteReport()
        {
            EnsureBasicParams(EnsureExtras.WorkspaceCollection | EnsureExtras.WorspaceId);
            reportId = userInput.EnsureParam(reportId, "Report Id");

            using (var client = await CreateClient())
            {
                await client.Reports.DeleteReportAsync(
                        workspaceCollectionName,
                        workspaceId,
                        reportId);
            }

            Console.ForegroundColor = ConsoleColor.Cyan;
            Console.WriteLine("Report deleted successfully.");
            reportId = null;
        }

        private static async Task CloneReport()
        {
            EnsureBasicParams(EnsureExtras.WorkspaceCollection | EnsureExtras.WorspaceId);
            reportId = userInput.EnsureParam(reportId, "Report Id");
            var newName = userInput.EnsureParam(null, "New Report Name");
            var targetWorkspace = userInput.EnterOptionalParam("Target Workspace Id", "clone to same workspace.");
            var targetDataset = userInput.EnterOptionalParam("Target Dataset Id", "keep the original dataset");

            var cloneReportRequest = new CloneReportRequest(newName, targetWorkspace, targetDataset);

            using (var client = await CreateClient())
            {
                await client.Reports.CloneReportAsync(
                        workspaceCollectionName,
                        workspaceId,
                        reportId,
                        cloneReportRequest);
            }

            if (!string.IsNullOrEmpty(targetWorkspace))
            {
                workspaceId = targetWorkspace;
            }
        }

        static async Task GetImportStatus()
        {
            workspaceId = null;
            EnsureBasicParams(EnsureExtras.WorkspaceCollection | EnsureExtras.WorspaceId);
            var importId = userInput.EnsureParam(null, "Import Id");

            var importResult = await GetImport(workspaceCollectionName, workspaceId, importId);
            if (importResult == null)
            {
                Console.ForegroundColor = ConsoleColor.Yellow;
                Console.WriteLine("Import state of {0} is not found.", importId);
            }
            else
            {
                Console.WriteLine("Name:     {0}", importResult.Name);
                Console.WriteLine("Id:       {0}", importResult.Id);
                Console.WriteLine("State:    {0}", importResult.ImportState);
                Console.WriteLine("DataSets: {0}", importResult.Datasets.Count);
                foreach (var dataset in importResult.Datasets)
                {
                    Console.WriteLine("\t{0}: {1}", dataset.Name, dataset.WebUrl);
                }
                Console.WriteLine("Reports: {0}", importResult.Reports.Count);
                foreach (var report in importResult.Reports)
                {
                    Console.WriteLine("\t{0}: {1}", report.Name, report.WebUrl);
                }
            }
        }

        static async Task<Import> GetImport(string workspaceCollectionName, string workspaceId, string importId)
        {
            using (var client = await CreateClient())
            {
                return await client.Imports.GetImportByIdAsync(workspaceCollectionName, workspaceId, importId);
            }
        }

        static Task ShowCachedMetadata()
        {
            return Task.Run(() =>
            {
                ConsoleHelper.WriteColoredValue("Workspace Collection Name", workspaceCollectionName, ConsoleColor.Magenta, "\n");
                var usedAccessKey = (accessKeys == null) ? null : accessKeys.Key1;
                ConsoleHelper.WriteColoredValue("Workspace Collection Access Key1", usedAccessKey, ConsoleColor.Magenta, "\n");

                ConsoleHelper.WriteColoredValue("Workspace Id", workspaceId, ConsoleColor.Magenta, "\n");
                ConsoleHelper.WriteColoredValue("DatasetId", datasetId, ConsoleColor.Magenta, "\n");
                ConsoleHelper.WriteColoredValue("CollectionLocation", collectionLocation, ConsoleColor.Magenta, "\n");

                Console.WriteLine();
            });
        }

        static Task ManageCachedMetadata(bool forceReset)
        {
            return Task.Run(() =>
            {
                // ManageCachedParam may throw, to quit the management
                try
                {
                    workspaceCollectionName = userInput.ManageCachedParam(workspaceCollectionName, "Workspace Collection Name", forceReset);

                    string accessKeysKey1 = accessKeys != null ? accessKeys.Key1 : null;
                    accessKeysKey1 = userInput.ManageCachedParam(accessKeysKey1, "Workspace Collection Access Key1", forceReset);
                    if (accessKeysKey1 == null)
                    {
                        accessKeys = null;
                    }
                    else
                    {
                        accessKeys = new WorkspaceCollectionKeys
                        {
                            Key1 = accessKeysKey1
                        };
                    }

                    workspaceId = userInput.ManageCachedParam(workspaceId, "Workspace Id", forceReset);
                    datasetId = userInput.ManageCachedParam(datasetId, "Dataset Id", forceReset);
                    collectionLocation = userInput.ManageCachedParam(collectionLocation, "Collection Location", forceReset);

                    if (forceReset)
                    {
                        Console.WriteLine("Entire cache was reset\n");
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine("\n" + e.Message);
                }
            });
        }

        static Task ToggleFlatDisplay()
        {
            return Task.Run(() =>
            {
                flatDisplay = !flatDisplay;
            });
        }

        static Task Exit()
        {
            return Task.Run(() =>
            {
                Environment.Exit(0);
            });
        }

        /// <summary>
        /// Creates a new Power BI Embedded workspace collection
        /// </summary>
        /// <param name="subscriptionId">The azure subscription id</param>
        /// <param name="resourceGroup">The azure resource group</param>
        /// <param name="workspaceCollectionName">The Power BI workspace collection name to create</param>
        /// <param name="region">The Power BI region</param>
        /// <returns></returns>
        static async Task CreateWorkspaceCollection(string subscriptionId, string resourceGroup, string workspaceCollectionName, string region)
        {
            var url = string.Format("{0}/subscriptions/{1}/resourceGroups/{2}/providers/Microsoft.PowerBI/workspaceCollections/{3}{4}", azureEndpointUri, subscriptionId, resourceGroup, workspaceCollectionName, version);

            HttpClient client = CreateHttpClient();

            using (client)
            {
                var content = new StringContent(@"{
                                                ""location"": """ + region + @""",
                                                ""tags"": {},
                                                ""sku"": {
                                                    ""name"": ""S1"",
                                                    ""tier"": ""Standard""
                                                }
                                            }", Encoding.UTF8);
                content.Headers.ContentType = MediaTypeHeaderValue.Parse("application/json; charset=utf-8");

                var request = new HttpRequestMessage(HttpMethod.Put, url);
                // Set authorization header from your acquired Azure AD token
                await SetAuthorizationHeaderIfNeeded(request);

                request.Content = content;

                var response = await client.SendAsync(request);
                if (response.StatusCode != HttpStatusCode.OK)
                {
                    var responseText = await response.Content.ReadAsStringAsync();
                    var message = string.Format("Status: {0}, Reason: {1}, Message: {2}", response.StatusCode, response.ReasonPhrase, responseText);
                    throw new Exception(message);
                }

                var json = await response.Content.ReadAsStringAsync();
                return;
            }
        }

        static async Task<IEnumerable<WorkspaceCollection>> GetWorkspaceCollectionsList(string subscriptionId, string resourceGroup)
        {
            var url = string.Format("{0}/subscriptions/{1}/resourceGroups/{2}/providers/Microsoft.PowerBI/workspaceCollections{3}", azureEndpointUri, subscriptionId, resourceGroup, version);

            HttpClient client = CreateHttpClient();

            using (client)
            {
                var request = new HttpRequestMessage(HttpMethod.Get, url);
                // Set authorization header from your acquired Azure AD token
                await SetAuthorizationHeaderIfNeeded(request);

                var response = await client.SendAsync(request);

                if (response.StatusCode != HttpStatusCode.OK)
                {
                    var responseText = await response.Content.ReadAsStringAsync();
                    var message = string.Format("Status: {0}, Reason: {1}, Message: {2}", response.StatusCode, response.ReasonPhrase, responseText);
                    throw new Exception(message);
                }

                var json = await response.Content.ReadAsStringAsync();
                return SafeJsonConvert.DeserializeObject<WorkspaceCollectionList>(json).value;
            }
        }

        /// <summary>
        /// Gets the workspace collection access keys for the specified collection
        /// </summary>
        /// <param name="subscriptionId">The azure subscription id</param>
        /// <param name="resourceGroup">The azure resource group</param>
        /// <param name="workspaceCollectionName">The Power BI workspace collection name</param>
        /// <returns></returns>
        static async Task<WorkspaceCollectionKeys> ListWorkspaceCollectionKeys(string subscriptionId, string resourceGroup, string workspaceCollectionName)
        {
            var url = string.Format("{0}/subscriptions/{1}/resourceGroups/{2}/providers/Microsoft.PowerBI/workspaceCollections/{3}/listkeys{4}", azureEndpointUri, subscriptionId, resourceGroup, workspaceCollectionName, version);

            HttpClient client = CreateHttpClient();

            using (client)
            {
                var request = new HttpRequestMessage(HttpMethod.Post, url);
                // Set authorization header from your acquired Azure AD token
                await SetAuthorizationHeaderIfNeeded(request);

                request.Content = new StringContent(string.Empty);
                var response = await client.SendAsync(request);

                if (response.StatusCode != HttpStatusCode.OK)
                {
                    var responseText = await response.Content.ReadAsStringAsync();
                    var message = string.Format("Status: {0}, Reason: {1}, Message: {2}", response.StatusCode, response.ReasonPhrase, responseText);
                    throw new Exception(message);
                }

                var json = await response.Content.ReadAsStringAsync();
                return SafeJsonConvert.DeserializeObject<WorkspaceCollectionKeys>(json);
            }
        }

        /// <summary>
        /// Gets the workspace collection metadata for the specified collection
        /// </summary>
        /// <param name="subscriptionId">The azure subscription id</param>
        /// <param name="resourceGroup">The azure resource group</param>
        /// <param name="workspaceCollectionName">The Power BI workspace collection name</param>
        /// <returns></returns>
        static async Task<string> GetWorkspaceCollectionMetadata(string subscriptionId, string resourceGroup, string workspaceCollectionName)
        {
            var url = string.Format("{0}/subscriptions/{1}/resourceGroups/{2}/providers/Microsoft.PowerBI/workspaceCollections/{3}{4}", azureEndpointUri, subscriptionId, resourceGroup, workspaceCollectionName, version);
            HttpClient client = CreateHttpClient();

            using (client)
            {
                var request = new HttpRequestMessage(HttpMethod.Get, url);
                // Set authorization header from your acquired Azure AD token
                await SetAuthorizationHeaderIfNeeded(request);

                var response = await client.SendAsync(request);

                if (response.StatusCode != HttpStatusCode.OK)
                {
                    var message = await response.Content.ReadAsStringAsync();
                    throw new Exception(message);
                }

                var json = await response.Content.ReadAsStringAsync();
                return json;
            }
        }

        static async Task<BillingUsage> GetBillingUsage(string subscriptionId, string resourceGroup, string workspaceCollectionName)
        {
            var url = string.Format("{0}/subscriptions/{1}/resourceGroups/{2}/providers/Microsoft.PowerBI/workspaceCollections/{3}/billingUsage{4}", azureEndpointUri, subscriptionId, resourceGroup, workspaceCollectionName, version);

            HttpClient client = CreateHttpClient();

            using (client)
            {
                var request = new HttpRequestMessage(HttpMethod.Post, url);
                // Set authorization header from your acquired Azure AD token
                await SetAuthorizationHeaderIfNeeded(request);

                request.Content = new StringContent(string.Empty);
                var response = await client.SendAsync(request);

                if (response.StatusCode != HttpStatusCode.OK)
                {
                    var responseText = await response.Content.ReadAsStringAsync();
                    var message = string.Format("Status: {0}, Reason: {1}, Message: {2}", response.StatusCode, response.ReasonPhrase, responseText);
                    throw new Exception(message);
                }

                var json = await response.Content.ReadAsStringAsync();
                return SafeJsonConvert.DeserializeObject<BillingUsage>(json);
            }
        }

        /// <summary>
        /// Creates a new Power BI Embedded workspace within the specified collection
        /// </summary>
        /// <param name="workspaceCollectionName">The Power BI workspace collection name</param>
        /// <returns></returns>
        static async Task<Workspace> CreateWorkspace(string workspaceCollectionName, string workspaceName)
        {
            CreateWorkspaceRequest request = null;
            if (!string.IsNullOrEmpty(workspaceName))
            {
                request = new CreateWorkspaceRequest(workspaceName);
            }

            using (var client = await CreateClient())
            {
                // Create a new workspace witin the specified collection
                return await client.Workspaces.PostWorkspaceAsync(workspaceCollectionName, request);
            }
        }

        /// <summary>
        /// Deletes Power BI Embedded workspace within the specified collection
        /// </summary>
        /// <param name="workspaceCollectionName">The Power BI workspace collection name</param>
        /// <returns></returns>
        //static async Task<Workspace> DeleteWorkspace(string workspaceCollectionName, string workspaceid)
        //{
        //    using (var client = await CreateClient())
        //    {
        //        // Delete a workspace witin the specified collection
        //        return await client.Workspaces.DeleteWorkspaceAsync(workspaceCollectionName, workspaceid);
        //    }
        //}

        /// <summary>
        /// Gets a list of Power BI Embedded workspaces within the specified collection
        /// </summary>
        /// <param name="workspaceCollectionName">The Power BI workspace collection name</param>
        /// <returns></returns>
        static async Task<IEnumerable<Workspace>> GetWorkspaces(string workspaceCollectionName)
        {
            using (var client = await CreateClient())
            {
                var response = await client.Workspaces.GetWorkspacesByCollectionNameAsync(workspaceCollectionName);
                return response.Value;
            }
        }

        /// <summary>
        /// Imports a Power BI Desktop file (pbix) into the Power BI Embedded service
        /// </summary>
        /// <param name="workspaceCollectionName">The Power BI workspace collection name</param>
        /// <param name="workspaceId">The target Power BI workspace id</param>
        /// <param name="datasetName">The dataset name to apply to the uploaded dataset</param>
        /// <param name="filePath">A local file path on your computer</param>
        /// <returns></returns>
        static async Task<Import> ImportPbix(string workspaceCollectionName, string workspaceId, string datasetName, string filePath)
        {
            using (var fileStream = File.OpenRead(filePath.Trim('"')))
            {
                using (var client = await CreateClient())
                {
                    // Set request timeout to support uploading large PBIX files
                    client.HttpClient.Timeout = TimeSpan.FromMinutes(60);
                    client.HttpClient.DefaultRequestHeaders.Add("ActivityId", Guid.NewGuid().ToString());

                    // Import PBIX file from the file stream
                    var import = await client.Imports.PostImportWithFileAsync(workspaceCollectionName, workspaceId, fileStream, datasetName);

                    // Example of polling the import to check when the import has succeeded.
                    while (import.ImportState != "Succeeded" && import.ImportState != "Failed")
                    {
                        import = await client.Imports.GetImportByIdAsync(workspaceCollectionName, workspaceId, import.Id);
                        Console.WriteLine("Checking import state... {0}", import.ImportState);
                        Thread.Sleep(1000);
                    }

                    return import;
                }
            }
        }

        /// <summary>
        /// Lists the datasets that are published in a given workspace.
        /// </summary>
        /// <param name="workspaceCollectionName">The Power BI workspace collection name</param>
        /// <param name="workspaceId">The target Power BI workspace id</param>
        /// <returns></returns>
        static async Task<IList<Dataset>> ListDatasets(string workspaceCollectionName, string workspaceId)
        {
            using (var client = await CreateClient())
            {
                var response = await client.Datasets.GetDatasetsAsync(workspaceCollectionName, workspaceId);
                return response.Value;
            }
        }

        /// <summary>
        /// Lists the reports that are published in a given workspace.
        /// </summary>
        /// <param name="workspaceCollectionName">The Power BI workspace collection name</param>
        /// <param name="workspaceId">The target Power BI workspace id</param>
        /// <returns></returns>
        static async Task<IList<Report>> ListReports(string workspaceCollectionName, string workspaceId)
        {
            using (var client = await CreateClient())
            {
                var response = await client.Reports.GetReportsAsync(workspaceCollectionName, workspaceId);
                return response.Value;
            }
        }

        /// <summary>
        /// Removes a published dataset from a given workspace.
        /// </summary>
        /// <param name="workspaceCollectionName">The Power BI workspace collection name</param>
        /// <param name="workspaceId">The target Power BI workspace id</param>
        /// <param name="datasetId">The Power BI dataset to delete</param>
        /// <returns></returns>
        static async Task DeleteDataset(string workspaceCollectionName, string workspaceId, string datasetId)
        {
            using (var client = await CreateClient())
            {
                await client.Datasets.DeleteDatasetByIdAsync(workspaceCollectionName, workspaceId, datasetId);

            }
        }

        /// <summary>
        /// Updates the Power BI dataset connection info for datasets with direct query connections
        /// </summary>
        /// <param name="workspaceCollectionName">The Power BI workspace collection name</param>
        /// <param name="workspaceId">The Power BI workspace id that contains the dataset</param>
        /// <param name="datasetId">The Power BI dataset to update connection for</param>
        /// <returns></returns>
        static async Task UpdateConnectionString(string workspaceCollectionName, string workspaceId, string datasetId, string connectionString)
        {
            using (var client = await CreateClient())
            {
                var connectionParameters = new Dictionary<string, object>
                {
                    { "connectionString", connectionString }
                };

                await client.Datasets.SetAllConnectionsAsync(workspaceCollectionName, workspaceId, datasetId, connectionParameters);
            }
        }

        /// <summary>
        /// Updates the Power BI dataset connection credentials for datasets with direct query connections
        /// </summary>
        /// <param name="workspaceCollectionName">The Power BI workspace collection name</param>
        /// <param name="workspaceId">The Power BI workspace id that contains the dataset</param>
        /// <param name="datasetId">The Power BI dataset to update connection for</param>
        /// <returns></returns>
        static async Task<ExecutionReport> UpdateConnectionCredentials(string workspaceCollectionName, string workspaceId, string datasetId, string username, string password)
        {
            using (var client = await CreateClient())
            {
                // Get the datasources from the dataset
                var datasources = await client.Datasets.GetGatewayDatasourcesAsync(workspaceCollectionName, workspaceId, datasetId);

                // Reset your connection credentials
                var delta = new GatewayDatasource
                {
                    CredentialType = "Basic",
                    BasicCredentials = new BasicCredentials
                    {
                        Username = username,
                        Password = password
                    }
                };

                ExecutionReport report = null;
                switch (datasources.Value.Count)
                {
                    case 0: return new ExecutionReport(ExecutionLevel.Error, "No datasources exist to update");
                    case 1:
                        report = new ExecutionReport(ExecutionLevel.OK, "Connection credentials updated successfully.");
                        break;
                    default:
                        report = new ExecutionReport(ExecutionLevel.Warning, string.Format("Expected one datasource, but {0} exist, Connection credentials updated for the first", datasources.Value.Count));
                        break;
                }

                // Update the datasource with the specified credentials
                await client.Gateways.PatchDatasourceAsync(workspaceCollectionName, workspaceId, datasources.Value[0].GatewayId, datasources.Value[0].Id, delta);
                return report;
            }
        }

        /// <summary>
        /// Get the single report out of the list of existing ones
        /// </summary>
        /// <param name="workspaceCollectionName"></param>
        /// <param name="workspaceId"></param>
        /// <param name="index">which, 0..n for specific, -1 for last if n > number of reports then last</param>
        /// <returns></returns>
        static async Task<Report> GetReport(string workspaceCollectionName, string workspaceId, int index = -1)
        {
            using (var client = await CreateClient())
            {
                var reports = await client.Reports.GetReportsAsync(workspaceCollectionName, workspaceId);
                if (reports.Value.Any())
                {
                    index = ((index < 0) || ((reports.Value.Count - 1) < index)) ? reports.Value.Count - 1 : index;
                    return reports.Value[index];
                }
            }
            return null;
        }

        static async Task<IList<Report>> GetReports(string workspaceCollectionName, string workspaceId)
        {
            using (var client = await CreateClient())
            {
                var reports = await client.Reports.GetReportsAsync(workspaceCollectionName, workspaceId);
                return reports.Value;
            }
        }

        static async Task<IList<Dataset>> GetDatasets(string workspaceCollectionName, string workspaceId)
        {
            using (var client = await CreateClient())
            {
                var datasets = await client.Datasets.GetDatasetsAsync(workspaceCollectionName, workspaceId);
                return datasets.Value;
            }
        }
    }
}
