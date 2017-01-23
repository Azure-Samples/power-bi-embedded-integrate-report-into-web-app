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
using ApiHost.Models;
using Microsoft.PowerBI.Api.V1;
using Microsoft.PowerBI.Api.V1.Models;
using Microsoft.PowerBI.Security;
using Microsoft.Rest;
using Microsoft.Rest.Serialization;
using Microsoft.Threading;
using ProvisionSample.Models;

namespace ProvisionSample 
{
    partial class Program
    {
        const string version = "?api-version=2016-01-29";
        const string armResource = "https://management.core.windows.net/";
        const string defaultRegion = "southcentralus";

        static string apiEndpointUri = ConfigurationManager.AppSettings["powerBiApiEndpoint"];
        static string azureEndpointUri = ConfigurationManager.AppSettings["azureApiEndpoint"];
        static string subscriptionId = ConfigurationManager.AppSettings["subscriptionId"];
        static string resourceGroup = ConfigurationManager.AppSettings["resourceGroup"];
        static string workspaceCollectionName = ConfigurationManager.AppSettings["workspaceCollectionName"];
        static string username = ConfigurationManager.AppSettings["username"];
        static string password = ConfigurationManager.AppSettings["password"];
        static string accessKey = ConfigurationManager.AppSettings["accessKey"];
        static string workspaceId = ConfigurationManager.AppSettings["workspaceId"];
        static string datasetId = ConfigurationManager.AppSettings["datasetId"];
        static string collectionLocation = defaultRegion;

        static WorkspaceCollectionKeys accessKeys = null;

        static Commands commands = new Commands();
        static UserInput userInput = null;

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
            commands.RegisterCommand("Get Workspace Collections", ListWorkspaceCollections);
            commands.RegisterCommand("Get metadata for a Workspace Collection", GetWorkspaceCollectionMetadata);
            commands.RegisterCommand("Get API keys for a Workspace Collection", ListWorkspaceCollectionApiKeys);
            commands.RegisterCommand("Provision a new Workspace Collection", ProvisionNewWorkspaceCollection);

            commands.RegisterCommand("Get Workspaces within a collection", ListWorkspacesInCollection);
            commands.RegisterCommand("Provision a new Workspace", ProvisionNewWorkspace);

            commands.RegisterCommand("Get Datasets in a workspace", ListDatasetInWorkspace);
            commands.RegisterCommand("Import PBIX Desktop file into a workspace", ImportPBIX);
            commands.RegisterCommand("Get status of PBIX import", GetImportStatus);
            commands.RegisterCommand("Delete an imported Dataset", DeleteDataset);
            commands.RegisterCommand("Update a Connection String for a dataset (Cloud only)", UpdateConnetionString);

            commands.RegisterCommand("Get embed url and token for existing report", GetEmbedInfo);
            commands.RegisterCommand("Get billing info", GetBillingInfo);

        }

        static async Task<bool> Run()
        {
            Console.ResetColor();
            AdminCommands? adminCommand = null;
            try
            {
                ConsoleHelper.PrintCommands(commands);

                int? numericCommand;
                userInput.GetUserCommandSelection(out adminCommand, out numericCommand);
                if (adminCommand.HasValue)
                {
                    switch (adminCommand.Value)
                    {
                        case AdminCommands.ExitTool: return false;
                        case AdminCommands.ClearSettings: ManageCachedMetadata(forceReset: true); break;
                        case AdminCommands.ManageSettings: ManageCachedMetadata(forceReset: false); break;
                        case AdminCommands.DisplaySettings: ShowCachedMetadata(); break;
                    }
                }
                else if (numericCommand.HasValue)
                {
                    int index = numericCommand.Value - 1;
                    Func<Task> operation = null;
                    if (index >= 0)
                    {
                        operation = commands.GetCommand(index);
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
                    Console.WriteLine("Missing admin or numeric operations");
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
            return (!adminCommand.HasValue || adminCommand.Value != AdminCommands.ExitTool);
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

        static async Task ListDatasetInWorkspace()
        {
            EnsureBasicParams(EnsureExtras.WorkspaceCollection | EnsureExtras.WorspaceId);
            var datasets = await ListDatasets(workspaceCollectionName, workspaceId);
            Console.ForegroundColor = ConsoleColor.Cyan;
            if (datasets.Any())
            {
                foreach (Dataset d in datasets)
                {
                    Console.WriteLine("{0}:  {1}", d.Name, d.Id);
                }
                datasetId = datasets.Last().Id;
            }
            else
            {
                Console.WriteLine("No Datasets found in this workspace");
            }
        }

        static async Task ImportPBIX()
        {
            EnsureBasicParams(EnsureExtras.WorkspaceCollection | EnsureExtras.WorspaceId);
            var datasetName = userInput.EnsureParam(null, "Dataset Name");
            var filePath = userInput.EnsureParam(null, "File Path");

            var import = await ImportPbix(workspaceCollectionName, workspaceId, datasetName, filePath);
            Console.ForegroundColor = ConsoleColor.Cyan;
            Console.WriteLine("Import: {0}", import.Id);
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

        static async Task DeleteDataset()
        {
            // reset datasetId to force assignment 
            EnsureBasicParams(EnsureExtras.WorkspaceCollection | EnsureExtras.WorspaceId | EnsureExtras.DatasetId, forceEntering: EnsureExtras.DatasetId);
            await DeleteDataset(workspaceCollectionName, workspaceId, datasetId);
            Console.ForegroundColor = ConsoleColor.Cyan;
            Console.WriteLine("Dataset deleted successfully.");
            datasetId = null;
        }

        static async Task UpdateConnetionString()
        {
            EnsureBasicParams(EnsureExtras.WorkspaceCollection | EnsureExtras.WorspaceId | EnsureExtras.DatasetId, forceEntering: EnsureExtras.DatasetId);

            await UpdateConnection(workspaceCollectionName, workspaceId, datasetId);
            Console.ForegroundColor = ConsoleColor.Cyan;
            Console.WriteLine("Connection information updated successfully.");
        }

        static async Task GetEmbedInfo()
        {
            EnsureBasicParams(EnsureExtras.WorkspaceCollection | EnsureExtras.WorspaceId);
            int? index = -1;

            IList<Report> reports = await GetReports(workspaceCollectionName, workspaceId);
            if (reports == null || !reports.Any())
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine("No report found in Workspace {0} from WorkspaceCollection {1}", workspaceId, workspaceCollectionName);
                return;
            }

            Console.WriteLine("Existing reports:");
            for (int i = 0; i < reports.Count; i++)
                ConsoleHelper.WriteColoredStringLine(string.Format("{0} report name:{1}, Id:{2}", i + 1, reports[i].Name, reports[i].Id), ConsoleColor.Green, 2);
            Console.WriteLine();

            index = userInput.EnsureIntParam(index, "Index of report to use (-1 for last in list)");
            if (!index.HasValue)
            {
                index = -1;
            }
            if (!index.HasValue || index.Value <= 0 || index.Value > reports.Count)
                index = reports.Count;

            Report report = reports[index.Value - 1];
            var embedToken = PowerBIToken.CreateReportEmbedToken(workspaceCollectionName, workspaceId, report.Id);
            Console.ForegroundColor = ConsoleColor.Cyan;
            Console.WriteLine("Embed Url: {0}", report.EmbedUrl);
            Console.WriteLine("Embed Token: {0}", embedToken.Generate(accessKeys.Key1));
            var embedToken2 = PowerBIToken.CreateReportEmbedToken(workspaceCollectionName, workspaceId, report.Id);
            embedToken2.Claims.Add(new System.Security.Claims.Claim("type", "embed"));
            Console.WriteLine("Fixed Token: {0}", embedToken2.Generate(accessKeys.Key1));
        }

        static async Task GetBillingInfo()
        {
            EnsureBasicParams(EnsureExtras.WorkspaceCollection | EnsureExtras.Azure);

            var billInfo = await GetBillingUsage(subscriptionId, resourceGroup, workspaceCollectionName);
            Console.ForegroundColor = ConsoleColor.Cyan;
            Console.WriteLine("Renders: {0}", billInfo.Renders);
        }

        static void ShowCachedMetadata()
        {
            ConsoleHelper.WriteColoredValue("Workspace Collection Name", workspaceCollectionName, ConsoleColor.Magenta, "\n");
            var usedAccessKey = (accessKeys == null) ? null : accessKeys.Key1;
            ConsoleHelper.WriteColoredValue("Workspace Collection Access Key1", usedAccessKey, ConsoleColor.Magenta, "\n");

            ConsoleHelper.WriteColoredValue("Workspace Id", workspaceId, ConsoleColor.Magenta, "\n");
            ConsoleHelper.WriteColoredValue("DatasetId", datasetId, ConsoleColor.Magenta, "\n");
            ConsoleHelper.WriteColoredValue("CollectionLocation", collectionLocation, ConsoleColor.Magenta, "\n");

            Console.WriteLine();
        }

        static void ManageCachedMetadata(bool forceReset)
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
        }

        static async Task<Import> GetImport(string workspaceCollectionName, string workspaceId, string importId)
        {
            using (var client = await CreateClient())
            {
                return await client.Imports.GetImportByIdAsync(workspaceCollectionName, workspaceId, importId);
            }
        }

        /// <summary>
        /// Creates a new Power BI Embedded workspace collection
        /// </summary>
        /// <param name="subscriptionId">The azure subscription id</param>
        /// <param name="resourceGroup">The azure resource group</param>
        /// <param name="workspaceCollectionName">The Power BI workspace collection name to create</param>
        ///         /// <param name="region">The Power BI region</param>
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
                // Set authorization header from you acquired Azure AD token
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
                // Set authorization header from you acquired Azure AD token
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
                // Set authorization header from you acquired Azure AD token
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
        /// Lists the datasets that are published to a given workspace.
        /// </summary>
        /// <param name="workspaceCollectionName">The Power BI workspace collection name</param>
        /// <param name="workspaceId">The target Power BI workspace id</param>
        /// <returns></returns>
        static async Task<IList<Dataset>> ListDatasets(string workspaceCollectionName, string workspaceId)
        {
            using (var client = await CreateClient())
            {
                ODataResponseListDataset response = await client.Datasets.GetDatasetsAsync(workspaceCollectionName, workspaceId);
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
        /// <param name="id"></param>
        /// <returns></returns>
        static async Task UpdateConnection(string workspaceCollectionName, string workspaceId, string datasetId)
        {
            var chachedUsername = username;
            username = userInput.EnsureParam(username, "Username", onlyFillIfEmpty: false);
            if (username != chachedUsername)
            {
                password = userInput.EnsureParam(null, "Password", onlyFillIfEmpty: false, isPassword: true);
            }

            string connectionString = userInput.EnterOptionalParam("Connection String", "leave empty");

            using (var client = await CreateClient())
            {
                // Optionally udpate the connectionstring details if preent
                if (!string.IsNullOrWhiteSpace(connectionString))
                {
                    var connectionParameters = new Dictionary<string, object>
                    {
                        { "connectionString", connectionString }
                    };
                    await client.Datasets.SetAllConnectionsAsync(workspaceCollectionName, workspaceId, datasetId, connectionParameters);
                }

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

                if (datasources.Value.Count != 1)
                {
                    Console.Write("Expected one datasource, updating the first");
                }

                // Update the datasource with the specified credentials
                await client.Gateways.PatchDatasourceAsync(workspaceCollectionName, workspaceId, datasources.Value[0].GatewayId, datasources.Value[0].Id, delta);
            }
        }

        static async Task<IList<Report>> GetReports(string workspaceCollectionName, string workspaceId)
        {
            using (var client = await CreateClient())
            {
                var reports = await client.Reports.GetReportsAsync(workspaceCollectionName, workspaceId);
                return reports.Value;
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
    }
}
