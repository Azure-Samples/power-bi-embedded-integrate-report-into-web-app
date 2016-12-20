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
using Microsoft.IdentityModel.Clients.ActiveDirectory;
using Microsoft.PowerBI.Api.V1;
using Microsoft.PowerBI.Api.V1.Models;
using Microsoft.Rest;
using Microsoft.Rest.Serialization;
using Microsoft.Threading;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace ProvisionSample
{
    class Program
    {
        const string version = "?api-version=2016-01-29";
        const string armResource = "https://management.core.windows.net/";
        static string clientId = "ea0616ba-638b-4df5-95b9-636659ae5121";
        static Uri redirectUri = new Uri("urn:ietf:wg:oauth:2.0:oob");

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
        static Commands commands = new Commands();
        static WorkspaceCollectionKeys accessKeys = null;
        static GatewayPublicKey gatewayPublicKey = null;
        static string gatewayId = null;
        static string datasourceId = null;
        static string azureToken = null;
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
            commands.RegisterCommand("Reserved for future use...", null);
            commands.RegisterCommand("Provision a new workspace collection", ProvisionNewWorkspaceCollection);
            commands.RegisterCommand("Get list of workspace collections", ListWorkspaceCollections);
            commands.RegisterCommand("Get workspace collection metadata", GetWorkspaceCollectionMetadata);
            commands.RegisterCommand("Get a workspace collection's API keys", ListWorkspaceCollectionApiKeys);
            commands.RegisterCommand("Get list of workspaces within a collection", ListWorkspacesInCollection);
            commands.RegisterCommand("Provision a new workspace in an existing workspace collection", ProvisionNewWorkspace);
            commands.RegisterCommand("Import PBIX Desktop file into an existing workspace", ImportPBIX);
            commands.RegisterCommand("Update connection string info for an existing dataset", UpdateConnetionString);
            ////commands.RegisterCommand("Get embed url and token for existing report", GetEmbedInfo);
            commands.RegisterCommand("Get a list of Datasets published to a workspace", ListDatasetInWorkspace);
            commands.RegisterCommand("Delete a published dataset from a workspace", DeleteDataset);
            commands.RegisterCommand("Get status of import", GetImportStatus);
            commands.RegisterCommand("Get Gateways for workspace collection", ListGatewaysForWorkspaceCollection);
            commands.RegisterCommand("Get Gateways for workspace", ListGatewaysForWorkspace);
            commands.RegisterCommand("Get Gateway metadata", GetGatewayMetadata);
            commands.RegisterCommand("Delete Gateway by id", DeleteGateway);
            commands.RegisterCommand("Create a new Datasource", CreateDatasource);
            commands.RegisterCommand("Get Datasources for gateway", ListDatasources);
            commands.RegisterCommand("Get Datasource by id", GetDatasourceById);
            commands.RegisterCommand("Delete Datasource by id", DeleteDatasource);
            commands.RegisterCommand("Bind Dataset to Gateway", BindDataset);
            commands.RegisterCommand("Update Datasource", UpdateDatasource);
        }

        static async Task<bool> Run()
        {
            Console.ResetColor();
            int? numericCommand = null;
            AdminCommands? adminCommand = null;
            try
            {
                ConsoleHelper.PrintCommands(commands);

                userInput.GetUserCommandSelection(out adminCommand, out numericCommand);
                if (adminCommand.HasValue)
                {
                    switch (adminCommand.Value)
                    {
                        case AdminCommands.Exit: return false;
                        case AdminCommands.ClearCache: ResetCachedMetadata(forceReset: true); break;
                        case AdminCommands.ManageCache: ResetCachedMetadata(forceReset: false); break;
                        case AdminCommands.ShowCache: ShowCachedMetadata(); break;
                    }
                }
                else if (numericCommand.HasValue)
                {
                    Func<Task> operation = null;
                    if (numericCommand.Value >= 0)
                    {
                        operation = commands.GetCommand(numericCommand.Value);
                    }

                    if (operation != null)
                    {
                        await operation();
                    }
                    else
                    {
                        Console.WriteLine("Numeric value {0} does not have a valid operation", numericCommand);
                    }
                }
                else
                    Console.WriteLine("Missing admin or numeric operations");
            }
            catch (Exception ex)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine("Ooops, something broke: {0}", ex.Message);
                Console.WriteLine();
            }
            return (!adminCommand.HasValue || adminCommand.Value != AdminCommands.Exit);
        }

        [Flags]
        enum EnsureExtras
        {
            None = 0,
            WorkspaceCollection = 0x1,
            GatewayId = 0x2,
            WorspaceId = 0x4,
            DatasetId = 0x8,
            DatasourceId = 0x10,

        }

        static void EnsureBasicParams(EnsureExtras extras, EnsureExtras reEnter = EnsureExtras.None)
        {
            subscriptionId = userInput.EnsureParam(subscriptionId, "Azure Subscription Id", onlyFillIfEmpty: true);
            resourceGroup = userInput.EnsureParam(resourceGroup, "Azure Resource Group", onlyFillIfEmpty: true);

            if ((extras & EnsureExtras.WorkspaceCollection) == EnsureExtras.WorkspaceCollection)
                workspaceCollectionName = userInput.EnsureParam(workspaceCollectionName, "Workspace Collection Name", forceReEnter: ((reEnter & EnsureExtras.WorkspaceCollection) == EnsureExtras.WorkspaceCollection));

            if ((extras & EnsureExtras.WorspaceId) == EnsureExtras.WorspaceId)
                workspaceId = userInput.EnsureParam(workspaceId, "Workspace Id", forceReEnter: ((reEnter & EnsureExtras.WorspaceId) == EnsureExtras.WorspaceId));

            if ((extras & EnsureExtras.DatasetId) == EnsureExtras.DatasetId)
                datasetId = userInput.EnsureParam(datasetId, "Dataset Id", forceReEnter: ((reEnter & EnsureExtras.DatasetId) == EnsureExtras.DatasetId));

            if ((extras & EnsureExtras.GatewayId) == EnsureExtras.GatewayId)
                gatewayId = userInput.EnsureParam(gatewayId, "Gateway Id", forceReEnter: ((reEnter & EnsureExtras.GatewayId) == EnsureExtras.GatewayId));

            if ((extras & EnsureExtras.DatasourceId) == EnsureExtras.DatasourceId)
                datasourceId = userInput.EnsureParam(datasourceId, "Datasource Id", forceReEnter: ((reEnter & EnsureExtras.DatasourceId) == EnsureExtras.DatasourceId));
        }

        static async Task ProvisionNewWorkspaceCollection()
        {
            workspaceCollectionName = null;
            EnsureBasicParams(EnsureExtras.WorkspaceCollection);

            await CreateWorkspaceCollection(subscriptionId, resourceGroup, workspaceCollectionName);
            accessKeys = await ListWorkspaceCollectionKeys(subscriptionId, resourceGroup, workspaceCollectionName);
            Console.ForegroundColor = ConsoleColor.Cyan;
            Console.WriteLine("Workspace collection created successfully");
        }

        static async Task ListWorkspaceCollections()
        {

            EnsureBasicParams(EnsureExtras.None);

            var workspaceCollections = await GetWorkspaceCollectionsList(subscriptionId, resourceGroup);
            Console.ForegroundColor = ConsoleColor.Cyan;

            foreach (WorkspaceCollection instance in workspaceCollections)
            {
                Console.WriteLine("Collection: {0}", instance.Name);
            }
        }

        static async Task GetWorkspaceCollectionMetadata()
        {
            EnsureBasicParams(EnsureExtras.WorkspaceCollection);

            var metadata = await GetWorkspaceCollectionMetadata(subscriptionId, resourceGroup, workspaceCollectionName);
            Console.ForegroundColor = ConsoleColor.Cyan;
            Console.WriteLine(metadata);
        }

        static async Task ListWorkspaceCollectionApiKeys()
        {
            EnsureBasicParams(EnsureExtras.WorkspaceCollection);

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
            EnsureBasicParams(EnsureExtras.WorkspaceCollection | EnsureExtras.WorspaceId);
            var datasetId = userInput.EnsureParam(null, "Dataset Id");

            await UpdateConnection(workspaceCollectionName, workspaceId, datasetId);
            Console.ForegroundColor = ConsoleColor.Cyan;
            Console.WriteLine("Connection information updated successfully.");
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

        static async Task DeleteDataset()
        {
            // reset datasetId to force assignment 
            EnsureBasicParams(EnsureExtras.WorkspaceCollection | EnsureExtras.WorspaceId | EnsureExtras.DatasetId, EnsureExtras.DatasetId);
            await DeleteDataset(workspaceCollectionName, workspaceId, datasetId);
            Console.ForegroundColor = ConsoleColor.Cyan;
            Console.WriteLine("Dataset deleted successfully.");
            datasetId = null;
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

        static async Task ListGatewaysForWorkspaceCollection()
        {
            EnsureBasicParams(EnsureExtras.WorkspaceCollection);

            var collectionGateways = await GetCollectionGateways(workspaceCollectionName);
            Console.ForegroundColor = ConsoleColor.Cyan;
            foreach (Gateway g in collectionGateways)
            {
                Console.WriteLine("Name:{0} ,Id:{1} ,PublicKey < Exponent:{2} ,Modulus:{3} >", g.Name, g.Id, g.PublicKey.Exponent, g.PublicKey.Modulus);
            }
            if (collectionGateways.Any())
            {
                gatewayId = collectionGateways.Last().Id;
                gatewayPublicKey = collectionGateways.Last().PublicKey;
            }
        }

        static async Task ListGatewaysForWorkspace()
        {
            EnsureBasicParams(EnsureExtras.WorkspaceCollection | EnsureExtras.WorspaceId);

            var workspaceGateways = await GetWorkspaceGateways(workspaceCollectionName, workspaceId);
            Console.ForegroundColor = ConsoleColor.Cyan;
            foreach (Gateway g in workspaceGateways)
            {
                Console.WriteLine("Name:{0} ,Id:{1} ,PublicKey < Exponent:{2} ,Modulus:{3} >", g.Name, g.Id, g.PublicKey.Exponent, g.PublicKey.Modulus);
            }
            if (workspaceGateways.Any())
            {
                gatewayId = workspaceGateways.Last().Id;
                gatewayPublicKey = workspaceGateways.Last().PublicKey;
            }
        }

        static async Task GetGatewayMetadata()
        {
            EnsureBasicParams(EnsureExtras.WorkspaceCollection | EnsureExtras.GatewayId);
            var gateway = await GetGatewayById(workspaceCollectionName, gatewayId);
            Console.ForegroundColor = ConsoleColor.Cyan;
            Console.WriteLine("Name:{0} ,Id:{1} ,PublicKey < Exponent:{2} ,Modulus:{3} >", gateway.Name, gateway.Id, gateway.PublicKey.Exponent, gateway.PublicKey.Modulus);
        }

        static async Task DeleteGateway()
        {
            EnsureBasicParams(EnsureExtras.WorkspaceCollection | EnsureExtras.GatewayId, EnsureExtras.GatewayId);
            await DeleteGateway(workspaceCollectionName, gatewayId);
            Console.WriteLine("Delete gateway id: {0} successfully", gatewayId);
            gatewayId = null;
        }

        static async Task CreateDatasource()
        {
            EnsureBasicParams(EnsureExtras.WorkspaceCollection | EnsureExtras.GatewayId);

            var publishDatasourceRequest = await GetPublishDatasourceRequestFromUser(workspaceCollectionName, gatewayId);
            if (publishDatasourceRequest == null)
            {
                return;
            }

            GatewayDatasource createdDatasource = await CreateDatasource(workspaceCollectionName, gatewayId, publishDatasourceRequest);
            Console.ForegroundColor = ConsoleColor.Cyan;
            Console.WriteLine("Id:{0} ", createdDatasource.Id);
        }

        static async Task ListDatasources()
        {
            EnsureBasicParams(EnsureExtras.WorkspaceCollection | EnsureExtras.GatewayId);

            var datasources = await GetDatasources(workspaceCollectionName, gatewayId);
            Console.ForegroundColor = ConsoleColor.Cyan;
            Console.WriteLine("Gateway Id: {0}", gatewayId);
            if (datasources.Any())
            {
                foreach (var ds in datasources)
                {
                    Console.WriteLine("Datasource Id:{0} connection details: {1}", ds.Id, ds.ConnectionDetails);
                }
            }
            else
            {
                Console.WriteLine("No datasources found in this gateway");
            }
        }

        static async Task GetDatasourceById()
        {
            EnsureBasicParams(EnsureExtras.WorkspaceCollection | EnsureExtras.GatewayId | EnsureExtras.DatasourceId, EnsureExtras.DatasourceId);
            var datasource = await GetDatasource(workspaceCollectionName, gatewayId, datasourceId);
            Console.ForegroundColor = ConsoleColor.Cyan;
            Console.WriteLine("Gateway Id: {0}", gatewayId);
            Console.WriteLine("Datasource Id:{0} ", datasource.Id);
        }

        static async Task DeleteDatasource()
        {
            EnsureBasicParams(EnsureExtras.WorkspaceCollection | EnsureExtras.GatewayId | EnsureExtras.DatasourceId, EnsureExtras.DatasourceId);
            await DeleteDatasource(workspaceCollectionName, gatewayId, datasourceId);
            Console.ForegroundColor = ConsoleColor.Cyan;
            Console.WriteLine("Delete datasource id: {0} successfully", datasourceId);
            datasourceId = null;
        }

        static async Task BindDataset()
        {
            EnsureBasicParams(EnsureExtras.WorkspaceCollection | EnsureExtras.GatewayId);
            var datasetObjectId = userInput.EnsureParam(null, "Dataset Id");

            await BindToGateway(workspaceCollectionName, Guid.Parse(datasetObjectId), gatewayId);
        }

        static async Task UpdateDatasource()
        {
            datasourceId = null;
            EnsureBasicParams(EnsureExtras.WorkspaceCollection | EnsureExtras.GatewayId | EnsureExtras.DatasourceId);

            var credentialDetails = await GetCredentialDetailsFromUser(workspaceCollectionName, gatewayId);
            if (credentialDetails == null)
            {
                return;
            }

            await UpdateDatasource(workspaceCollectionName, gatewayId, datasourceId, credentialDetails);
            Console.WriteLine("Datasource was updated.");
        }

        static async Task<Import> GetImport(string workspaceCollectionName, string workspaceId, string importId)
        {
            using (var client = await CreateClient())
            {
                return await client.Imports.GetImportByIdAsync(workspaceCollectionName, workspaceId, importId);
            }
        }

        static void ShowCachedMetadata()
        {
            ConsoleHelper.WriteColoredValue("Workspace Collection Name", workspaceCollectionName, ConsoleColor.Magenta, "\n");
            var usedAccessKey = (accessKeys == null) ? null : accessKeys.Key1;
            ConsoleHelper.WriteColoredValue("Workspace Collection Access Key1", usedAccessKey, ConsoleColor.Magenta, "\n");
 
            ConsoleHelper.WriteColoredValue("Workspace Id", workspaceId, ConsoleColor.Magenta, "\n");
            ConsoleHelper.WriteColoredValue("Gateway Id", gatewayId, ConsoleColor.Magenta, "\n");
            ConsoleHelper.WriteColoredValue("DatasourceId", datasourceId, ConsoleColor.Magenta, "\n");
            ConsoleHelper.WriteColoredValue("DatasetId", datasetId, ConsoleColor.Magenta, "\n");
            if (gatewayPublicKey != null)
            {
                ConsoleHelper.WriteColoredValue("gatewayPublicKey: Exponent", gatewayPublicKey.Exponent, ConsoleColor.Magenta);
                ConsoleHelper.WriteColoredValue(" Modulus", gatewayPublicKey.Modulus, ConsoleColor.Magenta, "\n");
            }
            else
                ConsoleHelper.WriteColoredValue("gatewayPublicKey", null, ConsoleColor.Magenta, "\n");

            Console.WriteLine();
        }
        static void ResetCachedMetadata(bool forceReset = false)
        {
            workspaceCollectionName = userInput.ResetCachedParam(workspaceCollectionName, "Workspace Collection Name", forceReset);
            accessKeys.Key1 = userInput.ResetCachedParam(accessKeys.Key1, "Workspace Collection Access Key1", forceReset);
            if (string.IsNullOrWhiteSpace(accessKeys.Key1))
            {
                accessKeys = null;
            }
            workspaceId = userInput.ResetCachedParam(workspaceId, "Workspace Id", forceReset);
            gatewayId = userInput.ResetCachedParam(gatewayId, "Gateway Id", forceReset);
            datasourceId = userInput.ResetCachedParam(datasourceId, "Datasource Id", forceReset);
            datasetId = userInput.ResetCachedParam(datasetId, "Dataset Id", forceReset);
            if (gatewayPublicKey != null)
            {
                string exponentAndModulus = "Exp:" + gatewayPublicKey.Exponent + " Mod:" + gatewayPublicKey.Modulus;
                exponentAndModulus = userInput.ResetCachedParam(exponentAndModulus, "Gateway Public Key", forceReset);
                if (string.IsNullOrWhiteSpace(exponentAndModulus))
                {
                    gatewayPublicKey = null;
                }
            }

            if (forceReset)
            {
                Console.WriteLine("Entire cache was reset\n");
            }
        }

        /// <summary>
        /// Creates a new Power BI Embedded workspace collection
        /// </summary>
        /// <param name="subscriptionId">The azure subscription id</param>
        /// <param name="resourceGroup">The azure resource group</param>
        /// <param name="workspaceCollectionName">The Power BI workspace collection name to create</param>
        /// <returns></returns>
        static async Task CreateWorkspaceCollection(string subscriptionId, string resourceGroup, string workspaceCollectionName)
        {
            var url = string.Format("{0}/subscriptions/{1}/resourceGroups/{2}/providers/Microsoft.PowerBI/workspaceCollections/{3}{4}", azureEndpointUri, subscriptionId, resourceGroup, workspaceCollectionName, version);

            HttpClient client = new HttpClient();

            using (client)
            {
                var content = new StringContent(@"{
                                                ""location"": ""southcentralus"",
                                                ""tags"": {},
                                                ""sku"": {
                                                    ""name"": ""S1"",
                                                    ""tier"": ""Standard""
                                                }
                                            }", Encoding.UTF8);
                content.Headers.ContentType = MediaTypeHeaderValue.Parse("application/json; charset=utf-8");

                var request = new HttpRequestMessage(HttpMethod.Put, url);
                // Set authorization header from you acquired Azure AD token
                request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", await GetAzureAccessTokenAsync());
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

            HttpClient client = new HttpClient();
            using (client)
            {
                var request = new HttpRequestMessage(HttpMethod.Get, url);
                // Set authorization header from you acquired Azure AD token
                request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", await GetAzureAccessTokenAsync());
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

            HttpClient client = new HttpClient();

            using (client)
            {
                var request = new HttpRequestMessage(HttpMethod.Post, url);
                // Set authorization header from you acquired Azure AD token
                request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", await GetAzureAccessTokenAsync());
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
            HttpClient client = new HttpClient();

            using (client)
            {
                var request = new HttpRequestMessage(HttpMethod.Get, url);
                // Set authorization header from you acquired Azure AD token
                request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", await GetAzureAccessTokenAsync());
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
        /// <param name="datasetId">The Power BI dataset to update connection for</param>
        /// <returns></returns>
        static async Task UpdateConnection(string workspaceCollectionName, string workspaceId, string datasetId)
        {
            username = userInput.EnsureParam(username, "Username", onlyFillIfEmpty: true);
            password = userInput.EnsureParam(password, "Password", onlyFillIfEmpty: true);

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

        static async Task<IEnumerable<Gateway>> GetCollectionGateways(string workspaceCollectionName)
        {
            using (var client = await CreateClient())
            {
                ODataResponseListGateway response = await client.Gateways.GetCollectionGatewaysAsync(workspaceCollectionName);
                return response.Value;
            }
        }

        static async Task<IEnumerable<Gateway>> GetWorkspaceGateways(string workspaceCollectionName, string workspaceId)
        {
            using (var client = await CreateClient())
            {
                ODataResponseListGateway response = await client.Gateways.GetWorkspaceGatewaysAsync(workspaceCollectionName, workspaceId);
                return response.Value;
            }
        }

        static async Task<Gateway> GetGatewayById(string workspaceCollectionName, string gatewayId)
        {
            using (var client = await CreateClient())
            {
                var gateway = await client.Gateways.GetGatewayByIdAsync(workspaceCollectionName, gatewayId);
                gatewayPublicKey = gateway.PublicKey;
                return gateway;
            }
        }

        static async Task DeleteGateway(string workspaceCollectionName, string gatewayId)
        {
            using (var client = await CreateClient())
            {
                await client.Gateways.DeleteGatewayByIdAsync(workspaceCollectionName, gatewayId);
            }
        }

        private static async Task<CredentialDetails> GetCredentialDetailsFromUser(string workspaceCollectionName, string gatewayId)
        {
            var credentialDetails = new CredentialDetails();
            string str = userInput.EnsureParam(null, "Credential Type (1:Windows 2:Basic)");
            if (str != "1" && str != "2")
            {
                return null;
            }
            credentialDetails.CredentialType = str == "1" ? "Windows" : (str == "2" ? "Basic" : null);
            credentialDetails.EncryptionAlgorithm = "RSA-OAEP";

            string username = userInput.EnsureParam(null, "Username");
            string password = userInput.GetPassword();

            if (string.IsNullOrEmpty(username) || string.IsNullOrEmpty(password))
            {
                return null;
            }

            await EnsureGatewayPublicKey(workspaceCollectionName, gatewayId);
            credentialDetails.Credentials = AsymmetricKeyEncryptionHelper.EncodeCredentials(username, password, gatewayPublicKey);

            str = userInput.EnsureParam(null, "Encrypted Connection (1:Encrypted  2:Not Encrypted)");
            if (str != "1" && str != "2")
            {
                return null;
            }
            credentialDetails.EncryptedConnection = str == "1" ? "Encrypted" : "NotEncrypted";
            str = userInput.EnsureParam(null, "Privacy Level (1:None, 2:Private, 3:Organizational, 4:Public)");

            switch (str)
            {
                case "1":
                    credentialDetails.PrivacyLevel = "None";
                    break;
                case "2":
                    credentialDetails.PrivacyLevel = "Private";
                    break;
                case "3":
                    credentialDetails.PrivacyLevel = "Organizational";
                    break;
                case "4":
                    credentialDetails.PrivacyLevel = "Public";
                    break;
                default:
                    return null;
            }
            Console.WriteLine();

            return credentialDetails;
        }

        private static async Task<PublishDatasourceToGatewayRequest> GetPublishDatasourceRequestFromUser(string workspaceCollectionName, string gatewayId)
        {
            var request = new PublishDatasourceToGatewayRequest();
            request.DataSourceName = userInput.EnsureParam(null, "Datasource Name:");
            string str = userInput.EnsureParam(null, "DataSourceType (1:SQL, 2:Analysis Services)");
            if (str != "1" && str != "2")
            {
                return null;
            }
            request.DataSourceType = str == "1" ? "SQL" : "AnalysisServices";
            request.ConnectionDetails = userInput.EnsureParam(null, "Connection Details: ex: {\"server\":\"<your server>\",\"database\":\"<your database>\"}");

            request.CredentialDetails = await GetCredentialDetailsFromUser(workspaceCollectionName, gatewayId);
            if (request.CredentialDetails == null)
            {
                return null;
            }

            return request;
        }

        private static async Task<GatewayDatasource> CreateDatasource(string workspaceCollectionName, string gatewayId, PublishDatasourceToGatewayRequest request)
        {
            using (var client = await CreateClient())
            {
                return await client.Gateways.CreateDatasourceAsync(workspaceCollectionName, gatewayId, request);
            }
        }

        private static async Task<IEnumerable<GatewayDatasource>> GetDatasources(string workspaceCollectionName, string gatewayId)
        {
            using (var client = await CreateClient())
            {
                var datasources = await client.Gateways.GetDatasourcesAsync(workspaceCollectionName, gatewayId);
                return datasources.Value;
            }
        }

        private static async Task<GatewayDatasource> GetDatasource(string workspaceCollectionName, string gatewayId, string datasourceId)
        {
            using (var client = await CreateClient())
            {
                var datasource = await client.Gateways.GetDatasourceByIdAsync(workspaceCollectionName, gatewayId, datasourceId);
                return datasource;
            }
        }

        private static async Task DeleteDatasource(string workspaceCollectionName, string gatewayId, string datasourceId)
        {
            using (var client = await CreateClient())
            {
                await client.Gateways.DeleteDatasourceAsync(workspaceCollectionName, gatewayId, datasourceId);
            }
        }

        private static async Task BindToGateway(string workspaceCollectionName, Guid datasetId, string gatewayId)
        {
            using (var client = await CreateClient())
            {
                await client.Datasets.BindToGatewayAsync(workspaceCollectionName, datasetId.ToString(), new BindToGatewayRequest(gatewayId));
            }
        }

        private static async Task UpdateDatasource(string workspaceCollectionName, string gatewayId, string datasourceId, CredentialDetails credentialDetails)
        {
            var request = new UpdateDatasourceRequest(credentialDetails);

            using (var client = await CreateClient())
            {
                await client.Gateways.UpdateDatasourceAsync(workspaceCollectionName, gatewayId, datasourceId, request);
            }
        }

        static async Task EnsureGatewayPublicKey(string workspaceCollectionName, string gatewayId)
        {
            if (gatewayPublicKey == null)
            {
                string exponent = userInput.EnsureParam(null, "Gateway Public Key exponent");
                string modulus = userInput.EnsureParam(null, "Gateway Public Key modulus");
                if (!string.IsNullOrWhiteSpace(exponent) && !string.IsNullOrWhiteSpace(modulus))
                {
                    gatewayPublicKey = new GatewayPublicKey(exponent, modulus);
                }
            }

            if (gatewayPublicKey == null)
            {
                gatewayPublicKey = (await GetGatewayById(workspaceCollectionName, gatewayId)).PublicKey;
            }
        }

        /// <summary>
        /// Creates a new instance of the PowerBIClient with the specified token
        /// </summary>
        /// <returns></returns>
        static async Task<PowerBIClient> CreateClient()
        {
            if (accessKeys == null)
            {
                var enteredKey = userInput.EnterOptionalParam("Access Key", "Auto select");
                if (!string.IsNullOrWhiteSpace(enteredKey))
                {
                    accessKey = enteredKey;
                    accessKeys = new WorkspaceCollectionKeys()
                    {
                        Key1 = accessKey
                    };
                }
            }

            if (accessKeys == null)
            {
                accessKeys = await ListWorkspaceCollectionKeys(subscriptionId, resourceGroup, workspaceCollectionName);
            }

            // Create a token credentials with "AppKey" type
            var credentials = new TokenCredentials(accessKeys.Key1, "AppKey");

            // Instantiate your Power BI client passing in the required credentials
            var client = new PowerBIClient(credentials);

            // Override the api endpoint base URL.  Default value is https://api.powerbi.com
            client.BaseUri = new Uri(apiEndpointUri);

            return client;
        }

        static async Task<IEnumerable<string>> GetTenantIdsAsync(string commonToken)
        {
            using (var httpClient = new HttpClient())
            {
                httpClient.DefaultRequestHeaders.Add("Authorization", "Bearer " + commonToken);
                var response = await httpClient.GetStringAsync("https://management.azure.com/tenants?api-version=2016-01-29");
                var tenantsJson = JsonConvert.DeserializeObject<JObject>(response);
                var tenants = tenantsJson["value"] as JArray;

                return tenants.Select(t => t["tenantId"].Value<string>());
            }
        }

        /// <summary>
        /// Gets an Azure access token that can be used to call into the Azure ARM apis.
        /// </summary>
        /// <returns>A user token to access Azure ARM</returns>
        static async Task<string> GetAzureAccessTokenAsync()
        {
            if (!string.IsNullOrWhiteSpace(azureToken))
            {
                return azureToken;
            }

            var commonToken = GetCommonAzureAccessToken();
            var tenantId = (await GetTenantIdsAsync(commonToken.AccessToken)).FirstOrDefault();

            if (string.IsNullOrWhiteSpace(tenantId))
            {
                throw new InvalidOperationException("Unable to get tenant id for user accout");
            }

            var authority = string.Format("https://login.windows.net/{0}/oauth2/authorize", tenantId);
            var authContext = new AuthenticationContext(authority);
            var result = await authContext.AcquireTokenByRefreshTokenAsync(commonToken.RefreshToken, clientId, armResource);

            return (azureToken = result.AccessToken);

        }

        /// <summary>
        /// Gets a user common access token to access ARM apis
        /// </summary>
        /// <returns></returns>
        static AuthenticationResult GetCommonAzureAccessToken()
        {
            var authContext = new AuthenticationContext("https://login.windows.net/common/oauth2/authorize");
            var result = authContext.AcquireToken(
                resource: armResource,
                clientId: clientId,
                redirectUri: redirectUri,
                promptBehavior: PromptBehavior.Auto);

            if (result == null)
            {
                throw new InvalidOperationException("Failed to obtain the JWT token");
            }

            return result;
        }
    }
}
