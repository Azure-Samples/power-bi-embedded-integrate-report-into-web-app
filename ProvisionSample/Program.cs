using System;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Rest;
using Microsoft.Threading;
using ApiHost.Models;
using System.IO;
using System.Threading;
using Microsoft.Rest.Serialization;
using System.Net.Http.Headers;
using System.Configuration;
using System.Net;
using System.Collections.Generic;
using Microsoft.IdentityModel.Clients.ActiveDirectory;
using Microsoft.PowerBI.Api.V1;
using Microsoft.PowerBI.Api.V1.Models;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Linq;

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
        static string datasetId = ConfigurationManager.AppSettings["datasetId"];
        static string workspaceId = ConfigurationManager.AppSettings["workspaceId"];
        static string azureToken = null;

        static WorkspaceCollectionKeys accessKeys = null;

        static void Main(string[] args)
        {
            if (!string.IsNullOrWhiteSpace(accessKey))
            {
                accessKeys = new WorkspaceCollectionKeys
                {
                    Key1 = accessKey
                };
            }

            AsyncPump.Run(async delegate
            {
                await Run();
            });

            Console.ReadKey(true);
        }

        static async Task Run()
        {
            Console.ResetColor();
            var exit = false;

            try
            {
                Console.WriteLine();
                Console.WriteLine("What do you want to do?");
                Console.WriteLine("=================================================================");
                Console.WriteLine("1. Provision a new workspace collection");
                Console.WriteLine("2. Get workspace collection metadata");
                Console.WriteLine("3. Retrieve a workspace collection's API keys");
                Console.WriteLine("4. Get list of workspaces within a collection");
                Console.WriteLine("5. Provision a new workspace in an existing workspace collection");
                Console.WriteLine("6. Import PBIX Desktop file into an existing workspace");
                Console.WriteLine("7. Update connection string info for an existing dataset");
                Console.WriteLine("8. Retrieve a list of Datasets published to a workspace");
                Console.WriteLine("9. Delete a published dataset from a workspace");
                Console.WriteLine();

                var key = Console.ReadKey(true);

                switch (key.KeyChar)
                {
                    case '1':
                        if (string.IsNullOrWhiteSpace(subscriptionId))
                        {
                            Console.Write("Azure Subscription ID:");
                            subscriptionId = Console.ReadLine();
                            Console.WriteLine();
                        }
                        if (string.IsNullOrWhiteSpace(resourceGroup))
                        {
                            Console.Write("Azure Resource Group:");
                            resourceGroup = Console.ReadLine();
                            Console.WriteLine();
                        }
                        Console.Write("Workspace Collection Name:");
                        workspaceCollectionName = Console.ReadLine();
                        Console.WriteLine();

                        await CreateWorkspaceCollection(subscriptionId, resourceGroup, workspaceCollectionName);
                        accessKeys = await ListWorkspaceCollectionKeys(subscriptionId, resourceGroup, workspaceCollectionName);
                        Console.ForegroundColor = ConsoleColor.Cyan;
                        Console.WriteLine("Workspace collection created successfully");

                        await Run();
                        break;
                    case '2':
                        if (string.IsNullOrWhiteSpace(subscriptionId))
                        {
                            Console.Write("Azure Subscription ID:");
                            subscriptionId = Console.ReadLine();
                            Console.WriteLine();
                        }
                        if (string.IsNullOrWhiteSpace(resourceGroup))
                        {
                            Console.Write("Azure Resource Group:");
                            resourceGroup = Console.ReadLine();
                            Console.WriteLine();
                        }
                        Console.Write("Workspace Collection Name:");
                        workspaceCollectionName = Console.ReadLine();
                        Console.WriteLine();

                        var metadata = await GetWorkspaceCollectionMetadata(subscriptionId, resourceGroup, workspaceCollectionName);
                        Console.ForegroundColor = ConsoleColor.Cyan;
                        Console.WriteLine(metadata);

                        await Run();
                        break;
                    case '3':
                        if (string.IsNullOrWhiteSpace(subscriptionId))
                        {
                            Console.Write("Azure Subscription ID:");
                            subscriptionId = Console.ReadLine();
                            Console.WriteLine();
                        }
                        if (string.IsNullOrWhiteSpace(resourceGroup))
                        {
                            Console.Write("Azure Resource Group:");
                            resourceGroup = Console.ReadLine();
                            Console.WriteLine();
                        }
                        Console.Write("Workspace Collection Name:");
                        workspaceCollectionName = Console.ReadLine();
                        Console.WriteLine();

                        accessKeys = await ListWorkspaceCollectionKeys(subscriptionId, resourceGroup, workspaceCollectionName);
                        Console.ForegroundColor = ConsoleColor.Cyan;
                        Console.WriteLine("Key1: {0}", accessKeys.Key1);
                        Console.WriteLine("===============================");
                        Console.WriteLine("Key2: {0}", accessKeys.Key2);

                        await Run();
                        break;
                    case '4':
                        Console.Write("Workspace Collection Name:");
                        workspaceCollectionName = Console.ReadLine();
                        Console.WriteLine();

                        var workspaces = await GetWorkspaces(workspaceCollectionName);
                        Console.ForegroundColor = ConsoleColor.Cyan;

                        foreach (var instance in workspaces)
                        {
                            Console.WriteLine("Collection: {0}, ID: {1}", instance.WorkspaceCollectionName, instance.WorkspaceId);
                        }

                        await Run();
                        break;
                    case '5':
                        if (string.IsNullOrWhiteSpace(workspaceCollectionName))
                        {
                            Console.Write("Workspace Collection Name:");
                            workspaceCollectionName = Console.ReadLine();
                            Console.WriteLine();
                        }

                        var workspace = await CreateWorkspace(workspaceCollectionName);
                        workspaceId = workspace.WorkspaceId;
                        Console.ForegroundColor = ConsoleColor.Cyan;
                        Console.WriteLine("Workspace ID: {0}", workspaceId);

                        await Run();
                        break;
                    case '6':
                        if (string.IsNullOrWhiteSpace(workspaceCollectionName))
                        {
                            Console.Write("Workspace Collection Name:");
                            workspaceCollectionName = Console.ReadLine();
                            Console.WriteLine();
                        }

                        if (string.IsNullOrWhiteSpace(workspaceId))
                        {
                            Console.Write("Workspace ID:");
                            workspaceId = Console.ReadLine();
                            Console.WriteLine();
                        }

                        Console.Write("Dataset Name:");
                        var datasetName = Console.ReadLine();
                        Console.WriteLine();

                        Console.Write("File Path:");
                        var filePath = Console.ReadLine();
                        Console.WriteLine();

                        var import = await ImportPbix(workspaceCollectionName, workspaceId, datasetName, filePath);
                        Console.ForegroundColor = ConsoleColor.Cyan;
                        Console.WriteLine("Import: {0}", import.Id);

                        await Run();
                        break;
                    case '7':
                        if (string.IsNullOrWhiteSpace(workspaceCollectionName))
                        {
                            Console.Write("Workspace Collection Name:");
                            workspaceCollectionName = Console.ReadLine();
                            Console.WriteLine();
                        }

                        if (string.IsNullOrWhiteSpace(workspaceId))
                        {
                            Console.Write("Workspace ID:");
                            workspaceId = Console.ReadLine();
                            Console.WriteLine();
                        }

                        await UpdateConnection(workspaceCollectionName, workspaceId);
                        Console.ForegroundColor = ConsoleColor.Cyan;
                        Console.WriteLine("Connection information updated successfully.");

                        await Run();
                        break;
                    case '8':
                        if (string.IsNullOrWhiteSpace(workspaceCollectionName))
                        {
                            Console.Write("Workspace Collection Name:");
                            workspaceCollectionName = Console.ReadLine();
                            Console.WriteLine();
                        }
                        if (string.IsNullOrWhiteSpace(workspaceId))
                        {
                            Console.Write("Workspace ID:");
                            workspaceId = Console.ReadLine();
                            Console.WriteLine();
                        }

                        await ListDatasets(workspaceCollectionName, workspaceId);
                        break;
                    case '9':
                        if (string.IsNullOrWhiteSpace(workspaceCollectionName))
                        {
                            Console.Write("Workspace Collection Name:");
                            workspaceCollectionName = Console.ReadLine();
                            Console.WriteLine();
                        }
                        if (string.IsNullOrWhiteSpace(workspaceId))
                        {
                            Console.Write("Workspace ID:");
                            workspaceId = Console.ReadLine();
                            Console.WriteLine();
                        }

                        Console.Write("Dataset ID:");
                        datasetId = Console.ReadLine();
                        Console.WriteLine();

                        await DeleteDataset(workspaceCollectionName, workspaceId, datasetId);
                        Console.ForegroundColor = ConsoleColor.Cyan;
                        Console.WriteLine("Dataset deleted successfully.");

                        break;
                    default:
                        Console.WriteLine("Press any key to exit..");
                        exit = true;
                        break;
                }
            }
            catch (Exception ex)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine("Ooops, something broke: {0}", ex.Message);
                Console.WriteLine();
            }

            if (!exit)
            {
                await Run();
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
        static async Task<Workspace> CreateWorkspace(string workspaceCollectionName)
        {
            using (var client = await CreateClient())
            {
                // Create a new workspace witin the specified collection
                return await client.Workspaces.PostWorkspaceAsync(workspaceCollectionName);
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
            using (var fileStream = File.OpenRead(filePath))
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
        static async Task ListDatasets(string workspaceCollectionName, string workspaceId)
        {
            using (var client = await CreateClient())
            {
                ODataResponseListDataset response = await client.Datasets.GetDatasetsAsync(workspaceCollectionName, workspaceId);

                foreach (Dataset d in response.Value)
                {
                    Console.WriteLine("{0}:  {1}", d.Name, d.Id);
                }
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
        /// <returns></returns>
        static async Task UpdateConnection(string workspaceCollectionName, string workspaceId)
        {
            if (string.IsNullOrWhiteSpace(username))
            {
                Console.Write("Username: ");
                username = Console.ReadLine();
                Console.WriteLine();
            }

            if (string.IsNullOrWhiteSpace(password))
            {
                Console.Write("Password: ");
                password = ConsoleHelper.ReadPassword();
                Console.WriteLine();
            }

            string connectionString = null;
            Console.Write("Connection String (enter to skip): ");
            connectionString = Console.ReadLine();
            Console.WriteLine();

            using (var client = await CreateClient())
            {
                // Get the newly created dataset from the previous import process
                var datasets = await client.Datasets.GetDatasetsAsync(workspaceCollectionName, workspaceId);

                // Optionally udpate the connectionstring details if preent
                if (!string.IsNullOrWhiteSpace(connectionString))
                {
                    var connectionParameters = new Dictionary<string, object>
                    {
                        { "connectionString", connectionString }
                    };
                    await client.Datasets.SetAllConnectionsAsync(workspaceCollectionName, workspaceId, datasets.Value[datasets.Value.Count - 1].Id, connectionParameters);
                }

                // Get the datasources from the dataset
                var datasources = await client.Datasets.GetGatewayDatasourcesAsync(workspaceCollectionName, workspaceId, datasets.Value[datasets.Value.Count - 1].Id);

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

                // Update the datasource with the specified credentials
                await client.Gateways.PatchDatasourceAsync(workspaceCollectionName, workspaceId, datasources.Value[datasources.Value.Count - 1].GatewayId, datasources.Value[datasources.Value.Count - 1].Id, delta);
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
                Console.Write("Access Key: ");
                accessKey = Console.ReadLine();
                Console.WriteLine();

                accessKeys = new WorkspaceCollectionKeys()
                {
                    Key1 = accessKey
                };
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
