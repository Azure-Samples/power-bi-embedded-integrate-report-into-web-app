using ApiHost.Models;
using Microsoft.IdentityModel.Clients.ActiveDirectory;
using Microsoft.PowerBI.Api.V1;
using Microsoft.Rest;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading.Tasks;


namespace ProvisionSample
{
    partial class Program
    {
        static string clientId = "ea0616ba-638b-4df5-95b9-636659ae5121";
        static Uri redirectUri = new Uri("urn:ietf:wg:oauth:2.0:oob");
        static string azureToken = null;

        private static HttpClient CreateHttpClient()
        {
            return new HttpClient();
        }
        private static async Task SetAuthorizationHeaderIfNeeded(HttpRequestMessage request)
        {
            request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", await GetAzureAccessTokenAsync());
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
                // get the current collection's keys
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
            using (var httpClient = CreateHttpClient())
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

