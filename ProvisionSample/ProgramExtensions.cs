using System;
using System.Configuration;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using ApiHost.Models;
using Microsoft.PowerBI.Api.V1;
using Microsoft.Rest;

namespace ProvisionSample
{
    /// <summary>
    /// This part of Program class includes all Onebox-specific code
    /// </summary>
    partial class Program
    {
        static string thumbprint = ConfigurationManager.AppSettings["thumbprint"];
        static bool useCertificate = bool.Parse(ConfigurationManager.AppSettings["useCertificate"]);

        private static HttpClient CreateHttpClient()
        {
            if (useCertificate)
            {
                var handler = new WebRequestHandler();
                var certificate = GetCertificate(thumbprint);
                handler.ClientCertificates.Add(certificate);
                return new HttpClient(handler);
            }
            return new HttpClient();
        }

        // this is async Task to be comparible with the "production" version program. forget about the "This async method lacks 'await'" warning
#pragma warning disable 1998
        private static async Task SetAuthorizationHeaderIfNeeded(HttpRequestMessage request)
        {
            if (!useCertificate)
            {
                request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", GetAzureAccessToken());
            }
        }
#pragma warning restore 1998

        /// <summary>
        /// Creates a new instance of the PowerBIClient with the specified token
        /// </summary>
        /// <returns></returns>
        static async Task<PowerBIClient> CreateClient()
        {
            await EnsureSigningKeys();
            return CreateClient("AppKey", accessKeys.Key1);
        }

        static PowerBIClient CreateClient(string bearerType, string token)
        {
            // Create a token credentials with "AppToken" type
            var credentials = new TokenCredentials(token, bearerType);

            // Instantiate your Power BI client passing in the required credentials
            var client = new PowerBIClient(credentials);

            // Override the api endpoint base URL.  Default value is https://api.powerbi.com
            client.BaseUri = new Uri(apiEndpointUri);

            return client;
        }

        static async Task EnsureSigningKeys()
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
        }

        static string GetAzureAccessToken()
        {
            return "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6IjF6bmJlNmV2ZWJPamg2TTNXR1E5X1ZmWXVJdyIsImtpZCI6IjF6bmJlNmV2ZWJPamg2TTNXR1E5X1ZmWXVJdyJ9.eyJhdWQiOiJodHRwczovL21hbmFnZW1lbnQuY29yZS53aW5kb3dzLm5ldC8iLCJpc3MiOiJodHRwczovL3N0cy53aW5kb3dzLXBwZS5uZXQvODNhYmU1Y2QtYmNjMy00NDFhLWJkODYtZTZhNzUzNjBjZWNjLyIsImlhdCI6MTQ1ODU3Mzk0MywibmJmIjoxNDU4NTczOTQzLCJleHAiOjE0NTg1Nzc4NDMsImFjciI6IjEiLCJhbHRzZWNpZCI6IjE6bGl2ZS5jb206MDAwM0JGRkRDM0Y4OEEyQiIsImFtciI6WyJwd2QiXSwiYXBwaWQiOiJjNDRiNDA4My0zYmIwLTQ5YzEtYjQ3ZC05NzRlNTNjYmRmM2MiLCJhcHBpZGFjciI6IjIiLCJlbWFpbCI6ImF1eHRtMTkyQGxpdmUuY29tIiwiaWRwIjoibGl2ZS5jb20iLCJpcGFkZHIiOiI0MC4xMjIuMjAyLjgxIiwibmFtZSI6ImF1eHRtMTkyQGxpdmUuY29tIiwib2lkIjoiMmMwNzAxOGMtZjVhZC00MDY1LThiYTAtYzA3MmE1NGNkNDNkIiwicHVpZCI6IjEwMDMwMDAwOEIwNDlBQzEiLCJzY3AiOiJ1c2VyX2ltcGVyc29uYXRpb24iLCJzdWIiOiJYOGF3eW9GRnowZWpDTjM4Sm5uRVl6Vy01ODhTNE02NVlldGlGelF5SDJrIiwidGlkIjoiODNhYmU1Y2QtYmNjMy00NDFhLWJkODYtZTZhNzUzNjBjZWNjIiwidW5pcXVlX25hbWUiOiJsaXZlLmNvbSNhdXh0bTE5MkBsaXZlLmNvbSIsInZlciI6IjEuMCJ9.aZmB2woGCRMfHfPcVcC-EmzoGToQfDSdDmbI6wAucHWRE5P9LAflZcBq-LCeUlXA8xzda_6rWo5IcS8nFK8thMofffOCSiyZdrJOZsBKpYhv-XCiWR6y9I-994AyL-Em-f6-Lxf74_pjqd8peT0mmZ_cJyqsY_n20MYmRCf1gKsqzcwObh-RJwP1HG1TXgCRF9zlHl_96nasZdjUShGDG9RYGFUW_qHxh01xn0DWWTr-mCgEvu6PKRXGhDgm2XPcEwhGc6aKnj7buDc7HU5j6nxPQtsaU-fz5RuAQ2ezwy9VUCfAz17HQEz12TQ42Ehhvo2bDtCVkvHwA2egBn9hZA";
        }

        static X509Certificate2 GetCertificate(string thumbprint)
        {
            X509Certificate2 cert = null;

            var certStore = new X509Store(StoreLocation.LocalMachine);
            certStore.Open(OpenFlags.ReadOnly);

            var certificates = certStore.Certificates.Find(X509FindType.FindByThumbprint, thumbprint, false);
            if (certificates.Count > 0)
            {
                cert = certificates[0];
            }
            else
            {
                throw new CryptographicException("Cannot find the certificate with the thumbprint: {0}", thumbprint);
            }

            certStore.Close();

            return cert;
        }
    }
}

