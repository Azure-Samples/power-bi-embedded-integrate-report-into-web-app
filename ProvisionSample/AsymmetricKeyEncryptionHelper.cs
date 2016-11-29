using System;
using System.Security.Cryptography;
using System.Text;
using Microsoft.PowerBI.Api.V1.Models;
using Newtonsoft.Json;

namespace ProvisionSample
{
    public static class AsymmetricKeyEncryptionHelper
    {

        private const int SegmentLength = 85;
        private const int EncryptedLength = 128;

        public static string EncodeCredentials(string userName, string password, GatewayPublicKey publicKey)
        {
            // using json serializer to handle escape characters in username and password
            var plainText = string.Format("{{\"credentialData\":[{{\"value\":{0},\"name\":\"username\"}},{{\"value\":{1},\"name\":\"password\"}}]}}", JsonConvert.SerializeObject(userName), JsonConvert.SerializeObject(password));
            using (RSACryptoServiceProvider rsa = new RSACryptoServiceProvider(publicKey.Modulus.Length * 8))
            {
                var parameters = rsa.ExportParameters(false);
                parameters.Exponent = Convert.FromBase64String(publicKey.Exponent);
                parameters.Modulus = Convert.FromBase64String(publicKey.Modulus);
                rsa.ImportParameters(parameters);
                return Encrypt(plainText, rsa);
            }
        }

        private static string Encrypt(string plainText, RSACryptoServiceProvider rsa)
        {
            byte[] plainTextArray = Encoding.UTF8.GetBytes(plainText);

            // Split the message into different segments, each segment's length is 85. So the result may be 85,85,85,20.
            bool hasIncompleteSegment = plainTextArray.Length%SegmentLength != 0;

            int segmentNumber = (!hasIncompleteSegment) ? (plainTextArray.Length/SegmentLength) : ((plainTextArray.Length/SegmentLength) + 1);

            byte[] encryptedData = new byte[segmentNumber*EncryptedLength];
            int encryptedDataPosition = 0;

            for (var i = 0; i < segmentNumber; i++)
            {
                int lengthToCopy;

                if (i == segmentNumber - 1 && hasIncompleteSegment)
                    lengthToCopy = plainTextArray.Length % SegmentLength;
                else
                    lengthToCopy = SegmentLength;

                var segment = new byte[lengthToCopy];

                Array.Copy(plainTextArray, i * SegmentLength, segment, 0, lengthToCopy);

                var segmentEncryptedResult = rsa.Encrypt(segment, true);

                Array.Copy(segmentEncryptedResult, 0, encryptedData, encryptedDataPosition, segmentEncryptedResult.Length);

                encryptedDataPosition += segmentEncryptedResult.Length;
            }

            return Convert.ToBase64String(encryptedData);
        }
    }
}
