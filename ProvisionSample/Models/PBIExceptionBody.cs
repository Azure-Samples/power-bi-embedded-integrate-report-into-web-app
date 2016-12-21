using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ApiHost.Models
{
    public class PBIExceptionBody
    {
        [JsonProperty(PropertyName = "error")]
        public PBIError Error { get; set; }
    }

    public class PBIError
    {
        [JsonProperty(PropertyName = "code")]
        public string Code { get; set; }

        [JsonProperty(PropertyName = "details")]
        public IEnumerable<ExceptionDetails> Details { get; set; }
    }

    public class ExceptionDetails
    {
        [JsonProperty(PropertyName = "message")]
        public string Message { get; set; }
    }
}
