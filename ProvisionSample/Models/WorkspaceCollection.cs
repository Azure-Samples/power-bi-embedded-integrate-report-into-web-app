using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ApiHost.Models
{
    public class WorkspaceCollection
    {
        [JsonProperty(PropertyName = "name")]
        public string Name { get; set; }
    }

    public class WorkspaceCollectionList
    {
        public List<WorkspaceCollection> value;
    }
}
