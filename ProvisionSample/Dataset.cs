
using System.Collections.Generic;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace ProvisionSample
{
    internal class Column
    {
        public string name { get; set; }
        public string dataType { get; set; }
        public string formatString { get; set; }
        public string sortByColumn { get; set; }
        public string dataCategory { get; set; }
        public bool isHidden { get; set; }
        public string summarizeBy { get; set; }
        public string type { get; set; }

        // This function makes JsonConvert.SerializeObject ignore type property
        public bool ShouldSerializetype()
        {
            return false;
        }
    }

    internal class Measure
    {
        public string name { get; set; }
        public string expression { get; set; }
        public string formatString { get; set; }
        public string isHidden { get; set; }
    }

    internal class Table
    {
        public string name { get; set; }
        public IList<Column> columns { get; set; }
        public bool isHidden { get; set; }
        public IList<Measure> measures { get; set; }
    }

    internal class Relationship
    {
        public string name { get; set; }
        public string crossFilteringBehavior { get; set; }
        public string fromTable { get; set; }
        public string fromColumn { get; set; }
        public string toTable { get; set; }
        public string toColumn { get; set; }
        public string isActive { get; set; }

        // This function makes JsonConvert.SerializeObject ignore isActive property
        public bool ShouldSerializeisActive()
        {
            return false;
        }
    }

    class DataSet
    {
        public string name { get; set; }
        public IList<Table> tables { get; set; }
        public IList<Relationship> relationships { get; set; }
    }
}
