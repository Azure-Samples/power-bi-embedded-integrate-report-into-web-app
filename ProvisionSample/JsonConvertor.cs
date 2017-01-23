using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.IO.Compression;

namespace ProvisionSample
{
    class JsonConvertor
    {
        private const string ModelSchema = "DataModelSchema";
        const int ModelSizeLimit = 75;

        public static async Task Convert(string datasetName, string pathToPbit)
        {
            Unzip(@pathToPbit);

            try
            {
                using (var sr = new StreamReader(ModelSchema, Encoding.Unicode))
                {
                    var parsedJson = JObject.Parse(await @sr.ReadToEndAsync());
                    var ds = JsonConvert.DeserializeObject<DataSet>(parsedJson["model"].ToString());
                    ds.name = datasetName;
                    RemoveDateTable(ds);
                    RemoveFirstColumn(ds.tables);
                    CheckIfLegal(ds);
                    UpdateMeasure(ds.tables);
                    UpdateRel(ds);
                    FilterInactiveRel(ds);
                    var output = JsonConvert.SerializeObject(ds);
                    Console.WriteLine("The new json is:");
                    Console.WriteLine(output);
                }
            }
            finally
            {
                File.Delete(ModelSchema);
            }
        }

        // This function porpuse is to extract "DataModelSchema" from the pbit file.
        static void Unzip(string pbitPath)
        {
            using (var archive = ZipFile.OpenRead(pbitPath))
            {
                foreach (var entry in archive.Entries)
                {
                    if (entry.ToString().Equals(ModelSchema, StringComparison.InvariantCultureIgnoreCase))
                    {
                        entry.ExtractToFile(entry.ToString());
                    }
                }
            }
        }

        // When a user creates a table in powerBi desktop, powerBi automatically adds to the table a column of type "rowNumber".
        // This column is redundant in a pushable dataSet, so we remove it from each table in the dataSet.
        static void RemoveFirstColumn(IList<Table> tables)
        {
            foreach (var t in tables)
            {
                t.columns = t.columns.Skip(1).ToList();
            }
        }

        // Measure property can't be null
        static void UpdateMeasure(IList<Table> tables)
        {
            foreach (Table t in tables)
            {
                t.measures = (t.measures) ?? new List<Measure>();
            }
        }

        // Checks if the dataset has unsupported dataTypes, contains any calculated tables/columns or if the dataset size exceeds the allowable limit.
        static void CheckIfLegal(DataSet ds)
        {
            int numOfTables = 0;
            foreach (var t in ds.tables)
            {
                numOfTables++;
                // (1) Check number of tables doesn't exceed maximum limit.
                if (numOfTables > ModelSizeLimit)
                {
                    throw new InvalidOperationException(string.Format("Dataset contains more then {0} tables which is not supported.", ModelSizeLimit));
                }

                int numOfColumns = 0;
                foreach (var c in t.columns)
                {
                    numOfColumns++;
                    // (2) Check number of columns doesn't exceed maximum limit.
                    if (numOfColumns > ModelSizeLimit)
                    {
                        throw new InvalidOperationException(string.Format("Dataset in table {0} contains more then {1} columns which is not supported.", t.name, ModelSizeLimit));
                    }

                    // (3) Check if model includes calculated column/table .
                    switch (c.type)
                    {
                        case "calculated":
                            throw new InvalidOperationException(string.Format("Dataset in table {0} includes an unsupported calculated column: {1}", t.name, c.type));
                        case "calculatedTableColumn":
                            throw new InvalidOperationException(string.Format("Dataset includes an unsupported calculated table: {0}", t.name));
                    }

                    // (4) Check that all dataTypes in the model are supported.
                    switch (c.dataType.ToLower())
                    {
                        case "int64":
                            continue;
                        case "double":
                            continue;
                        case "boolean":
                            continue;
                        case "datetime":
                            continue;
                        case "string":
                            continue;
                        case "decimal":
                            continue;
                        default:
                            throw new InvalidOperationException(string.Format("Dataset in column {0} of table {1} includes an unsupported dataType:{2}", t.name, c.name, c.dataType.ToLower()));
                    }

                }
            }
        }

        // Relatioship property can't be null
        static void UpdateRel(DataSet ds)
        {
            ds.relationships = (ds.relationships) ?? new List<Relationship>();
        }

        // In some cases When a pbix file i created, a calculated table with the name "DateTableTemplate" is added automatically to the dataSet as the first table.
        // Pushable dataset can't include calculated tables, so we remove this table from the dataSet
        static void RemoveDateTable(DataSet ds)
        {
            var t = ds.tables.FirstOrDefault();
            if (t == null)
            {
                return;
            }

            if (t.name.StartsWith("DateTableTemplate"))
            {
                ds.tables = ds.tables.Skip(1).ToList();
            }
        }

        // In powerBi desktop you can create inactive relatioships.
        // Pushable dataset don't support this option, so we remove every inactive relatioship.
        static void FilterInactiveRel(DataSet ds)
        {
            ds.relationships = ds.relationships.Where((r) => ((string.IsNullOrEmpty(r.isActive)) || (!r.isActive.Equals("False")))).ToList();
        }
    }
}
