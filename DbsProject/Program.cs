using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Schema;
using StackExchange.Redis;

namespace DbsProject
{
    class Program
    {
        static void Main(string[] args)
        {
            IList<Data> data = new List<Data>();

            Console.WriteLine("Begin import");
            using (var muxer = ConnectionMultiplexer.Connect("localhost"))
            {
                var server = muxer.GetServer("localhost", 6379);
                var db = muxer.GetDatabase();

                var keys = server.Keys(pattern: "*");
                foreach (var key in keys)
                {
                    Console.WriteLine($"import \"{key}\"");
                    var streamEntries = db.StreamRange(key);
                    foreach (var entry in streamEntries)
                    {
                        data.Add(new Data
                        {
                            BfsNr = entry.Values.First(x => x.Name == "BFS_NR").Value,
                            GebietName = entry.Values.First(x => x.Name == "GEBIET_NAME").Value,
                            ThemaName = entry.Values.First(x => x.Name == "THEMA_NAME").Value,
                            SetName = entry.Values.First(x => x.Name == "SET_NAME").Value,
                            SubsetName = entry.Values.First(x => x.Name == "SUBSET_NAME").Value,
                            IndikatorId = entry.Values.First(x => x.Name == "INDIKATOR_ID").Value,
                            IndikatorName = entry.Values.First(x => x.Name == "INDIKATOR_NAME").Value,
                            IndikatorJahr = entry.Values.First(x => x.Name == "INDIKATOR_JAHR").Value,
                            IndikatorValue = entry.Values.First(x => x.Name == "INDIKATOR_VALUE").Value,
                            EinheitKurz = entry.Values.First(x => x.Name == "EINHEIT_KURZ").Value,
                            EinheitLang = entry.Values.First(x => x.Name == "EINHEIT_LANG").Value
                        });
                    }
                    Console.WriteLine("done");
                }
                Console.WriteLine("End of import");
            }
            var exportData = new List<ExportData>();
            var groupedByLocation = data.GroupBy(x => x.GebietName).ToList();

            //foreach (Data d in data)
            //{
            //    if (!exportData.ContainsKey(d.GebietName))
            //        exportData[d.GebietName] = new ExportData();

            //    exportData[d.GebietName].Year = Convert.ToInt32(d.IndikatorJahr);
            //    switch (d.IndikatorName)
            //    {
            //        case "Unfälle [pro 1000 Einw.]":
            //            exportData[d.GebietName].AccidentsPer1000Citizen = Convert.ToSingle(d.IndikatorValue);
            //            break;
            //        case "Schül. Kindergarten [pro 1000 Einw.]":
            //            exportData[d.GebietName].SchoolChildrenPer1000Citizen = Convert.ToSingle(d.IndikatorValue);
            //            break;
            //        case "Nettoaufwand Bildung [Fr./Einw.]":
            //            exportData[d.GebietName].PublicSpendingsEducationPerCitizen = Convert.ToSingle(d.IndikatorValue);
            //            break;
            //        case "Nettoaufwand Verkehr [Fr./Einw.]":
            //            exportData[d.GebietName].PublicSpendingsTransportationPerCitizen = Convert.ToSingle(d.IndikatorValue);
            //            break;
            //        default:
            //            break;
            //    }
            //}

            foreach (var locationGroup in groupedByLocation)
            {
                var accidents = locationGroup.Where(x => x.IndikatorName == "Unfälle [pro 1000 Einw.]");
                var schoolChildren = locationGroup.Where(x => x.IndikatorName == "Schül. Kindergarten [pro 1000 Einw.]");
                var educationSpendings = locationGroup.Where(x => x.IndikatorName == "Nettoaufwand Bildung [Fr./Einw.]");
                var transportationSpendings = locationGroup.Where(x => x.IndikatorName == "Nettoaufwand Verkehr [Fr./Einw.]");
                exportData.Add(new ExportData
                {
                    Location = locationGroup.Key,
                    AverageAccidentsPer1000Citizen = accidents.DefaultIfEmpty(new Data()).Average(x => Convert.ToSingle(x.IndikatorValue)),
                    AverageSchoolChildrenPer1000Citizen = schoolChildren.DefaultIfEmpty(new Data()).Average(x => Convert.ToSingle(x.IndikatorValue)),
                    AveragePublicSpendingsEducationPerCitizen = educationSpendings.DefaultIfEmpty(new Data()).Average(x => Convert.ToInt32(x.IndikatorValue)),
                    AveragePublicSpendingsTransportationPerCitizen = transportationSpendings.DefaultIfEmpty(new Data()).Average(x => Convert.ToInt32(x.IndikatorValue))
                });
            }
            Console.WriteLine($"Reduced and converted {data.Count} entries to {exportData.Count}");
            Console.WriteLine("Begin CSV-Export");

            using (StreamWriter writer = new StreamWriter("dbs-export.csv", false))
            {
                writer.WriteLine(string.Join(";",
                    nameof(ExportData.Location),
                    nameof(ExportData.AverageAccidentsPer1000Citizen),
                    nameof(ExportData.AverageSchoolChildrenPer1000Citizen),
                    nameof(ExportData.AveragePublicSpendingsTransportationPerCitizen),
                    nameof(ExportData.AveragePublicSpendingsEducationPerCitizen)));

                foreach (var d in exportData)
                {
                    writer.WriteLine(string.Join(";",
                        d.Location,
                        d.AverageAccidentsPer1000Citizen,
                        d.AverageSchoolChildrenPer1000Citizen,
                        d.AveragePublicSpendingsTransportationPerCitizen,
                        d.AveragePublicSpendingsEducationPerCitizen));
                }
            }

            Console.WriteLine("done");

            Console.ReadLine();
        }
    }

    class ExportData
    {
        public string Location { get; set; }
        public float AverageAccidentsPer1000Citizen { get; set; }
        public float AverageSchoolChildrenPer1000Citizen { get; set; }
        public double AveragePublicSpendingsTransportationPerCitizen { get; set; }
        public double AveragePublicSpendingsEducationPerCitizen { get; set; }
    }
}
