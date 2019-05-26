using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
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

            // Console.WriteLine("Remove records with IndikatorValue = 0.0");
            // data = data.Where(x => x.IndikatorValue != "0.0").ToList();

            var exportData = new List<ExportData>();
            var groupedByLocation = data.GroupBy(x => x.GebietName).ToList();

            foreach (var locationGroup in groupedByLocation)
            {
                var accidents = locationGroup.Where(x => x.IndikatorName == "Unfälle [pro 1000 Einw.]");
                var schoolChildren = locationGroup.Where(x => x.IndikatorName == "Schül. Kindergarten [pro 1000 Einw.]");
                var educationSpendings = locationGroup.Where(x => x.IndikatorName == "Nettoaufwand Bildung [Fr./Einw.]");
                var transportationSpendings = locationGroup.Where(x => x.IndikatorName == "Nettoaufwand Verkehr [Fr./Einw.]");
                exportData.Add(new ExportData
                {
                    Location = locationGroup.Key,
                    AverageAccidentsPer1000Citizen = accidents
                        .DefaultIfEmpty(new Data())
                        .Average(x => Convert.ToSingle(x.IndikatorValue)),

                    AverageSchoolChildrenPer1000Citizen = schoolChildren
                        .DefaultIfEmpty(new Data())
                        .Average(x => Convert.ToSingle(x.IndikatorValue)),

                    AveragePublicSpendingsEducationPerCitizen = educationSpendings
                        .DefaultIfEmpty(new Data())
                        .Average(x => Convert.ToSingle(x.IndikatorValue)),

                    AveragePublicSpendingsTransportationPerCitizen = transportationSpendings
                        .DefaultIfEmpty(new Data())
                        .Average(x => Convert.ToSingle(x.IndikatorValue))
                });
            }
            Console.WriteLine($"Reduced and converted {data.Count} entries to {exportData.Count}");
            Console.WriteLine("Begin CSV-Export of min-max-normalized values");

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
                    // Normalization could be optimized by calculating the min and max beforehand.
                    writer.WriteLine(string.Join(";",
                        d.Location,
                        Normalize(d.AverageAccidentsPer1000Citizen, exportData, x => x.AverageAccidentsPer1000Citizen),
                        Normalize(d.AverageSchoolChildrenPer1000Citizen, exportData, x => x.AverageSchoolChildrenPer1000Citizen),
                        Normalize(d.AveragePublicSpendingsTransportationPerCitizen, exportData, x => x.AveragePublicSpendingsTransportationPerCitizen),
                        Normalize(d.AveragePublicSpendingsEducationPerCitizen, exportData, x => x.AveragePublicSpendingsEducationPerCitizen)));
                }
            }

            Console.WriteLine("done");

            Console.ReadLine();
        }

        private static float Normalize(float actualValue, IReadOnlyCollection<ExportData> exportData, Func<ExportData, float> selector)
        {
            var min = exportData.Min(selector);
            var max = exportData.Max(selector);
            return (actualValue - min) / (max - min);
        }
    }

    class ExportData
    {
        public string Location { get; set; }
        public float AverageAccidentsPer1000Citizen { get; set; }
        public float AverageSchoolChildrenPer1000Citizen { get; set; }
        public float AveragePublicSpendingsTransportationPerCitizen { get; set; }
        public float AveragePublicSpendingsEducationPerCitizen { get; set; }
    }
}
