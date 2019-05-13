using System;
using System.Collections.Generic;
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

            var groupedByLocation = data.GroupBy(x => x.GebietName).ToList();
            groupedByLocation.Select(x => x.s);

            Console.ReadLine();
        }
    }
}
