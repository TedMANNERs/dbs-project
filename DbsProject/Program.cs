using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using StackExchange.Redis;

namespace DbsProject
{
    class Program
    {
        static void Main(string[] args)
        {
            IList<string> values = new List<string>();

            Console.WriteLine("Begin import");
            using (var muxer = ConnectionMultiplexer.Connect("localhost"))
            {
                var server = muxer.GetServer("localhost", 6379);
                var db = muxer.GetDatabase();

                foreach (var key in server.Keys(pattern: "*"))
                {
                    Console.WriteLine(key);
                    var value = db.StringGet(key);
                    values.Add(value);
                }
            }

            Console.ReadLine();
        }
    }
}
