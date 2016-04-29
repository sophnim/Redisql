using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Redisql.Helper
{
    public class RedisqlHelper
    {
        public static void PrintRow(Dictionary<string, string> row)
        {
            foreach (var e in row.Keys)
            {
                Console.Write("{0,-10}", e);
            }

            Console.WriteLine();

            foreach (var e in row.Values)
            {
                Console.Write("{0,-10}", e);
            }

            Console.WriteLine();
        }

        public static void PrintRows(List<Dictionary<string, string>> rows)
        {
            foreach (var e in rows[0].Keys)
            {
                Console.Write("{0,-10}", e);
            }

            Console.WriteLine();

            foreach (var row in rows)
            {
                foreach (var e in row.Values)
                {
                    Console.Write("{0,-10}", e);
                }
                Console.WriteLine();
            }
        }
    }
}
