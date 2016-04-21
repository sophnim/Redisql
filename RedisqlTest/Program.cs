using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Diagnostics;

using Redisql;

namespace RedisqlTest
{
    class Program
    {
        static void Main(string[] args)
        {
            Redisql.Redisql redisql = new Redisql.Redisql("127.0.0.1", 6379, "");

            List<Tuple<string, Type, bool, bool>> fieldList = new List<Tuple<string, Type, bool, bool>>()
            {
                new Tuple<string, Type, bool, bool>("name", typeof(String), false, false), // name is primaryKey field : String type, primarykey no need to index, can not be sorted
                new Tuple<string, Type, bool, bool>("level", typeof(Int32), true, true), // level, Int32 type, index required, sort required
                new Tuple<string, Type, bool, bool>("exp", typeof(Int32), false, true) // exp, Int32 type, index not required, sort required
            };
            // Create Table
            redisql.CreateTable("Account_Table", "name", fieldList).Wait();

            /*
            List<Task> tasklist = new List<Task>();
            var stw = Stopwatch.StartNew();
            int testCount = 280000;
            for (var i = 0; i < testCount; i++)
            {
                var valueDic = new Dictionary<string, string>();
                valueDic.Add("name", string.Format("bruce{0}", i));
                valueDic.Add("level", i.ToString());
                valueDic.Add("exp", "100");
                tasklist.Add(redisql.InsertTableRow("Account_Table", valueDic));
            }

            foreach (var task in tasklist) task.Wait();

            Console.WriteLine("Total {0}ms  {1} per 1ms", stw.ElapsedMilliseconds, testCount / stw.ElapsedMilliseconds);
            */

            var valueDic = new Dictionary<string, string>()
            {
                { "name", "bruce" },
                { "level", "1" },
                { "exp", "100" }
            };
            redisql.InsertTableRow("Account_Table", valueDic).Wait();

            valueDic = new Dictionary<string, string>()
            {
                { "name", "jane" },
                { "level", "2" },
                { "exp", "200" }
            };
            redisql.InsertTableRow("Account_Table", valueDic).Wait();

            valueDic = new Dictionary<string, string>()
            {
                { "name", "tom" },
                { "level", "1" },
                { "exp", "200" }
            };
            redisql.InsertTableRow("Account_Table", valueDic).Wait();

            valueDic = new Dictionary<string, string>()
            {
                { "name", "bruce" },
                { "exp", "250" }
            };
            redisql.UpdateTableRow("Account_Table", valueDic).Wait();

            valueDic = new Dictionary<string, string>()
            {
                { "name", "jane" },
                { "level", "3" }
            };
            redisql.UpdateTableRow("Account_Table", valueDic).Wait();
            
            redisql.DeleteTableRow("Account_Table", "jane").Wait();

            Console.WriteLine("select from Account_Table where primaryKeyValue == bruce");
            var task1 = redisql.SelectTableRowByPrimaryKey("Account_Table", "bruce");
            task1.Wait();
            foreach (var e in task1.Result)
            {
                Console.WriteLine("{0} : {1}", e.Key, e.Value);
            }

            Console.WriteLine();

            Console.WriteLine("select from Account_Table where level == 1");
            var task2 = redisql.SelectTableRowByIndexedField("Account_Table", "level", "1");
            task2.Wait();
            foreach (var dic in task2.Result)
            {
                foreach (var e in dic)
                {
                    Console.WriteLine("{0} : {1}", e.Key, e.Value);
                }
            }

            Console.ReadLine();
        }
        
    }
}
