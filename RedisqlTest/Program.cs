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


            List<Tuple<string, Type, bool>> fieldList = new List<Tuple<string, Type, bool>>();
            fieldList.Add(new Tuple<string, Type, bool>("name", typeof(String), true));
            fieldList.Add(new Tuple<string, Type, bool>("level", typeof(Int32), true));
            fieldList.Add(new Tuple<string, Type, bool>("exp", typeof(Int32), false));

            redisql.CreateTable("Account_Table", "name", fieldList).Wait();

            //var valueDic = new Dictionary<string, string>();

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

            /*
            valueDic.Clear();
            valueDic.Add("name", "jane");
            valueDic.Add("level", "1");
            valueDic.Add("exp", "0");
            redisql.InsertTableRow("Account_Table", valueDic).Wait();

            valueDic.Clear();
            valueDic.Add("name", "jane");
            valueDic.Add("level", "2");
            valueDic.Add("exp", "10");
            redisql.UpdateTableRow("Account_Table", valueDic).Wait();

            valueDic.Clear();
            valueDic.Add("name", "jane");
            valueDic.Add("level", "50");
            valueDic.Add("exp", "10");
            redisql.UpdateTableRow("Account_Table", valueDic).Wait();

            valueDic.Clear();
            valueDic.Add("name", "bruce");
            valueDic.Add("level", "51");
            valueDic.Add("exp", "12");
            redisql.UpdateTableRow("Account_Table", valueDic).Wait();

            redisql.DeleteTableRow("Account_Table", "jane").Wait();
            */
            Console.ReadLine();
        }
    }
}
