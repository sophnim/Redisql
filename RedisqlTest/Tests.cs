using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Diagnostics;

using Redisql;

namespace RedisqlTest
{
    public class Tests
    {
        public void Test1()
        {
            Redisql.Redisql redisql = new Redisql.Redisql("192.168.25.5", 6379, "");

            List<Tuple<string, Type, bool, bool, object>> fieldList = new List<Tuple<string, Type, bool, bool, object>>()
            {
                new Tuple<string, Type, bool, bool, object>("name", typeof(String), true, false, null), // name, String type, index required, string can not be sorted, Default null means null value not allowed
                new Tuple<string, Type, bool, bool, object>("level", typeof(Int32), true, true, 1), // level, Int32 type, index required, sort required, Default 1
                new Tuple<string, Type, bool, bool, object>("exp", typeof(Int32), false, true, 0), // exp, Int32 type, index not required, sort required, Default 0
                new Tuple<string, Type, bool, bool, object>("money", typeof(Int32), false, true, 1000), // money, Int32 type, index not required, sort required, Default 1000
                new Tuple<string, Type, bool, bool, object>("time", typeof(DateTime), false, true, "now"), // time, DateTime type, index not required, sort required, Default now
            };
            // Create Table
            redisql.TableCreateAsync("Account_Table", "_id", fieldList).Wait(); // primary key field is '_id'. _id is auto generated field that auto incremented when insert.

            var valueDic = new Dictionary<string, string>()
            {
                { "name", "bruce" },
                { "level", "1" },
                { "exp", "100" }
            };
            var task1 = redisql.TableInsertRowAsync("Account_Table", valueDic);
            task1.Wait();
            var id1 = task1.Result; // get table row _id

            valueDic = new Dictionary<string, string>()
            {
                { "name", "jane" },
                { "level", "2" },
                { "exp", "200" }
            };
            var task2 = redisql.TableInsertRowAsync("Account_Table", valueDic);
            task2.Wait();
            var id2 = task2.Result;

            valueDic = new Dictionary<string, string>()
            {
                { "name", "tom" },
                { "level", "1" },
                { "exp", "300" }
            };
            var task3 = redisql.TableInsertRowAsync("Account_Table", valueDic);
            task3.Wait();
            var id3 = task3.Result;

            valueDic = new Dictionary<string, string>()
            {
                { "_id", id1.ToString() },
                { "name", "bruce" },
                { "exp", "250" }
            };
            redisql.TableUpdateRowAsync("Account_Table", valueDic).Wait();

            valueDic = new Dictionary<string, string>()
            {
                { "_id", id2.ToString() },
                { "name", "jane" },
                { "level", "2" }
            };
            redisql.TableUpdateRowAsync("Account_Table", valueDic).Wait();

            //redisql.DeleteTableRow("Account_Table", "jane").Wait();

            Console.WriteLine("select _id, name, level from Account_Table where primaryKeyValue == bruce");
            var task4 = redisql.TableSelectRowByPrimaryKeyFieldMatchAsync(new List<string> { "_id", "name", "level" }, "Account_Table", "bruce");
            task4.Wait();
            foreach (var e in task4.Result)
            {
                Console.WriteLine("{0} : {1}", e.Key, e.Value);
            }

            Console.WriteLine();

            Console.WriteLine("select name, level from Account_Table where level == 1");
            var task5 = redisql.TableSelectRowByIndexFieldMatchAsync(new List<string> { "name", "level" }, "Account_Table", "level", "1");
            task5.Wait();
            foreach (var dic in task5.Result)
            {
                foreach (var e in dic)
                {
                    Console.WriteLine("{0} : {1}", e.Key, e.Value);
                }
            }

            Console.WriteLine();

            Console.WriteLine("select name, level, exp from Account_Table where 0 <= exp <= 300");
            var task6 = redisql.TableSelectRowBySortFieldRangeAsync(new List<string> { "name", "level", "exp" }, "Account_Table", "exp", "0", "300");
            task6.Wait();
            foreach (var dic in task6.Result)
            {
                foreach (var e in dic)
                {
                    Console.WriteLine("{0} : {1}", e.Key, e.Value);
                }
            }

            Console.WriteLine();

            Console.WriteLine("select * from Account_Table where 250 <= exp <= 300");
            var task7 = redisql.TableSelectRowBySortFieldRangeAsync(null, "Account_Table", "exp", "250", "300");
            task7.Wait();
            foreach (var dic in task7.Result)
            {
                foreach (var e in dic)
                {
                    Console.WriteLine("{0} : {1}", e.Key, e.Value);
                }
            }

            Console.WriteLine();

            Console.WriteLine("select name, level from Account_Table where 1 <= level <= 2");
            var task8 = redisql.TableSelectRowBySortFieldRangeAsync(new List<string> { "name", "level" }, "Account_Table", "level", "1", "2");
            task8.Wait();
            foreach (var dic in task8.Result)
            {
                foreach (var e in dic)
                {
                    Console.WriteLine("{0} : {1}", e.Key, e.Value);
                }
            }
        }


        public void Test2()
        {
            Redisql.Redisql redisql = new Redisql.Redisql("192.168.25.5", 6379, "");

            List<Tuple<string, Type, bool, bool, object>> fieldList = new List<Tuple<string, Type, bool, bool, object>>()
            {
                new Tuple<string, Type, bool, bool, object>("name", typeof(String), false, false, null), // name is primaryKey field : String type, primarykey no need to index, can not be sorted
                new Tuple<string, Type, bool, bool, object>("level", typeof(Int32), true, true, 1), // level, Int32 type, index required, sort required
                new Tuple<string, Type, bool, bool, object>("exp", typeof(Int32), false, false, 0), // exp, Int32 type, index not required, sort required
                new Tuple<string, Type, bool, bool, object>("profile", typeof(String), false, false, "") // exp, Int32 type, index not required, sort required
            };
            // Create Table
            redisql.TableCreateAsync("Account_Table", "name", fieldList).Wait();
            
            List<Task> tasklist = new List<Task>();
            var stw = Stopwatch.StartNew();
            int testCount = 10;
            for (var i = 0; i < testCount; i++)
            {
                var valueDic = new Dictionary<string, string>() {
                    { "name", string.Format("bruce{0}", i) },
                    { "level", i.ToString() },
                    { "exp", "100" },
                    { "profile", "this is test account" }
                };
                tasklist.Add(redisql.TableInsertRowAsync("Account_Table", valueDic));
            }

            foreach (var task in tasklist) task.Wait();

            Console.WriteLine("Total {0}ms  {1} per 1ms", stw.ElapsedMilliseconds, testCount / stw.ElapsedMilliseconds);

            GC.Collect();

            Console.WriteLine();
            stw.Restart();

            redisql.TableRemoveIndexAsync("Account_Table", "level").Wait();

            Console.WriteLine("Remove Index: Total {0}ms", stw.ElapsedMilliseconds);

            Console.WriteLine();
            stw.Restart();

            redisql.TableAddIndexAsync("Account_Table", "level").Wait();

            Console.WriteLine("Add Index: Total {0}ms", stw.ElapsedMilliseconds);

            Console.WriteLine();
            stw.Restart();

            redisql.TableAddRangeIndexAsync("Account_Table", "exp").Wait();

            Console.WriteLine("Add Range Index: Total {0}ms", stw.ElapsedMilliseconds);

            Console.WriteLine();
            stw.Restart();

            redisql.TableRemoveRangeIndexAsync("Account_Table", "exp").Wait();

            Console.WriteLine("Remove Range Index: Total {0}ms", stw.ElapsedMilliseconds);

            Console.WriteLine();
            stw.Restart();

            redisql.TableAddNewFieldAsync("Account_Table", "age", typeof(Int32), true, true, 1).Wait();

            Console.WriteLine("Add New Field: Total {0}ms", stw.ElapsedMilliseconds);

            Console.WriteLine();
            stw.Restart();

            redisql.TableEraseExistingFieldAsync("Account_Table", "age").Wait();
            redisql.TableEraseExistingFieldAsync("Account_Table", "exp").Wait();

            Console.WriteLine("Erase Existing Field: Total {0}ms", stw.ElapsedMilliseconds);

            Console.WriteLine();
            stw.Restart();

            redisql.TableAddNewFieldAsync("Account_Table", "time", typeof(DateTime), true, true, "now").Wait();

            Console.WriteLine("Re-Add New Field: Total {0}ms", stw.ElapsedMilliseconds);

            Console.WriteLine();
            stw.Restart();

            redisql.TableDeleteAsync("Account_Table").Wait();

            Console.WriteLine("Delete Table: Total {0}ms", stw.ElapsedMilliseconds);

            Console.WriteLine();
            stw.Restart();

            Console.WriteLine("Re-Create Table");
            redisql.TableCreateAsync("Account_Table", "_id", fieldList).Wait();

            var newRowDic = new Dictionary<string, string>() {
                    { "name", "mike" },
                    { "level", "2" },
                    { "exp", "200" },
                    { "profile", "this is test account" }
                };
            tasklist.Add(redisql.TableInsertRowAsync("Account_Table", newRowDic));
            /*
            Console.WriteLine();
            stw.Restart();
            
            Console.WriteLine("select from Account_Table where 1 <= level <= 1000");
            var task2 = redisql.SelectTableRowBySortFieldRange(null, "Account_Table", "level", "1", "1000");
            task2.Wait();
            foreach (var dic in task2.Result)
            {
                foreach (var e in dic)
                {
                    Console.WriteLine("{0} : {1}", e.Key, e.Value);
                }
            }

            Console.WriteLine("Total {0}ms", stw.ElapsedMilliseconds);
            */
        }
    }
}
