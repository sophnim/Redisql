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
            Redisql.Redisql redisql = new Redisql.Redisql("127.0.0.1", 6379, "");

            var task0 = redisql.TableGetSettingAsync("Account_Table");
            task0.Wait();
            var ts = task0.Result;

            var columnConfigList = new List<ColumnConfig>()
            {
                new ColumnConfig("name", typeof(String), null), 
                new ColumnConfig("level", typeof(Int32), 1), 
                new ColumnConfig("exp", typeof(Int32), 0), 
                new ColumnConfig("money", typeof(Int32), 1000), 
                new ColumnConfig("time", typeof(DateTime), "now") 
            };
            // Create Table
            redisql.TableCreateAsync("Account_Table", "name", columnConfigList).Wait(); 

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
                { "name", "bruce" },
                { "exp", "250" }
            };
            redisql.TableUpdateRowAsync("Account_Table", valueDic).Wait();

            valueDic = new Dictionary<string, string>()
            {
                { "name", "jane" },
                { "level", "2" }
            };
            redisql.TableUpdateRowAsync("Account_Table", valueDic).Wait();

            //redisql.DeleteTableRow("Account_Table", "jane").Wait();

            
            Task.Run(() =>
            {
                while (true)
                {
                    Console.WriteLine("select _id, name, level from Account_Table where primaryKeyValue == bruce");
                    var task4 = redisql.TableSelectRowByPrimaryKeyColumnValueAsync(new List<string> { "_id", "name", "level" }, "Account_Table", "bruce");
                    task4.Wait();
                    foreach (var e in task4.Result)
                    {
                        Console.Write("{0} : {1} ", e.Key, e.Value);
                    }
                    Console.WriteLine("\n\n");
                }
            });


            Task.Run(() =>
            {
                while (true)
                {
                    Console.WriteLine("select name, level from Account_Table where level == 1");
                    var task5 = redisql.TableSelectRowByMatchIndexColumnValueAsync(new List<string> { "name", "level" }, "Account_Table", "level", "1");
                    task5.Wait();
                    foreach (var dic in task5.Result)
                    {
                        foreach (var e in dic)
                        {
                            Console.Write("{0} : {1} ", e.Key, e.Value);
                        }
                        Console.WriteLine();
                    }

                    Console.WriteLine("\n\n");
                }
            });


            Task.Run(() =>
            {
                while (true)
                {
                    Console.WriteLine("select name, level, exp from Account_Table where 0 <= exp <= 300");
                    var task6 = redisql.TableSelectRowByRangeIndexAsync(new List<string> { "name", "level", "exp" }, "Account_Table", "exp", "0", "300");
                    task6.Wait();
                    foreach (var dic in task6.Result)
                    {
                        foreach (var e in dic)
                        {
                            Console.Write("{0} : {1} ", e.Key, e.Value);
                        }
                        Console.WriteLine();
                    }

                    Console.WriteLine("\n\n");
                }
            });


            Task.Run(() =>
            {
                while (true)
                {
                    Console.WriteLine("select * from Account_Table where 250 <= exp <= 300");
                    var task7 = redisql.TableSelectRowByRangeIndexAsync(null, "Account_Table", "exp", "250", "300");
                    task7.Wait();
                    foreach (var dic in task7.Result)
                    {
                        foreach (var e in dic)
                        {
                            Console.Write("{0} : {1} ", e.Key, e.Value);
                        }
                        Console.WriteLine();
                    }

                    Console.WriteLine("\n\n");
                }
            });
            

            Task.Run(() =>
            {
                while (true)
                {
                    Console.WriteLine("select name, level from Account_Table where 1 <= level <= 2");
                    var task8 = redisql.TableSelectRowByRangeIndexAsync(new List<string> { "name", "level" }, "Account_Table", "level", "1", "2");
                    task8.Wait();
                    foreach (var dic in task8.Result)
                    {
                        foreach (var e in dic)
                        {
                            Console.Write("{0} : {1} ", e.Key, e.Value);
                        }
                        Console.WriteLine();
                    }

                    Console.WriteLine("\n\n");
                }
            });

            Console.WriteLine("\n\nEnd of Test");
        }


        public void Test2()
        {
            Redisql.Redisql redisql = new Redisql.Redisql("127.0.0.1", 6379, "");

            var columnConfigList = new List<ColumnConfig>()
            {
                new ColumnConfig("name", typeof(String), null), 
                new ColumnConfig("level", typeof(Int32), 1), 
                new ColumnConfig("exp", typeof(Int32), 0), 
                new ColumnConfig("profile", typeof(String), "") 
            };
            // Create Table
            redisql.TableCreateAsync("Account_Table", "name", columnConfigList).Wait();

            List<Task> tasklist = new List<Task>();
            var stw = Stopwatch.StartNew();
            int testCount = 1000;
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

            
            // select all rows
            foreach (var row in redisql.TableSelectRowAll(null, "Account_Table"))
            {
                foreach (var e in row)
                {
                    Console.Write("{0}:{1} ", e.Key, e.Value);
                }
                Console.WriteLine("");
            }
            

            Console.WriteLine();
            stw.Restart();

            redisql.TableRemoveMatchIndexAsync("Account_Table", "level").Wait();

            Console.WriteLine("Remove Index: Total {0}ms", stw.ElapsedMilliseconds);

            Console.WriteLine();
            stw.Restart();

            redisql.TableAddMatchIndexAsync("Account_Table", "level").Wait();

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

            redisql.TableCreateNewColumnAsync("Account_Table", "age", typeof(Int32), true, true, 1).Wait();

            Console.WriteLine("Add New Field: Total {0}ms", stw.ElapsedMilliseconds);

            Console.WriteLine();
            stw.Restart();

            redisql.TableDeleteExistingColumnAsync("Account_Table", "age").Wait();
            redisql.TableDeleteExistingColumnAsync("Account_Table", "exp").Wait();

            Console.WriteLine("Erase Existing Field: Total {0}ms", stw.ElapsedMilliseconds);

            Console.WriteLine();
            stw.Restart();

            redisql.TableCreateNewColumnAsync("Account_Table", "time", typeof(DateTime), true, true, "now").Wait();

            Console.WriteLine("Re-Add New Field: Total {0}ms", stw.ElapsedMilliseconds);

            Console.WriteLine();
            stw.Restart();

            redisql.TableDeleteAsync("Account_Table").Wait();

            Console.WriteLine("Delete Table: Total {0}ms", stw.ElapsedMilliseconds);

            Console.WriteLine();
            stw.Restart();

            Console.WriteLine("Re-Create Table");
            redisql.TableCreateAsync("Account_Table", "_id", columnConfigList).Wait();

            var newRowDic = new Dictionary<string, string>() {
                    { "name", "mike" },
                    { "level", "2" },
                    { "exp", "200" },
                    { "profile", "this is test account" }
                };
            tasklist.Add(redisql.TableInsertRowAsync("Account_Table", newRowDic));

            Console.WriteLine();
            
            

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

        public void Test3()
        {
            Redisql.Redisql redisql = new Redisql.Redisql("127.0.0.1", 6379, "");

            var task0 = redisql.TableGetSettingAsync("Account_Table");
            task0.Wait();
            var ts = task0.Result;

            var columnConfigList = new List<ColumnConfig>()
            {
                new ColumnConfig("name", typeof(String), null), 
                new ColumnConfig("level", typeof(Int32), 1), 
                new ColumnConfig("exp", typeof(Int32), 0), 
                new ColumnConfig("money", typeof(Int32), 1000), 
                new ColumnConfig("time", typeof(DateTime), "now"), 
            };
            // Create Table
            redisql.TableCreateAsync("Account_Table", "_id", columnConfigList).Wait(); // primary key field is '_id'. _id is auto generated field that auto incremented when insert.

            var stw = Stopwatch.StartNew();

            Console.WriteLine("Inserting...");

            int count = 100000;
            List<Task<long>> tasklist = new List<Task<long>>();
            for (var i = 0; i < count; i++)
            {
                var valueDic = new Dictionary<string, string>()
                {
                    { "name", string.Format("test{0}", i) },
                    { "level", i.ToString() },
                    { "exp", "0" }
                };
                tasklist.Add(redisql.TableInsertRowAsync("Account_Table", valueDic));
            }

            foreach (var task in tasklist)
                task.Wait();

            Console.WriteLine("{0}ms {1}per ms", stw.ElapsedMilliseconds, count / stw.ElapsedMilliseconds);
            stw.Restart();

            Console.WriteLine("Selecting...");

            count = 0;
            foreach (var row in redisql.TableSelectRowAll(null, "Account_Table"))
            {
                count++;
                /*
                foreach (var e in row)
                    Console.Write("{0}:{1} ", e.Key, e.Value);

                Console.WriteLine();
                */
            }

            Console.WriteLine("{0}ms", stw.ElapsedMilliseconds);

            Console.WriteLine("Done! {0}", count);
        }
    }
}
