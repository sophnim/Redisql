﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Diagnostics;

using Redisql.Core;
using Redisql.Helper;
using Redisql.Transaction;

namespace RedisqlTest
{
    public class Tests
    {
        RedisqlCore redisql;

        public Tests()
        {
            Console.WriteLine("Create RedisqlCore");
            this.redisql = new RedisqlCore("127.0.0.1", 6379, "");
            this.redisql.OnEvent += OnRedisqlEventHander;
        }

        private void OnRedisqlEventHander(Object sender, EventArgs args)
        {
            var e = args as RedisqlEventArgs;
            Console.WriteLine("{0} {1}", e.EventType, e.Message);
        }

        public async void AsyncTest()
        {

            // For the test, Clear all table lock 
            await this.redisql.TableLockClearAllAsync();

            // Prepare to create table : construct table column config list
            var columnConfigList = new List<ColumnConfig>()
            {
                new ColumnConfig("name", typeof(String), null, true, false),     // Column 'name', String type, default null means user must specify the column value(no default value).
                new ColumnConfig("level", typeof(Int32), 1, true, true),        // Column 'level', Int32 type, default 1
                new ColumnConfig("exp", typeof(Int32), 0, true, true),          // Column 'exp', Int32 type, default 0
                new ColumnConfig("money", typeof(Int32), 1000, false, true),     // Column 'money', Int32 type, default 1000
                new ColumnConfig("time", typeof(DateTime), "now", false, true)   // Column 'time', DateTime type, default "now" means current local time("utcnow" will have a current utc time)
            };

            // Create table : table name: Account_Table, primary key: name
            await this.redisql.TableCreateAsync("Account_Table", columnConfigList, "name");

            // prepare to insert table row: column name - value dictionary
            var valueDic = new Dictionary<string, string>()
            {
                { "name", "bruce" },
                { "level", "1" },
                { "exp", "100" }
            };
            // insert row to Account_Table and get row id. row id is auto-increment generated value.
            var rowid = await this.redisql.TableInsertRowAsync("Account_Table", valueDic);

            // insert another row : unspecified column will be filled with default value
            valueDic = new Dictionary<string, string>()
            {
                { "name", "jane" },
                { "level", "2" },
                { "exp", "200" }
            };
            var ret = await this.redisql.TableInsertRowAsync("Account_Table", valueDic);

            // insert another row
            valueDic = new Dictionary<string, string>()
            {
                { "name", "tom" },
                { "level", "1" },
                { "exp", "300" }
            };
            await this.redisql.TableInsertRowAsync("Account_Table", valueDic);

            // update row
            valueDic = new Dictionary<string, string>()
            {
                { "name", "bruce" },
                { "exp", "250" }
            };
            await this.redisql.TableUpdateRowAsync("Account_Table", valueDic);

            // update row : using transaction
            var tran = new RedisqlTransaction(this.redisql, new List<TransactionTarget>()
            {
                new TransactionTarget("Account_Table", "bruce"),    // specify row to transaction 
                new TransactionTarget("Account_Table", "jane")      // specify row to transaction  
            });

            if (await tran.TryBeginTransactionAsync()) 
            {
                // succeeded to begin transaction : table row locked. 
                var dic1 = await tran.TableSelectRowAsync(new List<string> { "name", "exp" }, "Account_Table", "bruce"); // read value from row
                var dic2 = await tran.TableSelectRowAsync(new List<string> { "name", "exp" }, "Account_Table", "jane"); // read value from row

                var exp1 = Convert.ToInt32(dic1["exp"]);
                dic1["exp"] = (exp1 + 10).ToString();
                await tran.TableUpdateRowAsync("Account_Table", dic1); // update row

                var exp2 = Convert.ToInt32(dic1["exp"]);
                dic2["exp"] = (exp2 + 10).ToString();
                await tran.TableUpdateRowAsync("Account_Table", dic2); // update row

                await tran.EndTransactionAsync(); // transaction end : table row unlocked.
            }
            else
            {
                // failed to begin transaction : Other transaction is ongoing.
                Console.WriteLine("Begin transaction fail");
            }

            // select a row that have a primary key value "bruce"
            Console.WriteLine("select * from Account_Table where name = bruce");
            var row = await redisql.TableSelectRowAsync(null, "Account_Table", "bruce");
            RedisqlHelper.PrintRow(row);

            Console.WriteLine("");

            // specify column name to select
            Console.WriteLine("select _id, name, level from Account_Table where name = bruce");
            row = await redisql.TableSelectRowAsync(new List<string> { "_id", "name", "level" }, "Account_Table", "bruce");
            RedisqlHelper.PrintRow(row);

            Console.WriteLine("");

            // select rows that matches value with match index column
            Console.WriteLine("select * from Account_Table where level == 1");
            var rows = await redisql.TableSelectRowAsync(null, "Account_Table", "level", compareColumnValue:"1");
            RedisqlHelper.PrintRows(rows);

            Console.WriteLine("");

            // select rows that has a proper range column value
            Console.WriteLine("select * from Account_Table where 250 <= exp <= 300");
            rows = await redisql.TableSelectRowAsync(null, "Account_Table", "exp", lowValue:"250", highValue:"300");
            RedisqlHelper.PrintRows(rows);


            // high-stress transaction test

            ThreadPool.SetMinThreads(50, 100);

            for (var i = 1; i <= 10; i++)
            {
                Task.Run(() =>
                {
                    while (true)
                    {
                        var tran2 = new RedisqlTransaction(this.redisql, new List<TransactionTarget>()
                        {
                        new TransactionTarget("Account_Table", "bruce"),
                        new TransactionTarget("Account_Table", "jane")
                        });

                        if (tran2.TryBeginTransaction())
                        {
                            var dic1 = tran2.TableSelectRow(new List<string> { "name", "exp" }, "Account_Table", "bruce");
                            var dic2 = tran2.TableSelectRow(new List<string> { "name", "exp" }, "Account_Table", "jane");

                            var exp1 = Convert.ToInt32(dic1["exp"]);
                            dic1["exp"] = (exp1 + 10).ToString();
                            tran2.TableUpdateRow("Account_Table", dic1);

                            var exp2 = Convert.ToInt32(dic1["exp"]);
                            dic2["exp"] = (exp2 + 10).ToString();
                            tran2.TableUpdateRow("Account_Table", dic2);

                            tran2.EndTransaction();
                        }
                        else
                        {
                            // failed to begin transaction
                            Console.WriteLine("Failed to begin transaction");
                        }
                        //Task.Delay(1).Wait();
                    }
                });
            }
            
            for (var i = 0; i <= 10; i++)
            {
                Task.Run(() =>
                {
                    while (true)
                    {
                        valueDic = new Dictionary<string, string>()
                        {
                        { "name", "bruce" },
                        { "exp", "250" }
                        };

                        if (!this.redisql.TableUpdateRow("Account_Table", valueDic))
                        {
                            Console.WriteLine("TableUpdateRow Fail!");
                        }

                        //Task.Delay(1).Wait();
                    }
                });
            }
            

            Console.WriteLine("\n\nEnd of Test");
        }


        public void Test2()
        {
            var columnConfigList = new List<ColumnConfig>()
            {
                new ColumnConfig("name", typeof(String), null), 
                new ColumnConfig("level", typeof(Int32), 1), 
                new ColumnConfig("exp", typeof(Int32), 0), 
                new ColumnConfig("profile", typeof(String), "") 
            };
            // Create Table
            this.redisql.TableCreateAsync("Account_Table", columnConfigList, "name").Wait();

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
            redisql.TableCreateAsync("Account_Table", columnConfigList, "_id").Wait();

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
            redisql.TableCreateAsync("Account_Table", columnConfigList, "_id").Wait(); // primary key field is '_id'. _id is auto generated field that auto incremented when insert.

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
