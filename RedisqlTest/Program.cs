﻿using System;
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
            Tests tests = new Tests();
            tests.AsyncTest();

            Console.ReadLine();
        }
        
    }
}
