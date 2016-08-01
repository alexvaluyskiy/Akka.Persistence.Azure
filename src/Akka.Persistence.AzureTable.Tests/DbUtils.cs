//-----------------------------------------------------------------------
// <copyright file="DbUtils.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2016 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2016 Akka.NET project <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------


using System;
using System.Configuration;
using System.Linq;
using StackExchange.Redis;

namespace Akka.Persistence.AzureTable.Tests
{
    public static class DbUtils
    {
        public static void Clean(string keyPrefix)
        {
            var connectionString = ConfigurationManager.ConnectionStrings["redis"].ConnectionString;
            var database = Convert.ToInt32(ConfigurationManager.AppSettings["redisDatabase"]);

            var redisConnection = ConnectionMultiplexer.Connect(connectionString);
            var server = redisConnection.GetServer(redisConnection.GetEndPoints().First());
            var db = redisConnection.GetDatabase(database);
            foreach (var key in server.Keys(database: database, pattern: $"{keyPrefix}:*"))
            {
                db.KeyDelete(key);
            }
        }
    }
}
