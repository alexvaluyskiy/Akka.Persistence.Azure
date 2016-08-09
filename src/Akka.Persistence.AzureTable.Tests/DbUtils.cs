//-----------------------------------------------------------------------
// <copyright file="DbUtils.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2016 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2016 Akka.NET project <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using System.Collections.Generic;
using System.Linq;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace Akka.Persistence.AzureTable.Tests
{
    public static class DbUtils
    {
        public static void Clean(string connectionString, string tableName)
        {
            CloudTableClient tableClient = CloudStorageAccount.Parse(connectionString).CreateCloudTableClient();
            CloudTable table = tableClient.GetTableReference(tableName);
            table.CreateIfNotExists();
            TableQuery<DynamicTableEntity> query = new TableQuery<DynamicTableEntity>();
            IEnumerable<DynamicTableEntity> results = table.ExecuteQuery(query);
            if (results.Count() > 0)
            {
                TableBatchOperation batchOperation = new TableBatchOperation();
                foreach (DynamicTableEntity s in results)
                {
                    batchOperation.Delete(s);
                }
                table.ExecuteBatch(batchOperation);
            }
        }
    }
}
