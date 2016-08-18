//-----------------------------------------------------------------------
// <copyright file="AzureTableSnapshotStoreSpec.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2016 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2016 Akka.NET project <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using System;
using Akka.Configuration;
using Akka.Persistence.TestKit.Snapshot;
using Xunit;
using Xunit.Abstractions;

namespace Akka.Persistence.AzureTable.Tests
{
    [Collection("AzureTableSpec")]
    public class AzureTableSnapshotStoreSpec : SnapshotStoreSpec
    {
        private static readonly Config SpecConfig;
        private static string _connectionString;
        private static string _tableName;

        static AzureTableSnapshotStoreSpec()
        {
#if CI
            var connectionString = Environment.GetEnvironmentVariable("ConnectionString");
#else
            var connectionString = "UseDevelopmentStorage=true;";
#endif

            SpecConfig = ConfigurationFactory.ParseString(@"
                akka.test.single-expect-default = 10s
                akka.persistence {
                    publish-plugin-commands = on
                    snapshot-store.plugin = ""akka.persistence.snapshot-store.azure-table""
                    snapshot-store.azure-table.connection-string = """ + connectionString + @"""
                    snapshot-store.azure-table.auto-initialize = on
                    snapshot-store.azure-table.table-name = snapshots
                }");

            _connectionString = SpecConfig.GetString("akka.persistence.snapshot-store.azure-table.connection-string");
            _tableName = SpecConfig.GetString("akka.persistence.snapshot-store.azure-table.table-name");
        }

        public AzureTableSnapshotStoreSpec(ITestOutputHelper output)
            : base(SpecConfig, typeof(AzureTableJournalSpec).Name, output)
        {
            DbUtils.Clean(_connectionString, _tableName);

            AzureTablePersistence.Get(Sys);
            Initialize();
        }

        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
            DbUtils.Clean(_connectionString, _tableName);
        }
    }
}
