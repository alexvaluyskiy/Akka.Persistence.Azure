//-----------------------------------------------------------------------
// <copyright file="AzureTableSnapshotStoreSpec.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2016 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2016 Akka.NET project <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

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
        private static string connectionString;
        private static string tableName;

        static AzureTableSnapshotStoreSpec()
        {
            SpecConfig = ConfigurationFactory.ParseString(@"
                akka.test.single-expect-default = 25s
                akka.persistence {
                    publish-plugin-commands = on
                    snapshot-store.plugin = ""akka.persistence.snapshot-store.azure-table""
                    snapshot-store.azure-table.connection-string = ""UseDevelopmentStorage=true""
                    snapshot-store.azure-table.auto-initialize = on
                    snapshot-store.azure-table.table-name = snapshots
                }");

            connectionString = SpecConfig.GetString("akka.persistence.snapshot-store.azure-table.connection-string");
            tableName = SpecConfig.GetString("akka.persistence.snapshot-store.azure-table.table-name");
        }

        public AzureTableSnapshotStoreSpec(ITestOutputHelper output)
            : base(SpecConfig, typeof(AzureTableJournalSpec).Name, output)
        {
            DbUtils.Clean(connectionString, tableName);

            AzureTablePersistence.Get(Sys);
            Initialize();
        }

        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
            DbUtils.Clean(connectionString, tableName);
        }
    }
}
