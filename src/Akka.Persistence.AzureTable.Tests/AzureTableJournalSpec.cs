//-----------------------------------------------------------------------
// <copyright file="AzureTableJournalSpec.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2016 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2016 Akka.NET project <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using Akka.Configuration;
using Akka.Persistence.TestKit.Journal;
using Xunit;
using Xunit.Abstractions;

namespace Akka.Persistence.AzureTable.Tests
{
    [Collection("AzureTableSpec")]
    public class AzureTableJournalSpec : JournalSpec
    {
        private static readonly Config SpecConfig;
        private static string connectionString;
        private static string tableName;
        private static string metadataTableName;

        static AzureTableJournalSpec()
        {
            SpecConfig = ConfigurationFactory.ParseString(@"
                akka.test.single-expect-default = 25s
                akka.persistence {
                    publish-plugin-commands = on
                    journal.plugin = ""akka.persistence.journal.azure-table""
                    journal.azure-table.connection-string = ""UseDevelopmentStorage=true""
                    journal.azure-table.auto-initialize = on
                    journal.azure-table.table-name = events
                    journal.azure-table.metadata-table-name = metadata
                }");

            connectionString = SpecConfig.GetString("akka.persistence.journal.azure-table.connection-string");
            tableName = SpecConfig.GetString("akka.persistence.journal.azure-table.table-name");
            metadataTableName = SpecConfig.GetString("akka.persistence.journal.azure-table.metadata-table-name");
        }

        public AzureTableJournalSpec(ITestOutputHelper output)
            : base(SpecConfig, typeof(AzureTableJournalSpec).Name, output)
        {
            DbUtils.Clean(connectionString, tableName);
            DbUtils.Clean(connectionString, metadataTableName);

            AzureTablePersistence.Get(Sys);
            Initialize();
        }

        protected override bool SupportsRejectingNonSerializableObjects { get; } = false;

        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
            DbUtils.Clean(connectionString, tableName);
            DbUtils.Clean(connectionString, metadataTableName);
        }
    }
}