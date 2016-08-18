//-----------------------------------------------------------------------
// <copyright file="AzureTableJournalSpec.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2016 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2016 Akka.NET project <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using System;
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
        private static string _connectionString;
        private static string _tableName;
        private static string _metadataTableName;

        static AzureTableJournalSpec()
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
                    journal.plugin = ""akka.persistence.journal.azure-table""
                    journal.azure-table.connection-string = """ + connectionString + @"""
                    journal.azure-table.auto-initialize = on
                    journal.azure-table.table-name = events
                    journal.azure-table.metadata-table-name = metadata
                }");

            _connectionString = SpecConfig.GetString("akka.persistence.journal.azure-table.connection-string");
            _tableName = SpecConfig.GetString("akka.persistence.journal.azure-table.table-name");
            _metadataTableName = SpecConfig.GetString("akka.persistence.journal.azure-table.metadata-table-name");
        }

        public AzureTableJournalSpec(ITestOutputHelper output)
            : base(SpecConfig, typeof(AzureTableJournalSpec).Name, output)
        {
            DbUtils.Clean(_connectionString, _tableName);
            DbUtils.Clean(_connectionString, _metadataTableName);

            AzureTablePersistence.Get(Sys);
            Initialize();
        }

        protected override bool SupportsRejectingNonSerializableObjects { get; } = false;

        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
            DbUtils.Clean(_connectionString, _tableName);
            DbUtils.Clean(_connectionString, _metadataTableName);
        }
    }
}