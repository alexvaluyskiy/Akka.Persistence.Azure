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

        static AzureTableJournalSpec()
        {
            SpecConfig = ConfigurationFactory.ParseString(@"
                akka.test.single-expect-default = 50s
                akka.persistence {
                    publish-plugin-commands = on
                    journal.plugin = ""akka.persistence.journal.azure-table""
                    journal.azure-table.connection-string = ""UseDevelopmentStorage=true""
                    journal.azure-table.auto-initialize = on
                    journal.azure-table.table-name = events3
                    journal.azure-table.metadata-table-name = metadata3
                }");
        }

        public AzureTableJournalSpec(ITestOutputHelper output)
            : base(SpecConfig, typeof(AzureTableJournalSpec).Name, output)
        {
            AzureTablePersistence.Get(Sys);
            Initialize();
        }

        protected override bool SupportsRejectingNonSerializableObjects { get; } = false;

        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
            DbUtils.Clean(
                SpecConfig.GetString("akka.persistence.journal.azure-table.connection-string"), 
                SpecConfig.GetString("akka.persistence.journal.azure-table.table-name"));
        }
    }
}