//-----------------------------------------------------------------------
// <copyright file="AzureTablePersistence.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2016 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2016 Akka.NET project <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using System;
using Akka.Actor;
using Akka.Configuration;

namespace Akka.Persistence.AzureTable
{
    public class AzureTableSettings
    {
        protected AzureTableSettings(string connectionString, string tableName, bool autoInitialize)
        {
            ConnectionString = connectionString;
            TableName = tableName;
            AutoInitialize = autoInitialize;
        }

        public string ConnectionString { get; }

        public string TableName { get; }

        public bool AutoInitialize { get; }
    }

    public sealed class AzureTableJournalSettings : AzureTableSettings
    {
        private AzureTableJournalSettings(string connectionString, string tableName, string metadataTableName, bool autoInitialize)
            : base(connectionString, tableName, autoInitialize)
        {
            MetadataTableName = metadataTableName;
        }

        public string MetadataTableName { get; }

        public static AzureTableJournalSettings Create(Config config)
        {
            if (config == null)
                throw new ArgumentNullException(nameof(config));

            return new AzureTableJournalSettings(
                connectionString: config.GetString("connection-string"),
                tableName: config.GetString("table-name"),
                metadataTableName: config.GetString("metadata-table-name"),
                autoInitialize: config.GetBoolean("auto-initialize"));
        }
    }

    public sealed class AzureTableSnapshotStoreSettings : AzureTableSettings
    {
        private AzureTableSnapshotStoreSettings(string connectionString, string tableName, bool autoInitialize)
            : base(connectionString, tableName, autoInitialize)
        {
        }

        public static AzureTableSnapshotStoreSettings Create(Config config)
        {
            if (config == null)
                throw new ArgumentNullException(nameof(config));

            return new AzureTableSnapshotStoreSettings(
                connectionString: config.GetString("connection-string"),
                tableName: config.GetString("table-name"),
                autoInitialize: config.GetBoolean("auto-initialize"));
        }
    }

    public class AzureTablePersistence : IExtension
    {
        public static AzureTablePersistence Get(ActorSystem system) => system.WithExtension<AzureTablePersistence, AzureTablePersistenceProvider>();
        public static Config DefaultConfig() => ConfigurationFactory.FromResource<AzureTablePersistence>("Akka.Persistence.AzureTable.reference.conf");

        public AzureTableJournalSettings JournalSettings { get; }

        public AzureTableSnapshotStoreSettings SnapshotStoreSettings { get; }

        public AzureTablePersistence(ExtendedActorSystem system)
        {
            system.Settings.InjectTopLevelFallback(DefaultConfig());

            JournalSettings = AzureTableJournalSettings.Create(system.Settings.Config.GetConfig("akka.persistence.journal.azure-table"));
            SnapshotStoreSettings = AzureTableSnapshotStoreSettings.Create(system.Settings.Config.GetConfig("akka.persistence.snapshot-store.azure-table"));
        }
    }

    public class AzureTablePersistenceProvider : ExtensionIdProvider<AzureTablePersistence>
    {
        public override AzureTablePersistence CreateExtension(ExtendedActorSystem system)
        {
            return new AzureTablePersistence(system);
        }
    }
}
