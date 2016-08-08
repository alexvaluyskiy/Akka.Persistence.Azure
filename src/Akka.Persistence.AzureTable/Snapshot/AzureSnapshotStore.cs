//-----------------------------------------------------------------------
// <copyright file="AzureTableJournal.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2016 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2016 Akka.NET project <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Persistence.Snapshot;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using Microsoft.WindowsAzure.Storage.Table.Queryable;
using Newtonsoft.Json;

namespace Akka.Persistence.AzureTable.Snapshot
{
    public class AzureSnapshotStore : SnapshotStore
    {
        private readonly AzureTableSettings _settings;
        private Lazy<CloudTableClient> _client;
        private ActorSystem _system;

        public AzureSnapshotStore()
        {
            _settings = AzureTablePersistence.Get(Context.System).SnapshotStoreSettings;
        }

        protected override void PreStart()
        {
            base.PreStart();
            _system = Context.System;
            _client = new Lazy<CloudTableClient>(() =>
            {
                CloudTableClient tableClient = CloudStorageAccount.Parse(_settings.ConnectionString).CreateCloudTableClient();

                if (_settings.AutoInitialize)
                {
                    tableClient.GetTableReference(_settings.TableName).CreateIfNotExists();
                }

                return tableClient;
            });
        }

        protected override async Task<SelectedSnapshot> LoadAsync(string persistenceId, SnapshotSelectionCriteria criteria)
        {
            CloudTable table = _client.Value.GetTableReference(_settings.TableName);

            var query = BuildSnapshotTableQuery(persistenceId, criteria);

            return table.ExecuteQuery(query).OrderByDescending(t => t.SequenceNr).Select(ToSelectedSnapshot).FirstOrDefault();
        }

        protected override Task SaveAsync(SnapshotMetadata metadata, object snapshot)
        {
            CloudTable table = _client.Value.GetTableReference(_settings.TableName);

            TableOperation upsertOperation = TableOperation.Insert(ToSnapshotEntry(metadata, snapshot));

            var entity = (SnapshotEntry)table.Execute(TableOperation.Retrieve<SnapshotEntry>(metadata.PersistenceId, SnapshotEntry.ToRowKey(metadata.SequenceNr))).Result;
            if (entity != null)
            {
                entity.Payload = JsonConvert.SerializeObject(snapshot);
                upsertOperation = TableOperation.Replace(entity);
            }

            return table.ExecuteAsync(upsertOperation);
        }

        protected override Task DeleteAsync(SnapshotMetadata metadata)
        {
            CloudTable table = _client.Value.GetTableReference(_settings.TableName);
            TableOperation getOperation = TableOperation.Retrieve<SnapshotEntry>(metadata.PersistenceId, SnapshotEntry.ToRowKey(metadata.SequenceNr));
            TableResult result = table.Execute(getOperation);
            TableOperation deleteOperation = TableOperation.Delete((SnapshotEntry)result.Result);
            return table.ExecuteAsync(deleteOperation);
        }

        protected override async Task DeleteAsync(string persistenceId, SnapshotSelectionCriteria criteria)
        {
            CloudTable table = _client.Value.GetTableReference(_settings.TableName);
            TableQuery<SnapshotEntry> query = BuildSnapshotTableQuery(persistenceId, criteria);

            var results = table.ExecuteQuery(query).OrderByDescending(t => t.SequenceNr).ToList();
            if (results.Count > 0)
            {
                TableBatchOperation batchOperation = new TableBatchOperation();
                foreach (SnapshotEntry s in results)
                {
                    batchOperation.Delete(s);
                }
                table.ExecuteBatch(batchOperation);
            }
        }

        private static TableQuery<SnapshotEntry> BuildSnapshotTableQuery(string persistenceId, SnapshotSelectionCriteria criteria)
        {
            string comparsion = TableQuery.CombineFilters(
                TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, persistenceId),
                TableOperators.And,
                TableQuery.GenerateFilterConditionForLong("SequenceNr", QueryComparisons.LessThanOrEqual, criteria.MaxSequenceNr));

            if (criteria.MaxTimeStamp != DateTime.MinValue && criteria.MaxTimeStamp != DateTime.MaxValue)
            {
                comparsion = TableQuery.CombineFilters(
                    comparsion,
                    TableOperators.And,
                    TableQuery.GenerateFilterConditionForLong("SnapshotTimestamp", QueryComparisons.LessThanOrEqual, criteria.MaxTimeStamp.Ticks));
            }

            return new TableQuery<SnapshotEntry>().Where(comparsion);
        }

        private static SnapshotEntry ToSnapshotEntry(SnapshotMetadata metadata, object snapshot)
        {
            var payload = JsonConvert.SerializeObject(snapshot);
            return new SnapshotEntry(metadata.PersistenceId, metadata.SequenceNr, metadata.Timestamp.Ticks, snapshot.GetType().TypeQualifiedNameForManifest(), payload);
        }

        private static SelectedSnapshot ToSelectedSnapshot(SnapshotEntry entry)
        {
            var payload = JsonConvert.DeserializeObject(entry.Payload, Type.GetType(entry.Manifest));
            return new SelectedSnapshot(new SnapshotMetadata(entry.PersistenceId, entry.SequenceNr, new DateTime(entry.SnapshotTimestamp)), payload);
        }
    }
}
