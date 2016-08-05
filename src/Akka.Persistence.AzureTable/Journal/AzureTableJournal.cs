//-----------------------------------------------------------------------
// <copyright file="AzureTableJournal.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2016 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2016 Akka.NET project <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Persistence.Journal;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using Newtonsoft.Json;

namespace Akka.Persistence.AzureTable.Journal
{
    public class AzureTableJournal : AsyncWriteJournal
    {
        private readonly AzureTableSettings _settings;
        private Lazy<CloudTableClient> _client;
        private ActorSystem _system;

        public AzureTableJournal()
        {
            _settings = AzureTablePersistence.Get(Context.System).JournalSettings;
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
                    tableClient.GetTableReference(_settings.MetadataTableName).CreateIfNotExists();
                }

                return tableClient;
            });
        }

        public override async Task ReplayMessagesAsync(
            IActorContext context,
            string persistenceId,
            long fromSequenceNr,
            long toSequenceNr,
            long max,
            Action<IPersistentRepresentation> recoveryCallback)
        {
            long count = 0;
            if (max > 0 && (toSequenceNr - fromSequenceNr) >= 0)
            {
                IEnumerable<JournalEntry> results = _client.Value
                    .GetTableReference(_settings.TableName)
                    .ExecuteQuery(BuildReplayTableQuery(persistenceId, fromSequenceNr, toSequenceNr));

                foreach (JournalEntry @event in results)
                {
                    recoveryCallback(ToPersistenceRepresentation(@event, Context.Self));
                    count++;
                    if (count == max) return;
                }
            }
        }

        public override async Task<long> ReadHighestSequenceNrAsync(string persistenceId, long fromSequenceNr)
        {
            var table = _client.Value.GetTableReference(_settings.MetadataTableName);

            var query = table.CreateQuery<MetadataEntry>()
                .Where(c => c.PersistenceId == persistenceId)
                .Select(c => c.SequenceNr)
                .FirstOrDefault();

            return query;
        }

        protected override async Task DeleteMessagesToAsync(string persistenceId, long toSequenceNr)
        {
            IEnumerable<JournalEntry> results = _client.Value
                    .GetTableReference(_settings.TableName)
                    .ExecuteQuery(BuildDeleteTableQuery(persistenceId, toSequenceNr)).OrderByDescending(t => t.SequenceNr);

            if (results.Any())
            {
                TableBatchOperation batchOperation = new TableBatchOperation();

                foreach (JournalEntry s in results)
                {
                    batchOperation.Delete(s);
                }

                await _client.Value.GetTableReference(_settings.TableName).ExecuteBatchAsync(batchOperation);
            }
        }

        protected override async Task<IImmutableList<Exception>> WriteMessagesAsync(IEnumerable<AtomicWrite> messages)
        {
            var messageList = messages.ToList();
            var writeTasks = messageList.Select(async message =>
            {
                var persistentMessages = ((IImmutableList<IPersistentRepresentation>)message.Payload).ToArray();

                var batchOperation = new TableBatchOperation();

                foreach (var write in persistentMessages)
                {
                    batchOperation.Insert(ToJournalEntry(write));
                }

                await _client.Value.GetTableReference(_settings.TableName).ExecuteBatchAsync(batchOperation);
            });

            // await SetHighestSequenceId(messageList);

            return await Task<IImmutableList<Exception>>
                .Factory
                .ContinueWhenAll(writeTasks.ToArray(),
                    tasks => tasks.Select(t => t.IsFaulted ? TryUnwrapException(t.Exception) : null).ToImmutableList());
        }

        private async Task SetHighestSequenceId(IList<AtomicWrite> messages)
        {
            var persistenceId = messages.Select(c => c.PersistenceId).First();
            var highSequenceId = messages.Max(c => c.HighestSequenceNr);

            var metadata = new MetadataEntry(persistenceId, highSequenceId);

            await _client.Value.GetTableReference(_settings.MetadataTableName).ExecuteAsync(TableOperation.Replace(metadata));
        }

        private static TableQuery<JournalEntry> BuildReplayTableQuery(string persistenceId, long fromSequenceNr, long toSequenceNr)
        {
            return new TableQuery<JournalEntry>().Where(
                                TableQuery.CombineFilters(TableQuery.CombineFilters(
                                    TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, persistenceId),
                                    TableOperators.And,
                                    TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.GreaterThanOrEqual, JournalEntry.ToRowKey(fromSequenceNr))),
                                TableOperators.And,
                                TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.LessThanOrEqual, JournalEntry.ToRowKey(toSequenceNr))));
        }

        private static TableQuery<JournalEntry> BuildDeleteTableQuery(string persistenceId, long sequenceNr)
        {
            return new TableQuery<JournalEntry>().Where(
                        TableQuery.CombineFilters(
                                TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, persistenceId),
                                TableOperators.And,
                                TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.LessThanOrEqual, JournalEntry.ToRowKey(sequenceNr))));
        }

        private JournalEntry ToJournalEntry(IPersistentRepresentation message)
        {
            var id = Guid.NewGuid().ToString();
            var payload = JsonConvert.SerializeObject(message.Payload);
            return new JournalEntry(id, message.PersistenceId, message.SequenceNr, message.IsDeleted, payload, message.Manifest);
        }

        private Persistent ToPersistenceRepresentation(JournalEntry entry, IActorRef sender)
        {
            var payload = JsonConvert.DeserializeObject(entry.Payload);
            return new Persistent(payload, entry.SequenceNr, entry.PersistenceId, entry.Manifest, entry.IsDeleted, sender);
        }
    }
}