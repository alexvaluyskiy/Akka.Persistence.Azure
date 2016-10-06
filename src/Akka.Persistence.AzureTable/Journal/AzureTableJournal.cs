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
using Akka.Util.Internal;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using Newtonsoft.Json;

namespace Akka.Persistence.AzureTable.Journal
{
    public class AzureTableJournal : AsyncWriteJournal
    {
        private readonly AzureTableJournalSettings _settings;
        private Lazy<CloudTableClient> _client;

        public AzureTableJournal()
        {
            _settings = AzureTablePersistence.Get(Context.System).JournalSettings;
        }

        protected override void PreStart()
        {
            base.PreStart();
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

        /// <summary>
        /// Asynchronously replays persistent messages.
        /// </summary>
        /// <param name="context">The contextual information about the actor processing replayed messages.</param>
        /// <param name="persistenceId">Persistent actor identifier</param>
        /// <param name="fromSequenceNr">Inclusive sequence number where replay should start</param>
        /// <param name="toSequenceNr">Inclusive sequence number where replay should end</param>
        /// <param name="max">Maximum number of messages to be replayed</param>
        /// <param name="recoveryCallback">Called to replay a message, may be called from any thread.</param>
        public override async Task ReplayMessagesAsync(
            IActorContext context,
            string persistenceId,
            long fromSequenceNr,
            long toSequenceNr,
            long max,
            Action<IPersistentRepresentation> recoveryCallback)
        {
            if (max == 0)
                return;

            long count = 0;

            TableContinuationToken continuationToken = null;
            var table = _client.Value.GetTableReference(_settings.TableName);
            var tableQuery = BuildReplayTableQuery(persistenceId, fromSequenceNr, toSequenceNr);

            do
            {
                var tableQueryResult = await table.ExecuteQuerySegmentedAsync(tableQuery, continuationToken);
                continuationToken = tableQueryResult.ContinuationToken;

                foreach (JournalEntry @event in tableQueryResult.Results)
                {
                    recoveryCallback(ToPersistenceRepresentation(@event, context.Self));

                    count++;
                    if (count == max) return;
                }
            } while (continuationToken != null);
        }

        /// <summary>
        /// Asynchronously reads a highest sequence number of the event stream related with provided <paramref name="persistenceId"/>.
        /// </summary>
        /// <param name="persistenceId">Persistent actor identifier</param>
        /// <param name="fromSequenceNr">Hint where to start searching for the highest sequence number</param>
        public override async Task<long> ReadHighestSequenceNrAsync(string persistenceId, long fromSequenceNr)
        {
            var table = _client.Value.GetTableReference(_settings.MetadataTableName);

            var tableResult = await table.ExecuteAsync(TableOperation.Retrieve<MetadataEntry>(persistenceId, persistenceId));

            return tableResult.HttpStatusCode == 200 
                ? tableResult.Result?.AsInstanceOf<MetadataEntry>()?.SequenceNr ?? 0 
                : 0;
        }

        /// <summary>
        /// Asynchronously deletes all persistent messages up to inclusive <paramref name="toSequenceNr" /> bound.
        /// </summary>
        /// <param name="persistenceId">Persistent actor identifier</param>
        protected override async Task DeleteMessagesToAsync(string persistenceId, long toSequenceNr)
        {
            // TODO: optimize the query
            IEnumerable<JournalEntry> results = _client.Value.GetTableReference(_settings.TableName)
                    .ExecuteQuery(BuildDeleteTableQuery(persistenceId, toSequenceNr))
                    .OrderByDescending(t => t.RowKey);

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

        /// <summary>
        /// Asynchronously writes a batch of persistent messages to the journal.
        /// </summary>
        /// <param name="messages">The atomic messages.</param>
        /// <returns></returns>
        protected override async Task<IImmutableList<Exception>> WriteMessagesAsync(IEnumerable<AtomicWrite> messages)
        {
            var table = _client.Value.GetTableReference(_settings.TableName);

            var messagesList = messages.ToList();
            var groupedTasks = messagesList.GroupBy(x => x.PersistenceId).ToDictionary(g => g.Key, async g =>
            {
                var persistentMessages = g.SelectMany(aw => (IImmutableList<IPersistentRepresentation>)aw.Payload).ToList();

                var persistenceId = g.Key;
                var highSequenceId = persistentMessages.Max(c => c.SequenceNr);

                var batchOperation = new TableBatchOperation();

                foreach (var write in persistentMessages)
                {
                    batchOperation.Insert(ToJournalEntry(write));
                }

                await table.ExecuteBatchAsync(batchOperation);

                await SetHighestSequenceId(persistenceId, highSequenceId);
            });

            return await Task<IImmutableList<Exception>>.Factory.ContinueWhenAll(
                    groupedTasks.Values.ToArray(),
                    tasks => messagesList.Select(
                        m =>
                        {
                            var task = groupedTasks[m.PersistenceId];
                            return task.IsFaulted ? TryUnwrapException(task.Exception) : null;
                        }).ToImmutableList());
        }

        private async Task SetHighestSequenceId(string persistenceId, long highSequenceId)
        {
            var table = _client.Value.GetTableReference(_settings.MetadataTableName);

            var tableResult = await table.ExecuteAsync(TableOperation.Retrieve<MetadataEntry>(persistenceId, persistenceId));

            MetadataEntry metadataEntry = tableResult.Result as MetadataEntry;

            if (metadataEntry != null)
            {
                metadataEntry.SequenceNr = highSequenceId;
                await table.ExecuteAsync(TableOperation.Replace(metadataEntry));
            }
            else
            {
                await table.ExecuteAsync(TableOperation.Insert(new MetadataEntry(persistenceId, highSequenceId)));
            }
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
            var payload = JsonConvert.SerializeObject(message.Payload);
            return new JournalEntry(message.PersistenceId, message.SequenceNr, payload, message.Payload.GetType().TypeQualifiedNameForManifest());
        }

        private static Persistent ToPersistenceRepresentation(JournalEntry entry, IActorRef sender)
        {
            var payload = JsonConvert.DeserializeObject(entry.Payload, Type.GetType(entry.Manifest));
            return new Persistent(payload, long.Parse(entry.RowKey), entry.PartitionKey, entry.Manifest, false, sender);
        }
    }
}