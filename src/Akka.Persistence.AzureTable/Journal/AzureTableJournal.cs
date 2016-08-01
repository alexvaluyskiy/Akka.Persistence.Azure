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
using Akka.Serialization;
using Akka.Util.Internal;
using StackExchange.Redis;

namespace Akka.Persistence.AzureTable.Journal
{
    public class AzureTableJournal : AsyncWriteJournal
    {
        private readonly AzureTableSettings _settings;
        private Lazy<Serializer> _serializer;
        private Lazy<IDatabase> _database;
        private ActorSystem _system;

        public AzureTableJournal()
        {
            _settings = AzureTablePersistence.Get(Context.System).JournalSettings;
        }

        protected override void PreStart()
        {
            base.PreStart();
            _system = Context.System;
            _database = new Lazy<IDatabase>(() =>
            {
                var redisConnection = ConnectionMultiplexer.Connect(_settings.ConnectionString);
                return redisConnection.GetDatabase(0);
            });
            _serializer = new Lazy<Serializer>(() => new NewtonSoftJsonSerializer(_system.AsInstanceOf<ExtendedActorSystem>()));
        }

        public override async Task ReplayMessagesAsync(
            IActorContext context,
            string persistenceId,
            long fromSequenceNr,
            long toSequenceNr,
            long max,
            Action<IPersistentRepresentation> recoveryCallback)
        {
            RedisValue[] journals = await _database.Value.SortedSetRangeByScoreAsync(GetJournalKey(persistenceId), fromSequenceNr, toSequenceNr, skip: 0L, take: max);

            foreach (var journal in journals)
            {
                recoveryCallback(ToPersistenceRepresentation(_serializer.Value.FromBinary<JournalEntry>(journal), context.Sender));
            }
        }

        public override async Task<long> ReadHighestSequenceNrAsync(string persistenceId, long fromSequenceNr)
        {
            var highestSequenceNr = await _database.Value.StringGetAsync(GetHighestSequenceNrKey(persistenceId));
            return highestSequenceNr.IsNull ? 0L : (long)highestSequenceNr;
        }

        protected override async Task DeleteMessagesToAsync(string persistenceId, long toSequenceNr)
        {
            await _database.Value.SortedSetRemoveRangeByScoreAsync(GetJournalKey(persistenceId), -1, toSequenceNr);
        }

        protected override async Task<IImmutableList<Exception>> WriteMessagesAsync(IEnumerable<AtomicWrite> messages)
        {
            var messageList = messages.ToList();
            var writeTasks = messageList.Select(async message =>
            {
                var persistentMessages = ((IImmutableList<IPersistentRepresentation>)message.Payload).ToArray();

                var transaction = _database.Value.CreateTransaction();

                foreach (var write in persistentMessages)
                {
                    transaction.SortedSetAddAsync(GetJournalKey(write.PersistenceId), _serializer.Value.ToBinary(ToJournalEntry(write)), write.SequenceNr);
                }

                if (!await transaction.ExecuteAsync())
                {
                    throw new Exception($"{nameof(WriteMessagesAsync)}: failed to write {typeof(JournalEntry).Name} to redis");
                }
            });

            await SetHighSequenceId(messageList);

            return await Task<IImmutableList<Exception>>
                .Factory
                .ContinueWhenAll(writeTasks.ToArray(),
                    tasks => tasks.Select(t => t.IsFaulted ? TryUnwrapException(t.Exception) : null).ToImmutableList());
        }

        private async Task SetHighSequenceId(IList<AtomicWrite> messages)
        {
            var persistenceId = messages.Select(c => c.PersistenceId).First();
            var highSequenceId = messages.Max(c => c.HighestSequenceNr);

            await _database.Value.StringSetAsync(GetHighestSequenceNrKey(persistenceId), highSequenceId);
        }

        private RedisKey GetJournalKey(string persistenceId) => $"{_settings.TableName}:{persistenceId}";

        private RedisKey GetHighestSequenceNrKey(string persistenceId)
        {
            return $"{GetJournalKey(persistenceId)}.highestSequenceNr";
        }

        private JournalEntry ToJournalEntry(IPersistentRepresentation message)
        {
            return new JournalEntry
            {
                PersistenceId = message.PersistenceId,
                SequenceNr = message.SequenceNr,
                IsDeleted = message.IsDeleted,
                Payload = message.Payload,
                Manifest = message.Manifest
            };
        }

        private Persistent ToPersistenceRepresentation(JournalEntry entry, IActorRef sender)
        {
            return new Persistent(entry.Payload, entry.SequenceNr, entry.PersistenceId, entry.Manifest, entry.IsDeleted, sender);
        }
    }
}