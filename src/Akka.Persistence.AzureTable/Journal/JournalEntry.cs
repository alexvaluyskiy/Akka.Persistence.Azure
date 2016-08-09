//-----------------------------------------------------------------------
// <copyright file="JournalEntry.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2016 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2016 Akka.NET project <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using Microsoft.WindowsAzure.Storage.Table;

namespace Akka.Persistence.AzureTable.Journal
{
    /// <summary>
    /// Class used for storing intermediate result of the <see cref="IPersistentRepresentation"/>
    /// </summary>
    public class JournalEntry : TableEntity
    {
        public JournalEntry() { }

        public JournalEntry(string persistenceId, long sequenceNr, bool isDeleted, string payload, string manifest)
        {
            PartitionKey = persistenceId;
            RowKey = ToRowKey(sequenceNr);

            PersistenceId = persistenceId;
            SequenceNr = sequenceNr;
            IsDeleted = isDeleted;
            Payload = payload;
            Manifest = manifest;
        }

        public string PersistenceId { get; set; }

        public long SequenceNr { get; set; }

        public bool IsDeleted { get; set; }

        public string Payload { get; set; }

        public string Manifest { get; set; }

        public static string ToRowKey(long version)
        {
            return version.ToString().PadLeft(10, '0');
        }
    }
}
