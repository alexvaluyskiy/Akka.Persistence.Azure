//-----------------------------------------------------------------------
// <copyright file="MetadataEntry.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2016 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2016 Akka.NET project <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using Microsoft.WindowsAzure.Storage.Table;

namespace Akka.Persistence.AzureTable.Journal
{
    public class MetadataEntry : TableEntity
    {
        public MetadataEntry() { }

        public MetadataEntry(string persistenceId, long sequenceNr)
        {
            PartitionKey = persistenceId;
            RowKey = persistenceId;

            SequenceNr = sequenceNr;
        }

        public long SequenceNr { get; set; }
    }
}