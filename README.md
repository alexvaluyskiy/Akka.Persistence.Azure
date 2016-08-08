# Akka.Persistence.Azure

Akka Persistence journal and snapshot store backed by Azure Table database.

## Configuration

Both journal and snapshot store share the same configuration keys (however they resides in separate scopes, so they are defined distinctly for either journal or snapshot store):

Remember that connection string must be provided separately to Journal and Snapshot Store.

```hocon
akka.persistence {
    journal {
        azure-table {
            # qualified type name of the Redis persistence journal actor
            class = "Akka.Persistence.AzureTable.Journal.AzureTableJournal, Akka.Persistence.AzureTable"

            # dispatcher used to drive journal actor
            plugin-dispatcher = "akka.actor.default-dispatcher"

			# connection string used for database access
			connection-string = "UseDevelopmentStorage=true"

			# table storage table corresponding with persistent journal
			table-name = events

			# metadata table
			metadata-table-name = metadata

			# should corresponding journal table be initialized automatically
			auto-initialize = off
        }
    }    
}
```

## Serialization
Azure plugin uses Json.Net to serialize all payloads