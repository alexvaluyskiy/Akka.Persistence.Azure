# Akka.Persistence.Azure

Akka Persistence journal and snapshot store backed by Azure Table database.

### Configuration

Both journal and snapshot store share the same configuration keys (however they resides in separate scopes, so they are defined distinctly for either journal or snapshot store):

Remember that connection string must be provided separately to Journal and Snapshot Store.

```hocon

```
