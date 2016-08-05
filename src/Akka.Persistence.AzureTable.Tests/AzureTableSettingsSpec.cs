﻿//-----------------------------------------------------------------------
// <copyright file="AzureTableSettingsSpec.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2016 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2016 Akka.NET project <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using FluentAssertions;
using Xunit;

namespace Akka.Persistence.AzureTable.Tests
{
    [Collection("AzureTableSpec")]
    public class AzureTableSettingsSpec : Akka.TestKit.Xunit2.TestKit
    {
        [Fact]
        public void AzureTable_JournalSettings_must_have_default_values()
        {
            var azurePersistence = AzureTablePersistence.Get(Sys);

            azurePersistence.JournalSettings.ConnectionString.Should().Be("UseDevelopmentStorage=true");
            azurePersistence.JournalSettings.TableName.Should().Be("events");
            azurePersistence.JournalSettings.MetadataTableName.Should().Be("metadata");
            azurePersistence.JournalSettings.AutoInitialize.Should().BeFalse();
        }
    }
}
