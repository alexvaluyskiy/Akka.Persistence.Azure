//-----------------------------------------------------------------------
// <copyright file="SerializerExtensions.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2016 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2016 Akka.NET project <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using System;
using Akka.Serialization;

namespace Akka.Persistence.AzureTable
{
    public static class SerializerExtensions
    {
        public static string TypeQualifiedNameForManifest(this Type type)
        {
            return type == null ? string.Empty : $"{type.FullName}, {type.Assembly.GetName().Name}";
        }
    }
}