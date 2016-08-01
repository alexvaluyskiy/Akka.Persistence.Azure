//-----------------------------------------------------------------------
// <copyright file="SerializerExtensions.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2016 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2016 Akka.NET project <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using Akka.Serialization;

namespace Akka.Persistence.AzureTable
{
    public static class SerializerExtensions
    {
        public static T FromBinary<T>(this Serializer serializer, byte[] bytes)
        {
            return (T)serializer.FromBinary(bytes, typeof(T));
        }
    }
}
