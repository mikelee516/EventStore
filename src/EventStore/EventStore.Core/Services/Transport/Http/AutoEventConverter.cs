﻿using System;
using System.Text;
using System.Xml.Linq;
using EventStore.Common.Log;
using EventStore.Core.Data;
using EventStore.Core.Messages;
using EventStore.Core.TransactionLog.LogRecords;
using EventStore.Transport.Http;
using EventStore.Transport.Http.Codecs;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Formatting = Newtonsoft.Json.Formatting;
using System.Linq;
using EventStore.Common.Utils;

namespace EventStore.Core.Services.Transport.Http
{
    public static class AutoEventConverter
    {
        private static readonly ILogger Log = LogManager.GetLogger("AutoEventConverter");

        public static string SmartFormat(ResolvedEvent evnt, ICodec targetCodec)
        {
            var dto = CreateDataDto(evnt);

            switch (targetCodec.ContentType)
            {
                case ContentType.Xml:
                case ContentType.ApplicationXml:
                    {
                        var serializeObject = JsonConvert.SerializeObject(dto.data);
                        var deserializeXmlNode = JsonConvert.DeserializeXmlNode(serializeObject, "data");
                        return deserializeXmlNode.InnerXml;
                    }
                case ContentType.Json:
                    return targetCodec.To(dto.data);


                case ContentType.Atom:
                case ContentType.EventXml:
                {
                    var serializeObject = JsonConvert.SerializeObject(dto);
                    var deserializeXmlNode = JsonConvert.DeserializeXmlNode(serializeObject, "event");
                    return deserializeXmlNode.InnerXml;
                }

                case ContentType.EventJson:
                    return targetCodec.To(dto);


                default:
                    throw new NotSupportedException();
            }
        }

        public static HttpClientMessageDto.ReadEventCompletedText CreateDataDto(ResolvedEvent evnt)
        {
            var dto = new HttpClientMessageDto.ReadEventCompletedText(evnt);
            if (evnt.Event.Flags.HasFlag(PrepareFlags.IsJson))
            {
                var deserializedData = Codec.Json.From<object>((string) dto.data);
                var deserializedMetadata = Codec.Json.From<object>((string) dto.metadata);

                if (deserializedData != null)
                    dto.data = deserializedData;
                if (deserializedMetadata != null)
                    dto.metadata = deserializedMetadata;
            }
            return dto;
        }

        public static Event[] SmartParse(string request, ICodec sourceCodec)
        {
            var writeEvents = Load(request, sourceCodec);
            if (writeEvents.IsEmpty())
                return null;
            var events = Parse(writeEvents);
            return events;
        }

        private static HttpClientMessageDto.ClientEventDynamic[] Load(string data, ICodec sourceCodec)
        {
            switch(sourceCodec.ContentType)
            {
                case ContentType.Json:
                case ContentType.EventsJson:
                case ContentType.AtomJson:
                    return LoadFromJson(data);

                case ContentType.Xml:
                case ContentType.EventsXml:
                case ContentType.ApplicationXml:
                case ContentType.Atom:
                    return LoadFromXml(data);

                default:
                    return null;
            }
        }

        private static HttpClientMessageDto.ClientEventDynamic[] LoadFromJson(string json)
        {
            return Codec.Json.From<HttpClientMessageDto.ClientEventDynamic[]>(json);
        }

        private static HttpClientMessageDto.ClientEventDynamic[] LoadFromXml(string xml)
        {
            try
            {
                XDocument doc = XDocument.Parse(xml);

                XNamespace jsonNsValue = "http://james.newtonking.com/projects/json";
                XName jsonNsName = XNamespace.Xmlns + "json";

                doc.Root.SetAttributeValue(jsonNsName, jsonNsValue);

                var events = doc.Root.Elements()/*.ToArray()*/;
                foreach (var @event in events)
                {
                    @event.Name = "events";
                    @event.SetAttributeValue(jsonNsValue + "Array", "true");
                }
                //doc.Root.ReplaceNodes(events);
//                foreach (var element in doc.Root.Descendants("data").Concat(doc.Root.Descendants("metadata")))
//                {
//                    element.RemoveAttributes();
//                }

                var json = JsonConvert.SerializeXNode(doc.Root, Formatting.None, true);
                var root = JsonConvert.DeserializeObject<HttpClientMessageDto.WriteEventsDynamic>(json);
                return root.events;
//                var root = JsonConvert.DeserializeObject<JObject>(json);
//                var dynamicEvents = root.ToObject<HttpClientMessageDto.WriteEventsDynamic>();
//                return dynamicEvents.events;
            }
            catch (Exception e)
            {
                Log.InfoException(e, "Failed to load xml. Invalid format");
                return null;
            }
        }

        private static Event[] Parse(HttpClientMessageDto.ClientEventDynamic[] dynamicEvents)
        {
            var events = new Event[dynamicEvents.Length];
            for (int i = 0, n = dynamicEvents.Length; i < n; ++i)
            {
                var textEvent = dynamicEvents[i];
                bool dataIsJson;
                bool metadataIsJson;
                var data = AsBytes(textEvent.data, out dataIsJson);
                var metadata = AsBytes(textEvent.metadata, out metadataIsJson);

                events[i] = new Event(textEvent.eventId, textEvent.eventType, dataIsJson || metadataIsJson, data, metadata);
            }
            return events.ToArray();
        }

        private static byte[] AsBytes(object obj, out bool isJson)
        {
            if (obj is JObject)
            {
                isJson = true;
                return Helper.UTF8NoBom.GetBytes(Codec.Json.To(obj));
            }

            isJson = false;
            return Helper.UTF8NoBom.GetBytes((obj as string) ?? string.Empty);
        }
    }
}
