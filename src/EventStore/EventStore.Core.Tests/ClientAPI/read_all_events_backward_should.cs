﻿using System;
using System.Collections.Generic;
using System.Linq;
using EventStore.ClientAPI;
using EventStore.ClientAPI.SystemData;
using EventStore.Core.Services;
using EventStore.Core.Tests.ClientAPI.Helpers;
using EventStore.Core.Tests.Helpers;
using NUnit.Framework;

namespace EventStore.Core.Tests.ClientAPI
{
    [TestFixture, Category("LongRunning")]
    public class read_all_events_backward_should: SpecificationWithMiniNode
    {
        private EventData[] _testEvents;

        protected override void When()
        {
            _conn.SetStreamMetadata("$all", -1,
                                    StreamMetadata.Build().SetReadRole(SystemRoles.All),
                                    new UserCredentials(SystemUsers.Admin, SystemUsers.DefaultAdminPassword));

            _testEvents = Enumerable.Range(0, 20).Select(x => TestEvent.NewTestEvent(x.ToString())).ToArray();
            _conn.AppendToStream("stream", ExpectedVersion.EmptyStream, _testEvents);
        }

        [Test, Category("LongRunning")]
        public void return_empty_slice_if_asked_to_read_from_start()
        {
            var read = _conn.ReadAllEventsBackward(Position.Start, 1, false);
            Assert.That(read.IsEndOfStream, Is.True);
            Assert.That(read.Events.Length, Is.EqualTo(0));
        }

        [Test, Category("LongRunning")]
        public void return_partial_slice_if_not_enough_events()
        {
            var read = _conn.ReadAllEventsBackward(Position.End, 30, false);
            Assert.That(read.Events.Length, Is.LessThan(30));
            Assert.That(EventDataComparer.Equal(_testEvents.Reverse().ToArray(),
                                                read.Events.Take(_testEvents.Length).Select(x => x.Event).ToArray()));
        }

        [Test, Category("LongRunning")]
        public void return_events_in_reversed_order_compared_to_written()
        {
            var read = _conn.ReadAllEventsBackward(Position.End, _testEvents.Length, false);
            Assert.That(EventDataComparer.Equal(_testEvents.Reverse().ToArray(), 
                                                read.Events.Select(x => x.Event).ToArray()));
        }

        [Test, Category("LongRunning")]
        public void be_able_to_read_all_one_by_one_until_end_of_stream()
        {
            var all = new List<RecordedEvent>();
            var position = Position.End;
            AllEventsSlice slice;

            while (!(slice = _conn.ReadAllEventsBackward(position, 1, false)).IsEndOfStream)
            {
                all.Add(slice.Events.Single().Event);
                position = slice.NextPosition;
            }

            Assert.That(EventDataComparer.Equal(_testEvents.Reverse().ToArray(), all.Take(_testEvents.Length).ToArray()));
        }

        [Test, Category("LongRunning")]
        public void be_able_to_read_events_slice_at_time()
        {
            var all = new List<RecordedEvent>();
            var position = Position.End;
            AllEventsSlice slice;

            while (!(slice = _conn.ReadAllEventsBackward(position, 5, false)).IsEndOfStream)
            {
                all.AddRange(slice.Events.Select(x => x.Event));
                position = slice.NextPosition;
            }

            Assert.That(EventDataComparer.Equal(_testEvents.Reverse().ToArray(), all.Take(_testEvents.Length).ToArray()));
        }
    }
}
