using System;
using System.Collections.Generic;
using System.Linq;
using EventStore.Core.Data;
using EventStore.Core.Messages;
using EventStore.Core.Messaging;
using EventStore.Core.Services;
using EventStore.Projections.Core.Messages;
using EventStore.Projections.Core.Services;
using EventStore.Projections.Core.Services.Management;
using NUnit.Framework;

namespace EventStore.Projections.Core.Tests.Services.projections_manager.continuous
{
    public static class a_new_posted_projection
    {
        public abstract class Base : TestFixtureWithProjectionCoreAndManagementServices
        {
            protected string _projectionName;
            protected string _projectionSource;
            protected Type _fakeProjectionType;
            protected ProjectionMode _projectionMode;
            protected bool _checkpointsEnabled;
            protected bool _emitEnabled;
            protected bool _projectionEnabled;

            protected override void Given()
            {
                base.Given();

                _projectionName = "test-projection";
                _projectionSource = @"";
                _fakeProjectionType = typeof (FakeProjection);
                _projectionMode = ProjectionMode.Continuous;
                _checkpointsEnabled = true;
                _emitEnabled = true;
                _projectionEnabled = true;
                
                NoStream("$projections-test-projection-checkpoint");
                NoStream("$projections-test-projection-order");
                NoOtherStreams();
                AllWritesSucceed();
            }

            protected override IEnumerable<WhenStep> When()
            {
                yield return (new SystemMessage.BecomeMaster(Guid.NewGuid()));
                yield return (
                    new ProjectionManagementMessage.Post(
                        new PublishEnvelope(_bus), _projectionMode, _projectionName, ProjectionManagementMessage.RunAs.System,
                        "native:" + _fakeProjectionType.AssemblyQualifiedName, _projectionSource, enabled: _projectionEnabled,
                        checkpointsEnabled: _checkpointsEnabled, emitEnabled: _emitEnabled));
            }
        }

        [TestFixture]
        public class when_get_query : Base
        {
            protected override IEnumerable<WhenStep> When()
            {
                foreach (var m in base.When())
                    yield return m;
                yield return
                    (new ProjectionManagementMessage.GetQuery(
                        new PublishEnvelope(_bus), _projectionName, ProjectionManagementMessage.RunAs.Anonymous));
            }

            [Test]
            public void returns_correct_source()
            {
                Assert.AreEqual(
                    1, _consumer.HandledMessages.OfType<ProjectionManagementMessage.ProjectionQuery>().Count());
                var projectionQuery =
                    _consumer.HandledMessages.OfType<ProjectionManagementMessage.ProjectionQuery>().Single();
                Assert.AreEqual(_projectionName, projectionQuery.Name);
                Assert.AreEqual("", projectionQuery.Query);
            }
        }

        [TestFixture]
        public class when_get_state : Base
        {
            protected override void Given()
            {
                base.Given();
                EnableReadAll();
                ExistingEvent("temp1", "test1", "{}", "{}");
            }

            protected override IEnumerable<WhenStep> When()
            {
                foreach (var m in base.When()) yield return m;
                yield return (new ProjectionManagementMessage.GetState(new PublishEnvelope(_bus), _projectionName, ""));
            }

            [Test]
            public void returns_correct_state()
            {
                Assert.AreEqual(
                    1, _consumer.HandledMessages.OfType<ProjectionManagementMessage.ProjectionState>().Count());
                Assert.AreEqual(
                    _projectionName,
                    _consumer.HandledMessages.OfType<ProjectionManagementMessage.ProjectionState>().Single().Name);
                //at least projection initializaed message is here
                Assert.AreEqual(
                    "{\"data\": 1}", _consumer.HandledMessages.OfType<ProjectionManagementMessage.ProjectionState>().Single().State);
                Assert.AreEqual(
                    _all.Last(v => !SystemStreams.IsSystemStream(v.Value.EventStreamId)).Key,
                    _consumer.HandledMessages.OfType<ProjectionManagementMessage.ProjectionState>()
                        .Single()
                        .Position.Position);
            }
        }

        [TestFixture]
        public class when_failing : Base
        {
            protected override IEnumerable<WhenStep> When()
            {
                foreach (var m in base.When()) yield return m;
                var readerAssignedMessage =
                    _consumer.HandledMessages.OfType<EventReaderSubscriptionMessage.ReaderAssignedReader>().LastOrDefault();
                Assert.IsNotNull(readerAssignedMessage);
                var reader = readerAssignedMessage.ReaderId;

                yield return
                    (ReaderSubscriptionMessage.CommittedEventDistributed.Sample(
                        reader, new TFPos(100, 50), new TFPos(100, 50), "stream", 1, "stream", 1, false, Guid.NewGuid(),
                        "fail", false, new byte[0], new byte[0], 100, 33.3f));
            }

            [Test]
            public void publishes_faulted_message()
            {
                Assert.AreEqual(1, _consumer.HandledMessages.OfType<CoreProjectionStatusMessage.Faulted>().Count());
            }

            [Test]
            public void the_projection_status_becomes_faulted()
            {
                _manager.Handle(
                    new ProjectionManagementMessage.GetStatistics(
                        new PublishEnvelope(_bus), null, _projectionName, false));

                Assert.AreEqual(1, _consumer.HandledMessages.OfType<ProjectionManagementMessage.Statistics>().Count());
                Assert.AreEqual(
                    1,
                    _consumer.HandledMessages.OfType<ProjectionManagementMessage.Statistics>()
                             .Single()
                             .Projections.Length);
                Assert.AreEqual(
                    _projectionName,
                    _consumer.HandledMessages.OfType<ProjectionManagementMessage.Statistics>()
                             .Single()
                             .Projections.Single()
                             .Name);
                Assert.AreEqual(
                    ManagedProjectionState.Faulted,
                    _consumer.HandledMessages.OfType<ProjectionManagementMessage.Statistics>()
                             .Single()
                             .Projections.Single()
                             .MasterStatus);
            }
        }
    }
}
