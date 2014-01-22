﻿// Copyright (c) 2012, Event Store LLP
// All rights reserved.
// 
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are
// met:
// 
// Redistributions of source code must retain the above copyright notice,
// this list of conditions and the following disclaimer.
// Redistributions in binary form must reproduce the above copyright
// notice, this list of conditions and the following disclaimer in the
// documentation and/or other materials provided with the distribution.
// Neither the name of the Event Store LLP nor the names of its
// contributors may be used to endorse or promote products derived from
// this software without specific prior written permission
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
// A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
// HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
// LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
// THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
// 

using EventStore.ClientAPI;
using EventStore.ClientAPI.Common.Log;
using EventStore.ClientAPI.SystemData;
using EventStore.Common.Options;
using EventStore.Core;
using EventStore.Core.Bus;
using EventStore.Core.Tests;
using EventStore.Core.Tests.Helpers;
using EventStore.Projections.Core.Services.Processing;
using NUnit.Framework;

namespace EventStore.Projections.Core.Tests.ClientAPI
{
    [TestFixture, Category("LongRunning")]
    public class when_running_standard_projections : SpecificationWithDirectoryPerTestFixture
    {
        private MiniNode _node;
        private IEventStoreConnection _conn;
        private ProjectionsSubsystem _projections;
        private UserCredentials _admin = new UserCredentials("admin", "changeit");
        private ProjectionsManager _manager;

        [TestFixtureSetUp]
        public override void TestFixtureSetUp()
        {
            base.TestFixtureSetUp();
            QueueStatsCollector.InitializeIdleDetection();
            _projections = new Projections.Core.ProjectionsSubsystem(
                projectionWorkerThreadCount: 1, runProjections: RunProjections.All);
            _node = new MiniNode(
                PathName, skipInitializeStandardUsersCheck: false, subsystems: new ISubsystem[] {_projections});
            _node.Start();

            _conn = EventStoreConnection.Create(_node.TcpEndPoint);
            _conn.Connect();

            _manager = new ProjectionsManager(new ConsoleLogger(), _node.HttpEndPoint);
        }

        [TestFixtureTearDown]
        public override void TestFixtureTearDown()
        {
            _conn.Close();
            _node.Shutdown();
            base.TestFixtureTearDown();
            QueueStatsCollector.InitializeIdleDetection(enable: false);
        }

        [Test, Category("LongRunning"), Category("Network"), Ignore]
        public void streams_stream_exists()
        {
            _manager.Enable(ProjectionNamesBuilder.StandardProjections.EventByCategoryStandardProjection, _admin);
            _manager.Enable(ProjectionNamesBuilder.StandardProjections.EventByTypeStandardProjection, _admin);
            _manager.Enable(ProjectionNamesBuilder.StandardProjections.StreamByCategoryStandardProjection, _admin);
            _manager.Enable(ProjectionNamesBuilder.StandardProjections.StreamsStandardProjection, _admin);
            QueueStatsCollector.WaitIdle();
            //TODO: enable system projections
            //TODO: await all worker threads empty
            Assert.AreEqual(
                SliceReadStatus.Success, _conn.ReadStreamEventsForward("$streams", 0, 10, false, _admin).Status);

        }

    }
}