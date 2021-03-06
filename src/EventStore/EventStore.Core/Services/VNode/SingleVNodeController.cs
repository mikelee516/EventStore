﻿using System;
using System.Net;
using EventStore.Common.Log;
using EventStore.Common.Utils;
using EventStore.Core.Bus;
using EventStore.Core.Data;
using EventStore.Core.Messages;
using EventStore.Core.Messaging;
using EventStore.Core.Services.TimerService;
using EventStore.Core.TransactionLog.Chunks;

namespace EventStore.Core.Services.VNode
{
    /// <summary>
    /// Implements finite state machine transitions for the Single VNode configuration.
    /// Also maps certain client messages to request messages. 
    /// </summary>
    public class SingleVNodeController : IHandle<Message>
    {
        public static readonly TimeSpan ShutdownTimeout = TimeSpan.FromSeconds(5);

        private static readonly ILogger Log = LogManager.GetLoggerFor<SingleVNodeController>();

        private readonly IPublisher _outputBus;
        private readonly IPEndPoint _httpEndPoint;
        private readonly TFChunkDb _db;
        private readonly SingleVNode _node;

        private VNodeState _state = VNodeState.Initializing;
        private QueuedHandler _mainQueue;
        private readonly VNodeFSM _fsm;

        private int _serviceInitsToExpect = 1 /* StorageChaser */
                                          + 1 /* StorageReader */
                                          + 1 /* StorageWriter */;
        private int _serviceShutdownsToExpect = 1 /* StorageChaser */ 
                                              + 1 /* StorageReader */ 
                                              + 1 /* StorageWriter */ 
                                              + 1 /* HttpService*/;
        private bool _exitProcessOnShutdown;

        public SingleVNodeController(IPublisher outputBus, IPEndPoint httpEndPoint, TFChunkDb db, SingleVNode node)
        {
            Ensure.NotNull(outputBus, "outputBus");
            Ensure.NotNull(httpEndPoint, "httpEndPoint");
            Ensure.NotNull(db, "db");
            Ensure.NotNull(node, "node");

            _outputBus = outputBus;
            _httpEndPoint = httpEndPoint;
            _db = db;
            _node = node;

            _fsm = CreateFSM();
        }

        public void SetMainQueue(QueuedHandler mainQueue)
        {
            Ensure.NotNull(mainQueue, "mainQueue");

            _mainQueue = mainQueue;
        }

        private VNodeFSM CreateFSM()
        {
            var stm = new VNodeFSMBuilder(() => _state)
                .InAnyState()
                    .When<SystemMessage.StateChangeMessage>()
                        .Do(m => Application.Exit(ExitCode.Error, string.Format("{0} message was unhandled in {1}.", m.GetType().Name, GetType().Name)))

                .InState(VNodeState.Initializing)
                    .When<SystemMessage.SystemInit>().Do(Handle)
                    .When<SystemMessage.SystemStart>().Do(Handle)
                    .When<SystemMessage.BecomePreMaster>().Do(Handle)
                    .When<SystemMessage.ServiceInitialized>().Do(Handle)
                    .When<ClientMessage.ScavengeDatabase>().Ignore()
                    .WhenOther().ForwardTo(_outputBus)

                .InStates(VNodeState.Initializing, VNodeState.ShuttingDown, VNodeState.Shutdown)
                    .When<ClientMessage.ReadRequestMessage>().Do(msg => DenyRequestBecauseNotReady(msg.Envelope, msg.CorrelationId))
                .InAllStatesExcept(VNodeState.Initializing, VNodeState.ShuttingDown, VNodeState.Shutdown)
                    .When<ClientMessage.ReadRequestMessage>().ForwardTo(_outputBus)

                .InAllStatesExcept(VNodeState.PreMaster)
                    .When<SystemMessage.WaitForChaserToCatchUp>().Ignore()
                    .When<SystemMessage.ChaserCaughtUp>().Ignore()

                .InState(VNodeState.PreMaster)
                    .When<SystemMessage.BecomeMaster>().Do(Handle)
                    .When<SystemMessage.WaitForChaserToCatchUp>().ForwardTo(_outputBus)
                    .When<SystemMessage.ChaserCaughtUp>().Do(Handle)
                    .WhenOther().ForwardTo(_outputBus)

                .InState(VNodeState.Master)
                    .When<ClientMessage.WriteEvents>().ForwardTo(_outputBus)
                    .When<ClientMessage.TransactionStart>().ForwardTo(_outputBus)
                    .When<ClientMessage.TransactionWrite>().ForwardTo(_outputBus)
                    .When<ClientMessage.TransactionCommit>().ForwardTo(_outputBus)
                    .When<ClientMessage.DeleteStream>().ForwardTo(_outputBus)
                    .When<StorageMessage.WritePrepares>().ForwardTo(_outputBus)
                    .When<StorageMessage.WriteDelete>().ForwardTo(_outputBus)
                    .When<StorageMessage.WriteTransactionStart>().ForwardTo(_outputBus)
                    .When<StorageMessage.WriteTransactionData>().ForwardTo(_outputBus)
                    .When<StorageMessage.WriteTransactionPrepare>().ForwardTo(_outputBus)
                    .When<StorageMessage.WriteCommit>().ForwardTo(_outputBus)
                    .WhenOther().ForwardTo(_outputBus)

                .InAllStatesExcept(VNodeState.Master)
                    .When<ClientMessage.WriteRequestMessage>().Do(msg => DenyRequestBecauseNotReady(msg.Envelope, msg.CorrelationId))
                    .When<StorageMessage.WritePrepares>().Ignore()
                    .When<StorageMessage.WriteDelete>().Ignore()
                    .When<StorageMessage.WriteTransactionStart>().Ignore()
                    .When<StorageMessage.WriteTransactionData>().Ignore()
                    .When<StorageMessage.WriteTransactionPrepare>().Ignore()
                    .When<StorageMessage.WriteCommit>().Ignore()

                .InAllStatesExcept(VNodeState.ShuttingDown, VNodeState.Shutdown)
                    .When<ClientMessage.RequestShutdown>().Do(Handle)
                    .When<SystemMessage.BecomeShuttingDown>().Do(Handle)

                .InState(VNodeState.ShuttingDown)
                    .When<SystemMessage.BecomeShutdown>().Do(Handle)
                    .When<SystemMessage.ShutdownTimeout>().Do(Handle)

                .InStates(VNodeState.ShuttingDown, VNodeState.Shutdown)
                    .When<SystemMessage.ServiceShutdown>().Do(Handle)
                    .WhenOther().ForwardTo(_outputBus)

                .Build();
            return stm;
        }

        void IHandle<Message>.Handle(Message message)
        {
            _fsm.Handle(message);
        }

        private void Handle(SystemMessage.SystemInit message)
        {
            Log.Info("========== [{0}] SYSTEM INIT...", _httpEndPoint);
            _outputBus.Publish(message);
        }

        private void Handle(SystemMessage.SystemStart message)
        {
            Log.Info("========== [{0}] SYSTEM START....", _httpEndPoint);
            _outputBus.Publish(message);
            _fsm.Handle(new SystemMessage.BecomePreMaster(Guid.NewGuid()));
        }

        private void Handle(SystemMessage.BecomePreMaster message)
        {
            Log.Info("========== [{0}] PRE-MASTER STATE, WAITING FOR CHASER TO CATCH UP...", _httpEndPoint);
            _state = VNodeState.PreMaster;
            _mainQueue.Publish(new SystemMessage.WaitForChaserToCatchUp(Guid.NewGuid(), TimeSpan.Zero));
            _outputBus.Publish(message);
        }

        private void Handle(SystemMessage.BecomeMaster message)
        {
            Log.Info("========== [{0}] IS WORKING!!! SPARTA!!!", _httpEndPoint);
            _state = VNodeState.Master;
            _outputBus.Publish(message);
        }

        private void Handle(SystemMessage.BecomeShuttingDown message)
        {
            if (_state == VNodeState.ShuttingDown || _state == VNodeState.Shutdown)
                return;

            Log.Info("========== [{0}] IS SHUTTING DOWN!!! FAREWELL, WORLD...", _httpEndPoint);
            _exitProcessOnShutdown = message.ExitProcess;
            _state = VNodeState.ShuttingDown;
            _mainQueue.Publish(TimerMessage.Schedule.Create(ShutdownTimeout, new PublishEnvelope(_mainQueue), new SystemMessage.ShutdownTimeout()));
            _outputBus.Publish(message);
        }

        private void Handle(SystemMessage.BecomeShutdown message)
        {
            Log.Info("========== [{0}] IS SHUT DOWN!!! SWEET DREAMS!!!", _httpEndPoint);
            _state = VNodeState.Shutdown;
            try
            {
                _outputBus.Publish(message);
            }
            catch (Exception exc)
            {
                Log.ErrorException(exc, "Error when publishing {0}.", message);
            }
            if (_exitProcessOnShutdown)
            {
                try
                {
                    _node.WorkersHandler.Stop();
                    _mainQueue.RequestStop();
                }
                catch (Exception exc)
                {
                    Log.ErrorException(exc, "Error when stopping workers/main queue.");
                }
                Application.Exit(ExitCode.Success, "Shutdown with exiting from process was requested.");
            }
        }

        private void Handle(SystemMessage.ServiceInitialized message)
        {
            Log.Info("========== [{0}] Service '{1}' initialized.", _httpEndPoint, message.ServiceName);
            _serviceInitsToExpect -= 1;
            _outputBus.Publish(message);
            if (_serviceInitsToExpect == 0)
                _mainQueue.Publish(new SystemMessage.SystemStart());
        }

        private void Handle(SystemMessage.ChaserCaughtUp message)
        {
            _outputBus.Publish(message);
            _fsm.Handle(new SystemMessage.BecomeMaster(Guid.NewGuid()));   
        }

        private void DenyRequestBecauseNotReady(IEnvelope envelope, Guid correlationId)
        {
            envelope.ReplyWith(new ClientMessage.NotHandled(correlationId, TcpClientMessageDto.NotHandled.NotHandledReason.NotReady, null));
        }

        private void Handle(ClientMessage.RequestShutdown message)
        {
            _fsm.Handle(new SystemMessage.BecomeShuttingDown(Guid.NewGuid(), message.ExitProcess));
        }

        private void Handle(SystemMessage.ServiceShutdown message)
        {
            Log.Info("========== [{0}] Service '{1}' has shut down.", _httpEndPoint, message.ServiceName);

            _serviceShutdownsToExpect -= 1;
            if (_serviceShutdownsToExpect == 0)
            {
                Log.Info("========== [{0}] All Services Shutdown.", _httpEndPoint);
                Shutdown();
            }
        }

        private void Handle(SystemMessage.ShutdownTimeout message)
        {
            if (_state != VNodeState.ShuttingDown) throw new Exception();

            Log.Info("========== [{0}] Shutdown Timeout.", _httpEndPoint);
            Shutdown();
        }

        private void Shutdown()
        {
            if (_state != VNodeState.ShuttingDown) throw new Exception();

            _db.Close();
            _fsm.Handle(new SystemMessage.BecomeShutdown(Guid.NewGuid()));
        }
    }
}
