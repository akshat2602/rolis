
#include "commo.h"
#include "../rcc/graph.h"
#include "../rcc/graph_marshaler.h"
#include "../command.h"
#include "../procedure.h"
#include "../command_marshaler.h"
#include "../rcc_rpc.h"

namespace janus
{

  MultiPaxosCommo::MultiPaxosCommo(PollMgr *poll) : Communicator(poll)
  {
    //  verify(poll != nullptr);
  }

  // not used
  void MultiPaxosCommo::BroadcastPrepare(parid_t par_id,
                                         slotid_t slot_id,
                                         ballot_t ballot,
                                         const function<void(Future *)> &cb)
  {
    verify(0);
    // Log_debug("**** inside void BroadcastPrepare");
    auto proxies = rpc_par_proxies_[par_id];
    for (auto &p : proxies)
    {
      auto proxy = (MultiPaxosProxy *)p.second;
      FutureAttr fuattr;
      fuattr.callback = cb;
      Future::safe_release(proxy->async_Prepare(slot_id, ballot, fuattr));
    }
  }

  // not used
  shared_ptr<PaxosPrepareQuorumEvent>
  MultiPaxosCommo::BroadcastPrepare(parid_t par_id,
                                    slotid_t slot_id,
                                    ballot_t ballot)
  {
    verify(0);
    // Log_debug("**** inside shared_ptr<PaxosPrepareQuorumEvent> MultiPaxosCommo::BroadcastPrepare");
    int n = Config::GetConfig()->GetPartitionSize(par_id);
    auto e = Reactor::CreateSpEvent<PaxosPrepareQuorumEvent>(n, n); // marker:ansh debug
    auto proxies = rpc_par_proxies_[par_id];
    for (auto &p : proxies)
    {
      auto proxy = (MultiPaxosProxy *)p.second;
      FutureAttr fuattr;
      fuattr.callback = [e, ballot](Future *fu)
      {
        ballot_t b = 0;
        fu->get_reply() >> b;
        e->FeedResponse(b == ballot);
        // TODO add max accepted value.
      };
      Future::safe_release(proxy->async_Prepare(slot_id, ballot, fuattr));
    }
    return e;
  }

  // not used
  shared_ptr<PaxosAcceptQuorumEvent>
  MultiPaxosCommo::BroadcastAccept(parid_t par_id,
                                   slotid_t slot_id,
                                   ballot_t ballot,
                                   shared_ptr<Marshallable> cmd)
  {
    verify(0);
    // Log_debug("**** inside shared_ptr<PaxosAcceptQuorumEvent> MultiPaxosCommo::BroadcastAccept");
    int n = Config::GetConfig()->GetPartitionSize(par_id);
    //  auto e = Reactor::CreateSpEvent<PaxosAcceptQuorumEvent>(n, /2n/2+1);
    auto e = Reactor::CreateSpEvent<PaxosAcceptQuorumEvent>(n, n);
    auto proxies = rpc_par_proxies_[par_id];
    vector<Future *> fus;
    for (auto &p : proxies)
    {
      auto proxy = (MultiPaxosProxy *)p.second;
      FutureAttr fuattr;
      fuattr.callback = [e, ballot](Future *fu)
      {
        ballot_t b = 0;
        fu->get_reply() >> b;
        e->FeedResponse(b == ballot);
      };
      MarshallDeputy md(cmd);
      auto f = proxy->async_Accept(slot_id, ballot, md, fuattr);
      Future::safe_release(f);
    }
    return e;
  }

  // not used
  void MultiPaxosCommo::BroadcastAccept(parid_t par_id,
                                        slotid_t slot_id,
                                        ballot_t ballot,
                                        shared_ptr<Marshallable> cmd,
                                        const function<void(Future *)> &cb)
  {
    verify(0);
    // Log_debug("**** inside void BroadcastAccept");
    auto proxies = rpc_par_proxies_[par_id];
    vector<Future *> fus;
    for (auto &p : proxies)
    {
      auto proxy = (MultiPaxosProxy *)p.second;
      FutureAttr fuattr;
      fuattr.callback = cb;
      MarshallDeputy md(cmd);
      auto f = proxy->async_Accept(slot_id, ballot, md, fuattr);
      Future::safe_release(f);
    }
    //  verify(0);
  }

  // not used
  void MultiPaxosCommo::BroadcastDecide(const parid_t par_id,
                                        const slotid_t slot_id,
                                        const ballot_t ballot,
                                        const shared_ptr<Marshallable> cmd)
  {
    verify(0);
    // Log_debug("**** inside void BroadcastDecide");
    auto proxies = rpc_par_proxies_[par_id];
    vector<Future *> fus;
    for (auto &p : proxies)
    {
      auto proxy = (MultiPaxosProxy *)p.second;
      FutureAttr fuattr;
      fuattr.callback = [](Future *fu) {};
      MarshallDeputy md(cmd);
      auto f = proxy->async_Decide(slot_id, ballot, md, fuattr);
      Future::safe_release(f);
    }
  }

  // not used
  shared_ptr<PaxosAcceptQuorumEvent>
  MultiPaxosCommo::BroadcastBulkPrepare(parid_t par_id,
                                        shared_ptr<Marshallable> cmd,
                                        function<void(ballot_t, int)> cb)
  {
    // Log_debug("**** BroadcastBulkPrepare: i am here"); // not used
    int n = Config::GetConfig()->GetPartitionSize(par_id);
    int k = (n % 2 == 0) ? n / 2 : (n / 2 + 1);
    auto e = Reactor::CreateSpEvent<PaxosAcceptQuorumEvent>(n, k); // marker:debug
    // Log_debug("BroadcastBulkPrepare: i am here partition size %d", n);
    auto proxies = rpc_par_proxies_[par_id];
    vector<Future *> fus;
    for (auto &p : proxies)
    {
      auto proxy = (MultiPaxosProxy *)p.second;
      FutureAttr fuattr;
      fuattr.callback = [e, cb](Future *fu)
      {
        i32 valid;
        i32 ballot;
        fu->get_reply() >> ballot >> valid;
        // Log_debug("Received response %d %d", ballot, valid);
        cb(ballot, valid);
        e->FeedResponse(valid);
      };
      verify(cmd != nullptr);
      MarshallDeputy md(cmd);
      auto f = proxy->async_BulkPrepare(md, fuattr);
      Future::safe_release(f);
    }
    return e;
  }

  // used
  shared_ptr<PaxosAcceptQuorumEvent>
  MultiPaxosCommo::BroadcastPrepare2(parid_t par_id,
                                     shared_ptr<Marshallable> cmd,
                                     const std::function<void(MarshallDeputy, ballot_t, int)> &cb)
  {
    // Log_debug("**** BroadcastBulkPrepare2: i am here");
    int n = Config::GetConfig()->GetPartitionSize(par_id);
    int k = (n % 2 == 0) ? n / 2 : (n / 2 + 1);
    auto e = Reactor::CreateSpEvent<PaxosAcceptQuorumEvent>(n, k); // marker:debug
    auto proxies = rpc_par_proxies_[par_id];
    vector<Future *> fus;
    // Log_debug("paxos commo bulkaccept: length proxies %d", proxies.size());
    for (auto &p : proxies)
    {
      auto proxy = (MultiPaxosProxy *)p.second;
      FutureAttr fuattr;
      fuattr.callback = [e, cb](Future *fu)
      {
        i32 valid;
        i32 ballot;
        MarshallDeputy response_val;
        fu->get_reply() >> ballot >> valid >> response_val;
        // Log_debug("BroadcastPrepare2: received response: %d %d", ballot, valid);
        cb(response_val, ballot, valid);
        e->FeedResponse(valid);
      };
      verify(cmd != nullptr);
      MarshallDeputy md(cmd);
      auto f = proxy->async_BulkPrepare2(md, fuattr);
      Future::safe_release(f);
    }
    return e;
  }

  // used
  shared_ptr<PaxosAcceptQuorumEvent>
  MultiPaxosCommo::BroadcastHeartBeat(parid_t par_id,
                                      shared_ptr<Marshallable> cmd,
                                      const function<void(ballot_t, int)> &cb)
  {
    int n = Config::GetConfig()->GetPartitionSize(par_id);
    int k = (n % 2 == 0) ? n / 2 : (n / 2 + 1);
    auto e = Reactor::CreateSpEvent<PaxosAcceptQuorumEvent>(n, k);
    auto proxies = rpc_par_proxies_[par_id];
    vector<Future *> fus;
    for (auto &p : proxies)
    {
      auto proxy = (MultiPaxosProxy *)p.second;
      FutureAttr fuattr;
      fuattr.callback = [e, cb](Future *fu)
      {
        i32 valid;
        i32 ballot;
        fu->get_reply() >> ballot >> valid;
        cb(ballot, valid);
        e->FeedResponse(valid);
      };
      verify(cmd != nullptr);
      MarshallDeputy md(cmd);
      auto f = proxy->async_Heartbeat(md, fuattr);
      Future::safe_release(f);
    }
    return e;
  }

  shared_ptr<PaxosAcceptQuorumEvent>
  MultiPaxosCommo::CrpcBroadcastHeartBeat(parid_t par_id,
                                          shared_ptr<Marshallable> cmd,
                                          const function<void(ballot_t, int)> &cb,
                                          siteid_t leader_site_id)
  {
    Log_debug("inside CrpcBroadcastHeartbeat");
    int n = Config::GetConfig()->GetPartitionSize(par_id);
    int k = (n % 2 == 0) ? n / 2 : (n / 2 + 1);
    auto e = Reactor::CreateSpEvent<PaxosAcceptQuorumEvent>(n, k);
    auto proxies = rpc_par_proxies_[par_id];

    std::vector<uint16_t> sitesInfo_;

    for (auto &p : proxies)
    {
      auto id = p.first;
      Log_debug("id is: %d and leader_site_id is: %d", id, leader_site_id);
      if (id != leader_site_id)
      {                           // #cPRC additional
        sitesInfo_.push_back(id); // #cPRC additional
      }                           // #cPRC additional
    }

    sitesInfo_.push_back(leader_site_id);

    for (auto &p : proxies)
    {
      auto proxy = (MultiPaxosProxy *)p.second;
      // FutureAttr fuattr;
      // fuattr.callback = [e, cb] (Future* fu) {
      //   i32 valid;
      //   i32 ballot;
      //   fu->get_reply() >> ballot >> valid;
      //   cb(ballot, valid);
      //   e->FeedResponse(valid);
      // };
      if (p.first != sitesInfo_[0])
      {
        continue;
      }
      verify(cmd != nullptr);
      MarshallDeputy md(cmd);
      std::vector<BalValResult> state;
      uint64_t crpc_id = reinterpret_cast<uint64_t>(&e);
      // // Log_debug("*** crpc_id is: %d", crpc_id); // verify it's never the same
      verify(cRPCEvents.find(crpc_id) == cRPCEvents.end());

      auto f = proxy->async_CrpcHeartbeat(crpc_id, md, sitesInfo_, state);
      Future::safe_release(f);
      cRPCEvents[crpc_id] = std::make_pair(cb, e);
      break;
    }
    return e;
  }

  void MultiPaxosCommo::CrpcHeartbeat(parid_t par_id,
                                      uint64_t id,
                                      MarshallDeputy cmd,
                                      std::vector<uint16_t> &addrChain,
                                      std::vector<BalValResult> &state)
  {
    // Log_debug("**** inside MultiPaxosCommo::CrpcHeartbeat");
    auto proxies = rpc_par_proxies_[par_id];
    for (auto &p : proxies)
    {
      if (p.first != addrChain[0])
      {
        continue;
      }
      // Log_debug("**** inside MultiPaxosCommo::CrpcHeartbeat; p.first:%d", p.first);
      auto proxy = (MultiPaxosProxy *)p.second;
      auto f = proxy->async_CrpcHeartbeat(id, cmd, addrChain, state);
      Future::safe_release(f);
      // Log_debug("**** returning MultiPaxosCommo::CrpcHeartbeat");
      break;
    }
  }

  // not used
  shared_ptr<PaxosAcceptQuorumEvent>
  MultiPaxosCommo::BroadcastSyncLog(parid_t par_id,
                                    shared_ptr<Marshallable> cmd,
                                    const std::function<void(shared_ptr<MarshallDeputy>, ballot_t, int)> &cb)
  {
    Log_debug("**** inside BroadcastSyncLog");
    int n = Config::GetConfig()->GetPartitionSize(par_id);
    int k = (n % 2 == 0) ? n / 2 : (n / 2 + 1);
    auto e = Reactor::CreateSpEvent<PaxosAcceptQuorumEvent>(n, k);
    auto proxies = rpc_par_proxies_[par_id];
    vector<Future *> fus;
    for (auto &p : proxies)
    {
      auto proxy = (MultiPaxosProxy *)p.second;
      FutureAttr fuattr;
      fuattr.callback = [e, cb](Future *fu)
      {
        i32 valid;
        i32 ballot;
        MarshallDeputy response_val;
        fu->get_reply() >> ballot >> valid >> response_val;
        auto sp_md = make_shared<MarshallDeputy>(response_val);
        cb(sp_md, ballot, valid);
        e->FeedResponse(valid);
      };
      verify(cmd != nullptr);
      MarshallDeputy md(cmd);
      auto f = proxy->async_SyncLog(md, fuattr);
      Future::safe_release(f);
    }
    return e;
  }

  // not used
  shared_ptr<PaxosAcceptQuorumEvent>
  MultiPaxosCommo::BroadcastSyncNoOps(parid_t par_id,
                                      shared_ptr<Marshallable> cmd,
                                      const std::function<void(ballot_t, int)> &cb)
  {
    // Log_debug("**** inside BroadcastSyncNoOps");
    int n = Config::GetConfig()->GetPartitionSize(par_id);
    int k = (n % 2 == 0) ? n / 2 : (n / 2 + 1);
    auto e = Reactor::CreateSpEvent<PaxosAcceptQuorumEvent>(n, k);
    auto proxies = rpc_par_proxies_[par_id];
    vector<Future *> fus;
    for (auto &p : proxies)
    {
      auto proxy = (MultiPaxosProxy *)p.second;
      FutureAttr fuattr;
      fuattr.callback = [e, cb](Future *fu)
      {
        i32 valid;
        i32 ballot;
        fu->get_reply() >> ballot >> valid;
        cb(ballot, valid);
        e->FeedResponse(valid);
      };
      verify(cmd != nullptr);
      MarshallDeputy md(cmd);
      auto f = proxy->async_SyncNoOps(md, fuattr);
      Future::safe_release(f);
    }
    return e;
  }

  // not used
  shared_ptr<PaxosAcceptQuorumEvent>
  MultiPaxosCommo::BroadcastSyncCommit(parid_t par_id,
                                       shared_ptr<Marshallable> cmd,
                                       const std::function<void(ballot_t, int)> &cb)
  {
    Log_debug("**** inside BroadcastSyncCommit");
    // Log_debug("**** inside BroadcastSyncCommit, with size of cmd is: %d", sizeof(cmd));
    int n = Config::GetConfig()->GetPartitionSize(par_id);
    int k = (n % 2 == 0) ? n / 2 : (n / 2 + 1);
    auto e = Reactor::CreateSpEvent<PaxosAcceptQuorumEvent>(n, k);
    auto proxies = rpc_par_proxies_[par_id];
    vector<Future *> fus;
    for (auto &p : proxies)
    {
      auto proxy = (MultiPaxosProxy *)p.second;
      FutureAttr fuattr;
      fuattr.callback = [e, cb](Future *fu)
      {
        i32 valid;
        i32 ballot;
        fu->get_reply() >> ballot >> valid;
        cb(ballot, valid);
        e->FeedResponse(valid);
      };
      verify(cmd != nullptr);
      MarshallDeputy md(cmd);
      auto f = proxy->async_SyncCommit(md, fuattr);
      Future::safe_release(f);
    }
    return e;
  }

  // used
  shared_ptr<PaxosAcceptQuorumEvent>
  MultiPaxosCommo::BroadcastBulkAccept(parid_t par_id,
                                       shared_ptr<Marshallable> cmd,
                                       const function<void(ballot_t, int)> &cb)
  {
    // Log_debug("**** inside BroadcastBulkAccept");
    static bool hasPrinted = false; // Static variable to track if it has printed

    if (!hasPrinted)
    {
      Log_debug("in no cRPC;");
      // Log_debug("in no cRPC; tid of leader is %d", gettid());
      hasPrinted = true; // Update the static variable
    }
    int n = Config::GetConfig()->GetPartitionSize(par_id);
    int k = (n % 2 == 0) ? n / 2 : (n / 2 + 1);
    auto e = Reactor::CreateSpEvent<PaxosAcceptQuorumEvent>(n, k); // marker:debug
    auto proxies = rpc_par_proxies_[par_id];
    vector<Future *> fus;
    // Log_debug("Sending bulk accept for some slot");
    // Log_debug("paxos commo bulkaccept: length proxies %d", proxies.size());
    for (auto &p : proxies)
    {
      auto proxy = (MultiPaxosProxy *)p.second;
      FutureAttr fuattr;
      int st = p.first;
      fuattr.callback = [e, cb, st](Future *fu)
      {
        i32 valid;
        i32 ballot;
        fu->get_reply() >> ballot >> valid;
        Log_debug("Accept response received from %d site", st);
        cb(ballot, valid);
        e->FeedResponse(valid);
      };
      verify(cmd != nullptr);
      MarshallDeputy md(cmd);
      // Log_debug("Sending bulk accept for some slot");
      auto f = proxy->async_BulkAccept(md, fuattr);
      Future::safe_release(f);
    }
    return e;
  }

  shared_ptr<PaxosAcceptQuorumEvent>
  MultiPaxosCommo::CrpcBroadcastBulkAccept(parid_t par_id,
                                           shared_ptr<Marshallable> cmd,
                                           const function<void(ballot_t, int)> &cb,
                                           siteid_t leader_site_id)
  {
    // Log_debug("**** inside CrpcBroadcastBulkAccept, with par_id: %d", par_id);
    // Log_debug("**** inside CrpcBroadcastBulkAccept, with size of cmd is: %d", sizeof(cmd));
    static bool hasPrinted = false; // Static variable to track if it has printed

    if (!hasPrinted)
    {
      Log_debug("in cRPC");
      hasPrinted = true; // Update the static variable
    }
    int n = Config::GetConfig()->GetPartitionSize(par_id);
    int k = (n % 2 == 0) ? n / 2 : (n / 2 + 1);
    auto e = Reactor::CreateSpEvent<PaxosAcceptQuorumEvent>(n, k);
    auto proxies = rpc_par_proxies_[par_id];

    std::vector<uint16_t> sitesInfo_;

    sitesInfo_.push_back(leader_site_id);

    for (auto &p : proxies)
    {
      auto id = p.first;
      // Log_debug("**** id is: %d and leader_site_id is: %d", id, leader_site_id);
      if (id != leader_site_id)
      {                           // #cPRC additional
        sitesInfo_.push_back(id); // #cPRC additional
      }                           // #cPRC additional
    }

    sitesInfo_.push_back(leader_site_id);

    for (auto &p : proxies)
    {
      auto proxy = (MultiPaxosProxy *)p.second;
      // if(p.first == leader_site_id){
      //   FutureAttr fuattr;
      //   int st = p.first;
      //   fuattr.callback = [e, cb, st] (Future* fu) {
      //     i32 valid;
      //     i32 ballot;
      //     fu->get_reply() >> ballot >> valid;
      //     Log_debug("Accept response received from %d site", st);
      //     cb(ballot, valid);
      //     e->FeedResponse(valid);
      //   };
      //   verify(cmd != nullptr);
      //   MarshallDeputy md(cmd);
      //   // Log_debug("**** inside CrpcBroadcastBulkAccept, with size of md is: %d", sizeof(md));
      //   // // Log_debug("*** crpc_id is: %d", crpc_id); // verify it's never the same
      //   auto f = proxy->async_BulkAccept(md, fuattr);
      //   Future::safe_release(f);
      //   continue;
      // }
      if (p.first == sitesInfo_[0])
      {
        verify(cmd != nullptr);
        MarshallDeputy md(cmd);
        // Log_debug("**** inside CrpcBroadcastBulkAccept, with size of md is: %d", sizeof(md));
        std::vector<BalValResult> state;
        int sizeA = sizeof(&e);
        int sizeB = sizeof(uint64_t);
        verify(sizeA == sizeB);
        uint64_t crpc_id = crpc_id_counter++;
        Log_debug("#### MultiPaxosCommo::; par_id: %d,  crpc_id is: %d", par_id, crpc_id); // verify it's never the same
        // uint64_t crpc_id = reinterpret_cast<uint64_t>(&e);
        // // Log_debug("*** crpc_id is: %d", crpc_id); // verify it's never the same
        verify(cRPCEvents.find(crpc_id) == cRPCEvents.end());
        cRPCEvents[crpc_id] = std::make_pair(cb, e);
        auto f = proxy->async_CrpcBulkAccept(crpc_id, md, sitesInfo_, state);
        Future::safe_release(f);
        break;
      }
    }
    return e;
  }

  void MultiPaxosCommo::CrpcBulkAccept(parid_t par_id,
                                       uint16_t recv_id,
                                       uint64_t id,
                                       MarshallDeputy cmd,
                                       std::vector<uint16_t> &addrChain,
                                       std::vector<BalValResult> &state)
  {
    // Log_debug("#### inside MultiPaxosCommo::CrpcBulkAccept cp0 with par_id: %d", par_id);
    // Log_debug("#### MultiPaxosCommo::CrpcBulkAccept; cp 0 with crpc_id: %ld", id);
    auto proxies = rpc_par_proxies_[par_id];
    for (auto &p : proxies)
    {
      if (p.first == recv_id)
      {
        auto proxy = (MultiPaxosProxy *)p.second;
        auto f = proxy->async_CrpcBulkAccept(id, cmd, addrChain, state);

        Future::safe_release(f);

        break;
      }
    }
    // Log_debug("#### MultiPaxosCommo::CrpcBulkAccept; cp 2 with crpc_id: %ld", id);
  }

  // used
  shared_ptr<PaxosAcceptQuorumEvent>
  MultiPaxosCommo::BroadcastBulkDecide(parid_t par_id,
                                       shared_ptr<Marshallable> cmd,
                                       const function<void(ballot_t, int)> &cb)
  {
    // Log_debug("**** inside BroadcastBulkDecide");
    // Log_debug("**** inside BroadcastBulkDecide, with size of cmd is: %d", sizeof(cmd));
    auto proxies = rpc_par_proxies_[par_id];
    int n = Config::GetConfig()->GetPartitionSize(par_id);
    int k = (n % 2 == 0) ? n / 2 : (n / 2 + 1);
    auto e = Reactor::CreateSpEvent<PaxosAcceptQuorumEvent>(n, k); // marker:debug
    vector<Future *> fus;
    for (auto &p : proxies)
    {
      auto proxy = (MultiPaxosProxy *)p.second;
      FutureAttr fuattr;
      fuattr.callback = [e, cb](Future *fu)
      {
        i32 valid;
        i32 ballot;
        fu->get_reply() >> ballot >> valid;
        cb(ballot, valid);
        e->FeedResponse(valid);
      };
      MarshallDeputy md(cmd);
      auto f = proxy->async_BulkDecide(md, fuattr);
      Future::safe_release(f);
    }
    return e;
  }

  shared_ptr<PaxosAcceptQuorumEvent>
  MultiPaxosCommo::CrpcBroadcastBulkDecide(parid_t par_id,
                                           shared_ptr<Marshallable> cmd,
                                           const function<void(ballot_t, int)> &cb,
                                           siteid_t leader_site_id)
  {
    // Log_debug("**** inside CrpcBroadcastBulkDecide, with par_id: %d", par_id);
    // Log_debug("**** inside CrpcBroadcastBulkDecide, with size of cmd is: %d", sizeof(cmd));
    auto proxies = rpc_par_proxies_[par_id];
    int n = Config::GetConfig()->GetPartitionSize(par_id);
    int k = (n % 2 == 0) ? n / 2 : (n / 2 + 1);
    auto e = Reactor::CreateSpEvent<PaxosAcceptQuorumEvent>(n, k);

    std::vector<uint16_t> sitesInfo_;

    for (auto &p : proxies)
    {
      auto id = p.first;
      // Log_debug("**** CrpcBroadcastBulkDecide; id is: %d and leader_site_id is: %d", id, leader_site_id);
      if (id != leader_site_id)
      {                           // #cPRC additional
        sitesInfo_.push_back(id); // #cPRC additional
      }                           // #cPRC additional
    }

    sitesInfo_.push_back(leader_site_id);

    for (auto &p : proxies)
    {
      auto proxy = (MultiPaxosProxy *)p.second;
      if (p.first != sitesInfo_[0])
      {
        continue;
      }
      verify(cmd != nullptr);
      MarshallDeputy md(cmd);
      std::vector<BalValResult> state;
      uint64_t crpc_id = reinterpret_cast<uint64_t>(&e);
      // // Log_debug("*** crpc_id is: %d", crpc_id); // verify it's never the same
      verify(cRPCEvents.find(crpc_id) == cRPCEvents.end());

      auto f = proxy->async_CrpcBulkDecide(crpc_id, md, sitesInfo_, state);
      Future::safe_release(f);
      cRPCEvents[crpc_id] = std::make_pair(cb, e);
      break;
    }
    return e;
  }

  void MultiPaxosCommo::CrpcBulkDecide(parid_t par_id,
                                       uint64_t id,
                                       MarshallDeputy cmd,
                                       std::vector<uint16_t> &addrChain,
                                       std::vector<BalValResult> &state)
  {
    // Log_debug("**** inside MultiPaxosCommo::CrpcBulkDecide");
    auto proxies = rpc_par_proxies_[par_id];
    for (auto &p : proxies)
    {
      if (p.first != addrChain[0])
      {
        continue;
      }
      // Log_debug("**** inside MultiPaxosCommo::CrpcBulkDecide; p.first:%d", p.first);
      auto proxy = (MultiPaxosProxy *)p.second;
      auto f = proxy->async_CrpcBulkDecide(id, cmd, addrChain, state);
      Future::safe_release(f);
      // Log_debug("**** returning MultiPaxosCommo::CrpcBulkDecide");
      break;
    }
  }

} // namespace janus
