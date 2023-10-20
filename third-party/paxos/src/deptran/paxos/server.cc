

#include "server.h"
#include "../paxos_worker.h"
#include "exec.h"
#include "commo.h"

namespace janus
{

  shared_ptr<ElectionState> es = ElectionState::instance();

  void PaxosServer::OnPrepare(slotid_t slot_id,
                              ballot_t ballot,
                              ballot_t *max_ballot,
                              const function<void()> &cb)
  {
    std::lock_guard<std::recursive_mutex> lock(mtx_);
    Log_debug("multi-paxos scheduler receives prepare for slot_id: %llx",
              slot_id);
    auto instance = GetInstance(slot_id);
    verify(ballot != instance->max_ballot_seen_);
    if (instance->max_ballot_seen_ < ballot)
    {
      instance->max_ballot_seen_ = ballot;
    }
    else
    {
      // TODO if accepted anything, return;
      verify(0);
    }
    *max_ballot = instance->max_ballot_seen_;
    n_prepare_++;
    cb();
  }

  void PaxosServer::OnAccept(const slotid_t slot_id,
                             const ballot_t ballot,
                             shared_ptr<Marshallable> &cmd,
                             ballot_t *max_ballot,
                             const function<void()> &cb)
  {
    std::lock_guard<std::recursive_mutex> lock(mtx_);
    Log_debug("multi-paxos scheduler accept for slot_id: %llx", slot_id);
    auto instance = GetInstance(slot_id);
    verify(instance->max_ballot_accepted_ < ballot);
    if (instance->max_ballot_seen_ <= ballot)
    {
      instance->max_ballot_seen_ = ballot;
      instance->max_ballot_accepted_ = ballot;
    }
    else
    {
      // TODO
      verify(0);
    }
    *max_ballot = instance->max_ballot_seen_;
    n_accept_++;
    cb();
  }

  void PaxosServer::OnCommit(const slotid_t slot_id,
                             const ballot_t ballot,
                             shared_ptr<Marshallable> &cmd)
  {
    std::lock_guard<std::recursive_mutex> lock(mtx_);
    Log_debug("multi-paxos scheduler decide for slot: %lx", slot_id);
    auto instance = GetInstance(slot_id);
    instance->committed_cmd_ = cmd;
    if (slot_id > max_committed_slot_)
    {
      max_committed_slot_ = slot_id;
    }
    verify(slot_id > max_executed_slot_);
    for (slotid_t id = max_executed_slot_ + 1; id <= max_committed_slot_; id++)
    {
      auto next_instance = GetInstance(id);
      if (next_instance->committed_cmd_)
      {
        app_next_(*next_instance->committed_cmd_);
        Log_debug("multi-paxos par:%d loc:%d executed slot %lx now", partition_id_, loc_id_, id);
        max_executed_slot_++;
        n_commit_++;
      }
      else
      {
        break;
      }
    }
    FreeSlots();
  }
  // marker:ansh change the args to accomodate objects
  // marker:ansh add a suitable reply at bottom
  void PaxosServer::OnBulkPrepare(shared_ptr<Marshallable> &cmd,
                                  i32 *ballot,
                                  i32 *valid,
                                  const function<void()> &cb)
  {

    auto bp_log = dynamic_pointer_cast<BulkPrepareLog>(cmd);
    es->state_lock();
    if (bp_log->epoch < es->cur_epoch)
    {
      // es->state_unlock();
      *valid = 0;
      *ballot = es->cur_epoch;
      es->state_unlock();
      cb();
      return;
    }

    if (bp_log->epoch == es->cur_epoch && bp_log->leader_id != es->machine_id)
    {
      // es->state_unlock();
      *valid = 0;
      *ballot = es->cur_epoch;
      es->state_unlock();
      cb();
      return;
    }

    /* acquire all other server locks one by one */
    Log_debug("Paxos workers size %d %d %d", pxs_workers_g.size(), bp_log->leader_id, bp_log->epoch, es->cur_epoch);
    for (int i = 0; i < bp_log->min_prepared_slots.size(); i++)
    {
      // if(pxs_workers_g[i])
      //	Log_debug("cast successfull %d", i);
      PaxosServer *ps = (PaxosServer *)(pxs_workers_g[i]->rep_sched_);
      ps->mtx_.lock();
    }

    /*verify possibility before modification*/
    for (int i = 0; i < bp_log->min_prepared_slots.size(); i++)
    {
      slotid_t slot_id_min = bp_log->min_prepared_slots[i].second;
      PaxosServer *ps = dynamic_cast<PaxosServer *>(pxs_workers_g[i]->rep_sched_);
      BulkPrepare *bp = &ps->bulk_prepares[make_pair(ps->cur_min_prepared_slot_, ps->max_possible_slot_)];
      if (ps->bulk_prepares.size() != 0 && bp->seen_ballot > bp_log->epoch)
      {
        verify(0); // should not happen, should have been caught bp_log->epoch.
      }
      else
      {
        // debug
        // if(slot_id_min <= ps->max_committed_slot_){
        //  verify(0); // marker:ansh to handle. // handle later
        //}
      }
    }

    for (int i = 0; i < bp_log->min_prepared_slots.size(); i++)
    {
      slotid_t slot_id_min = bp_log->min_prepared_slots[i].second;
      PaxosServer *ps = dynamic_cast<PaxosServer *>(pxs_workers_g[i]->rep_sched_);
      BulkPrepare *bp = &ps->bulk_prepares[make_pair(ps->cur_min_prepared_slot_, ps->max_possible_slot_)];
      ps->bulk_prepares.erase(make_pair(ps->cur_min_prepared_slot_, ps->max_possible_slot_));
      bp->seen_ballot = bp_log->epoch;
      bp->leader_id = bp_log->leader_id;
      ps->bulk_prepares[make_pair(slot_id_min, max_possible_slot_)] = *bp;
      ps->cur_min_prepared_slot_ = slot_id_min;
      ps->cur_epoch = bp_log->epoch;
      // ps->clear_accepted_entries(); // pending bulk-prepare-return
    }

  unlock_and_return:

    /* change election state holder */
    if (es->machine_id != bp_log->leader_id)
      es->set_state(0);
    es->set_leader(bp_log->leader_id);
    es->set_lastseen();
    Log_debug("Leader set to %d", bp_log->leader_id);
    es->set_epoch(bp_log->epoch);

    for (int i = 0; i < bp_log->min_prepared_slots.size(); i++)
    {
      PaxosServer *ps = dynamic_cast<PaxosServer *>(pxs_workers_g[i]->rep_sched_);
      ps->mtx_.unlock();
    }
    *ballot = es->cur_epoch;
    es->state_unlock();
    Log_debug("BulkPrepare: Terminating RPC here");
    *valid = 1;
    cb();
  }

  void PaxosServer::OnHeartbeat(shared_ptr<Marshallable> &cmd,
                                i32 *ballot,
                                i32 *valid,
                                const function<void()> &cb)
  {

    auto hb_log = dynamic_pointer_cast<HeartBeatLog>(cmd);
    es->state_lock();
    if (hb_log->epoch < es->cur_epoch)
    {
      es->state_unlock();
      *valid = 0;
      *ballot = es->cur_epoch;
      cb();
      return;
    }
    if (hb_log->leader_id == 1 && es->machine_id == 2)
      Log_debug("OnHeartbeat: received heartbeat from machine is %d %d", hb_log->leader_id, es->leader_id);
    if (hb_log->epoch == es->cur_epoch)
    {
      if (hb_log->leader_id != es->leader_id)
      {
        Log_debug("Req leader is %d while machine leader is %d", hb_log->leader_id, es->leader_id);
        es->state_unlock();
        verify(0); // should not happen, means there are two leaders with different in the same epoch.
      }
      else if (hb_log->leader_id == es->leader_id)
      {
        if (hb_log->leader_id != es->machine_id)
          es->set_state(0);
        es->set_epoch(hb_log->epoch);
        es->set_lastseen();
        es->state_unlock();
        *valid = 1;
        cb();
        return;
      }
      else
      {
        es->set_lastseen();
        es->state_unlock();
        *valid = 1;
        cb();
        return;
      }
    }
    else
    {
      // in this case reply needs to be that it needs a prepare.
      *valid = 2 + es->machine_id; // hacky way.
      es->set_state(0);
      es->set_epoch(hb_log->epoch);
      for (int i = 0; i < pxs_workers_g.size() - 1; i++)
      {
        PaxosServer *ps = dynamic_cast<PaxosServer *>(pxs_workers_g[i]->rep_sched_);
        ps->mtx_.lock();
        ps->cur_epoch = hb_log->epoch;
        ps->leader_id = hb_log->leader_id;
        ps->mtx_.unlock();
      }
      es->set_lastseen();
      es->set_leader(hb_log->leader_id);
      es->state_unlock();
      *valid = 1;
      cb();
      return;
    }
  }

  void PaxosServer::OnCrpcHeartbeat(const uint64_t &id,
                                    const MarshallDeputy &cmd,
                                    const std::vector<uint16_t> &addrChain,
                                    const std::vector<BalValResult> &state)
  {
    // Log_debug("**** inside PaxosServer::OnCrpcHeartbeat cp0, with state.size():%d", state.size());
    if (addrChain.size() == 1)
    {
      Log_debug("==== OnCrpcHeartbeat reached the final link in the chain");
      auto x = (MultiPaxosCommo *)(this->commo_);
      verify(x->cRPCEvents.find(id) != x->cRPCEvents.end()); // #profile - 1.40%
      // Log_debug("inside FpgaRaftServer::OnCRPC2; checkpoint 2 @ %d", gettid());
      auto ev = x->cRPCEvents[id]; // imagine this to be a pair
      x->cRPCEvents.erase(id);

      // Log_debug("==== inside demoserviceimpl::cRPC; results state is following");
      // auto st = dynamic_pointer_cast<AppendEntriesCommandState>(state.sp_data_);   // #profile - 0.54%
      for (auto el : state)
      {
        // Log_debug("inside FpgaRaftServer::OnCRPC2; checkpoint 3 @ %d", gettid());
        ev.first(el.ballot, el.valid);
        // bool y = ((el.followerAppendOK == 1) && (this->IsLeader()) && (currentTerm == el.followerCurrentTerm));
        ev.second->FeedResponse(el.valid);
      }

      Log_debug("==== OnCrpcHeartbeat returning from cRPC");
      return;
    }

    // Log_debug("**** inside PaxosServer::OnCrpcHeartbeat cp1");
    // Log_debug("calling dynamic_pointer_cast<AppendEntriesCommand>(state.sp_data_)");
    // auto c = dynamic_pointer_cast<AppendEntriesCommand>(cmd.sp_data_);
    // Log_debug("return dynamic_pointer_cast<AppendEntriesCommand>(state.sp_data_)");
    BalValResult res;
    auto r = Coroutine::CreateRun([&]()
                                  { this->OnHeartbeat(const_cast<MarshallDeputy &>(cmd).sp_data_,
                                                      &res.ballot,
                                                      &res.valid,
                                                      []() {}); }); // #profile - 2.88%
    // Log_debug("**** inside PaxosServer::OnCrpcHeartbeat cp2, with ballot: %d, and valid: %d", res.ballot, res.valid);
    std::vector<BalValResult> st(state);
    // auto st = dynamic_pointer_cast<AppendEntriesCommandState>(state.sp_data_);  // #profile - 1.23%  ==> dont think can do anything about it
    // Log_debug("returned dynamic_pointer_cast<AppendEntriesCommandState>(state.sp_data_)");
    st.push_back(res);
    // Log_debug("**** inside PaxosServer::OnCrpcHeartbeat cp3; current size of addrChain is: %d", addrChain.size());
    vector<uint16_t> addrChainCopy(addrChain.begin() + 1, addrChain.end());
    // Log_debug("**** inside PaxosServer::OnCrpcHeartbeat cp4");
    // auto addrChainCopy = addrChain;
    // addrChainCopy.erase(addrChainCopy.begin());
    // Log_debug("inside FpgaRaftServer::OnCRPC3; calling CrpcAppendEntries3");
    // Log_debug("*** inside FpgaRaftServer::OnCRPC; cp 2 tid: %d", gettid());
    parid_t par_id = this->frame_->site_info_->partition_id_;
    // Log_debug("**** inside PaxosServer::OnCrpcHeartbeat cp5; par_id: %d", par_id);
    ((MultiPaxosCommo *)(this->commo_))->CrpcHeartbeat(par_id, id, cmd, addrChainCopy, st);

    // Log_debug("**** inside PaxosServer::OnCrpcHeartbeat cp6");
    // Log_debug("==== returning from void FpgaRaftServer::OnCRPC");
    // Log_debug("*** inside FpgaRaftServer::OnCRPC; cp 3 tid: %d", gettid());
  }

  void PaxosServer::OnBulkPrepare2(shared_ptr<Marshallable> &cmd,
                                   i32 *ballot,
                                   i32 *valid,
                                   shared_ptr<BulkPaxosCmd> ret_cmd,
                                   const function<void()> &cb)
  {
    pthread_setname_np(pthread_self(), "Follower server thread");
    auto bcmd = dynamic_pointer_cast<PaxosPrepCmd>(cmd);
    ballot_t cur_b = bcmd->ballots[0];
    slotid_t cur_slot = bcmd->slots[0];
    int req_leader = bcmd->leader_id;
    if (req_leader == 1 && es->machine_id != 1)
      Log_debug("Prepare Received from new leader");
    // Log_debug("Received paxos Prepare for slot %d ballot %d machine %d",cur_slot, cur_b, req_leader);
    *valid = 1;
    // cb();
    // return;
    auto rbcmd = make_shared<BulkPaxosCmd>();
    Log_debug("Received paxos Prepare for slot %d ballot %d machine %d", cur_slot, cur_b, req_leader);
    // es->state_lock();
    mtx_.lock();
    if (cur_b < cur_epoch)
    {
      *ballot = cur_epoch;
      // es->state_unlock();
      *valid = 0;
      mtx_.unlock();
      cb();
      return;
    }
    mtx_.unlock();

    es->state_lock();
    es->set_lastseen();
    if (req_leader != es->machine_id)
      es->set_state(0);
    es->state_unlock();

    mtx_.lock();
    max_touched_slot = max(max_touched_slot, cur_slot);
    if (cur_b > cur_epoch)
    {
      mtx_.unlock();
      es->state_lock();
      es->set_epoch(cur_b);
      es->set_leader(req_leader); // marker:ansh send leader in every request.
      for (int i = 0; i < pxs_workers_g.size() - 1; i++)
      {
        PaxosServer *ps = dynamic_cast<PaxosServer *>(pxs_workers_g[i]->rep_sched_);
        ps->mtx_.lock();
        ps->cur_epoch = cur_b;
        ps->leader_id = req_leader;
        ps->mtx_.unlock();
      }
      es->state_unlock();
    }
    else
    {
      mtx_.unlock();
      if (req_leader != es->leader_id)
      { // kshivam: triggered in one of the crpc runs, how?
        Log_debug("Req leader is %d and prev leader is %d", req_leader, es->leader_id);
        verify(0); // more than one leader in a term, should not send prepare if not leader.
      }
    }

    mtx_.lock();
    auto instance = GetInstance(cur_slot);
    Log_debug("OnBulkPrepare2: Checks successfull preparing response for slot %d %d", cur_slot, partition_id_);
    if (!instance || !instance->accepted_cmd_)
    {
      mtx_.unlock();
      *valid = 2;
      *ballot = cur_b;
      //*ret_cmd = *bcmd;
      // ret_cmd->ballots.push_back(bcmd->ballots[0]);
      // ret_cmd->slots.push_back(bcmd->slots[0]);
      // ret_cmd->cmds.push_back(bcmd->cmds[0]);
      // Log_debug("OnBulkPrepare2: the kind_ of the response object is");
      // es->state_unlock();
      cb();
      // es->state_unlock();
      return;
    }
    // es->state_unlock();
    Log_debug("OnBulkPrepare2: instance found, Preparing response");
    ret_cmd->ballots.push_back(instance->max_ballot_accepted_);
    ret_cmd->slots.push_back(cur_slot);
    ret_cmd->cmds.push_back(make_shared<MarshallDeputy>(instance->accepted_cmd_));
    mtx_.unlock();
    cb();
  }

  void PaxosServer::OnSyncLog(shared_ptr<Marshallable> &cmd,
                              i32 *ballot,
                              i32 *valid,
                              shared_ptr<SyncLogResponse> ret_cmd,
                              const function<void()> &cb)
  {
    auto bcmd = dynamic_pointer_cast<SyncLogRequest>(cmd);
    es->state_lock();
    if (bcmd->epoch < es->cur_epoch)
    {
      // es->state_unlock();
      *valid = 0;
      *ballot = es->cur_epoch;
      es->state_unlock();
      cb();
      return;
    }
    es->state_unlock();
    *valid = 1;
    for (int i = 0; i < pxs_workers_g.size() - 1; i++)
    {
      ret_cmd->missing_slots.push_back(vector<slotid_t>{});
      PaxosServer *ps = dynamic_cast<PaxosServer *>(pxs_workers_g[i]->rep_sched_);
      auto bp_cmd = make_shared<BulkPaxosCmd>();
      ps->mtx_.lock();

      for (int j = bcmd->sync_commit_slot[i]; j <= ps->max_committed_slot_; j++)
      {
        auto inst = ps->GetInstance(j);
        if (inst->committed_cmd_)
        {
          bp_cmd->slots.push_back(j);
          bp_cmd->ballots.push_back(inst->max_ballot_accepted_);
          auto temp_cmd = inst->committed_cmd_;
          MarshallDeputy md(temp_cmd);
          auto shrd_ptr = make_shared<MarshallDeputy>(md);
          bp_cmd->cmds.push_back(shrd_ptr);
        }
      }
      Log_debug("The partition %d, and max executed slot is %d and sync commit is %d", i, ps->max_executed_slot_, bcmd->sync_commit_slot[i]);
      for (int j = ps->max_executed_slot_; j < bcmd->sync_commit_slot[i]; j++)
      {
        auto inst = ps->GetInstance(j);
        if (!inst->committed_cmd_)
        {
          ret_cmd->missing_slots[i].push_back(j);
        }
      }
      Log_debug("The partition %d has missing slots size %d", i, ret_cmd->missing_slots[i].size());
      auto sp_marshallable = dynamic_pointer_cast<Marshallable>(bp_cmd);
      MarshallDeputy bp_md_cmd(sp_marshallable);
      auto bp_sp_md = make_shared<MarshallDeputy>(bp_md_cmd);
      ret_cmd->sync_data.push_back(bp_sp_md);
      ps->mtx_.unlock();
    }
    cb();
  }

  void PaxosServer::OnBulkAccept(shared_ptr<Marshallable> &cmd,
                                 i32 *ballot,
                                 i32 *valid,
                                 const function<void()> &cb)
  {
    // Log_debug("here accept");
    // std::lock_guard<std::recursive_mutex> lock(mtx_);
    auto bcmd = dynamic_pointer_cast<BulkPaxosCmd>(cmd);
    *valid = 1;
    ballot_t cur_b = bcmd->ballots[0];
    slotid_t cur_slot = bcmd->slots[0];
    int req_leader = bcmd->leader_id;
    if (req_leader == 1 && es->machine_id != 1)
      Log_debug("Accept Received from new leader");

    // Log_debug("**** OnBulkAccept; current epoch is: %d", cur_epoch);
    // Log_debug("multi-paxos scheduler accept for slot: %lx", bcmd->slots.size());
    // es->state_lock();
    mtx_.lock();
    if (cur_b < cur_epoch)
    {
      *ballot = cur_epoch;
      // es->state_unlock();
      *valid = 0;
      mtx_.unlock();
      cb();
      return;
    }
    mtx_.unlock();
    es->state_lock();
    es->set_lastseen();
    if (req_leader != es->machine_id)
      es->set_state(0);
    es->state_unlock();
    // cb();
    // return;
    //  Log_debug("multi-paxos scheduler accept for slot: %ld, par_id: %d", cur_slot, partition_id_);
    Log_debug("**** inside PaxosServer::OnBulkAccept; bcmd->slots.size(): %d", bcmd->slots.size());
    for (int i = 0; i < bcmd->slots.size(); i++)
    {
      slotid_t slot_id = bcmd->slots[i];
      ballot_t ballot_id = bcmd->ballots[i];
      Log_debug("**** inside PaxosServer::OnBulkAccept; inside loop, slot_id: %ld", slot_id);
      // Log_debug("**** OnBulkAccept; ballot_id:%d", ballot_id);
      mtx_.lock();
      if (cur_epoch > ballot_id)
      {
        *valid = 0;
        *ballot = cur_epoch;
        mtx_.unlock();
        break;
      }
      else
      {
        if (cur_epoch < ballot_id)
        {
          mtx_.unlock();
          // Log_debug("I am here");
          for (int i = 0; i < pxs_workers_g.size() - 1; i++)
          {
            PaxosServer *ps = dynamic_cast<PaxosServer *>(pxs_workers_g[i]->rep_sched_);
            ps->mtx_.lock();
            ps->cur_epoch = ballot_id;
            ps->leader_id = req_leader;
            ps->mtx_.unlock();
          }
        }
        else
        {
          mtx_.unlock();
        }
        es->state_lock();
        es->set_leader(req_leader);
        es->state_unlock();
        auto instance = GetInstance(slot_id);
        // verify(instance->max_ballot_accepted_ < ballot_id);
        instance->max_ballot_seen_ = ballot_id;
        instance->max_ballot_accepted_ = ballot_id;
        Log_debug("#### inside PaxosServer::OnBulkAccept; slot_id: %ld, max_ballot_accepted_ set to: %ld", slot_id, instance->max_ballot_accepted_);
        instance->accepted_cmd_ = bcmd->cmds[i].get()->sp_data_;
        max_accepted_slot_ = slot_id;
        n_accept_++;
        *valid &= 1;
        *ballot = ballot_id;

        if (commit_coro.find(slot_id) != commit_coro.end())
        {
          Log_debug("#### inside PaxosServer::OnBulkAccept; slot_id: %ld, continuing commit coroutine", slot_id);
          commit_coro[slot_id]->Continue();
          commit_coro.erase(slot_id);
        }
      }
    }
    if (req_leader != 0)
      Log_debug("multi-paxos scheduler accept for slot: %ld, par_id: %d", cur_slot, partition_id_);
    // es->state_unlock();
    cb();
    Log_debug("multi-paxos scheduler accept for slot: %ld, par_id: %d", cur_slot, partition_id_);
  }

  void PaxosServer::OnCrpcBulkAccept(const uint64_t &id,
                                     const MarshallDeputy &cmd,
                                     const std::vector<uint16_t> &addrChain,
                                     const std::vector<BalValResult> &state)
  {
    // Log_debug("#### inside paxosServer::OnCrpcBulkAccept, with crpc_id: %ld", id);
    if (addrChain.size() == 1)
    {
      // Log_debug("#### inside paxosServer::OnCrpcBulkAccept, inside chain , with crpc_id: %ld", id);
      auto x = (MultiPaxosCommo *)(this->commo_);
      // verify(x->cRPCEvents.find(id) != x->cRPCEvents.end()); // #profile - 1.40%
      if (x->cRPCEvents.find(id) == x->cRPCEvents.end())
      {
        Log_debug("#### OnCrpcBulkAccept; crpc_id not found in map, crpc_id:%ld already processed probably", id);
        return;
      }
      auto ev = x->cRPCEvents[id]; // imagine this to be a pair
      // x->cRPCEvents.erase(id);
      Log_debug("#### OnCrpcBulkAccept; size of the state is: %d with crpc_id: %ld", state.size(), id);
      int start_index = ev.second->n_voted_yes_ + ev.second->n_voted_no_;
      Log_debug("#### OnCrpcBulkAccept; value of start_index: %d", start_index);
      for (int i = start_index; i < state.size(); i++)
      {
        auto el = state[i];
        Log_debug("#### OnCrpcBulkAccept; inside the last link in the chain with crpc_id: %ld; el.ballot:%d, el.valid:%d", id, el.ballot, el.valid);
        ev.first(el.ballot, el.valid);
        ev.second->FeedResponse(el.valid);
      }
      if (ev.second->IsReady())
      {
        Log_debug("#### OnCrpcBulkAccept; event is ready, for crpc_id: %ld; will erase id from crpc_Events", id);
        x->cRPCEvents.erase(id);
      }
      else
      {
        Log_debug("#### OnCrpcBulkAccept; event is not ready,for crpc_id: %ld", id);
      }
      return;
    }
    BalValResult res;

    this->OnBulkAccept(const_cast<MarshallDeputy &>(cmd).sp_data_,
                       &res.ballot,
                       &res.valid,
                       []() {});
    std::vector<BalValResult> st(state);

    st.push_back(res);
    vector<uint16_t> addrChainCopy(addrChain.begin() + 1, addrChain.end());

    parid_t par_id = this->frame_->site_info_->partition_id_;
    int chain_size = addrChainCopy.size();
    if (chain_size == 1)
    {
      auto sp_cmd = make_shared<LogEntry>();
      // Log_debug("#### checkpoint check check check with crpc_id: %ld", id);
      MarshallDeputy ph(sp_cmd);
      ((MultiPaxosCommo *)(this->commo_))->CrpcBulkAccept(par_id, addrChainCopy[0], id, ph, addrChainCopy, st);
      Log_debug("#### PaxosServer::OnCrpcBulkAccept; last follower in chain, sending response back to leader, par_id: %d, crpc_id: %ld", par_id, id);
      return;
    }

    int n = Config::GetConfig()->GetPartitionSize(par_id);
    int k = (n % 2 == 0) ? n / 2 : (n / 2 + 1);
    Log_debug("#### PaxosServer::OnCrpcBulkAccept; cp 0, crpc_id: %ld; value of k: %d", id, k);
    if (st.size() >= k)
    {
      auto temp_addrChain = vector<uint16_t>{addrChainCopy.back()};
      ((MultiPaxosCommo *)(this->commo_))->CrpcBulkAccept(par_id, addrChainCopy[chain_size - 1], id, cmd, temp_addrChain, st);
      Log_debug("#### PaxosServer::OnCrpcBulkAccept; quorum reached, sending response back to leader, crpc_id: %ld; value of k: %d", id, k);
    }

    Log_debug("#### PaxosServer::OnCrpcBulkAccept; cp 1, crpc_id: %ld; value of k: %d", id, k);
    ((MultiPaxosCommo *)(this->commo_))->CrpcBulkAccept(par_id, addrChainCopy[0], id, cmd, addrChainCopy, st);

    // Log_debug("#### inside PaxosServer::CrpcBulkAccept cp6 with crpc_id: %ld", id);
  }

  void PaxosServer::OnSyncCommit(shared_ptr<Marshallable> &cmd,
                                 i32 *ballot,
                                 i32 *valid,
                                 const function<void()> &cb)
  {
    // Log_debug("here");
    // std::lock_guard<std::recursive_mutex> lock(mtx_);
    // mtx_.lock();
    // Log_debug("here");
    // Log_debug("multi-paxos scheduler decide for slot: %ld", bcmd->slots.size());
    auto bcmd = dynamic_pointer_cast<BulkPaxosCmd>(cmd);
    *valid = 1;
    ballot_t cur_b = bcmd->ballots[0];
    slotid_t cur_slot = bcmd->slots[0];
    // Log_debug("multi-paxos scheduler decide for slot: %ld", cur_slot);
    int req_leader = bcmd->leader_id;
    // es->state_lock();
    mtx_.lock();
    if (cur_b < cur_epoch)
    {
      *ballot = cur_epoch;
      // es->state_unlock();
      *valid = 0;
      mtx_.unlock();
      cb();
      return;
    }
    mtx_.unlock();
    es->state_lock();
    es->set_lastseen();
    if (req_leader != es->machine_id)
      es->set_state(0);
    es->state_unlock();
    vector<shared_ptr<PaxosData>> commit_exec;
    for (int i = 0; i < bcmd->slots.size(); i++)
    {
      // break;
      slotid_t slot_id = bcmd->slots[i];
      ballot_t ballot_id = bcmd->ballots[i];
      mtx_.lock();
      if (cur_epoch > ballot_id)
      {
        *valid = 0;
        *ballot = cur_epoch;
        mtx_.unlock();
        break;
      }
      else
      {
        if (cur_epoch < ballot_id)
        {
          mtx_.unlock();
          for (int i = 0; i < pxs_workers_g.size() - 1; i++)
          {
            PaxosServer *ps = dynamic_cast<PaxosServer *>(pxs_workers_g[i]->rep_sched_);
            ps->mtx_.lock();
            ps->cur_epoch = ballot_id;
            ps->leader_id = req_leader;
            ps->mtx_.unlock();
          }
        }
        else
        {
          mtx_.unlock();
        }
        es->state_lock();
        es->set_leader(req_leader);
        es->state_unlock();

        auto instance = GetInstance(slot_id);
        verify(instance->max_ballot_accepted_ <= ballot_id);
        instance->max_ballot_seen_ = ballot_id;
        instance->max_ballot_accepted_ = ballot_id;
        Log_debug("#### inside PaxosServer::OnSyncCommit; slot_id: %ld, max_ballot_accepted_ set to: %ld", slot_id, instance->max_ballot_accepted_);
        instance->committed_cmd_ = bcmd->cmds[i].get()->sp_data_;
        *valid &= 1;
        if (slot_id > max_committed_slot_)
        {
          max_committed_slot_ = slot_id;
        }
      }
    }
    // es->state_unlock();
    if (*valid == 0)
    {
      cb();
      return;
    }
    // mtx_.lock();
    // Log_debug("The commit batch size is %d", bcmd->slots.size());
    for (slotid_t id = max_executed_slot_ + 1; id <= max_committed_slot_; id++)
    {
      // break;
      auto next_instance = GetInstance(id);
      if (next_instance->committed_cmd_)
      {
        // app_next_(*next_instance->committed_cmd_);
        commit_exec.push_back(next_instance);
        // Log_debug("multi-paxos par:%d loc:%d executed slot %lx now", partition_id_, loc_id_, id);
        max_executed_slot_++;
        n_commit_++;
      }
      else
      {
        break;
      }
    }
    // mtx_.unlock();
    // Log_debug("Committing %d", commit_exec.size());
    for (int i = 0; i < commit_exec.size(); i++)
    {
      // auto x = new PaxosData();
      app_next_(*commit_exec[i]->committed_cmd_);
    }

    *valid = 1;
    // cb();

    // mtx_.lock();
    // FreeSlots();
    // mtx_.unlock();
    cb();
  }

  void PaxosServer::OnBulkCommit(shared_ptr<Marshallable> &cmd,
                                 i32 *ballot,
                                 i32 *valid,
                                 const function<void()> &cb)
  {
    // Log_debug("here");
    // std::lock_guard<std::recursive_mutex> lock(mtx_);
    // mtx_.lock();
    // Log_debug("here");
    // Log_debug("multi-paxos scheduler decide for slot: %ld", bcmd->slots.size());
    auto bcmd = dynamic_pointer_cast<PaxosPrepCmd>(cmd);
    *valid = 1;
    ballot_t cur_b = bcmd->ballots[0];
    slotid_t cur_slot = bcmd->slots[0];
    // Log_debug("multi-paxos scheduler decide for slot: %ld", cur_slot);
    auto instance1 = GetInstance(cur_slot);
    Log_debug("**** OnBulkCommit cp 0; slot_id: %d, max_ballot_accepted: %d, ballot_id: %d", cur_slot, instance1->max_ballot_accepted_, cur_b);
    // verify(instance->max_ballot_accepted_ == ballot_id);
    if (instance1->max_ballot_accepted_ != cur_b)
    {
      Log_debug("**** OnBulkCommit cp 1; slot_id: %d, veify failed, yielding coroutine now, hopefully we will be back", cur_slot);
      auto coro = Coroutine::CurrentCoroutine();
      commit_coro[cur_slot] = coro;
      coro->Yield();
    }

    Log_debug("**** OnBulkCommit cp 2; slot_id: %d, we are back", cur_slot);
    int req_leader = bcmd->leader_id;
    // es->state_lock();
    mtx_.lock();

    // Log_debug("**** OnBulkCommit; current epoch is: %d", cur_epoch);
    if (cur_b < cur_epoch)
    {
      *ballot = cur_epoch;
      // es->state_unlock();
      *valid = 0;
      mtx_.unlock();
      cb();
      return;
    }
    /*if(req_leader != 0 && es->machine_id == 2)
    Log_debug("Stuff in getting committed on machine %d", bcmd->slots[0]);
    */
    mtx_.unlock();
    es->state_lock();
    es->set_lastseen();
    if (req_leader != es->machine_id)
      es->set_state(0);
    es->state_unlock();
    vector<shared_ptr<PaxosData>> commit_exec;
    for (int i = 0; i < bcmd->slots.size(); i++)
    {
      // break;
      slotid_t slot_id = bcmd->slots[i];
      ballot_t ballot_id = bcmd->ballots[i];
      // Log_debug("**** OnBulkCommit; ballot_id:%d", ballot_id);
      mtx_.lock();
      if (cur_epoch > ballot_id)
      {
        *valid = 0;
        *ballot = cur_epoch;
        mtx_.unlock();
        break;
      }
      else
      {
        if (cur_epoch < ballot_id)
        {
          mtx_.unlock();
          for (int i = 0; i < pxs_workers_g.size() - 1; i++)
          {
            PaxosServer *ps = dynamic_cast<PaxosServer *>(pxs_workers_g[i]->rep_sched_);
            ps->mtx_.lock();
            ps->cur_epoch = ballot_id;
            ps->leader_id = req_leader;
            ps->mtx_.unlock();
          }
        }
        else
        {
          mtx_.unlock();
        }
        es->state_lock();
        es->set_leader(req_leader);
        es->state_unlock();

        auto instance = GetInstance(slot_id);
        Log_debug("**** OnBulkCommit; slot_id: %d, max_ballot_accepted: %d, ballot_id: %d", slot_id, instance->max_ballot_accepted_, ballot_id);
        verify(instance->max_ballot_accepted_ == ballot_id); // todo: for correctness, if a new commit comes, sync accept.
        instance->max_ballot_seen_ = ballot_id;
        instance->max_ballot_accepted_ = ballot_id;
        Log_debug("#### inside PaxosServer::OnBulkCommit;; slot_id: %ld, max_ballot_accepted_ set to: %ld", slot_id, instance->max_ballot_accepted_);
        instance->committed_cmd_ = instance->accepted_cmd_;
        *valid &= 1;
        if (slot_id > max_committed_slot_)
        {
          max_committed_slot_ = slot_id;
        }
      }
    }
    // es->state_unlock();
    if (*valid == 0)
    {
      cb();
      return;
    }
    // mtx_.lock();
    // Log_debug("The commit batch size is %d", bcmd->slots.size());
    for (slotid_t id = max_executed_slot_ + 1; id <= max_committed_slot_; id++)
    {
      // break;
      auto next_instance = GetInstance(id);
      if (next_instance->committed_cmd_)
      {
        // app_next_(*next_instance->committed_cmd_);
        commit_exec.push_back(next_instance);
        if (req_leader == 0)
          Log_debug("multi-paxos par:%d loc:%d executed slot %ld now", partition_id_, loc_id_, id);
        max_executed_slot_++;
        n_commit_++;
      }
      else
      {
        if (req_leader != 0)
          Log_debug("Some slot is stopping commit %d %d and partition %d", id, bcmd->slots[0], partition_id_);
        break;
      }
    }
    if (commit_exec.size() > 0)
    {
      Log_debug("Something is getting committed %d", commit_exec.size());
    }
    // mtx_.unlock();
    // Log_debug("Committing %d", commit_exec.size());
    for (int i = 0; i < commit_exec.size(); i++)
    {
      // auto x = new PaxosData();
      app_next_(*commit_exec[i]->committed_cmd_); // kshivam: Next is invoked here
    }

    *valid = 1;
    // cb();

    // mtx_.lock();
    // FreeSlots();
    // mtx_.unlock();
    cb();

    // Log_debug("**** OnBulkCommit; ballot:%d, and valid: %d", *ballot, *valid);
  }

  void PaxosServer::OnCrpcBulkCommit(const uint64_t &id,
                                     const MarshallDeputy &cmd,
                                     const std::vector<uint16_t> &addrChain,
                                     const std::vector<BalValResult> &state)
  {
    // Log_debug("**** inside PaxosServer::OnCrpcBulkCommit cp0, with state.size():%d", state.size());
    BalValResult res;
    auto r = Coroutine::CreateRun([&]()
                                  { this->OnBulkCommit(const_cast<MarshallDeputy &>(cmd).sp_data_,
                                                       &res.ballot,
                                                       &res.valid,
                                                       []() {}); }); // #profile - 2.88%
    // Log_debug("**** OnCrpcBulkCommit; res.ballot:%d, res.valid:%d", res.ballot, res.valid);
    if (addrChain.size() == 1)
    {
      // Log_debug("**** OnCrpcBulkCommit reached the final link in the chain");
      auto x = (MultiPaxosCommo *)(this->commo_);
      verify(x->cRPCEvents.find(id) != x->cRPCEvents.end()); // #profile - 1.40%
      // Log_debug("inside FpgaRaftServer::OnCRPC2; checkpoint 2 @ %d", gettid());
      auto ev = x->cRPCEvents[id]; // imagine this to be a pair
      x->cRPCEvents.erase(id);

      ev.first(res.ballot, res.valid);
      ev.second->FeedResponse(res.valid);
      // Log_debug("==== inside demoserviceimpl::cRPC; results state is following");
      // auto st = dynamic_pointer_cast<AppendEntriesCommandState>(state.sp_data_);   // #profile - 0.54%
      for (auto el : state)
      {
        // Log_debug("**** inside the last link in the chain OnCrpcBulkCommit; el.ballot:%d, el.valid:%d", el.ballot, el.valid);
        // Log_debug("inside FpgaRaftServer::OnCRPC2; checkpoint 3 @ %d", gettid());
        ev.first(el.ballot, el.valid);
        // bool y = ((el.followerAppendOK == 1) && (this->IsLeader()) && (currentTerm == el.followerCurrentTerm));
        ev.second->FeedResponse(el.valid);
      }

      // Log_debug("**** OnCrpcBulkCommit returning from cRPC");
      return;
    }
    // Log_debug("**** inside PaxosServer::OnCrpcBulkCommit cp2, with ballot: %d, and valid: %d", res.ballot, res.valid);
    std::vector<BalValResult> st(state);
    st.push_back(res);
    // Log_debug("**** inside PaxosServer::OnCrpcBulkCommit cp3; current size of addrChain is: %d", addrChain.size());
    vector<uint16_t> addrChainCopy(addrChain.begin() + 1, addrChain.end());
    // Log_debug("**** inside PaxosServer::OnCrpcBulkCommit cp4");

    parid_t par_id = this->frame_->site_info_->partition_id_;
    // Log_debug("**** inside PaxosServer::OnCrpcBulkCommit cp5; par_id: %d", par_id);
    ((MultiPaxosCommo *)(this->commo_))->CrpcBulkDecide(par_id, id, cmd, addrChainCopy, st);

    // Log_debug("**** inside PaxosServer::OnCrpcBulkCommit cp6");
  }

  void PaxosServer::OnSyncNoOps(shared_ptr<Marshallable> &cmd,
                                i32 *ballot,
                                i32 *valid,
                                const function<void()> &cb)
  {

    auto bcmd = dynamic_pointer_cast<SyncNoOpRequest>(cmd);
    *valid = 1;
    ballot_t cur_b = bcmd->epoch;
    int req_leader = bcmd->leader_id;
    // es->state_lock();
    mtx_.lock();
    if (cur_b < cur_epoch)
    {
      *ballot = cur_epoch;
      // es->state_unlock();
      *valid = 0;
      mtx_.unlock();
      cb();
      return;
    }
    mtx_.unlock();

    for (int i = 0; i < pxs_workers_g.size() - 1; i++)
    {
      PaxosServer *ps = dynamic_cast<PaxosServer *>(pxs_workers_g[i]->rep_sched_);
      ps->mtx_.lock();
      if (bcmd->sync_slots[i] <= ps->max_executed_slot_)
      {
        Log_debug("The sync slot is %d for partition %d and committed slot is %d", bcmd->sync_slots[i], i, ps->max_executed_slot_);
        verify(0);
      }
      Log_debug("NoOps sync slot is %d for partition %d", bcmd->sync_slots[i], i);
      for (int j = bcmd->sync_slots[i]; j <= ps->max_committed_slot_; j++)
      {
        auto instance = ps->GetInstance(j);
        if (instance->committed_cmd_)
          continue;
        instance->committed_cmd_ = make_shared<LogEntry>();
        instance->is_no_op = true;
        instance->max_ballot_accepted_ = cur_b;
        Log_debug("#### inside PaxosServer::OnSyncNoOps;; slot_id: %ld, max_ballot_accepted_ set to: %ld", j, instance->max_ballot_accepted_);
      }
      for (slotid_t id = ps->max_executed_slot_ + 1; id <= ps->max_committed_slot_; id++)
      {
        auto next_instance = ps->GetInstance(id);
        if (next_instance->committed_cmd_ && !next_instance->is_no_op)
        {
          ps->app_next_(*next_instance->committed_cmd_);
          ps->max_executed_slot_++;
          ps->n_commit_++;
        }
        else
        {
          verify(0);
        }
      }
      ps->max_committed_slot_ = ps->max_committed_slot_;
      ps->max_executed_slot_ = ps->max_committed_slot_;
      ps->cur_open_slot_ = ps->max_committed_slot_ + 1;
      ps->mtx_.unlock();
    }

    *valid = 1;
    cb();
  }

} // namespace janus
