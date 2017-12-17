module Raft

open System

/// A correlation id used to correlate requests with responses.
type CorrelationId = Guid

/// Time component used by scheduling mechanics.
type TimePart =
    /// Send scheduled message immediatelly. Equivalent to send.
    | Immediatelly
    /// Send scheduled message once after some initial delay.
    | After   of delay:TimeSpan
    /// Send scheduled message once in some randomized interval between provided range.
    | Between of min:TimeSpan * max:TimeSpan
    /// Send scheduled message repeatedly after some initial delay.
    | Repeat  of init:TimeSpan * interval:TimeSpan

/// Result of a receiver processing logic.
type [<Struct>]Reaction<'state,'msg,'eff> = 
    /// A positive response from the receiver, with the next receiver (or the same)
    /// used for message processing, a new actor state as result of current receiver
    /// and a list of side effect performed as output of current processing.
    | Become    of receive:Receive<'state,'msg,'eff> * next:'state * effects:'eff list
    /// A negative response, used when current combination of receiver/state/message
    /// doesn't explicitly define any behavior.
    | Unhandled of state:'state * message:'msg

/// A receiver function used to impement a free protocols.
and Receive<'state,'msg,'eff> = 'state -> 'msg -> Reaction<'state, 'msg, 'eff>

/// Possible side effects produced by receiver. A protocol interpreter must define
/// those effect as real operations in order to make protocol work.
type Effect<'id,'msg> = 
    /// Send a message to the receiver, potentially on a different machine.
    | Send       of receiver:'id * message:'msg
    /// Schedule or reshedule a message to be send to the receive at some 
    /// point in time. In case of Raft protocol, a sub-millisecond precision 
    /// is required.
    | Schedule   of key:string * after:TimePart * receiver:'id * message:'msg
    /// Unschedule previously scheduled message (or ignore if it was not found).
    | Unschedule of key:string
    | Stash      of message:'msg
    | Unstash
    
type Settings = 
    { ElectionTimeout: TimeSpan;
      HeartbeatTimeout: TimeSpan }         
    
type Commit<'op> = CorrelationId * 'op

[<Struct>]
type Committable<'op> = 
    | Committed of committed:Commit<'op>
    | Uncommitted of uncommitted:Commit<'op>
    member x.CorrelationId =
        match x with
        | Committed(id, _) -> id
        | Uncommitted(id, _) -> id

type ReplicationProgress<'id,'op when 'id : comparison> =
    { Message: Committable<'op>; ReplyTo: 'id; Remaining: Set<'id> }
        
/// Internal state of the node with all data necessary to participate in Raft
/// consensus protocol.
type State<'id,'s,'op when 'id : comparison> =
    { Settings: Settings        // Raft cluster settings. Better to be the same for every node.
      Self: 'id                 // Unique node identifier in scope of the current cluster.
      KnownNodes: Set<'id>      // List of nodes known to the present node.
      Epoch: int                // Info about the latest known election.
      State: 's }               // Entries used for Raft log replication.

/// Contains info about Raft node state with internal details. Some Raft roles
/// may define some extra data necessary to perform their duties.
type NodeState<'id,'s,'op when 'id : comparison> =
    /// Initial state of the node or follower with well-known Leader.
    | Follower of state:State<'id,'s,'op> * leader:'id * lastCommit:CorrelationId option
    /// Candidate for the new leader. Has it's vote counter.
    | Candidate of state:State<'id,'s,'op> * voters:Set<'id>
    /// Current leader of Raft cluster.
    | Leader of state:State<'id,'s,'op> * progress:ReplicationProgress<'id,'op> option
    member x.State =
        match x with
        | Follower(state,_,_)  -> state
        | Candidate(state,_) -> state
        | Leader(state,_)    -> state
      
/// Messages used for communication between nodes to establish a Raft protocol.
type Message<'id,'op when 'id : comparison> =
    /// Follower which receives candidate call will become candidate 
    /// and starts a next election epoch.
    | BecomeCandidate of epoch:int
    /// Candidate may send request to vote for itself for all known followers.
    | RequestVote     of epoch:int * candidate:'id
    /// Followers may respond with the node id about which candidate they've
    /// choosen in which epoch.
    | Vote            of epoch:int * voter:'id * candidate:'id
    | HeartbeatTick 
    // Request send from the client to potential Leader
    | Update          of corrId:CorrelationId * op:'op * replyTo:'id
    | UpdateSuccess   of corrId:CorrelationId
    | UpdateFailure   of corrId:CorrelationId
    // Request send from the Leader to its Followers to replicate the state
    | AppendEntries   of epoch:int * leader:'id * op:Committable<'op> option
    // Response to Replicate request: follower confirms that replication succeeds.
    | Confirm         of corrId:CorrelationId * follower:'id

/// Checks if current set represents majority among total known set of nodes.
let isMajority current total = 
    let currentCount = current |> Set.count
    let totalCount = total |> Set.count
    float currentCount >= ceil (float totalCount / 2.)

let inline isMinority current total = isMajority current total |> not

let inline scheduleHeartbeatTimeout state =
    Schedule(state.Self + "-hbtimeout", After state.Settings.HeartbeatTimeout, state.Self, BecomeCandidate(state.Epoch+1))

let inline scheduleElectionTimeout state =
    Schedule(state.Self + "-eltimeout", After state.Settings.ElectionTimeout, state.Self, BecomeCandidate(state.Epoch+1))
        
let commit =
    function
    | Uncommitted(id, op) -> Committed(id, op)
    | other -> other

let rec raft update behavior message =
    match behavior, message with

    // Follower was nominated to candidate for the next epoch. It nominates itself
    // and request all known nodes for their votes. Also schedules timeout after which
    // it will retry election again (useful, when requests were lost).
    | Follower(state, _, _), BecomeCandidate epoch when epoch > state.Epoch ->
        let requestVote = RequestVote(epoch, state.Self)
        let leaflets = 
            state.KnownNodes 
            |> Seq.map (fun node -> Send(node, requestVote)) 
            |> Seq.toList
        let state' = { state with Epoch = epoch }
        let voters = Set.singleton state'.Self
        let timeout = scheduleElectionTimeout state'
        Become(raft update, Candidate(state', voters), timeout::leaflets)
    
    // Follower was requested to vote on another candidate for the next epoch.
    // Older epoch has higher precedence, so no matter what follower will always to vote
    // on that candidate.
    | Follower(state, _, _), RequestVote(epoch, candidate) when epoch > state.Epoch ->
        let vote = Send(candidate, Vote(epoch, state.Self, candidate))
        Become(raft update, Follower({ state with Epoch = state.Epoch }, candidate, None), [vote])

    // Follower was requested to vote on candidate in current epoch. However
    // this also means, that it has already given it's vote for another candidate.
    // So it responds with existing vote.
    | Follower(state, leader, _), RequestVote(epoch, candidate) ->
        let alreadyVoted = Send(candidate, Vote(epoch, state.Self, leader))
        Become(raft update, behavior, [alreadyVoted])

    // Follower got request to append entries, but it's not his responsibility.
    // It forwards this request to the leader instead
    | Follower(_, leader, _), Update _ ->
        let send = Send(leader, message)
        Become(raft update, behavior, [send])
       
    // Follower received heartbeat from the current or newer leader.
    // Refresh timeout and optionally update leader.
    | Follower(state, _, commit), AppendEntries(epoch, leader, None) when epoch >= state.Epoch ->
        let timeout = Schedule(state.Self+"-candidating", After state.Settings.HeartbeatTimeout, state.Self, BecomeCandidate(state.Epoch + 1))
        Become(raft update, Follower(state, leader, commit), [timeout])
        
    // Follower received replication request at higher or current epoch. All previously 
    // uncommitted changes are discarded.
    | Follower(state, _, _), AppendEntries(epoch, leader, Some(Uncommitted(id, _))) when epoch >= state.Epoch ->
        let timeout = Schedule(state.Self+"-candidating", After state.Settings.HeartbeatTimeout, state.Self, BecomeCandidate(state.Epoch + 1))
        let confirm = Send(leader, Confirm(id, state.Self))
        Become(raft update, Follower(state, leader, Some id), [timeout;confirm])
        
    // Follower received committed request. Update internal state.
    | Follower(state, _, commit), AppendEntries(epoch, leader, Some(Committed(id, op))) when epoch >= state.Epoch->
        let timeout = Schedule(state.Self+"-candidating", After state.Settings.HeartbeatTimeout, state.Self, BecomeCandidate(state.Epoch + 1))
        let state' = 
            match commit with
            | Some id' when id' <> id -> behavior // there was a pending commit, but IDs don't match
            | _ -> 
                let next = update state.State op
                Follower({state with State = next }, leader, None)
        Become(raft update, state', [timeout])
        
    // Candidate was requested to vote on another candidate in higher epoch, which 
    // takes precedence over him. It becomes new follower and gives its vote on the
    // candidate.
    | Candidate(state, _), RequestVote(epoch, candidate) when epoch > state.Epoch ->
        let vote = Send(candidate, Vote(epoch, state.Self, candidate))
        Become(raft update, Follower({ state with Epoch = epoch }, candidate, None), [vote])

    // Candidate was requested to vote on another candidate the same of lover epoch.
    // It responds, that it already voted on himself.
    | Candidate(state, _), RequestVote(_, otherCandidate) ->
        let alreadyVotedForSelf = Send(otherCandidate, Vote(state.Epoch, state.Self, state.Self))
        Become(raft update, behavior, [alreadyVotedForSelf])
    
    // Candidate received vote from one of its followers, incrementing voters count.
    // If it reached majority, it becomes Leader and starts heartbeating others.
    // If it didn't, it continues to count votes.
    | Candidate(state, voters), Vote(epoch, voter, candidate) when epoch = state.Epoch && candidate = state.Self ->
        let voters' = Set.add voter voters
        if isMajority voters' state.KnownNodes
        then 
            // candidate has reached majority of votes, it can become a leader
            let interval = Repeat(TimeSpan.Zero, state.Settings.HeartbeatTimeout)
            let heartbeat = Schedule(state.Self + "-heartbeat", interval, state.Self, HeartbeatTick)
            Become(raft update, Leader(state, None), [heartbeat])
        else
            // candidate is still waiting for remaining votes
            Become(raft update, Candidate(state, voters'), [])

    // Candidate received heartbeat from higher or equal epoch. It means it had loose
    // the election, so it becomes follower instead.
    | Candidate(state, _), AppendEntries(epoch, leader, None) when epoch >= state.Epoch ->
        let timeout = Schedule(state.Self+"-candidating", After state.Settings.HeartbeatTimeout, state.Self, BecomeCandidate(state.Epoch + 1))
        let state'= { state with Epoch = epoch }
        Become(raft update, Follower(state', leader, None), [timeout])

    // Candidate received heartbeat from higher or equal epoch. It means it had loose
    // the election, so it becomes follower instead. Also confirms uncommited request.
    | Candidate(state, _), AppendEntries(epoch, leader, Some(Uncommitted(id,_))) when epoch >= state.Epoch ->
        let timeout = Schedule(state.Self+"-candidating", After state.Settings.HeartbeatTimeout, state.Self, BecomeCandidate(state.Epoch + 1))
        let confirm = Send(leader, Confirm(id, state.Self))
        let state'= { state with Epoch = epoch }
        Become(raft update, Follower(state', leader, Some id), [timeout;confirm])

    // Candidate received heartbeat from higher or equal epoch. It means it had loose
    // the election, so it becomes follower instead. Also immediatelly applies an update.
    | Candidate(state, _), AppendEntries(epoch, leader, Some(Committed(id,op))) when epoch >= state.Epoch ->
        let timeout = Schedule(state.Self+"-candidating", After state.Settings.HeartbeatTimeout, state.Self, BecomeCandidate(state.Epoch + 1))
        let confirm = Send(leader, Confirm(id, state.Self))
        let updated = update state.State op
        let state'= { state with Epoch = epoch; State = updated }
        Become(raft update, Follower(state', leader, None), [timeout;confirm])
        
    // On a heartbeat tick, we'll send updated AppendEntries
    | Leader(state, progress), HeartbeatTick ->
        let op = progress |> Option.map (fun p -> p.Message)
        let msg = AppendEntries(state.Epoch, state.Self, op)
        let broadcast = 
            state.KnownNodes 
            |> Set.remove state.Self
            |> Seq.map (fun node -> Send(node, msg))
            |> Seq.toList        
        Become(raft update, behavior, broadcast)

    // Leader received next update, but previous is being committed. Stash it.
    | Leader(_, Some { Message = pending }), Update(corrId, _, _) ->
        if pending.CorrelationId <> corrId
        then Become(raft update, behavior, [Stash message])
        else Become(raft update, behavior, []) // ignore redelivered request

    // Leader received new set entry request
    | Leader(state, None), Update(corrId, replyTo, op) ->
        let remaining = 
            state.KnownNodes
            |> Set.remove state.Self
        let uncommitted = Uncommitted(corrId, op)
        let entry = { ReplyTo = replyTo; Remaining = remaining; Message = uncommitted }
        Become(raft update, Leader(state, Some entry), []) // no need to send anything, uncomitted will be picked during next heartbeat
    
    // Leader received confirmed replication response from follower
    | Leader(state, Some progress), Confirm(id, follower) when progress.Message.CorrelationId = id ->
        let remaining = Set.remove follower progress.Remaining
        if isMinority remaining state.KnownNodes
        then
            // replication reached majority of the nodes
            let reply = Send (progress.ReplyTo, UpdateSuccess id)
            let append = AppendEntries(state.Epoch, state.Self, Some (commit progress.Message))
            let commits = 
                state.KnownNodes 
                |> Set.remove state.Self
                |> Seq.map (fun node -> Send(node, append))
                |> Seq.toList
            Become(raft update, Leader(state, None), reply::commits)
        else
            // there are still nodes waiting for replication
            let progress' = { progress with Remaining = remaining }
            Become(raft update, Leader(state, Some progress'), [])

    | state, msg -> Unhandled(state,msg)