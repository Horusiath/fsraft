module Raft

open System

/// Alias for node identifier. It could also be actor id, remote machine address etc.
type NodeId = string

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
type Reaction<'s,'m> = 
    /// A positive response from the receiver, with the next receiver (or the same)
    /// used for message processing, a new actor state as result of current receiver
    /// and a list of side effect performed as output of current processing.
    | Become    of receive:Receive<'s,'m> * state:'s * effects:Effect<'m> list
    /// A negative response, used when current combination of receiver/state/message
    /// doesn't explicitly define any behavior.
    | Unhandled of state:'s * message:'m

/// A receiver function used to impement a free protocols.
and Receive<'s,'m> = 's -> 'm -> Reaction<'s, 'm>

/// Possible side effects produced by receiver. A protocol interpreter must define
/// those effect as real operations in order to make protocol work.
and Effect<'m> = 
    /// Send a message to the receiver, potentially on a different machine.
    | Send       of receiver:NodeId * message:'m
    /// Schedule or reshedule a message to be send to the receive at some 
    /// point in time. In case of Raft protocol, a sub-millisecond precision 
    /// is required.
    | Schedule   of key:string * after:TimePart * receiver:NodeId * message:'m
    /// Unschedule previously scheduled message (or ignore if it was not found).
    | Unschedule of key:string
    
type Settings = 
    { ElectionTimeout: TimeSpan;
      HeartbeatTimeout: TimeSpan }    

type ReplicationProgress<'op> =
    { CorrelationId: CorrelationId; Entries: 'op list; ReplyTo: NodeId; Remaining: Set<NodeId> }

/// Internal state of the node with all data necessary to participate in Raft
/// consensus protocol.
type State<'s> =
    { Settings: Settings      // Raft cluster settings. Better to be the same for every node.
      Self: NodeId            // Unique node identifier in scope of the current cluster.
      KnownNodes: Set<NodeId> // List of nodes known to the present node.
      Epoch: int              // Info about the latest known election.
      ManagedState: 's }      // Entries used for Raft log replication.

/// Contains info about Raft node state with internal details. Some Raft roles
/// may define some extra data necessary to perform their duties.
type NodeState<'s, 'op> =
    /// Initial state of the node or follower with well-known Leader.
    | Follower of state:State<'s> * leader:NodeId
    /// Candidate for the new leader. Has it's vote counter.
    | Candidate of state:State<'s> * voters:Set<NodeId>
    /// Current leader of Raft cluster.
    | Leader of state:State<'s> * replication:Map<CorrelationId, ReplicationProgress<'op>>
    member x.State =
        match x with
        | Follower(state,_)  -> state
        | Candidate(state,_) -> state
        | Leader(state,_)    -> state
      
/// Messages used for communication between nodes to establish a Raft protocol.
type Message<'op> =
    | Init            of knownNodes: Set<NodeId>
    /// Follower which receives candidate call will become candidate 
    /// and starts a next election epoch.
    | BecomeCandidate of epoch:int
    /// Candidate may send request to vote for itself for all known followers.
    | RequestVote     of epoch:int * candidate:NodeId
    /// Followers may respond with the node id about which candidate they've
    /// choosen in which epoch.
    | Vote            of epoch:int * voter:NodeId * candidate:NodeId
    | Heartbeat       of epoch:int * voter:NodeId 
    // log replication
    | AppendEntries   of corrId:CorrelationId * replyTo:NodeId * ops:'op list
    | Replicate       of corrId:CorrelationId * ops:'op list
    | ReplicateAck    of corrId:CorrelationId * follower:NodeId
    | CommitEntry     of corrId:CorrelationId 
    | EntriesAppended of corrId:CorrelationId

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
   
let apply update state ops = ops |> List.fold update state

let rec receive update behavior message =
    match behavior, message with
    // init follower state
    | Follower(state, leader), Init nodes ->
        let heartbeatTimeout = scheduleHeartbeatTimeout state
        Become(receive update, Follower({ state with KnownNodes = nodes }, leader), [heartbeatTimeout])

    // Follower was nominated to candidate for the next epoch. It nominates itself
    // and request all known nodes for their votes. Also schedules timeout after which
    // it will retry election again (useful, when requests were lost).
    | Follower(state, _), BecomeCandidate epoch when epoch > state.Epoch ->
        let requestVotes = 
            state.KnownNodes 
            |> Seq.map (fun node -> Send(node, RequestVote(epoch, state.Self))) 
            |> Seq.toList
        let state' = { state with Epoch = epoch }
        let voters = Set.singleton state'.Self
        let timeout = scheduleElectionTimeout state'
        Become(receive update, Candidate(state', voters), timeout::requestVotes)
    
    // Follower was requested to vote on another candidate for the next epoch.
    // Older epoch has higher precedence, so no matter what follower will always to vote
    // on that candidate.
    | Follower(state, _), RequestVote(epoch, candidate) when epoch > state.Epoch ->
        let vote = Send(candidate, Vote(epoch, state.Self, candidate))
        Become(receive update, Follower({ state with Epoch = state.Epoch }, candidate), [vote])

    // Follower was requested to vote on candidate in current epoch. However
    // this also means, that it has already given it's vote for another candidate.
    // So it responds with existing vote.
    | Follower(state, leader), RequestVote(epoch, candidate) ->
        let alreadyVoted = Send(candidate, Vote(epoch, state.Self, leader))
        Become(receive update, behavior, [alreadyVoted])

    // Follower received heartbeat from Leader. It will reshedule heartbeat and respond
    // back to the Leader.
    | Follower(state, _), Heartbeat(epoch, leader) when epoch >= state.Epoch ->
        //let election = { Epoch = epoch; Elected = leader }
        let timeout = Schedule(state.Self+"-candidating", After state.Settings.HeartbeatTimeout, state.Self, BecomeCandidate(state.Epoch + 1))
        let hearbeat = Send(leader, Heartbeat(epoch, state.Self))
        Become(receive update, Follower({ state with Epoch = epoch }, leader), [hearbeat; timeout])

    // Follower got request to append entries, but it's not his responsibility.
    // It forwards this request to the leader instead
    | Follower(_, leader), AppendEntries _ ->
        let send = Send(leader, message)
        Become(receive update, behavior, [send])

    // Follower received replication request. It performs an update and sends ACK back.
    | Follower(state, leader), Replicate(corrId, ops) ->
        let state' = apply update state.ManagedState ops
        let send = Send(leader, ReplicateAck(corrId, state.Self))
        Become(receive update, Follower({ state with ManagedState = state' }, leader), [send])
        
    // Candidate was requested to vote on another candidate in higher epoch, which 
    // takes precedence over him. It becomes new follower and gives its vote on the
    // candidate.
    | Candidate(state, _), RequestVote(epoch, candidate) when epoch > state.Epoch ->
        let vote = Send(candidate, Vote(epoch, state.Self, candidate))
        Become(receive update, Follower({ state with Epoch = epoch }, candidate), [vote])

    // Candidate was requested to vote on another candidate the same of lover epoch.
    // It responds, that it already voted on himself.
    | Candidate(state, _), RequestVote(_, otherCandidate) ->
        let alreadyVotedForSelf = Send(otherCandidate, Vote(state.Epoch, state.Self, state.Self))
        Become(receive update, behavior, [alreadyVotedForSelf])
    
    // Candidate received vote from one of its followers, incrementing voters count.
    // If it reached majority, it becomes Leader and starts heartbeating others.
    // If it didn't, it continues to count votes.
    | Candidate(state, voters), Vote(epoch, voter, candidate) when epoch = state.Epoch && candidate = state.Self ->
        let voters' = Set.add voter voters
        if isMajority voters' state.KnownNodes
        then 
            // candidate has reached majority of votes, it can become a leader
            let self = state.Self
            let scheduleHeartbeat node = Schedule(self + "-heartbeat", Repeat(TimeSpan.Zero, state.Settings.HeartbeatTimeout), node, Heartbeat(state.Epoch, self))
            let schedules = 
                state.KnownNodes
                |> Seq.map scheduleHeartbeat
                |> Seq.toList
            Become(receive update, Leader(state, Map.empty), schedules)
        else
            // candidate is still waiting for remaining votes
            Become(receive update, Candidate(state, voters'), [])

    // Candidate received heartbeat from higher or equal epoch. It means it had loose
    // the election, so it becomes follower instead.
    | Candidate(state, _), Heartbeat(epoch, leader) when epoch >= state.Epoch ->
        let timeout = Schedule(state.Self+"-candidating", After state.Settings.HeartbeatTimeout, state.Self, BecomeCandidate(state.Epoch + 1))
        let hearbeat = Send(leader, Heartbeat(epoch, state.Self))
        Become(receive update, Follower({ state with Epoch = epoch }, leader), [hearbeat; timeout])
        
    | Leader _, Heartbeat _->
        Become(receive update, behavior, [])

    // Leader received new set entry request
    | Leader(state, replications), AppendEntries(corrId, replyTo, ops) ->
        let replicate = Replicate(corrId,ops)
        let replication =
            state.KnownNodes
            |> Seq.map (fun node -> Send(node, replicate))
            |> Seq.toList
        let progress = { CorrelationId = corrId; Entries = ops; ReplyTo = replyTo; Remaining = state.KnownNodes; }
        let replications' = Map.add corrId progress replications
        let state' = apply update state.ManagedState ops
        Become(receive update, Leader({ state with ManagedState = state' }, replications'), replication)
    
    // Leader received confirmed replication response from follower
    | Leader(state, replications), ReplicateAck(id, follower) ->
        let progress = Map.find id replications
        let remaining = Set.remove follower progress.Remaining
        if isMinority remaining state.KnownNodes
        then
            // replication reached majority of the nodes
            let reply = Send (progress.ReplyTo, EntriesAppended(progress.CorrelationId))
            let replications' = Map.remove id replications
            Become(receive update, Leader(state, replications'), [reply])
        else
            // there are still nodes waiting for replication
            let replications' = Map.add id { progress with Remaining = remaining } replications
            Become(receive update, Leader(state, replications'), [])

    | state, msg -> Unhandled(state,msg)