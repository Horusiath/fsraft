module Raft

open System

/// Alias for node identifier. It could also be actor id, remote machine address etc.
type NodeId = string

/// Time component used by scheduling mechanics.
type TimePart =
    /// Send scheduled message immediatelly. Equivalent to send.
    | Immediatelly
    /// Send scheduled message once after some initial delay.
    | After of delay:TimeSpan
    /// Send scheduled message once in some randomized interval between provided range.
    | Between of min:TimeSpan * max:TimeSpan
    /// Send scheduled message repeatedly after some initial delay.
    | Repeat of init:TimeSpan * interval:TimeSpan

/// Result of a receiver processing logic.
type Reaction<'S,'M> = 
    /// A positive response from the receiver, with the next receiver (or the same)
    /// used for message processing, a new actor state as result of current receiver
    /// and a list of side effect performed as output of current processing.
    | Become of receive:Receive<'S,'M> * state:'S * effects:Effect<'M> list
    /// A negative response, used when current combination of receiver/state/message
    /// doesn't explicitly define any behavior.
    | Unhandled of state:'S * message:'M

/// A receiver function used to impement a free protocols.
and Receive<'S,'M> = 'S -> 'M -> Reaction<'S, 'M>

/// Possible side effects produced by receiver. A protocol interpreter must define
/// those effect as real operations in order to make protocol work.
and Effect<'M> = 
    /// Send a message to the receiver, potentially on a different machine.
    | Send of receiver:NodeId * message:'M
    /// Schedule or reshedule a message to be send to the receive at some 
    /// point in time. In case of Raft protocol, a sub-millisecond precision 
    /// is required.
    | Schedule of key:string * after:TimePart * receiver:NodeId * message:'M
    /// Unschedule previously scheduled message (or ignore if it was not found).
    | Unschedule of key:string
    
type Settings = 
    { ElectionTimeout: TimeSpan;
      HeartbeatTimeout: TimeSpan }    

/// Describes current election from a particular node perspective.
type Election = { Epoch: int; Elected: NodeId }

type ReplicationStatus<'a> =
    { Entry: 'a; TxId: int; ReplyTo: NodeId; Remaining: Set<NodeId> }

/// Internal state of the node with all data necessary to participate in Raft
/// consensus protocol.
type State<'a> =
    { Settings: Settings           // Raft cluster settings. Better to be the same for every node.
      Self: NodeId                 // Unique node identifier in scope of the current cluster.
      KnownNodes: Set<NodeId>      // List of nodes known to the present node.
      Election: Election           // Info about the latest known election.
      Entries: Map<string, 'a> }   // Entries used for Raft log replication.

/// Contains info about Raft node state with internal details. Some Raft roles
/// may define some extra data necessary to perform their duties.
type NodeState<'a> =
    /// Initial state of the node or follower with well-known Leader.
    | Follower of state:State<'a>
    /// Candidate for the new leader. Has it's vote counter.
    | Candidate of state:State<'a> * voters:Set<NodeId>
    /// Current leader of Raft cluster.
    | Leader of state:State<'a> * clock:int * replication:Map<string, ReplicationStatus<'a>>
    member x.State =
        match x with
        | Follower state     -> state
        | Candidate(state,_) -> state
        | Leader(state,_,_)  -> state
      
/// Messages used for communication between nodes to establish a Raft protocol.
type Message<'a> =
    | Init of knownNodes: Set<NodeId>
    /// Follower which receives candidate call will become candidate 
    /// and starts a next election epoch.
    | BecomeCandidate of epoch:int
    /// Candidate may send request to vote for itself for all known followers.
    | RequestVote of epoch:int * candidate:NodeId
    /// Followers may respond with the node id about which candidate they've
    /// choosen in which epoch.
    | Vote of epoch:int * voter:NodeId * candidate:NodeId
    /// Hearbeat send between Leader and its Followers.
    | Heartbeat of epoch:int * leader:NodeId
    // log replication
    | SetEntry of key:string * entry:'a * replyTo:NodeId
    | SetEntryAck of key:string * entry:'a
    | ReplicateEntry of key:string * entry:'a
    | ConfirmReplicate of key:string * follower:NodeId
    | CommitEntry of key:string

/// Checks if current set represents majority among total known set of nodes.
let isMajority current total = 
    let currentCount = current |> Set.count
    let totalCount = total |> Set.count
    float currentCount >= ceil (float totalCount / 2.)

let scheduleHeartbeatTimeout state =
    Schedule(state.Self + "-hbtimeout", After state.Settings.HeartbeatTimeout, state.Self, BecomeCandidate(state.Election.Epoch+1))

let scheduleElectionTimeout state =
    Schedule(state.Self + "-eltimeout", After state.Settings.ElectionTimeout, state.Self, BecomeCandidate(state.Election.Epoch+1))
   
let rec receive behavior message =
    match behavior, message with
    // init follower state
    | Follower state, Init nodes ->
        let heartbeatTimeout = scheduleHeartbeatTimeout state
        Become(receive, Follower { state with KnownNodes = nodes }, [heartbeatTimeout])

    // Follower was nominated to candidate for the next epoch. It nominates itself
    // and request all known nodes for their votes. Also schedules timeout after which
    // it will retry election again (useful, when requests were lost).
    | Follower state, BecomeCandidate epoch when epoch > state.Election.Epoch ->
        let requestVotes = 
            state.KnownNodes 
            |> Seq.map (fun node -> Send(node, RequestVote(epoch, state.Self))) 
            |> Seq.toList
        let election = { Epoch = epoch; Elected = state.Self }
        let state' = { state with Election = election }
        let voters = Set.singleton state'.Self
        let timeout = scheduleElectionTimeout state'
        Become(receive, Candidate(state', voters), timeout::requestVotes)
    
    // Follower was requested to vote on another candidate for the next epoch.
    // Older epoch has higher precedence, so no matter what follower will always to vote
    // on that candidate.
    | Follower state, RequestVote(epoch, candidate) when epoch > state.Election.Epoch ->
        let votedFor = { Epoch = epoch; Elected = candidate }
        let vote = Send(candidate, Vote(epoch, state.Self, candidate))
        Become(receive, Follower { state with Election = votedFor }, [vote])

    // Follower was requested to vote on candidate in current epoch. However
    // this also means, that it has already given it's vote for another candidate.
    // So it responds with existing vote.
    | Follower state, RequestVote(epoch, candidate) ->
        let alreadyVoted = Send(candidate, Vote(epoch, state.Self, state.Election.Elected))
        Become(receive, behavior, [alreadyVoted])

    // Follower received heartbeat from Leader. It will reshedule heartbeat and respond
    // back to the Leader.
    | Follower state, Heartbeat(epoch, leader) when epoch >= state.Election.Epoch ->
        let election = { Epoch = epoch; Elected = leader }
        let timeout = Schedule(state.Self+"-candidating", After state.Settings.HeartbeatTimeout, state.Self, BecomeCandidate(state.Election.Epoch + 1))
        let hearbeat = Send(leader, Heartbeat(epoch, state.Self))
        Become(receive, Follower { state with Election = election}, [hearbeat; timeout])
        
    // Candidate was requested to vote on another candidate in higher epoch, which 
    // takes precedence over him. It becomes new follower and gives its vote on the
    // candidate.
    | Candidate(state, _), RequestVote(epoch, candidate) when epoch > state.Election.Epoch ->
        let votedFor = { Epoch = epoch; Elected = candidate }
        let vote = Send(candidate, Vote(epoch, state.Self, candidate))
        Become(receive, Follower { state with Election = votedFor }, [vote])

    // Candidate was requested to vote on another candidate the same of lover epoch.
    // It responds, that it already voted on himself.
    | Candidate(state, _), RequestVote(_, otherCandidate) ->
        let alreadyVotedForSelf = Send(otherCandidate, Vote(state.Election.Epoch, state.Self, state.Self))
        Become(receive, behavior, [alreadyVotedForSelf])
    
    // Candidate received vote from one of its followers, incrementing voters count.
    // If it reached majority, it becomes Leader and starts heartbeating others.
    // If it didn't, it continues to count votes.
    | Candidate(state, voters), Vote(epoch, voter, candidate) when epoch = state.Election.Epoch && candidate = state.Self ->
        let voters' = Set.add voter voters
        if isMajority voters' state.KnownNodes
        then 
            // candidate has reached majority of votes, it can become a leader
            let self = state.Self
            let scheduleHeartbeat node = Schedule(self + "-heartbeat", Repeat(TimeSpan.Zero, state.Settings.HeartbeatTimeout), node, Heartbeat(state.Election.Epoch, self))
            let schedules = 
                state.KnownNodes
                |> Seq.map scheduleHeartbeat
                |> Seq.toList
            Become(receive, Leader(state, 0, Map.empty), schedules)
        else
            // candidate is still waiting for remaining votes
            Become(receive, Candidate(state, voters'), [])

    // Candidate received heartbeat from higher or equal epoch. It means it had loose
    // the election, so it becomes follower instead.
    | Candidate(state, _), Heartbeat(epoch, leader) when epoch >= state.Election.Epoch ->
        let election = { Epoch = epoch; Elected = leader }
        let timeout = Schedule(state.Self+"-candidating", After state.Settings.HeartbeatTimeout, state.Self, BecomeCandidate(state.Election.Epoch + 1))
        let hearbeat = Send(leader, Heartbeat(epoch, state.Self))
        Become(receive, Follower { state with Election = election }, [hearbeat; timeout])
        

    | Leader _, Heartbeat _->
        Become(receive, behavior, [])

    // Leader received new set entry request
    | Leader(state, clock, replications), SetEntry(key, entry, replyTo) ->
        let clock' = clock + 1
        let msg = ReplicateEntry(key, entry)
        let replication =
            state.KnownNodes
            |> Seq.map (fun node -> Send(node, msg))
            |> Seq.toList
        let replications' = Map.add key { Entry = entry; TxId = clock'; ReplyTo = replyTo; Remaining = state.KnownNodes; } replications
        let entries = Map.add key entry state.Entries
        Become(receive, Leader({ state with Entries = entries }, clock', replications'), replication)
    
    // Leader received confirmed replication response from follower
    | Leader(state, clock, replications), ConfirmReplicate(key, follower) ->
        let progress = Map.find key replications
        let remaining = Set.remove follower progress.Remaining
        if not <| isMajority remaining state.KnownNodes
        then
            // replication reached majority of the nodes
            let reply = Send (progress.ReplyTo, SetEntryAck(key, progress.Entry))
            let replications' = Map.remove key replications
            Become(receive, Leader(state, clock, replications'), [reply])
        else
            // there are still nodes waiting for replication
            let replications' = Map.add key { progress with Remaining = remaining } replications
            Become(receive, Leader(state, clock, replications'), [])

    | state, msg -> Unhandled(state,msg)