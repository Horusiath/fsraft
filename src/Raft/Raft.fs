module Raft

open System

type NodeId = string

type Result<'S,'M> = 
    | Become of receive:Receive<'S,'M> * state:'S * effects:Effect<'M> list
    | Unhandled

and Receive<'S,'M> = 'S -> 'M -> Result<'S, 'M>

and Effect<'M> = 
    | Send of receiver:NodeId * message:'M
    | Schedule of key:string * after:TimeSpan * receiver:NodeId * message:'M
    | ScheduleRepeated of key:string * after:TimeSpan * every:TimeSpan * receiver:NodeId * message:'M
    | Unschedule of key:string
    
type Settings = 
    { ElectionTimeout: TimeSpan;
      HeartbeatTimeout: TimeSpan }    

type Election = { Epoch: int; Elected: NodeId }

type ReplicationStatus<'a> =
    { Entry: 'a; TxId: int; ReplyTo: NodeId; Remaining: Set<NodeId> }

type State<'a> =
    { Settings: Settings
      Self: NodeId
      KnownNodes: Set<NodeId>
      Election: Election
      Entries: Map<string, 'a> }

type NodeState<'a> =
    | Follower of state:State<'a>
    | Candidate of state:State<'a> * voters:Set<NodeId>
    | Leader of state:State<'a> * clock:int * replication:Map<string, ReplicationStatus<'a>>
    member x.State =
        match x with
        | Follower state     -> state
        | Candidate(state,_) -> state
        | Leader(state,_,_)  -> state
      
type Message<'a> =
    | Init of knownNodes: Set<NodeId>
    | BecomeCandidate of epoch:int
    | RequestVote of epoch:int * candidate:NodeId
    | Vote of epoch:int * voter:NodeId * candidate:NodeId
    | Heartbeat of epoch:int * leader:NodeId
    // log replication
    | SetEntry of key:string * entry:'a * replyTo:NodeId
    | SetEntryAck of key:string * entry:'a
    | ReplicateEntry of key:string * entry:'a
    | ConfirmReplicate of key:string * follower:NodeId
    | CommitEntry of key:string

let isMajority current total = 
    let currentCount = current |> Set.count
    let totalCount = total |> Set.count
    float currentCount >= ceil (float totalCount / 2.)

let scheduleHeartbeatTimeout state =
    Schedule(state.Self + "-hbtimeout", state.Settings.HeartbeatTimeout, state.Self, BecomeCandidate(state.Election.Epoch+1))

let scheduleElectionTimeout state =
    Schedule(state.Self + "-eltimeout", state.Settings.ElectionTimeout, state.Self, BecomeCandidate(state.Election.Epoch+1))
   
let rec receive behavior message =
    match behavior, message with
    // init follower state
    | Follower state, Init nodes ->
        let heartbeatTimeout = scheduleHeartbeatTimeout state
        Become(receive, Follower { state with KnownNodes = nodes }, [heartbeatTimeout])

    // Follower was nominated to candidate of the next epoch
    | Follower state, BecomeCandidate epoch when epoch > state.Election.Epoch ->
        let requestVotes = 
            state.KnownNodes 
            |> Seq.map (fun node -> Send(node, RequestVote(epoch, state.Self))) 
            |> Seq.toList
        let election = { Epoch = epoch; Elected = state.Self }
        let voters = Set.singleton state.Self
        let timeout = scheduleElectionTimeout state
        Become(receive, Candidate({ state with Election = election }, voters), timeout::requestVotes)
    
    // Follower was requested to vote on another candidate for the next epoch
    | Follower state, RequestVote(epoch, candidate) when epoch > state.Election.Epoch ->
        let votedFor = { Epoch = epoch; Elected = candidate }
        let vote = Send(candidate, Vote(epoch, state.Self, candidate))
        Become(receive, Follower { state with Election = votedFor }, [vote])

    // Follower was requested to vote on candidate in current epoch
    | Follower state, RequestVote(epoch, candidate) ->
        let alreadyVoted = Send(candidate, Vote(epoch, state.Self, state.Election.Elected))
        Become(receive, behavior, [alreadyVoted])

    // Follower received heartbeat from leader
    | Follower state, Heartbeat(epoch, leader) when epoch >= state.Election.Epoch ->
        let election = { Epoch = epoch; Elected = leader }
        let timeout = Schedule(state.Self+"-candidating", state.Settings.HeartbeatTimeout, state.Self, BecomeCandidate(state.Election.Epoch + 1))
        let hearbeat = Send(leader, Heartbeat(epoch, state.Self))
        Become(receive, Follower { state with Election = election}, [hearbeat; timeout])
        
    // Candidate was requested to vote on another candidate in higher epoch
    | Candidate(state, _), RequestVote(epoch, candidate) when epoch > state.Election.Epoch ->
        let votedFor = { Epoch = epoch; Elected = candidate }
        let vote = Send(candidate, Vote(epoch, state.Self, candidate))
        Become(receive, Follower { state with Election = votedFor }, [vote])

    // Candidate was requested to vote on another candidate the same of lover epoch
    | Candidate(state, _), RequestVote(_, otherCandidate) ->
        let alreadyVotedForSelf = Send(otherCandidate, Vote(state.Election.Epoch, state.Self, state.Self))
        Become(receive, behavior, [alreadyVotedForSelf])
    
    // Candidate received vote from one of its followers
    | Candidate(state, voters), Vote(epoch, voter, candidate) when epoch = state.Election.Epoch && candidate = state.Self ->
        let voters' = Set.add voter voters
        if isMajority voters' state.KnownNodes
        then 
            // candidate has reached majority of votes, it can become a leader
            let self = state.Self
            let scheduleHeartbeat node = ScheduleRepeated(self + "-heartbeat", TimeSpan.Zero, state.Settings.HeartbeatTimeout, node, Heartbeat(state.Election.Epoch, self))
            let schedules = 
                state.KnownNodes
                |> Seq.map scheduleHeartbeat
                |> Seq.toList
            Become(receive, Leader(state, 0, Map.empty), schedules)
        else
            // candidate is still waiting for remaining votes
            Become(receive, Candidate(state, voters'), [])

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

    | _, _ -> Unhandled