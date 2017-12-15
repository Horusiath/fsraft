module Raft.LeaderElectionTests

open System
open Xunit
open Raft

#nowarn "0025"

let a = node "A"
let b = node "B"
let c = node "C"
let d = node "D"
let e = node "E"

let all = [a;b;c;d;e] 
let addresses = ["A";"B";"C";"D";"E"]

let update state =
    function
    | Set state' -> state'
    | Inc value  -> state + value
    | Dec value  -> state - value

let receive = Raft.receive update

[<Fact>]
let ``Leader election should work in happy case`` () = 
    // Step 1: initialize nodes - it should request for heartbeat timeout (msg: BecomeCandidate)
    let init = Init (Set.ofList addresses)
    let [a1;b1;c1;d1;e1] =
        all 
        |> List.map (fun node -> receive node init)
        |> List.map (fun (Become(_, node, [schedule])) -> (node, schedule))
        |> List.map (fun (node, schedule) ->
            let (Follower { Self = self }) = node
            let expected = Schedule(self + "-hbtimeout", After testSettings.HeartbeatTimeout, self, BecomeCandidate 1)
            schedule |> equals expected
            node)

    // Step 2: elevate A to a candidate state - it should request others for their support and nominate itself
    let (Become(_, a2, timeout::reqs)) = receive a1 <| BecomeCandidate 1
    let (Candidate(_, voters)) = a2
    let expectedReqs = addresses |> List.map (fun n -> Send(n, RequestVote(1, "A")))

    timeout |> equals (Schedule("A-eltimeout", After testSettings.ElectionTimeout, "A", BecomeCandidate 2))
    reqs    |> equals expectedReqs
    voters  |> Set.toList |> equals ["A"]

    // Step 3: let B & C respond with their votes
    let (Become(_, b2, [sendB])) = receive b1 <| RequestVote(1, "A")
    let voteB = Vote(1, "B", "A")
    sendB |> equals (Send("A", voteB))
    let (Become(_, c2, [sendC])) = receive c1 <| RequestVote(1, "A")
    let voteC = Vote(1, "C", "A")
    sendC |> equals (Send("A", voteC))

    // Step 4: A receives majority of votes, becomes the leader
    let (Become(_, a3, [])) = receive a2 voteB
    let (Become(_, a4, heartbeats)) = receive a3 voteC
    a4 |> equals (Leader(a4.State, 0, Map.empty))

[<Fact>]
let ``Leader election should be retried after leader got unresponsive`` () = ()

[<Fact>]
let ``Leader election should be retries after concurrent candidates and equal split vote happened`` () = ()

[<Fact>]
let ``Log replication starting from leader should be confirmed after update reached majority of followers`` () = ()

[<Fact>]
let ``Log replication starting from follower be redirected to leader`` () = ()


//TODO: split brain scenarios
