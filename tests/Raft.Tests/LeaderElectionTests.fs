module LeaderElectionTests

open System
open Xunit
open Raft

#nowarn "0025"

let update state =
    function
    | Set state' -> state'
    | Inc value  -> state + value
    | Dec value  -> state - value

let receive = Raft.raft update

[<Fact>]
let ``Leader election should work in happy case`` () = 
    let addresses = ["A";"B";"C";"D";"E"]
    let withoutLeader = ["B";"C";"D";"E"]
    let a1 = follower "" "A" addresses
    let b1 = follower "" "B" addresses
    let c1 = follower "" "C" addresses
    let d1 = follower "" "D" addresses
    let e1 = follower "" "E" addresses
        
    // Step 1: elevate A to a candidate state - it should request others for their support and nominate itself
    let (Become(_, a2, timeout::reqs)) = receive a1 <| BecomeCandidate 1
    let (Candidate(_, voters)) = a2
    let expectedReqs = addresses |> List.map (fun n -> Send(n, RequestVote(1, "A")))

    timeout |> equals (Schedule("A-eltimeout", After testSettings.ElectionTimeout, "A", BecomeCandidate 2))
    reqs    |> equals expectedReqs
    voters  |> Set.toList |> equals ["A"]

    // Step 2: let B & C respond with their votes
    let (Become(_, _, [sendB])) = receive b1 <| RequestVote(1, "A")
    let voteB = Vote(1, "B", "A")
    sendB |> equals (Send("A", voteB))
    let (Become(_, _, [sendC])) = receive c1 <| RequestVote(1, "A")
    let voteC = Vote(1, "C", "A")
    sendC |> equals (Send("A", voteC))

    // Step 3: A receives majority of votes, becomes the leader
    let (Become(_, a3, [])) = receive a2 voteB
    let (Become(_, a4, heartbeats)) = receive a3 voteC
    a4 |> equals (Leader(a4.State, Map.empty))
    
    // Step 4: send heartbeats to other nodes, confirm they've accepted leader
    heartbeats
    |> List.map (fun (Schedule(_,Repeat(zero, timeout), node, heartbeat)) ->
        zero |> equals TimeSpan.Zero
        timeout |> equals testSettings.HeartbeatTimeout
        match node with
        | "B" -> receive b1 heartbeat
        | "C" -> receive c1 heartbeat
        | "D" -> receive d1 heartbeat
        | "E" -> receive e1 heartbeat)
    |> List.fold (fun remaining (Become(_, Follower(state, leader), [hb;_])) ->
        leader |> equals "A"
        hb |> equals (Send("A", Heartbeat(1, state.Self)))
        remaining |> Set.remove state.Self
    ) (Set.ofList withoutLeader)
    |> equals Set.empty

[<Fact>]
let ``Leader election should be retried after leader got unresponsive`` () = ()

[<Fact>]
let ``Leader election should be retries after concurrent candidates and equal split vote happened`` () = ()

[<Fact>]
let ``Log replication starting from leader should be confirmed after update reached majority of followers`` () = ()

[<Fact>]
let ``Log replication starting from follower be redirected to leader`` () = ()


//TODO: split brain scenarios
