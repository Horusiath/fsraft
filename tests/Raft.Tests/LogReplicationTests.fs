module LogReplicationTests

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
let ``Log replication should be confirmed after it reached majority of nodes`` () = 
    let addresses = ["A";"B";"C";"D";"E"]
    let withoutLeader = ["B";"C";"D";"E"]
    let a1 = leader "A" addresses
    let b1 = follower "A" "B" addresses
    let c1 = follower "A" "C" addresses
    let d1 = follower "A" "D" addresses
    let e1 = follower "A" "E" addresses
    
    let all = [a1;b1;c1;d1;e1]
    let id = Guid.NewGuid()

    // Step 1: leader receives append entries request. It should send Replicate
    // request to all followers and update its replication progress status
    let (Become(_, a2, replications)) = receive a1 <| AppendEntries(id, "C", [Set 5])
    let (Leader(_, progresses)) = a2
    progresses |> Map.find id |> equals { CorrelationId = id; Entries = [Set 5]; ReplyTo = "C"; Remaining = Set.ofList withoutLeader }
    replications |> List.iter (fun (Send(node, Replicate(i, [Set 5]))) -> 
        i |> equals id
        List.contains node withoutLeader |> equals true
    )
    let replicate = Replicate(id, [Set 5])
    let (Become(_, Follower(b2, _), [Send("A", ReplicateAck(_, "B"))])) = receive b1 replicate
    let (Become(_, Follower(c2, _), [Send("A", ReplicateAck(_, "C"))])) = receive c1 replicate



    ()
