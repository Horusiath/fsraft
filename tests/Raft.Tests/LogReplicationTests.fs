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
    let a = leader "A"
    let b = follower "A" "B"
    let c = follower "A" "C"
    let d = follower "A" "D"
    let e = follower "A" "E"

    let all = [a;b;c;d;e] 
    let addresses = ["A";"B";"C";"D";"E"]



    ()
