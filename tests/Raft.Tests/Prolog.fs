[<AutoOpen>]
module Prolog

open Raft
open System

type Operation =
    | Set of int
    | Inc of int
    | Dec of int

let testSettings = { HeartbeatTimeout = TimeSpan.FromMilliseconds 150.; ElectionTimeout = TimeSpan.FromMilliseconds 500. }

let follower leader self knownNodes = 
    let state = { Settings = testSettings
                  Self = self
                  KnownNodes = Set.ofList knownNodes
                  Epoch = 0
                  State = 0 }
    Follower(state, leader)

let candidate voters self knownNodes = 
    let state = { Settings = testSettings
                  Self = self
                  KnownNodes = Set.ofList knownNodes
                  Epoch = 1
                  State = 0 }
    Candidate(state, voters)

let leader self knownNodes = 
    let state = { Settings = testSettings
                  Self = self
                  KnownNodes = Set.ofList knownNodes
                  Epoch = 1
                  State = 0 }
    Leader(state, Map.empty)

let inline flip f a b = f b a
let inline equals (x:'a) (y:'a) = Xunit.Assert.Equal<'a>(x, y)