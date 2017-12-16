[<AutoOpen>]
module Prolog

open Raft
open System

type Operation =
    | Set of int
    | Inc of int
    | Dec of int

let testSettings = { HeartbeatTimeout = TimeSpan.FromMilliseconds 150.; ElectionTimeout = TimeSpan.FromMilliseconds 500. }

let follower leader self = 
    let state = { Settings = testSettings
                  Self = self
                  KnownNodes = Set.empty
                  Epoch = 0
                  ManagedState = 0 }
    Follower(state, leader)

let candidate voters self = 
    let state = { Settings = testSettings
                  Self = self
                  KnownNodes = Set.empty
                  Epoch = 1
                  ManagedState = 0 }
    Candidate(state, voters)

let leader self = 
    let state = { Settings = testSettings
                  Self = self
                  KnownNodes = Set.empty
                  Epoch = 1
                  ManagedState = 0 }
    Leader(state, Map.empty)

let inline flip f a b = f b a
let inline equals (x:'a) (y:'a) = Xunit.Assert.Equal<'a>(x, y)