[<AutoOpen>]
module Prolog

open Raft
open System

type Operation =
    | Set of int
    | Inc of int
    | Dec of int

let testSettings = { HeartbeatTimeout = TimeSpan.FromMilliseconds 150.; ElectionTimeout = TimeSpan.FromMilliseconds 500. }
let node self = 
    { Settings = testSettings
      Self = self
      KnownNodes = Set.empty
      Election = { Epoch = 0; Elected = Unchecked.defaultof<NodeId> } 
      ManagedState = 0 } |> Follower

let inline flip f a b = f b a
let inline equals (x:'a) (y:'a) = Xunit.Assert.Equal<'a>(x, y)