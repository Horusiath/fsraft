[<AutoOpen>]
module Prolog

open Raft
open System

let testSettings = { HeartbeatTimeout = TimeSpan.FromMilliseconds 150.; ElectionTimeout = TimeSpan.FromMilliseconds 500. }
let node self = 
    { Settings = testSettings
      Self = self
      KnownNodes = Set.empty
      Election = { Epoch = 0; Elected = Unchecked.defaultof<NodeId> } 
      Entries = Map.empty<string, int> } |> Follower

let inline flip f a b = f b a
let inline equals (x:'a) (y:'a) = Xunit.Assert.Equal<'a>(x, y)