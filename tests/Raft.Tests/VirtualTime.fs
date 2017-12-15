namespace TestKit

open System
open Raft

type TimeEvent<'e> =
    { Timestamp: DateTime
      Emitter: NodeId
      Event: 'e }
    
/// Virtual representation of events occuring as time passes.
/// Allows to freely abstract cause and effect from the physical time clock.
type Timeline<'e when 'e : comparison> =
    { Events: Map<DateTime, Set<TimeEvent<'e>>> }
    
    member this.Linearize predicate = { Events = this.Events |> Map.filter predicate }
    member this.EventsSince offset = this.Linearize (fun off e -> off > offset)
    member this.EventsUpTo offset =  this.Linearize (fun off e -> off <= offset)
    member this.Merge other =
        let events' = 
            other.Events
            |> Map.fold (fun acc k e -> 
                match Map.tryFind k acc with
                | Some e' -> Map.add k (e + e') acc
                | None -> Map.add k e acc
            ) this.Events
        { Events = events' }
    member this.ToList () = 
        this.Events
        |> Map.toSeq
        |> Seq.map snd
        |> Seq.collect id
        |> Seq.toList
        |> List.sortBy (fun te -> te.Timestamp)
    
    member this.AddEvent(timestamp, emitter, event) =
        let e = { Timestamp = timestamp; Emitter = emitter; Event = event }
        let events' =
            match Map.tryFind timestamp this.Events with
            | Some e' -> Map.add timestamp (Set.add e e') this.Events
            | None -> Map.add timestamp (Set.singleton e) this.Events
        { Events = events' }

    member this.Next (after) =
        this.EventsSince(after)
            .Events
        |> Map.toSeq
        |> Seq.map snd
        |> Seq.tryHead
          
[<RequireQualifiedAccess>]
module Timeline =
    
    /// Return all events from the current timeline, that happened since the provided offset.
    let inline since offset (timeline: Timeline<_>) = timeline.EventsSince offset

    /// Return all events from the current timeline, that happened up to the provided offset.
    let inline upto offset (timeline: Timeline<_>) = timeline.EventsUpTo offset

    /// Merges two timelines together
    let inline merge other (timeline: Timeline<_>) = timeline.Merge other

    /// Flattens current timeline, returning all events in time order in which they occurred.
    let inline toList (timeline: Timeline<_>) = timeline.ToList ()

    /// Adds new event to the timeline, returning new timeline version.
    let inline add offset emitter e (timeline: Timeline<_>) = timeline.AddEvent(offset, emitter, e)

    /// Returns the next pack of events that happens right after specified threshold.
    let inline next after (timeline: Timeline<_>) = timeline.Next after
    
module TestConductor =

    let initState settings id =
        { Settings = settings
          Self = id
          KnownNodes = Set.empty
          Election = { Epoch = 0; Elected = Unchecked.defaultof<NodeId> } 
          ManagedState = 0 } |> Follower
        
    let actionDelay = TimeSpan.FromMilliseconds 1.
    