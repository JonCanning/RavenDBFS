module Raven.Client

open Raven.Abstractions.Data
open Raven.Client
open Raven.Client.Document
open Raven.Json.Linq
open System
open System.Linq

let openSession (documentStore : IDocumentStore) = documentStore.OpenSession()
let createDocumentStore connectionStringName = 
  (new DocumentStore(ConnectionStringName = connectionStringName)).Initialize()

let load<'a> (id : string) (documentSession : IDocumentSession) = 
  try 
    match documentSession.Load<'a> id with
    | o when box o = null -> None
    | o -> Some o
  with
  | :? InvalidCastException -> None
  | ex -> raise ex

let store o (documentSession : IDocumentSession) = documentSession.Store o
let saveChanges (documentSession : IDocumentSession) = documentSession.SaveChanges()

let clear (documentSession : IDocumentSession) = 
  documentSession.Advanced.Clear()
  documentSession

let save o documentSession = 
  documentSession |> store o
  documentSession |> saveChanges

let saveTo documentStore o = 
  use session = documentStore |> openSession
  session |> save o

let load'<'a> id documentStore = 
  use session = documentStore |> openSession
  session |> load<'a> id

let delete id (documentSession : IDocumentSession) = documentSession.Delete id
let delete' id (documentStore : IDocumentStore) = documentStore.DatabaseCommands.Delete(id, null)
let setExpiration o (dateTime : DateTime) (documentSession : IDocumentSession) = 
  documentSession.Advanced.GetMetadataFor(o).["Raven-Expiration-Date"] <- RavenJValue(dateTime)

let forEachInIndex<'a> index f documentStore = 
  use session = documentStore |> openSession
  let enumerator = session.Query<'a> index |> session.Advanced.Stream
  seq { 
    while enumerator.MoveNext() do
      yield enumerator.Current.Document
  }
  |> Seq.iter f

let forEachInIndex'<'a, 'b> f documentStore = forEachInIndex<'a> typeof<'b>.Name f documentStore

let replace e n (documentSession : IDocumentSession) = 
  documentSession.Advanced.Evict e
  documentSession |> save n

let getAllFromQuery documentStore (query : IDocumentSession -> (IDocumentQuery<'a> * RavenQueryStatistics)) = 
  let rec load count = 
    seq { 
      use session = documentStore |> openSession
      let queryable, statistics = query session
      let results = queryable.Take(1024).Skip(count).ToArray()
      let count = count + 1024
      for result in results do
        yield result
      match count with
      | i when i >= statistics.TotalResults -> ()
      | _ -> yield! load count
    }
  load 0

let bulkInsert options (documentStore : IDocumentStore) = 
  documentStore.BulkInsert(options = match options with
                                     | None -> null
                                     | Some options -> options)

let metadata() = RavenJObject()

let add (key, value) (metadata : RavenJObject) = 
  metadata.Add(key, RavenJToken.FromObject value)
  metadata

let defaultMetadata<'a> (documentStore : IDocumentStore) (o : 'a) = 
  metadata()
  |> add (Constants.RavenEntityName, documentStore.Conventions.GetDynamicTagName o)
  |> add (Constants.RavenClrType, typeof<'a> |> documentStore.Conventions.GetClrTypeName)

let withExpirationDate expirationDate (metadata : RavenJObject) = 
  metadata |> add ("Raven-Expiration-Date", expirationDate)
