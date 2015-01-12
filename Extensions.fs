module Raven.Client

open Raven.Client
open Raven.Client.Document
open Raven.Json.Linq
open System

let openSession (documentStore : IDocumentStore) = documentStore.OpenSession()
let createDocumentStore connectionStringName = 
  (new DocumentStore(ConnectionStringName = connectionStringName)).Initialize()

let load<'a> (id : string) (documentSession : IDocumentSession) = 
  match documentSession.Load<'a> id |> box with
  | null -> None
  | o -> unbox<'a> o |> Some

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

let setExpiration o (dateTime : DateTime) (documentSession : IDocumentSession) = 
  documentSession.Advanced.GetMetadataFor(o).["Raven-Expiration-Date"] <- RavenJValue(dateTime)

let forEachInIndex<'a, 'b> f documentStore = 
  use session = documentStore |> openSession
  let enumerator = session.Query<'a> typeof<'b>.Name |> session.Advanced.Stream
  seq { 
    while enumerator.MoveNext() do
      yield enumerator.Current.Document
  }
  |> Seq.iter f

let replace e n (documentSession : IDocumentSession) = 
  documentSession.Advanced.Evict e
  documentSession |> save n
  n
