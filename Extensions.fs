module Raven.Client

open Raven.Client
open System
open Raven.Json.Linq

let openSession (documentStore : IDocumentStore) = documentStore.OpenSession()

let load<'a> (id : string) (documentSession : IDocumentSession) = 
  match documentSession.Load<'a> id |> box with
  | null -> None
  | o -> unbox<'a> o |> Some

let store o (documentSession : IDocumentSession) = documentSession.Store o
let saveChanges (documentSession : IDocumentSession) = documentSession.SaveChanges()

let storeAndSave o documentSession = 
  documentSession |> store o
  documentSession |> saveChanges

let setExpiration o (dateTime : DateTime) (documentSession : IDocumentSession) = 
  documentSession.Advanced.GetMetadataFor(o).["Raven-Expiration-Date"] <- RavenJValue(dateTime)

let forEachInIndex<'a, 'b> f documentStore = 
  let session = documentStore |> openSession
  let enumerator = session.Query<'a> typeof<'b>.Name |> session.Advanced.Stream
  seq { 
    while enumerator.MoveNext() do
      yield enumerator.Current.Document
  }
  |> Seq.iter f
