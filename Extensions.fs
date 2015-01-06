module Raven.Client

open Raven.Client

let openSession (documentStore : IDocumentStore) = documentStore.OpenSession()

let load<'a> (id : string) (documentSession : IDocumentSession) =
  match documentSession.Load<'a> id |> box with
  | null -> None
  | o -> Some o

let store o (documentSession : IDocumentSession) = documentSession.Store o
let saveChanges (documentSession : IDocumentSession) = documentSession.SaveChanges()
