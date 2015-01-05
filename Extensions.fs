module Raven.Client

open Raven.Client

let openSession (documentStore : IDocumentStore) = documentStore.OpenSession()

let load<'a when 'a : null> (id : string) (documentSession : IDocumentSession) =
  match documentSession.Load<'a> id with
  | null -> None
  | o -> Some o

let store o (documentSession : IDocumentSession) = documentSession.Store o
let saveChanges (documentSession : IDocumentSession) = documentSession.SaveChanges()
