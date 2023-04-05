package coop.rchain

import coop.rchain.fs2.Fs2StreamSyntax
import coop.rchain.sdk.primitive.MapSyntax
import coop.rchain.store.{KeyValueStoreManagerSyntax, KeyValueStoreSyntax, KeyValueTypedStoreSyntax}

package object shared {
  // Importing syntax object means using all extensions in the project
  object syntax extends AllSyntaxShared
}

// Syntax for shared project
// TODO: unify syntax (extensions) with catscontrib,
//  one import per project similar as `import cats.syntax.all._`
trait AllSyntaxShared
    extends KeyValueStoreSyntax
    with KeyValueTypedStoreSyntax
    with KeyValueStoreManagerSyntax
    with Fs2StreamSyntax
    with catscontrib.ToBooleanF
    with MapSyntax
