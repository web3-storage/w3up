## Bidirectional Paging
### Status
Implemented in:
https://github.com/web3-storage/w3infra/pull/139
https://github.com/web3-storage/w3infra/pull/148
https://github.com/web3-storage/w3protocol/pull/442
https://github.com/web3-storage/w3protocol/pull/437
https://github.com/web3-storage/w3ui/pull/381
https://github.com/web3-storage/w3cli/pull/45

### Context

It is currently not possible to page backwards through a list of uploads. This limits the UX we can offer to users of w3ui, w3cli and w3console.

Bidirectional paging through long lists is a pattern that has been implemented many times in many similar systems. A brief survey of similar systems turned up the following links:

https://theburningmonk.com/2018/02/guys-were-doing-pagination-wrong/ 
https://gist.github.com/tspecht/f4b53e34a2ccf1bdef2c8fb9a475ca05
https://medium.com/hackernoon/bi-directional-cursor-pagination-with-react-js-relay-and-graphql-dc4ad6f9cbb0
https://www.apollographql.com/docs/react/pagination/cursor-based/#relay-style-cursor-pagination
https://relay.dev/graphql/connections.htm

### Decision

We have decided to add `before` and `after` cursors to the  `store/list` and `upload/list` responses and adding a `pre` caveat each of their invocations. If `store/list` or `upload/list` capabilities are invoked with the `pre` and `cursor` caveats, the service will return the results _before_ the cursor rather than returning results _after_ the cursor as it does by default.

We decided to leave the existing `cursor` in each of their responses. This adds a bit of weight to the list responses, but conforms with the most common unidirectional and bidirectional paging APIs we found in the wild, which should provide an optimal developer experience.

### Consequences

With this change implemented it will now be possible for users of `w3cli` and `w3ui` to page forward or backward through `store/list` or `upload/list` results. The responses for the invocations of these two results will now have two additional properties, `before` and `after`. `after` will always be the same as the existing `cursor` property, and `before` will represent the position at the beginning of the list in the `results` of the response.