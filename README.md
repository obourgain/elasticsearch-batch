elasticsearch-batch
===================

This project aims at providing a toolbox with most of the technical stuff that gets reimplemented each time a batch to index
date in Elasticsearch is written.

This includes :
* setting the refresh interval to a higher value or disabling it
* settings replicas to 0
* switching an alias atomically
* More to come

The toolbox comes in two flavors, one is synchronous, meaning that the caller will wait until the operation is completed, 
the other flavor is asynchronous, just fire and forget, just returning a Future that the called may get the result of the operation from.
Both provide the same functionality, and in fact the synchronous one is simply delegating to the asynchronous one and blocking on the future.

## How to

Simply instaciate the _ElasticsearchBatchOperationsSync_ or _ElasticsearchBatchOperationsAsync_ depending on your needs. Both have a constructor 
taking an Elasticsearch _Client_.

You can then simply call methods on it, but be careful that most method can take a vararg of indices to apply the operation on. The compiler will allow to 
pass no arguments, but it will fail at runtime.

For exemple to disable refresh:

    ActionFuture<UpdateSettingsResponse> future = new ElasticsearchBatchOperationsAsync(client).disableRefresh(INDEX);

Note: both classes are thread-safe.

## Compatibility
<table>
    <tr>
        <td>ES-batch</td>
        <td>ES</td>
    </tr>
    <tr>
        <td>0.1</td>
        <td>1.4.0</td>
    </tr>
</table>
 
Other versions may work but are not tested.
