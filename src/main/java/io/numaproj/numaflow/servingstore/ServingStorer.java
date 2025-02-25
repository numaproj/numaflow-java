package io.numaproj.numaflow.servingstore;

/**
 * ServingStorer is the interface for serving store to store and retrieve from a
 * custom store.
 */
public abstract class ServingStorer {
    /**
     * Puts data into the Serving Store.
     *
     * @param putDatum the data to be put into the store
     */
    public abstract void put(PutDatum putDatum);

    /**
     * Retrieves data from the Serving Store.
     *
     * @param getDatum the data to be retrieved from the store
     *
     * @return the stored result
     */
    public abstract StoredResult get(GetDatum getDatum);
}
